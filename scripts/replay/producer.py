import os, csv, json, time, heapq
from typing import Iterator, List, Tuple, Dict

from dotenv import load_dotenv
from kafka import KafkaProducer
import s3fs

load_dotenv(override=False)

# --- ENV (valeurs par défaut pour le fallback) ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ACKS = os.getenv("ACKS", "all")
LINGER_MS = int(os.getenv("LINGER_MS", "20"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "32768"))

TOPIC_CO2 = os.getenv("TOPIC_CO2", "iot.co2")
TOPIC_PIR = os.getenv("TOPIC_PIR", "iot.pir")
TOPIC_LUMINOSITY = os.getenv("TOPIC_LUMINOSITY", "iot.luminosity")
TOPIC_TEMPERATURE = os.getenv("TOPIC_TEMPERATURE", "iot.temperature")
TOPIC_HUMIDITY = os.getenv("TOPIC_HUMIDITY", "iot.humidity")
TOPIC_DEFAULT = os.getenv("TOPIC_DEFAULT", "iot.sensor")

SENSORS = [s.strip().lower() for s in os.getenv("SENSORS", "co2,pir").split(",") if s.strip()]
ROOMS = [r.strip() for r in os.getenv("ROOMS", "").split(",") if r.strip()]

SOURCE_URL = os.getenv("SOURCE_URL", "s3://datalake-iot-smart-building/raw/KETI/")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")

REPLAY_MODE = os.getenv("REPLAY_MODE", "timewarp").lower()        # "rate" | "timewarp"
RATE_MSG_PER_SEC = float(os.getenv("RATE_MSG_PER_SEC", "5"))      # rate only
TIMEWARP_FACTOR = float(os.getenv("TIMEWARP_FACTOR", "10"))       # timewarp only
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "10"))

# Topics par capteur normalisé
TOPIC_BY_SENSOR = {
    "co2": TOPIC_CO2,
    "pir": TOPIC_PIR,
    "luminosity": TOPIC_LUMINOSITY,
    "temperature": TOPIC_TEMPERATURE,
    "humidity": TOPIC_HUMIDITY,
}

# Alias simples
SENSOR_ALIASES: Dict[str, List[str]] = {
    "co2": ["co2"],
    "pir": ["pir", "motion"],
    "luminosity": ["light", "luminosity"],
    "temperature": ["temperature", "temp"],
    "humidity": ["humidity", "hum", "rh"],
}

# --- Helpers ---
def coerce_ts_unix(raw) -> int:
    x = float(raw)
    if x > 1e15:   # µs
        return int(x / 1e6)
    if x > 1e12:   # ms
        return int(x / 1e3)
    return int(x)  # s

def build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=AWS_ACCESS_KEY_ID,
        secret=AWS_SECRET_ACCESS_KEY,
        client_kwargs={"region_name": AWS_DEFAULT_REGION},
    )

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=ACKS,
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    )

def normalize_sensor_from_filename(filename: str) -> str | None:
    name = filename.lower()
    for norm, aliases in SENSOR_ALIASES.items():
        if any(a in name for a in aliases):
            return norm
    return None

def iter_files_s3(fs: s3fs.S3FileSystem) -> Iterator[Tuple[str, str, str]]:
    candidates = fs.glob(f"{SOURCE_URL}*/*.csv")
    wanted = set(SENSORS) if SENSORS else set()
    for path in sorted(candidates):
        room = os.path.basename(os.path.dirname(path))
        filename = os.path.splitext(os.path.basename(path))[0]
        if ROOMS and room not in ROOMS:
            continue
        sensor = normalize_sensor_from_filename(filename)
        if sensor is None:
            continue
        if wanted and sensor not in wanted:
            continue
        yield (room, sensor, path)

def iter_csv_s3(fs: s3fs.S3FileSystem, s3_path: str) -> Iterator[List[str]]:
    with fs.open(s3_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2: 
                continue
            try:
                float(row[0])
            except Exception:
                continue
            yield row

def format_mode() -> str:
    return f"rate {int(RATE_MSG_PER_SEC)}/s" if REPLAY_MODE == "rate" else f"timewarp x{int(TIMEWARP_FACTOR)}"

def print_progress(sensor_counts: Dict[str, int], total: int) -> None:
    parts = [f"{k}={v}" for k, v in sensor_counts.items()]
    print("\r" + f"[{format_mode()}] " + " | ".join(parts) + f" (total={total})", end="", flush=True)

# --- Orchestration ---
def run_timewarp(fs: s3fs.S3FileSystem, producer: KafkaProducer) -> int:
    # streams: [room, sensor, path, iterator, prev_ts, sent_count]
    streams: List[List] = []
    for room, sensor, path in iter_files_s3(fs):
        streams.append([room, sensor, path, iter_csv_s3(fs, path), None, 0])
    
    
    if not streams:
        print("[WARN] No data streams found."); return 0

    now = time.time()
    heap: List[Tuple[float, int, dict]] = []
    sensor_counts: Dict[str, int] = {}

    # init
    for i, (room, sensor, path, it, prev_ts, sent) in enumerate(streams):
        try:
            row = next(it)
            ts = coerce_ts_unix(row[0])
            val = int(float(row[1])) if sensor == "pir" else float(row[1])
            heapq.heappush(heap, (now, i, {"room": room, "sensor": sensor, "ts": ts, "value": val}))
            streams[i][4] = ts
            sensor_counts.setdefault(sensor, 0)
        except StopIteration:
            continue
        except Exception:
            continue

    total = 0
    print(f"[INFO] Capteurs actifs: {len(streams)}")
    
    while heap:
        send_time, i, msg = heapq.heappop(heap)
        delay = send_time - time.time()
        if delay > 0: time.sleep(delay)

        topic = TOPIC_BY_SENSOR.get(msg["sensor"], TOPIC_DEFAULT)
        key = f"{msg['room']}:{msg['sensor']}"
        producer.send(topic, value=msg, key=key)
        total += 1
        sensor_counts[msg["sensor"]] = sensor_counts.get(msg["sensor"], 0) + 1
        streams[i][5] += 1
        
        if total % PROGRESS_EVERY == 0:
            print_progress(sensor_counts, total)

        room, sensor, path, it, prev_ts, sent_count = streams[i]
        try:
            row = next(it)
            ts_next = coerce_ts_unix(row[0])
            val = int(float(row[1])) if sensor == "pir" else float(row[1])
            delta = 0 if prev_ts is None else max(ts_next - prev_ts, 0)
            interval = float(delta) / max(TIMEWARP_FACTOR, 1.0)
            next_send = max(send_time, time.time()) + interval
            heapq.heappush(heap, (next_send, i, {"room": room, "sensor": sensor, "ts": ts_next, "value": val}))
            streams[i][4] = ts_next
        except StopIteration:
            print(f"[END] room={room} sensor={sensor} file={path} — sent={sent_count}")
        except Exception:
            pass

    print_progress(sensor_counts, total); print()
    producer.flush()
    print(f"[END] flush() — total sent={total}")
    return total

def run_rate(fs: s3fs.S3FileSystem, producer: KafkaProducer) -> int:
    streams: List[Tuple[str, str, Iterator[List[str]]]] = []
    for room, sensor, path in iter_files_s3(fs):
        streams.append((room, sensor, iter_csv_s3(fs, path)))

    if not streams:
        print("[WARN] No data streams found."); return 0

    sensor_counts: Dict[str, int] = {s:0 for _, s, _ in streams}
    total = 0
    idx = 0
    interval = 1.0 / max(RATE_MSG_PER_SEC, 1.0)

    for room, sensor, _ in streams:
        print(f"[START] room={room} sensor={sensor}")

    while streams:
        room, sensor, it = streams[idx]
        try:
            row = next(it)
        except StopIteration:
            streams.pop(idx)
            if not streams: break
            idx %= len(streams); continue

        ts = coerce_ts_unix(row[0])
        val = int(float(row[1])) if sensor == "pir" else float(row[1])
        topic = TOPIC_BY_SENSOR.get(sensor, TOPIC_DEFAULT)
        key = f"{room}:{sensor}"
        producer.send(topic, value={"room": room, "sensor": sensor, "ts": ts, "value": val}, key=key)

        total += 1
        sensor_counts[sensor] = sensor_counts.get(sensor, 0) + 1
        if total % PROGRESS_EVERY == 0:
            print_progress(sensor_counts, total)

        idx = (idx + 1) % len(streams)
        time.sleep(interval)

    print_progress(sensor_counts, total); print()
    producer.flush()
    print(f"[END] flush() — total sent={total}")
    return total

def main():
    fs = build_fs()
    producer = build_producer()
    if REPLAY_MODE == "timewarp":
        print(f"mode={REPLAY_MODE} timewarp_factor={TIMEWARP_FACTOR}")
    else: 
        print(f"mode={REPLAY_MODE} rate={RATE_MSG_PER_SEC}/s")
    total = run_rate(fs, producer) if REPLAY_MODE == "rate" else run_timewarp(fs, producer)
    print(f"[DONE] Messages envoyés: {total}")
    producer.close()

if __name__ == "__main__":
    main()
