import csv
import json
import os
import time
from urllib.parse import urljoin

from dotenv import load_dotenv
from kafka import KafkaProducer

import s3fs  # <-- clé: lecture directe S3

load_dotenv()

# ===== Config =====
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
ACKS = os.getenv("ACKS", "all")
COMPRESSION_TYPE = os.getenv("COMPRESSION_TYPE", "snappy")
LINGER_MS = int(os.getenv("LINGER_MS", "20"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "32768"))

TOPIC_CO2 = os.getenv("TOPIC_CO2", "iot.co2")
TOPIC_PIR = os.getenv("TOPIC_PIR", "iot.pir")
TOPIC_BY_SENSOR = {"co2": TOPIC_CO2, "pir": TOPIC_PIR}

SENSORS = [s.strip().lower() for s in os.getenv("SENSORS", "co2,pir").split(",") if s.strip()]
ROOMS = [r.strip() for r in os.getenv("ROOMS", "").split(",") if r.strip()]

SOURCE_URL = os.getenv("SOURCE_URL")  # ex: s3://bucket/prefix
if not SOURCE_URL or not SOURCE_URL.startswith("s3://"):
    raise SystemExit("SOURCE_URL doit être un préfixe S3 (ex: s3://mon-bucket/raw)")

REPLAY_MODE = os.getenv("REPLAY_MODE", "rate")  # rate | timewarp
RATE_MSG_PER_SEC = float(os.getenv("RATE_MSG_PER_SEC", "50"))
TIMEWARP_FACTOR = float(os.getenv("TIMEWARP_FACTOR", "10"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
SLEEP_ON_ERROR_MS = int(os.getenv("SLEEP_ON_ERROR_MS", "250"))

AWS_REGION = os.getenv("AWS_REGION", None)  # utile localement

# ===== Kafka =====
def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=ACKS,
        compression_type=COMPRESSION_TYPE,
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE,
        value_serializer=lambda d: json.dumps(d).encode("utf-8"),
        key_serializer=lambda s: s.encode("utf-8"),
    )

# ===== S3 =====
def build_s3fs():
    # S3FileSystem utilisera automatiquement: IAM Role (prod) OU variables d’env si présentes (local)
    return s3fs.S3FileSystem(client_kwargs={"region_name": AWS_REGION} if AWS_REGION else {})

def iter_files_s3(fs: s3fs.S3FileSystem):
    """
    Cherche des chemins de type: s3://bucket/prefix/<room>/<sensor>.csv
    - Si ROOMS non vide: filtre uniquement ces rooms
    - Sinon: autodétecte les rooms comme sous-prefixes de SOURCE_URL
    """
    base = SOURCE_URL.rstrip("/")
    sensors = set(SENSORS)

    rooms_to_scan = ROOMS
    if not rooms_to_scan:
        # autodétecter les rooms: liste des sous-dossiers immédiats
        # s3fs.glob renvoie des chemins complets s3://...
        pattern = f"{base}/*"
        candidates = fs.glob(pattern)
        rooms_to_scan = [p.split("/")[-1] for p in candidates if fs.isdir(p)]

    for room in rooms_to_scan:
        for sensor in sensors:
            path = f"{base}/{room}/{sensor}.csv"
            if fs.exists(path):
                yield room, sensor, path

# ===== Timing =====
def coerce_ts_unix(x: str) -> int:
    ts = int(float(x))
    if ts > 10**12:  # ms -> s
        ts //= 1000
    return ts

def rate_sleep():
    if RATE_MSG_PER_SEC > 0:
        time.sleep(1.0 / RATE_MSG_PER_SEC)

def timewarp_sleep(prev_ts, curr_ts):
    if prev_ts is None:
        return
    delta = max(0, curr_ts - prev_ts)
    scaled = delta / max(1.0, TIMEWARP_FACTOR)
    if scaled > 0:
        time.sleep(scaled)

# ===== Publish =====
def publish_csv_s3(fs: s3fs.S3FileSystem, producer, room_id, sensor, s3_path: str):
    topic = TOPIC_BY_SENSOR.get(sensor)
    if not topic:
        print(f"[WARN] Pas de topic pour '{sensor}', ignoré: {s3_path}")
        return 0

    sensor_id = f"{room_id}_{sensor}"
    seq = 0
    prev_ts = None
    sent = 0

    # Ouverture streaming depuis S3
    with fs.open(s3_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue

            raw_ts = row[0]
            raw_val = row[1] if len(row) > 1 else None
            if raw_val is None:
                continue

            try:
                ts_unix = coerce_ts_unix(raw_ts)
                value = int(float(raw_val)) if sensor == "pir" else float(raw_val)
            except Exception as e:
                print(f"[WARN] Ligne ignorée ({s3_path}): {row} | err={e}")
                continue

            msg = {
                "sensor_type": sensor,
                "sensor_id": sensor_id,
                "ts_unix": ts_unix,
                "value": value,
                "source_file": s3_path.split("/")[-1],
                "seq": seq,
            }

            # Envoi (retries simples)
            for attempt in range(MAX_RETRIES + 1):
                try:
                    producer.send(topic, key=sensor_id, value=msg)
                    break
                except Exception as e:
                    if attempt >= MAX_RETRIES:
                        print(f"[ERROR] Envoi échoué après retries: {e}")
                        raise
                    time.sleep(SLEEP_ON_ERROR_MS / 1000.0)

            # Contrôle du rythme
            if REPLAY_MODE == "rate":
                rate_sleep()
            elif REPLAY_MODE == "timewarp":
                timewarp_sleep(prev_ts, ts_unix)

            prev_ts = ts_unix
            seq += 1
            sent += 1

    producer.flush()
    print(f"[OK] {s3_path} → topic={topic} | envoyés={sent}")
    return sent

def main():
    fs = build_s3fs()
    producer = build_producer()

    total = 0
    for room, sensor, s3_path in iter_files_s3(fs):
        total += publish_csv_s3(fs, producer, room, sensor, s3_path)

    print(f"[DONE] Messages envoyés: {total}")

if __name__ == "__main__":
    main()
