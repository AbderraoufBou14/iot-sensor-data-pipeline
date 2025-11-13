from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
import os
import time

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def wait_for_kafka(timeout_sec: int = 30):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    start = time.time()

    while time.time() - start < timeout_sec:
        try:
            md = admin.list_topics(timeout=5)
            print(f"✅ Kafka ready on {BOOTSTRAP} (brokers: {len(md.brokers)})")
            return admin
        except KafkaException as e:
            print(f"⏳ Waiting for Kafka controller... ({e})")
            time.sleep(2)

    print("⚠️ Kafka may not be fully ready, continuing anyway...")
    return admin


def create_topics():
    admin = wait_for_kafka()

    topics = [
        NewTopic("iot.co2", num_partitions=3, replication_factor=1),
        NewTopic("iot.pir", num_partitions=3, replication_factor=1),
        NewTopic("iot.humidity", num_partitions=3, replication_factor=1),
        NewTopic("iot.luminosity", num_partitions=3, replication_factor=1),
        NewTopic("iot.temperature", num_partitions=3, replication_factor=1),
    ]

    print(f"Creating topics on {BOOTSTRAP}...")
    futures = admin.create_topics(topics)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"✅ Topic created: {topic}")
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"⚠️ Topic {topic} already exists, skipping.")
            elif err.code() == KafkaError._TIMED_OUT:
                print(f"⚠️ Timeout while creating {topic}: {err}. "
                      f"Topic may already exist or Kafka was still starting.")
            else:
                print(f"❌ Failed to create topic {topic}: {err}")
        except Exception as e:
            print(f"❌ Unexpected error for topic {topic}: {e}")


if __name__ == "__main__":
    create_topics()
