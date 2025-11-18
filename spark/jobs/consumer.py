
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

# --- Chargement du .env (depuis la racine du projet) ---
load_dotenv()

# --- Config lue depuis les variables d'environnement ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.19.0.3:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.sensors")

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://datalake-iot-smart-building/bronze/",)
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "s3a://datalake-iot-smart-building/bronze/checkpoints/consumer_iot")

SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")


def get_bronze_schema() -> StructType:
    """
    Schéma du payload JSON envoyé par le producer dans Kafka.
    Exemple de message :
    {"room": "101", "sensor": "co2", "ts": 1377299108, "value": 488.0}
    """
    return StructType(
        [
            StructField("room", StringType(), True),
            StructField("sensor", StringType(), True),
            StructField("ts", LongType(), True),      # timestamp Unix en secondes
            StructField("value", DoubleType(), True),
        ]
    )


def build_spark_session() -> SparkSession:
    """ Construit la SparkSession avec le connecteur Kafka + support S3. """
    extra_packages = [
        SPARK_KAFKA_PACKAGE,
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.696",
    ]

    spark = (
        SparkSession.builder.appName("iot_kafka_consumer")
        .config("spark.jars.packages", ",".join(extra_packages))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Config S3A pour Hadoop
    hconf = spark._jsc.hadoopConfiguration()
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        hconf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        hconf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        hconf.set("fs.s3a.endpoint", f"s3.{AWS_DEFAULT_REGION}.amazonaws.com")
        hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    else:
        print("[WARN] AWS_ACCESS_KEY_ID ou AWS_SECRET_ACCESS_KEY " "manquants dans l'environnement.")

    return spark


def read_stream_from_kafka(spark: SparkSession):
    """ Lit le flux Kafka avec Spark Structured Streaming. """
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")  # pratique pour rejouer l'historique
        .option("failOnDataLoss", "false") 
        .load()
    )
    return df_raw


def transform_to_bronze(df_raw):
    """
    Transforme le DataFrame brut Kafka (key, value, offset, ...) en schéma bronze.
    - cast value -> string
    - parse JSON -> colonnes room, sensor, ts, value
    - ajoute des métadonnées : kafka_ts, partition, offset, ingest_ts
    """
    bronze_schema = get_bronze_schema()

    df_parsed = (
        df_raw.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value_str"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("payload", F.from_json("value_str", bronze_schema))
    )

    df_bronze = (
        df_parsed.select(
            F.col("payload.room").alias("room"),
            F.col("payload.sensor").alias("sensor"),
            F.col("payload.ts").cast("long").alias("ts_unix"),
            F.col("payload.value").cast("double").alias("value"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_ts"),
        )
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime("ts_unix")))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    return df_bronze



def write_stream_to_bronze(df_bronze):
    """ Écrit le flux bronze en parquet dans BRONZE_PATH, avec checkpoint CHECKPOINT_PATH. """
    print(f"[INFO] Écriture bronze vers : {BRONZE_PATH}")
    print(f"[INFO] Checkpoints vers    : {CHECKPOINT_PATH}")

    query = (
        df_bronze.writeStream.format("parquet")
        .partitionBy("room", "sensor", "event_date")  
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("path", BRONZE_PATH)
        .trigger(processingTime="10 seconds")  # micro-batch toutes les 10s
        .start()
    )
    return query


def main():
    print("=== Consumer IoT Spark ===")
    print(f"KAFKA_BOOTSTRAP = {KAFKA_BOOTSTRAP}")
    print(f"KAFKA_TOPIC     = {KAFKA_TOPIC}")
    print(f"BRONZE_PATH     = {BRONZE_PATH}")
    print(f"CHECKPOINT_PATH = {CHECKPOINT_PATH}")

    spark = build_spark_session()
    df_raw = read_stream_from_kafka(spark)
    df_bronze = transform_to_bronze(df_raw)
    query = write_stream_to_bronze(df_bronze)

    print("Consumer démarré, en attente de messages Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
