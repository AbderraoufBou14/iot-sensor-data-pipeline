import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F, types as T

load_dotenv()

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://datalake-iot-smart-building/bronze/")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://datalake-iot-smart-building/silver_by_date/")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

BRONZE_SCHEMA = T.StructType([
    T.StructField("room",       T.StringType(),    True),
    T.StructField("sensor",     T.StringType(),    True),
    T.StructField("ts_unix",    T.LongType(),      True),
    T.StructField("value",      T.DoubleType(),    True),
    T.StructField("topic",      T.StringType(),    True),
    T.StructField("partition",  T.IntegerType(),   True),
    T.StructField("offset",     T.LongType(),      True),
    T.StructField("kafka_ts",   T.TimestampType(), True),
    T.StructField("event_ts",   T.TimestampType(), True),
    T.StructField("event_date", T.DateType(),      True),
    T.StructField("ingest_ts",  T.TimestampType(), True),
])


def build_spark_session() -> SparkSession:
    """ Construit la SparkSession, support S3. """
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.696",
    ]
    spark = (
        SparkSession.builder.appName("iot_silver_job")
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
        hconf.set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    else:
        print(
            "[WARN] AWS_ACCESS_KEY_ID ou AWS_SECRET_ACCESS_KEY "
            "manquants dans l'environnement."
        )

    return spark


def load_bronze_stream(spark, bronze_path: str):
    """
    Lecture en streaming de la couche bronze (format parquet).
    Hypothèse : le schéma bronze contient au moins room, sensor, ts_unix, value, event_date.
    """
    return (
        spark.readStream
        .format("parquet")
        .schema(BRONZE_SCHEMA)
        .load(f"{bronze_path}/room=*/sensor=*/event_date=*")
    )


def with_silver_schema(df):
    """Applique la transformation Bronze -> Silver : normalisation des colonnes"""

    # 0) On récupère le chemin physique du fichier (pour la room)
    df = df.withColumn("source_file", F.input_file_name())

    # 1) Reconstruire room depuis le chemin S3 si absente ou NULL
    #    Exemple chemin : .../bronze/room=413/sensor=co2/event_date=2013-08-24/part-...
    df = df.withColumn(
        "room",
        F.when(
            F.col("room").isNotNull(),     # si un jour elle existe vraiment en Bronze
            F.col("room")
        ).otherwise(
            F.regexp_extract(
                F.col("source_file"),
                r"room=([0-9A-Za-z_-]+)",
                1
            )
        )
    )

    # 2) Reconstruire sensor_type à partir de sensor OU du topic
    #    topic ex : "iot.co2" -> on récupère "co2"
    df = df.withColumn(
        "sensor_type",
        F.when(
            F.col("sensor").isNotNull(),
            F.lower(F.col("sensor"))
        ).otherwise(
            F.regexp_extract(
                F.col("topic"),
                r"iot\.([^.]+)",
                1
            )
        )
    )

    # 3) ts_unix (on s’assure du bon type)
    df = df.withColumn("ts_unix", F.col("ts_unix").cast(T.LongType()))

    # 4) value_raw : toujours string pour garder la trace brute
    df = df.withColumn("value_raw", F.col("value").cast(T.StringType()))

    # 5) sensor_id : room + sensor_type
    df = df.withColumn(
        "sensor_id",
        F.concat_ws("-", F.col("room"), F.col("sensor_type"))
    )

    # 6) ts_utc : conversion en timestamp à partir de ts_unix (ou event_ts si tu préfères)
    df = df.withColumn(
        "ts_utc",
        F.to_timestamp(F.from_unixtime(F.col("ts_unix")))
    )

    # 7) event_date : à partir de event_ts (ou ts_utc)
    df = df.withColumn("event_date", F.to_date(F.col("event_ts")))

    # 8) Nettoyage simple pour value_clean
    df = df.withColumn("value_trimmed", F.trim(F.col("value_raw")))
    df = df.withColumn(
        "value_clean",
        F.when(
            (F.col("value_trimmed") == "") |
            (F.col("value_trimmed").isin("NaN", "nan")),
            F.lit(None).cast(T.DoubleType())
        ).otherwise(F.col("value_trimmed").cast(T.DoubleType()))
    )

    # 9) Unités selon le type de capteur
    df = df.withColumn(
        "unit",
        F.when(F.col("sensor_type") == "co2",         F.lit("ppm"))
         .when(F.col("sensor_type") == "humidity",    F.lit("%"))
         .when(F.col("sensor_type") == "temperature", F.lit("°C"))
         .when(F.col("sensor_type") == "pir",         F.lit("binary"))
         .when(F.col("sensor_type") == "luminosity",  F.lit("lux"))
         .otherwise(F.lit("unknown"))
    )

    # 10) Qualité
    df = df.withColumn("is_valid", F.col("value_clean").isNotNull())
    df = df.withColumn(
        "quality_flag",
        F.when(F.col("value_clean").isNull(), F.lit("MISSING"))
         .otherwise(F.lit("OK"))
    )

    # 11) ingest_date / created_at
    df = df.withColumn("ingest_date", F.current_date())
    df = df.withColumn("created_at", F.current_timestamp())

    # 12) On enlève les colonnes temporaires
    df = df.drop("value_trimmed")

    return df.select(
        "room",
        "sensor_type",
        "sensor_id",
        "ts_unix",
        "ts_utc",
        "event_date",
        "value_raw",
        "value_clean",
        "unit",
        "is_valid",
        "quality_flag",
        "ingest_date",
        "source_file",
        "created_at",
    )


def write_silver_detail_stream(df, silver_path: str):
    """
    Écriture de la table Silver détaillée :
    - format parquet
    - mode append
    - partitionnement: event_date (date de la mesure)
    """
    return (
        df.writeStream
        .format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", os.path.join(silver_path, "_checkpoints_detail"),)
        .partitionBy("event_date")
        .outputMode("append")
        .start()
    )


def main():
    spark = build_spark_session()

    # 1) Lecture bronze
    bronze_df = load_bronze_stream(spark, BRONZE_PATH)

    # 2) Transformation -> Silver détaillé
    silver_df = with_silver_schema(bronze_df)

    # 3) Écriture Silver
    write_silver_detail_stream(silver_df, SILVER_PATH)

    # Attente des streams
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
