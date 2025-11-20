import os
import argparse
from datetime import datetime

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

# Paths Datalake
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://datalake-iot-smart-building/silver/")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://datalake-iot-smart-building/gold/")

# AWS
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")


# 1. Spark session
def build_spark_session() -> SparkSession:
    extra_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.696",
    ]
    spark = (
        SparkSession.builder.appName("iot_gold_job")
        .config("spark.jars.packages", ",".join(extra_packages))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

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
        print("[WARN] AWS creds manquants dans l'environnement.")

    return spark


# 2. Args / date
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="IoT Gold job (Silver -> Gold)")
    parser.add_argument(
        "--date",
        "--process-date",
        dest="process_date",
        help="Date à traiter (YYYY-MM-DD).",
        required=False,
    )
    return parser.parse_args()


def resolve_process_date(date_str) -> str:

    if not date_str:
        raise ValueError(
            "Aucune date fournie. Utilise --date YYYY-MM-DD (ex: --date 2013-08-23)."
        )
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"Date invalide: {date_str}. Format attendu: YYYY-MM-DD.")
    return date_str


# 3. Lecture Silver
def read_silver_for_date(spark: SparkSession, process_date: str):
    silver_path = os.path.join(SILVER_PATH, f"event_date={process_date}")
    print(f"[INFO] Lecture Silver : {silver_path}")
    df = spark.read.parquet(silver_path)
    return df


# 4. Agrégats horaires
def build_hourly_aggregates(df_silver, process_date: str):
    df = df_silver.withColumn("event_date", F.lit(process_date).cast("date"))

    df = df.withColumn("hour_utc", F.date_trunc("hour", "ts_utc"))

    agg = (
        df.groupBy("room", "sensor_type", "event_date", "hour_utc")
        .agg(
            F.count("*").alias("nb_readings"),
            F.sum(F.when(F.col("is_valid") == True, 1).otherwise(0)).alias("nb_valid"),
            F.sum(F.when(F.col("is_valid") == False, 1).otherwise(0)).alias("nb_invalid"),
            F.min("value_clean").alias("value_min"),
            F.max("value_clean").alias("value_max"),
            F.avg("value_clean").alias("value_avg"),
            F.stddev("value_clean").alias("value_std"),
        )
    )

    agg = agg.withColumn(
        "pct_valid",
        F.when(F.col("nb_readings") > 0, F.col("nb_valid") / F.col("nb_readings"))
        .otherwise(F.lit(0.0)),
    )

    agg = agg.withColumn(
        "quality_flag_hour",
        F.when(F.col("pct_valid") >= 0.9, F.lit("OK"))
        .when(F.col("pct_valid") >= 0.7, F.lit("WARNING"))
        .otherwise(F.lit("BAD")),
    )

    agg = agg.withColumn("created_at", F.current_timestamp())

    return agg


# 5. Métriques journalières
def build_daily_room_metrics(df_hourly):
    temp_hourly = df_hourly.filter(F.col("sensor_type") == "temperature")
    temp_daily = (
        temp_hourly.groupBy("room", "event_date")
        .agg(
            (F.sum(F.col("value_avg") * F.col("nb_valid")) /
             F.sum("nb_valid")).alias("temp_avg"),
            F.min("value_min").alias("temp_min"),
            F.max("value_max").alias("temp_max"),
            F.avg(
                F.when(
                    (F.col("value_avg") >= 20.0) & (F.col("value_avg") <= 24.0),
                    F.lit(1.0),
                ).otherwise(0.0)
            ).alias("temp_comfort_ratio"),
        )
    )

    co2_hourly = df_hourly.filter(F.col("sensor_type") == "co2")
    co2_daily = (
        co2_hourly.groupBy("room", "event_date")
        .agg(
            (F.sum(F.col("value_avg") * F.col("nb_valid")) /
             F.sum("nb_valid")).alias("co2_avg"),
            F.max("value_max").alias("co2_max"),
            F.avg(
                F.when(F.col("value_avg") > 1000.0, F.lit(1.0)).otherwise(0.0)
            ).alias("co2_above_1000_ratio"),
        )
    )

    humidity_hourly = df_hourly.filter(F.col("sensor_type") == "humidity")
    humidity_daily = (
        humidity_hourly.groupBy("room", "event_date")
        .agg(
            (F.sum(F.col("value_avg") * F.col("nb_valid")) /
             F.sum("nb_valid")).alias("humidity_avg"),
            F.avg(
                F.when(
                    (F.col("value_avg") >= 30.0) & (F.col("value_avg") <= 60.0),
                    F.lit(1.0),
                ).otherwise(0.0)
            ).alias("humidity_in_range_ratio"),
        )
    )

    luminosity_hourly = df_hourly.filter(F.col("sensor_type") == "luminosity")
    luminosity_daily = (
        luminosity_hourly.groupBy("room", "event_date")
        .agg(
            (F.sum(F.col("value_avg") * F.col("nb_valid")) /
             F.sum("nb_valid")).alias("luminosity_avg"),
            F.min("value_min").alias("luminosity_min"),
            F.max("value_max").alias("luminosity_max"),
        )
    )

    pir_hourly = df_hourly.filter(F.col("sensor_type") == "pir")
    pir_daily = (
        pir_hourly.groupBy("room", "event_date")
        .agg(
            F.avg("value_avg").alias("occupancy_rate"),
        )
    )

    quality_daily = (
        df_hourly.groupBy("room", "event_date")
        .agg(
            F.avg("pct_valid").alias("data_quality_score_raw")
        )
    ).withColumn(
        "data_quality_score",
        (F.col("data_quality_score_raw") * 100.0).cast("double")
    ).drop("data_quality_score_raw")

    df_daily = temp_daily \
        .join(co2_daily, ["room", "event_date"], "outer") \
        .join(humidity_daily, ["room", "event_date"], "outer") \
        .join(luminosity_daily, ["room", "event_date"], "outer") \
        .join(pir_daily, ["room", "event_date"], "outer") \
        .join(quality_daily, ["room", "event_date"], "outer")

    df_daily = df_daily.withColumn(
        "comfort_score",
        (
            100.0 * (
                0.5 * F.coalesce(F.col("temp_comfort_ratio"), F.lit(0.0)) +
                0.3 * (1.0 - F.coalesce(F.col("co2_above_1000_ratio"), F.lit(0.0))) +
                0.2 * F.coalesce(F.col("humidity_in_range_ratio"), F.lit(0.0))
            )
        ).cast("double")
    )

    df_daily = df_daily.withColumn(
        "data_quality_score",
        F.when(F.col("data_quality_score") < 0.0, 0.0)
         .when(F.col("data_quality_score") > 100.0, 100.0)
         .otherwise(F.col("data_quality_score"))
    )

    df_daily = df_daily.withColumn("created_at", F.current_timestamp())

    return df_daily


# 6. Écriture Gold
def write_parquet_partitioned(df, output_path: str, partition_col: str = "event_date"):
    print(f"[INFO] Écriture Parquet : {output_path}")
    (
        df.write
        .mode("append")
        .format("parquet")
        .partitionBy(partition_col)
        .save(output_path)
    )


# 7. Main
def main():
    args = parse_args()
    process_date = resolve_process_date(args.process_date)
    print(f"[INFO] Date traitée (event_date) : {process_date}")

    spark = build_spark_session()

    df_silver = read_silver_for_date(spark, process_date)
    df_hourly = build_hourly_aggregates(df_silver, process_date)
    df_daily = build_daily_room_metrics(df_hourly)

    hourly_output = os.path.join(GOLD_PATH, "sensor_timeseries_hourly")
    daily_output = os.path.join(GOLD_PATH, "room_daily_metrics")

    write_parquet_partitioned(df_hourly, hourly_output, partition_col="event_date")
    write_parquet_partitioned(df_daily, daily_output, partition_col="event_date")

    print("[INFO] Job Gold terminé avec succès.")
    spark.stop()


if __name__ == "__main__":
    main()
