import pandas as pd
from spark.jobs.silver_job import build_spark_session
spark = build_spark_session()
"""

silver_23 = spark.read.parquet(
    "s3a://datalake-iot-smart-building/silver/event_date=2013-08-23/"
)
gold_hourly_23 = spark.read.parquet(
    "s3a://datalake-iot-smart-building/gold/sensor_timeseries_hourly/event_date=2013-08-23/"
)

print("Rooms Silver :")
silver_23.select("room").distinct().orderBy("room").show(1000, truncate=False)

print("Rooms Gold hourly :")
gold_hourly_23.select("room").distinct().orderBy("room").show(1000, truncate=False)
"""


# ============================================================================================
# df_silver = spark.read.parquet("s3a://datalake-iot-smart-building/silver/event_date=2013-08-23/")

df_gold = spark.read.parquet("s3a://datalake-iot-smart-building/gold/room_daily_metrics/event_date=2013-08-23/part-00000-db91af96-311e-40b1-bfa4-a9bbaf6ba45b.c000.snappy.parquet")

# df_bronze =spark.read.parquet(path)

# print("Nombre de lignes :", df_gold.count())
print(df_gold.show(1000))

# df_silver.groupBy("room").count().show(200, truncate=False)

# df_25.groupBy("sensor_type").count().show(200, truncate=False)
