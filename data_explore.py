
import pandas as pd
from spark.jobs.silver_job import build_spark_session
spark = build_spark_session()

path = "/home/abdou/Downloads/part-00000-2d0f3027-c2db-4a76-b76b-95db2e35bd30.c000.snappy.parquet"


df_bronze =spark.read.parquet(path)

df_bronze.printSchema()
df_bronze.show(20, False)