import os
import pandas as pd
import datetime

S3_URI_CO2 = "s3://datalake-iot-smart-building/raw/KETI/413/co2.csv"
S3_URI_PIR = "s3://datalake-iot-smart-building/raw/KETI/413/pir.csv"

df_co2 = pd.read_csv(S3_URI_CO2)
df_pir = pd.read_csv(S3_URI_PIR)

print("\n=== Aperçu CO2 ===")
print(df_co2.head())

print("\n=== infos CO2 ===")
print(df_co2.info())

print("\n=== Aperçu PIR ===")
print(df_pir.head())

print("\n=== infos PIR ===")
print(df_pir.info())

"""utc_time_stamp = datetime.datetime.utcfromtimestamp(df_pir["ts_unix"][0])
print(utc_time_stamp)"""