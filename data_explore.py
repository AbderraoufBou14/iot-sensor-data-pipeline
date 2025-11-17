
import pandas as pd


path = "/home/abdou/Downloads/part-00011-00c2ee56-a7cf-4a71-9b9e-0b8e08f14b7b.c000.snappy.parquet"


df = pd.read_parquet(path)
# df_filtered = df.loc[df["sensor"] == "co2"]


df.info()