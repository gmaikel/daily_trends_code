import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Compare Partitioned Parquet Files") \
    .getOrCreate()

def list_parquet_files(path):
    return [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]

if __name__ == '__main__':
    parquet_path_scala = "C:/Users/maikel.gali-ext/Desktop/project/daily-stats/data/destination/believe/apple-music-demo/2024/04/24/structuring.parquet"
    parquet_path_pyspark = "../data/destination/believe/apple-music-demo/2024/04/24/structuring.parquet"

    parquet_files_scala = list_parquet_files(parquet_path_scala)
    parquet_files_pyspark = list_parquet_files(parquet_path_pyspark)

    df_scala = spark.read.parquet(*parquet_files_scala)
    df_pyspark = spark.read.parquet(*parquet_files_pyspark)

    is_equal = df_scala.exceptAll(df_pyspark).isEmpty() and df_pyspark.exceptAll(df_scala).isEmpty()

    if is_equal:
        print("Les fichiers Parquet sont identiques.")
    else:
        diff_scala = df_scala.exceptAll(df_pyspark)
        print("Lignes présentes dans df_scala mais pas dans df_pyspark :")
        print(diff_scala.count())
        diff_scala.show(truncate=False)

        diff_pyspark = df_pyspark.exceptAll(df_scala)
        print(diff_pyspark.count())
        print("Lignes présentes dans df_pyspark mais pas dans df_scala :")
        diff_pyspark.show(truncate=False)
