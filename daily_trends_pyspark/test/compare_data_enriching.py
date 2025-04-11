import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, IntegerType, DateType, TimestampType, ArrayType

spark = SparkSession.builder \
    .appName("Compare Partitioned Parquet Files") \
    .getOrCreate()

def list_parquet_files(path):
    return [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]

def cast_columns(df, schema):
    for col_name, col_type in schema.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
    return df

if __name__ == '__main__':
    scala_schema = {
        "PKID_DSP_DEMOGRAPHIC_RAW": StringType(),
        "FKID_DP_PRODUCT": LongType(),
        "ID_REPORT": LongType(),
        "FKID_DISTRIBUTOR": StringType(),
        "FKID_DSP": StringType(),
        "STREAM_DATE_DSP": DateType(),
        "FKID_STREAM_DATE": IntegerType(),
        "STREAM_DAY": IntegerType(),
        "STREAM_MONTH": IntegerType(),
        "STREAM_YEAR": IntegerType(),
        "STREAM_COUNTRY_DSP": StringType(),
        "FKID_STREAM_COUNTRY": IntegerType(),
        "TRACK_STORE_ID_DSP": StringType(),
        "CLIENT_AGE_BAND_DSP": StringType(),
        "FKID_CLIENT_AGE_BAND": IntegerType(),
        "CLIENT_GENDER_DSP": StringType(),
        "FKID_CLIENT_GENDER": IntegerType(),
        "CLIENT_OFFER_DSP": StringType(),
        "FKID_CLIENT_OFFER": IntegerType(),
        "DSP_OFFER_DSP": StringType(),
        "FKID_DSP_OFFER": IntegerType(),
        "QUANTITY": LongType(),
        "UNIQUE_LISTENER": LongType(),
        "INSERT_TIMESTAMP": TimestampType(),
        "BLV_ERROR_CATEGORY": StringType(),
        "BLV_MESSAGES": ArrayType(StringType()),
        "ID_STRUCTURING": LongType()
    }

    scala_path_blv_error_false = "C:/Users/maikel.gali-ext/Desktop/project/daily-stats/data/destination/believe/apple-music-demo/2024/04/24/validating.parquet/BLV_ERROR=false/RETRY_NUMBER=0"
    pyspark_path_blv_error_false = "../data/destination/believe/apple-music-demo/2024/04/24/validating.parquet/BLV_ERROR=false/RETRY_NUMBER=0"

    scala_path_blv_error_true = "C:/Users/maikel.gali-ext/Desktop/project/daily-stats/data/destination/believe/apple-music-demo/2024/04/24/validating.parquet/BLV_ERROR=true/RETRY_NUMBER=0"
    pyspark_path_blv_error_true = "../data/destination/believe/apple-music-demo/2024/04/24/validating.parquet/BLV_ERROR=true/RETRY_NUMBER=0"

    scala_data_blv_error_false = cast_columns(df=spark.read.parquet(*list_parquet_files(scala_path_blv_error_false)), schema=scala_schema)
    pyspark_data_blv_error_false = cast_columns(df=spark.read.parquet(*list_parquet_files(pyspark_path_blv_error_false)), schema=scala_schema)

    scala_data_blv_error_true =  cast_columns(df=spark.read.parquet(*list_parquet_files(scala_path_blv_error_true)), schema=scala_schema)
    pyspark_data_blv_error_true = cast_columns(df=spark.read.parquet(*list_parquet_files(pyspark_path_blv_error_true)), schema=scala_schema)

    is_equal_blv_error_false = scala_data_blv_error_false.exceptAll(pyspark_data_blv_error_false).isEmpty() and pyspark_data_blv_error_false.exceptAll(scala_data_blv_error_false).isEmpty()
    is_equal_blv_error_true = scala_data_blv_error_true.exceptAll(pyspark_data_blv_error_true).isEmpty() and pyspark_data_blv_error_true.exceptAll(scala_data_blv_error_true).isEmpty()

    if is_equal_blv_error_false:
        print("Les fichiers Parquet sont identiques.")
    else:
        diff_scala = scala_data_blv_error_false.exceptAll(pyspark_data_blv_error_false)

    if is_equal_blv_error_true:
        print("Les fichiers Parquet sont identiques.")
    else:
        print("Les fichier BLV_ERROR = TRUE ne sont pas identiques")