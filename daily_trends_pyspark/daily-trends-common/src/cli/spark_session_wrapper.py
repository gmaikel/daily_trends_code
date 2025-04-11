from pyspark.sql import SparkSession
from delta import *

class SparkSessionWrapper:
    _spark = None

    @classmethod
    def get_spark_session(cls):
        if cls._spark is None:
            cls._spark = cls.init_spark_session()
        return cls._spark

    @staticmethod
    def init_spark_session():
        builder = (
            SparkSession.builder
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.parquet.mergeSchema", "true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

    @staticmethod
    def description(description: str):
        spark = SparkSessionWrapper.get_spark_session()
        spark.sparkContext.setJobDescription(description)

    @staticmethod
    def description_with_group(group_id: str, description: str):
        spark = SparkSessionWrapper.get_spark_session()
        spark.sparkContext.setJobGroup(group_id, description)

    @staticmethod
    def clear_description():
        spark = SparkSessionWrapper.get_spark_session()
        spark.sparkContext.clearJobGroup()
