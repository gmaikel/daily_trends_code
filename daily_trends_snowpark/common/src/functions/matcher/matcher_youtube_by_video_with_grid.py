from pyspark.sql import DataFrame, functions as F
from src.constants.product_ref_type import ProductRefType
from src.schemas.product_reference_schema import ProductReferenceSchema
from src.schemas.scehmas import Schemas
from src.cli.spark_session_wrapper import SparkSessionWrapper
from src.transformations.product_reference_transformation import ProductReferenceTransformation
from src.catalog.db_external_data import DBExternalData

spark_wrapper = SparkSessionWrapper
spark = spark_wrapper.get_spark_session()

class MatcherYoutubeByVideoWithGrid:
    def __init__(self, match_params):
        self.match_params = match_params

    def product_reference(self, table: DataFrame) -> DataFrame:
        spark.catalog.setCurrentDatabase(DBExternalData.DB_RIGHT_MGMT)

        return spark.read.table(self.match_params.product_referential) \
            .transform(ProductReferenceTransformation.reference_youtube_track_from_video_grid()) \
            .withColumn(ProductReferenceSchema.REF_TYPE, F.lit(self.product_type()))

    def join_condition(self):
        return F.col(Schemas.VIDEO_ID) == F.col(Schemas.FKID_VIDEO)

    def product_type(self):
        return ProductRefType.TRACK
