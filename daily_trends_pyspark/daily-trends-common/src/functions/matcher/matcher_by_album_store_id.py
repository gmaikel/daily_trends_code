from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.constants.product_ref_type import ProductRefType
from src.schemas.product_matching_schema import ProductMatchingSchema
from src.schemas.product_reference_schema import ProductReferenceSchema


class MatcherByAlbumStoreId:
    def __init__(self, reference: DataFrame):
        self.reference = reference

    def product_reference(self, table: DataFrame) -> DataFrame:

        window = Window.partitionBy(
            F.col(ProductReferenceSchema.ALIAS_STORE_ID),
            F.col(ProductReferenceSchema.START_DATE_VALIDITY),
            F.col(ProductReferenceSchema.END_DATE_VALIDITY)
        ).orderBy(F.col(ProductReferenceSchema.ID_ALBUM).desc_nulls_last())

        return self.reference.withColumn(
            ProductMatchingSchema.ROW_NUMBER_MATCHING, F.row_number().over(window)
        ).where(F.col(ProductMatchingSchema.ROW_NUMBER_MATCHING) == 1)

    def join_condition(self):
        return (F.col(ProductReferenceSchema.ALIAS_STORE_ID) == F.col(ProductMatchingSchema.TRACK_STORE_ID_MATCHING)) & \
            (F.col(ProductReferenceSchema.REF_TYPE) == F.lit(ProductRefType.ALBUM)) & \
            (F.col(ProductMatchingSchema.DATE_MATCHING).between(
                F.col(ProductReferenceSchema.START_DATE_VALIDITY),
                F.col(ProductReferenceSchema.END_DATE_VALIDITY)
            ))

    def product_type(self):
        return ProductRefType.ALBUM
