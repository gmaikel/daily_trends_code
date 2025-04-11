from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.constants.product_ref_type import ProductRefType
from src.schemas.product_matching_schema import ProductMatchingSchema
from src.schemas.product_reference_schema import ProductReferenceSchema
from src.transformations.product_reference_transformation import ProductReferenceTransformation


class MatcherByUpc:
    def __init__(self, reference: DataFrame):
        self.reference = reference

    def product_reference(self, table: DataFrame) -> DataFrame:
        df = self.reference.join(
            table.select(ProductMatchingSchema.UPC_MATCHING, ProductMatchingSchema.DATE_MATCHING),
            self.join_condition(), "left_semi"
        )

        window = Window.partitionBy(df[ProductReferenceSchema.UPC]).orderBy(F.col(ProductReferenceSchema.ID_ALBUM).desc_nulls_last())

        return df.withColumn(ProductMatchingSchema.ROW_NUMBER_MATCHING, F.row_number().over(window)) \
            .where(F.col(ProductMatchingSchema.ROW_NUMBER_MATCHING) == 1)

    def join_condition(self):
        return (F.col(ProductReferenceSchema.UPC) == F.col(ProductMatchingSchema.UPC_MATCHING)) & \
            (F.col(ProductReferenceSchema.REF_TYPE) == F.lit(ProductRefType.ALBUM)) & \
            (F.col(ProductMatchingSchema.DATE_MATCHING).between(
                F.col(ProductReferenceSchema.START_DATE_VALIDITY),
                F.col(ProductReferenceSchema.END_DATE_VALIDITY)
            ))

    def product_type(self):
        return ProductRefType.ALBUM
