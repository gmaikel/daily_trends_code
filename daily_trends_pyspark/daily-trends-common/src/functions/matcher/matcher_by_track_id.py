from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from src.constants.product_ref_type import ProductRefType
from src.schemas.product_matching_schema import ProductMatchingSchema
from src.schemas.product_reference_schema import ProductReferenceSchema


class MatcherByTrackId:
    def __init__(self, reference: DataFrame):
        self.reference = reference

    def product_reference(self, table: DataFrame) -> DataFrame:
        # Preprocess table to prevent duplicates (same track id but distinct validity periods)
        df = self.reference.join(
            table.select(ProductMatchingSchema.GRID_ID_TRACK_MATCHING, ProductMatchingSchema.DATE_MATCHING),
            self.join_condition(),
            "left_semi"
        )
        window = Window.partitionBy(F.col(ProductReferenceSchema.ID_TRACK)).orderBy(F.col(ProductReferenceSchema.ID_TRACK).desc_nulls_last())

        # Keep only the track with the max ID_TRACK
        return df.withColumn(
            ProductMatchingSchema.ROW_NUMBER_MATCHING, F.row_number().over(window)
        ).where(F.col(ProductMatchingSchema.ROW_NUMBER_MATCHING) == 1)

    def join_condition(self):
        # Define the condition for joining
        return (F.col(ProductReferenceSchema.ID_TRACK) == F.col(ProductMatchingSchema.GRID_ID_TRACK_MATCHING)) & \
            (F.col(ProductReferenceSchema.REF_TYPE) == F.lit(ProductRefType.TRACK)) & \
            (F.col(ProductMatchingSchema.DATE_MATCHING).between(
                F.col(ProductReferenceSchema.START_DATE_VALIDITY),
                F.col(ProductReferenceSchema.END_DATE_VALIDITY)
            ))

    def product_type(self):
        return ProductRefType.TRACK
