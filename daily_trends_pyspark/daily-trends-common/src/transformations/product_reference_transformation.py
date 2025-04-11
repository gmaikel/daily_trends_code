from src.constants.product_ref_type import ProductRefType
from src.functions.commons_map_functions import extract_grid_id_track
from src.schemas.match_parameters import DistributorMatchParameters, DistributorMatchParametersIsTuneCore, IsrcUpcMatchParameters, StoreIdMatchParameters
from src.schemas.product_reference_schema import ProductReferenceSchema
from src.schemas.scehmas import Schemas
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, regexp_replace, row_number, count

class ProductReferenceTransformation:
    ROW_NUMBER = "ROW_NUMBER"
    COUNT = "COUNT"

    @staticmethod
    def check_if_is_tunecore(match_parameters: IsrcUpcMatchParameters, df: DataFrame) -> DataFrame:
        if isinstance(match_parameters, DistributorMatchParametersIsTuneCore):
            return df.filter(col(ProductReferenceSchema.IS_TUNECORE) == lit(True))
        return df

    @staticmethod
    def reference_product_by_isrc_upc(match_parameters: IsrcUpcMatchParameters, df: DataFrame) -> DataFrame:
        filtered = ProductReferenceTransformation.check_if_is_tunecore(match_parameters, df)

        window = Window.partitionBy(
            col(match_parameters.isrc_col_name),
            col(match_parameters.upc_col_name),
            col(ProductReferenceSchema.START_DATE_VALIDITY),
            col(ProductReferenceSchema.END_DATE_VALIDITY)
        ).orderBy(col(ProductReferenceSchema.ID_TRACK).desc_nulls_last())

        return (filtered.filter(
            (col(match_parameters.isrc_col_name).isNotNull()) &
            (col(match_parameters.upc_col_name).isNotNull()) &
            (col(ProductReferenceSchema.ID_TRACK).isNotNull()) &
            (~col(ProductReferenceSchema.IS_DELETED))
        )
                .withColumn(ProductReferenceTransformation.ROW_NUMBER, row_number().over(window))
                .filter(col(ProductReferenceTransformation.ROW_NUMBER) == 1)
                .select(ProductReferenceSchema.ID_TRACK, match_parameters.isrc_col_name, match_parameters.upc_col_name, ProductReferenceSchema.START_DATE_VALIDITY,
                        ProductReferenceSchema.END_DATE_VALIDITY, ProductReferenceSchema.REF_TYPE, ProductReferenceSchema.DURATION, ProductReferenceSchema.PKID_DP_PRODUCT)
                .withColumnRenamed(match_parameters.isrc_col_name, ProductReferenceSchema.ISRC)
                .withColumnRenamed(match_parameters.upc_col_name, ProductReferenceSchema.UPC))

    @staticmethod
    def reference_product_by_upc(match_parameters: DistributorMatchParameters, df: DataFrame) -> DataFrame:
        window = Window.partitionBy(
            col(match_parameters.upc_col_name),
            col(ProductReferenceSchema.START_DATE_VALIDITY),
            col(ProductReferenceSchema.END_DATE_VALIDITY)
        ).orderBy(col(ProductReferenceSchema.ID_ALBUM).desc_nulls_last())

        return (df.filter(
            (col(match_parameters.upc_col_name).isNotNull()) &
            (col(ProductReferenceSchema.ID_ALBUM).isNotNull()) &
            (~col(ProductReferenceSchema.IS_DELETED)) &
            (col(ProductReferenceSchema.REF_TYPE) == ProductRefType.ALBUM)
        )
                .withColumn(ProductReferenceTransformation.ROW_NUMBER, row_number().over(window))
                .filter(col(ProductReferenceTransformation.ROW_NUMBER) == 1)
                .select(ProductReferenceSchema.ID_ALBUM, match_parameters.upc_col_name, ProductReferenceSchema.START_DATE_VALIDITY,
                        ProductReferenceSchema.END_DATE_VALIDITY, ProductReferenceSchema.REF_TYPE, ProductReferenceSchema.DURATION, ProductReferenceSchema.PKID_DP_PRODUCT)
                .withColumnRenamed(match_parameters.upc_col_name, ProductReferenceSchema.UPC))

    @staticmethod
    def reference_product_by_ISRC(match_parameters: DistributorMatchParameters, df: DataFrame) -> DataFrame:
        window = Window.partitionBy(
            col(match_parameters.isrc_col_name),
            col(ProductReferenceSchema.START_DATE_VALIDITY),
            col(ProductReferenceSchema.END_DATE_VALIDITY)
        ).orderBy(col(ProductReferenceSchema.ID_ALBUM).desc_nulls_last())

        return (df.filter(
            (col(match_parameters.isrc_col_name).isNotNull()) &
            (col(ProductReferenceSchema.ID_ALBUM).isNotNull()) &
            (~col(ProductReferenceSchema.IS_DELETED)) &
            (col(ProductReferenceSchema.REF_TYPE) == ProductRefType.ALBUM)
        )
                .withColumn(ProductReferenceTransformation.ROW_NUMBER, row_number().over(window))
                .filter(col(ProductReferenceTransformation.ROW_NUMBER) == 1)
                .select(ProductReferenceSchema.ID_ALBUM, match_parameters.isrc_col_name, ProductReferenceSchema.START_DATE_VALIDITY,
                        ProductReferenceSchema.END_DATE_VALIDITY, ProductReferenceSchema.REF_TYPE, ProductReferenceSchema.DURATION, ProductReferenceSchema.PKID_DP_PRODUCT)
                .withColumnRenamed(match_parameters.isrc_col_name, ProductReferenceSchema.UPC))

    @staticmethod
    def reference_product_for_platform(match_parameters: StoreIdMatchParameters, df: DataFrame) -> DataFrame:
        return (df.filter(
            ~col(ProductReferenceSchema.IS_DELETED)
        ).select(
            col(ProductReferenceSchema.ID_TRACK), col(ProductReferenceSchema.ID_ALBUM), col(match_parameters.store_id_col).alias(ProductReferenceSchema.ALIAS_STORE_ID),
            col(ProductReferenceSchema.ISRC), col(ProductReferenceSchema.UPC), col(ProductReferenceSchema.START_DATE_VALIDITY), col(ProductReferenceSchema.END_DATE_VALIDITY),
            col(ProductReferenceSchema.REF_TYPE), col(ProductReferenceSchema.DURATION), col(ProductReferenceSchema.PKID_DP_PRODUCT), col(ProductReferenceSchema.CONTENT_TYPE))
        )

    @staticmethod
    def reference_product(df):
        df.select(ProductReferenceSchema.ALIAS_STORE_ID, ProductReferenceSchema.ID_TRACK, ProductReferenceSchema.ISRC, ProductReferenceSchema.UPC,
                  ProductReferenceSchema.START_DATE_VALIDITY, ProductReferenceSchema.END_DATE_VALIDITY, ProductReferenceSchema.REF_TYPE, ProductReferenceSchema.DURATION, ProductReferenceSchema.PKID_DP_PRODUCT)

    @staticmethod
    def reference_product_by_id_track(df: DataFrame) -> DataFrame:
        window = Window.partitionBy(
            col(ProductReferenceSchema.ID_TRACK),
            col(ProductReferenceSchema.START_DATE_VALIDITY),
            col(ProductReferenceSchema.END_DATE_VALIDITY)).orderBy(col(ProductReferenceSchema.ID_TRACK).desc_nulls_last())

        return (df.filter(
            col(ProductReferenceSchema.ID_TRACK).isNotNull()
        )
                .withColumn(ProductReferenceTransformation.ROW_NUMBER, row_number().over(window))
                .filter(col(ProductReferenceTransformation.ROW_NUMBER) == 1)
                .select(col(ProductReferenceSchema.ID_TRACK), col(ProductReferenceSchema.ISRC), col(ProductReferenceSchema.UPC), col(ProductReferenceSchema.START_DATE_VALIDITY),
                        col(ProductReferenceSchema.END_DATE_VALIDITY), col(ProductReferenceSchema.REF_TYPE), col(ProductReferenceSchema.DURATION), col(ProductReferenceSchema.PKID_DP_PRODUCT)))

    @staticmethod
    def reference_product_by_id_album(df: DataFrame) -> DataFrame:
        window = Window.partitionBy(
            col(ProductReferenceSchema.ID_ALBUM),
            col(ProductReferenceSchema.START_DATE_VALIDITY),
            col(ProductReferenceSchema.END_DATE_VALIDITY)).orderBy(col(ProductReferenceSchema.ID_ALBUM).desc_nulls_last())

        return (df.filter(
            col(ProductReferenceSchema.ID_ALBUM).isNotNull()
        )
                .withColumn(ProductReferenceTransformation.ROW_NUMBER, row_number().over(window))
                .filter(col(ProductReferenceTransformation.ROW_NUMBER) == 1)
                .select(col(ProductReferenceSchema.ID_ALBUM), col(ProductReferenceSchema.ISRC), col(ProductReferenceSchema.UPC), col(ProductReferenceSchema.START_DATE_VALIDITY),
                        col(ProductReferenceSchema.END_DATE_VALIDITY), col(ProductReferenceSchema.REF_TYPE), col(ProductReferenceSchema.DURATION), col(ProductReferenceSchema.PKID_DP_PRODUCT)))

    @staticmethod
    def reference_youtube_track_from_deliveries(delivery_type: str, df):
        yt_id_and_tracks = df.filter(col(ProductReferenceSchema.DELIVERY_TYPE) == delivery_type) \
            .select(col(ProductReferenceSchema.ID_YOUTUBE), col(ProductReferenceSchema.ID_SONG).alias(ProductReferenceSchema.ID_TRACK)) \
            .distinct()

        yt_id_with_unique_track = yt_id_and_tracks.groupBy(ProductReferenceSchema.ID_YOUTUBE).count() \
            .filter(col(ProductReferenceTransformation.COUNT) == 1)

        return yt_id_and_tracks.join(yt_id_with_unique_track, on=ProductReferenceSchema.ID_YOUTUBE, how="left_semi")

    @staticmethod
    def reference_youtube_track_from_asset_grid(df: DataFrame) -> DataFrame:
        asset_and_tracks = (df.select(ProductReferenceSchema.CUSTOM_ID, Schemas.ASSET_ID, Schemas.CMS_TECHNICAL_NAME)
                            .withColumn(ProductReferenceSchema.CUSTOM_ID, regexp_replace(col(ProductReferenceSchema.CUSTOM_ID), "^AT_", ""))
                            .withColumn(ProductReferenceSchema.ID_TRACK, extract_grid_id_track(col(ProductReferenceSchema.CUSTOM_ID)))
                            .filter(col(ProductReferenceSchema.ID_TRACK).isNotNull())
                            .drop(ProductReferenceSchema.CUSTOM_ID)
                            .distinct())

        asset_with_unique_track = asset_and_tracks.groupBy(
            Schemas.ASSET_ID, Schemas.CMS_TECHNICAL_NAME
        ).agg(count("*").alias(ProductReferenceTransformation.COUNT)).filter(col(ProductReferenceTransformation.COUNT) == 1)

        return asset_and_tracks.join(asset_with_unique_track, [Schemas.ASSET_ID, Schemas.CMS_TECHNICAL_NAME], "left_semi")

    @staticmethod
    def reference_youtube_track_from_video_grid(df: DataFrame) -> DataFrame:
        video_and_tracks = (df.select(ProductReferenceSchema.CUSTOM_ID, Schemas.VIDEO_ID)
                            .withColumn(ProductReferenceSchema.CUSTOM_ID, regexp_replace(col(ProductReferenceSchema.CUSTOM_ID), "^AT_", ""))
                            .withColumn(ProductReferenceSchema.ID_TRACK, extract_grid_id_track(col(ProductReferenceSchema.CUSTOM_ID)))
                            .filter(col(ProductReferenceSchema.ID_TRACK).isNotNull())
                            .drop(ProductReferenceSchema.CUSTOM_ID)
                            .distinct())

        video_with_unique_track = video_and_tracks.groupBy(
            Schemas.VIDEO_ID
        ).agg(count("*").alias(ProductReferenceTransformation.COUNT)).filter(col(ProductReferenceTransformation.COUNT) == 1)

        return video_and_tracks.join(video_with_unique_track, [Schemas.VIDEO_ID], "left_semi")
