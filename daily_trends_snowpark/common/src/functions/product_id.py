from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import LongType, IntegerType, StringType
from delta.tables import DeltaTable
from src.functions.product_id_builder import ProductIdBuilder


class ProductIdColumns:
    def __init__(self, main_product_key, extra_product_keys=None):
        if extra_product_keys is None:
            extra_product_keys = []
        self.main_product_key = main_product_key
        self.extra_product_keys = extra_product_keys

class ProductIdCol:
    def __init__(self, source_name, destination_name, datatype):
        self.source_name = source_name
        self.destination_name = destination_name
        self.datatype = datatype


class ProductId:
    def __init__(self, matchers):
        self.matchers = matchers
        self.default_product_columns = ProductIdColumns(
            main_product_key=ProductIdCol("PKID_DP_PRODUCT", "FKID_DP_PRODUCT", LongType()),
            extra_product_keys=[ProductIdCol("DURATION", "TRACK_DURATION", IntegerType())]
        )

    def add_tracks_informations(self, df: DataFrame, destination: str) -> DataFrame:
        return self.add_product_informations(df.withColumn("REF_TYPE_MATCHING", lit("TRACK")), destination)

    def add_product_informations(self, df: DataFrame, destination: str, product_id_columns=None) -> DataFrame:
        if product_id_columns is None:
            product_id_columns = self.default_product_columns

        for idx, matching in enumerate(self.matchers):
            if idx == 0:
                self.first_product_matching(df, destination, matching, product_id_columns)
            else:
                self.append(destination, matching, product_id_columns)

        # Rename all extra keys we want to retrieve
        for col_with_rename in product_id_columns.extra_product_keys:
            df = df.withColumnRenamed(col_with_rename.source_name, col_with_rename.destination_name)

        return df.drop("TRACK_STORE_ID_MATCHING", "ISRC_MATCHING", "UPC_MATCHING", "MATCHING_ALGORITHM") \
            .withColumnRenamed(product_id_columns.main_product_key.source_name, product_id_columns.main_product_key.destination_name)

    def first_product_matching(self, df: DataFrame, destination: str, matching, product_id_columns):
        columns_enhanced = [col(c) for c in df.columns] + \
                           [col(c.source_name) for c in product_id_columns.extra_product_keys] + \
                           [col(product_id_columns.main_product_key.source_name), col("MATCHING_ALGORITHM")]

        if matching.name() == "MatcherByTrackStoreId":
            df.join(matching.product_reference(None), matching.join_condition(), "left_outer") \
                .withColumn(
                "MATCHING_ALGORITHM",
                when(col(product_id_columns.main_product_key.source_name).isNotNull(), lit(matching.name()))
                .otherwise(lit(None).cast(StringType()))
            ) \
                .select(*columns_enhanced) \
                .write \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .format("delta") \
                .save(destination)
        else:
            for c in product_id_columns.extra_product_keys:
                df = df.withColumn(c.source_name, lit(None).cast(c.data_type))

            df.withColumn(product_id_columns.main_product_key.source_name, lit(None).cast(product_id_columns.main_product_key.data_type)) \
                .withColumn("MATCHING_ALGORITHM", lit(None).cast(StringType())) \
                .select(*columns_enhanced) \
                .write \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .format("delta") \
                .save(destination)

            self.append(destination, matching, product_id_columns)

    def append(self, destination: str, matcher, product_id_columns: ProductIdColumns):
        table = DeltaTable.forPath(destination)
        df = table.toDF().where(col(product_id_columns.main_product_key[0]).isNull())

        if df.count() != 0:
            reference = matcher.product_reference(df)
            update_map = {c[0]: reference[c[0]] for c in product_id_columns.extra_product_keys}
            update_map[product_id_columns.main_product_key[0]] = reference[product_id_columns.main_product_key[0]]
            update_map["MATCHING_ALGORITHM"] = lit(matcher.name())

            table.merge(reference,
                        df[product_id_columns.main_product_key[0]].isNull() & col("REF_TYPE") == matcher.product_type() & matcher.join_condition()) \
                .whenMatched() \
                .update(update_map) \
                .execute()


class ProductIdMother:
    def __init__(self):
        self.builder = ProductIdBuilder()

    def match_by_track_id(self, reference):
        return self.builder.match_by_track_store_id(reference=reference).build()