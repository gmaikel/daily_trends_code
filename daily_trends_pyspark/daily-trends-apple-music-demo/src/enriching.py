from src.catalog.matching_parameters import MatchingParameters
from src.catalog.db_data_enrichment import DBDataEnrichment
from schemas.apple_music_demo_schema import AppleMusicDemoSchema
from src.catalog.local_catalog import LocalCatalog
from src.cli.default_spark_batch import DefaultSparkBatch
from src.cli.spark_session_wrapper import SparkSessionWrapper
from src.constants.jobs_paths import JobsPaths
from src.schemas.product_matching_schema import ProductMatchingSchema
from src.schemas.scehmas import Schemas
from src.constants.retry_utils_used_for_enriching import RetryUtilsUsedForEnriching
from src.transformations.validating_transformations import ValidatingTransformations
from src.transformations.product_reference_transformation import ProductReferenceTransformation
from src.functions.product_id import ProductIdMother
from pyspark.sql import functions as F
from transformations.enriching_transformation import EnrichingTransformation
from src.transformations.mapping_transformation import MappingTransformations
from src.transformations.pivot_transformations import PivotTransformations
from src.transformations.schema_transformations import SchemaTransformations



class Enriching(DefaultSparkBatch):

    def run(self):
        if self.config.local:
            LocalCatalog.register_databases(config=self.config)

        spark_wrapper = SparkSessionWrapper
        spark = spark_wrapper.get_spark_session()

        spark_wrapper.description( "Enrich apple-music-demo daily-stats")

        streams = RetryUtilsUsedForEnriching(self.config).load_structuring()
        products = self.product_matching(df=streams)

        streams.join(
            products,
            on=[Schemas.TRACK_STORE_ID_DSP, Schemas.STREAM_DATE_DSP],
            how="left_outer"
        ) \
            .transform(lambda df: EnrichingTransformation.generate_pkid_dsp_demographic_raw(df)) \
            .transform(lambda df: MappingTransformations.map_stream_date(df=df)) \
            .transform(lambda df: MappingTransformations.map_stream_country(df=df, dsp_code=self.config.dsp)) \
            .transform(lambda df: MappingTransformations.map_client_offer(df=df, dsp_code=self.config.dsp)) \
            .transform(lambda df: MappingTransformations.map_client_age_band(df=df, dsp_code=self.config.dsp)) \
            .transform(lambda df: MappingTransformations.map_retailer_offer(df=df, dsp_code=self.config.dsp)) \
            .transform(lambda df: MappingTransformations.map_client_gender(df=df, dsp_code=self.config.dsp)) \
            .transform(lambda df: PivotTransformations.add_audit_columns(df=df)) \
            .transform(lambda df: PivotTransformations.add_partitions(df=df)) \
            .transform(lambda df: ValidatingTransformations.classify_rejects_enriching(df=df,
                                                                                       pivot_schema=AppleMusicDemoSchema.PIVOT)) \
            .transform(lambda df: ValidatingTransformations.resolve_categories(df=df)) \
            .transform(lambda df: SchemaTransformations.enforce_columns(AppleMusicDemoSchema.PIVOT, df)) \
            .write.mode(RetryUtilsUsedForEnriching(self.config).save_mode()) \
            .partitionBy(Schemas.BLV_ERROR, Schemas.RETRY_NUMBER) \
            .parquet(JobsPaths.validating_path(self.config))

    def product_matching(self, df):
        spark_wrapper = SparkSessionWrapper
        spark = spark_wrapper.get_spark_session()

        product = df.select(
            F.col(Schemas.TRACK_STORE_ID_DSP),
            F.col(Schemas.STREAM_DATE_DSP),
            F.col(Schemas.TRACK_STORE_ID_DSP).alias(ProductMatchingSchema.TRACK_STORE_ID_MATCHING),
            F.col(Schemas.STREAM_DATE_DSP).alias(ProductMatchingSchema.DATE_MATCHING)
        ).distinct()

        params = MatchingParameters(self.config.dsp, self.config.distributor)

        store_id_params = params.get_store_id_match_parameters()

        spark.catalog.setCurrentDatabase(DBDataEnrichment.DB_DATA_ENRICHMENT)

        reference = spark.read.table(store_id_params.product_referential)

        reference_transformed_track_store_id = reference.transform(lambda dataframe: ProductReferenceTransformation.reference_product_for_platform(match_parameters=store_id_params, df=dataframe))

        return ProductIdMother().match_by_track_id(
            reference=reference_transformed_track_store_id
        ).add_tracks_informations(product, JobsPaths.tracks_with_referential(self.config))