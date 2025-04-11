from src.catalog.matching_parameters import MatchingParameters
from src.catalog.db_data_enrichment import DBDataEnrichment
from schemas.apple_music_demo_schema import AppleMusicDemoSchema

# import com.believedigital.dtp.dailytrends.datapreparation.apple_music_demo.transformations.EnrichingTransformation.generatePkidDspDemographicRaw
# import com.believedigital.dtp.dailytrends.datapreparation.commons.productid.{MatchingParameters, ProductId}
# import com.believedigital.dtp.dailytrends.datapreparation.commons.transformations.MappingTransformations._
# import com.believedigital.dtp.dailytrends.datapreparation.commons.transformations.{PivotTransformations, SchemaTransformations, ValidatingTransformations}

from src.catalog.local_catalog import LocalCatalog
from src.cli.default_spark_batch import DefaultSparkBatch
from src.cli.spark_session_wrapper import SparkSessionWrapper
from src.constants.jobs_paths import JobsPaths
from src.schemas.product_matching_schema import ProductMatchingSchema
from src.schemas.scehmas import Schemas
from src.constants.retry_utils_used_for_enriching import RetryUtilsUsedForEnriching
from src.transformations.validating_transformations import ValidatingTransformations
from src.transformations.product_reference_transformation import ProductReferenceTransformation
from src.functions.product_id import ProductId, ProductIdMother

from pyspark.sql import functions as F




class Enriching(DefaultSparkBatch):

    def run(self):
        if self.config.local:
            LocalCatalog.register_databases(config=self.config)

        spark_wrapper = SparkSessionWrapper
        spark = spark_wrapper.get_spark_session()

        spark_wrapper.description( "Enrich apple-music-demo daily-stats")

        streams = RetryUtilsUsedForEnriching(self.config).load_structuring()
        products = self.product_matching(df=streams)


        # TODO: listener à refacto plus tard
        # StructuringMetrics.register_structuring_listener_metrics(config=self.config)

        from pyspark.sql.window import Window

        window_spec = Window.orderBy(
            "TRACK_STORE_ID_DSP",
            "CLIENT_AGE_BAND_DSP",
            "CLIENT_GENDER_DSP",
            "DSP_OFFER_DSP",
            "QUANTITY",
            "UNIQUE_LISTENER"
        )



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

        # TODO: refacto la transfo car peut être amélioré
        reference_transformed_track_store_id = reference.transform(lambda dataframe: ProductReferenceTransformation.reference_product_for_platform(match_parameters=store_id_params, df=dataframe))

        a = 3

        return ProductIdMother().match_by_track_id(
            reference=reference_transformed_track_store_id
        ).add_tracks_informations(product, JobsPaths.tracks_with_referential(self.config))










