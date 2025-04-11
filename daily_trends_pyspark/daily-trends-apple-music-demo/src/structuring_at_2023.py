from pyspark.sql import functions as F
from src.cli.default_spark_batch import DefaultSparkBatch
from schemas.apple_music_demo_schema import AppleMusicDemoSchema
from apple_music_demo import streams_path
from src.constants.jobs_paths import JobsPaths
from src.catalog.local_catalog import LocalCatalog
from src.cli.spark_session_wrapper import SparkSessionWrapper
from src.schemas.scehmas import Schemas
from src.transformations.metrics_transformation import MetricsTransformation
from src.transformations.validating_transformations import ValidatingTransformations
from src.transformations.schema_transformations import SchemaTransformations
from src.transformations.structuring_transformation import keep_real_listeners_and_deduplicate

class StructuringAt20230424(DefaultSparkBatch):

    def run(self):
        if self.config.local:
            a = 3
            #LocalCatalog.register_databases(config=self.config)

        options = self.build_raw_options()

        spark_wrapper = SparkSessionWrapper
        spark = spark_wrapper.get_spark_session()

        streams = spark.read.options(**options) \
            .schema(AppleMusicDemoSchema.MAINSTREAMS) \
            .csv(streams_path(self.config)) \
            .transform(MetricsTransformation.observe_raw_data)

        spark_wrapper.description("Structure Apple Music Demo Daily Trends")

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

        # Transformation Pipeline
        transformed_streams = (streams
                               .transform(keep_real_listeners_and_deduplicate)
                               .transform(MetricsTransformation.observe_useful_data)
                               .withColumn(Schemas.ID_REPORT, F.lit(self.config.report_id).cast("long"))
                               .withColumn(Schemas.FKID_DSP, F.lit(self.config.dsp))
                               .withColumn(Schemas.FKID_DISTRIBUTOR, F.lit(self.config.distributor))
                               .withColumn(Schemas.STREAM_DATE_DSP, streams[AppleMusicDemoSchema.REPORT_DATE])
                               .withColumn(Schemas.STREAM_COUNTRY_DSP, streams[AppleMusicDemoSchema.STOREFRONT_NAME])
                               .withColumn(Schemas.TRACK_STORE_ID_DSP, streams[AppleMusicDemoSchema.APPLE_IDENTIFIER])
                               .withColumn(Schemas.CLIENT_AGE_BAND_DSP, streams[AppleMusicDemoSchema.AGE_BAND])
                               .withColumn(Schemas.CLIENT_GENDER_DSP, streams[AppleMusicDemoSchema.GENDER])
                               .withColumn(Schemas.CLIENT_OFFER_DSP, streams[AppleMusicDemoSchema.SUBSCRIPTION_MODE])
                               .withColumn(Schemas.DSP_OFFER_DSP, streams[AppleMusicDemoSchema.SUBSCRIPTION_TYPE])
                               .withColumn(Schemas.QUANTITY, streams[AppleMusicDemoSchema.STREAMS])
                               .withColumn(AppleMusicDemoSchema.UNIQUE_LISTENER, streams[AppleMusicDemoSchema.LISTENERS])
                               .withColumn(Schemas.ID_STRUCTURING, F.row_number().over(window_spec).cast("long"))
                               .transform(lambda df: ValidatingTransformations.classify_rejects_structuring(
            ValidatingTransformations.check_mandatory_fields(AppleMusicDemoSchema.RAW_MANDATORY_COLUMNS)
            + ValidatingTransformations.check_quantity_values(F.col(Schemas.QUANTITY)),
            df
        ))
        .transform(lambda df: SchemaTransformations.enforce_columns(AppleMusicDemoSchema.STRUCTURING, df))
        )

        # Write the transformed DataFrame to a parquet file
        transformed_streams.write.mode("overwrite").parquet(JobsPaths.structuring_path(self.config))

    def build_raw_options(self):
        return {
            "encoding": "UTF-8",
            "dateFormat": "yyyy-MM-dd",
            "header": "true",
            "sep": "\t",
            "quote": "",
            "mode": "PERMISSIVE"
        }
