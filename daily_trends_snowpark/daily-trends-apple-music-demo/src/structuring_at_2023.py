# from pyspark.sql import functions as F
from common.src.catalog.local_catalog import LocalCatalog
# from common.src.cli.default_spark_batch import DefaultSparkBatch
# from common.src.cli.job_config import JobConfig
# from common.src.metrics.structuring_metrics import StructuringMetrics
# from schemas.apple_music_demo_schema import AppleMusicDemoSchema
from apple_music_demo import streams_path
# from common.src.constants.jobs_paths import JobsPaths
# from common.src.cli.spark_session_wrapper import SparkSessionWrapper
# from common.src.schemas.scehmas import Schemas
from common.src.transformations.metrics_transformation import MetricsTransformation
# from common.src.transformations.validating_transformations import ValidatingTransformations
# from common.src.transformations.schema_transformations import SchemaTransformations
# from src.transformations.structuring_transformation import keep_real_listeners_and_deduplicate
from snowflake.snowpark import Window
from snowflake.snowpark.functions import row_number, desc, col, min
from snowflake.snowpark import functions as F
from conn_snowpark import SessionManager
import pandas as pd

import glob
import os
from src.schemas.apple_music_demo_schema import AppleMusicDemoSchema


class StructuringAt20230424:
    def __init__(self, config):
        self.config = config

    def run(self):
        if self.config.local:
            a = 3
            #LocalCatalog.register_dataframes(config=self.config)

        options = self.build_raw_options()

        session = SessionManager.get_session()

        # TODO: read csv from csv stage
        df_pandas = multiple_csv_to_df_from_config(
            config=self.config,
            options=options
        )
        # FIN TODO

        df_snowpark = session.create_dataframe(df_pandas)
        df_snowpark_schema = session.create_dataframe(df_snowpark.collect(), schema=AppleMusicDemoSchema.MAINSTREAMS)
        df_transformed = MetricsTransformation.observe_raw_data(df_snowpark_schema)

        # # TODO: listener à refacto plus tard
        # # StructuringMetrics.register_structuring_listener_metrics(config=self.config)


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

        a = 3
        # Write the transformed DataFrame to a parquet file
        transformed_streams.write.mode("overwrite").parquet(JobsPaths.structuring_path(self.config))

        b = 4

    def build_raw_options(self):
        return {
            "encoding": "UTF-8",
            "header": 0,
            "sep": "\t",
            #"mode": "PERMISSIVE"
        }



def multiple_csv_to_df_from_config(config, options) -> pd.DataFrame:
    all_files = glob.glob(streams_path(config))
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, **options)
        li.append(df)
    df_concat = pd.concat(li, axis=0, ignore_index=True)

    return df_concat