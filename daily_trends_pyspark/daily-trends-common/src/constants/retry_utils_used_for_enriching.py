from src.constants.jobs_paths import JobsPaths
from src.schemas.scehmas import Schemas
from src.cli.spark_session_wrapper import SparkSessionWrapper
from src.cli.default_spark_batch import DefaultSparkBatch

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class RetryUtilsUsedForEnriching(DefaultSparkBatch):

    def save_mode(self) -> str:
        """
        Determines the save mode based on the retry configuration.
        """
        return "append" if self.config.retry else "overwrite"

    def load_structuring(self) -> DataFrame:
        """
        Loads the structuring data, applying filtering based on retry logic.
        """
        spark = SparkSessionWrapper.get_spark_session()

        if self.config.retry:
            remaining_errors = (
                spark.read.parquet(JobsPaths.validating_path(self.config))
                .filter((F.col(Schemas.BLV_ERROR) == True) & (F.col(Schemas.RETRY_NUMBER) == (self.config.retry_number - 1)))
                .select(Schemas.ID_STRUCTURING)
            )

            structuring = spark.read.parquet(JobsPaths.structuring_path(self.config))

            return (
                structuring.join(remaining_errors, Schemas.ID_STRUCTURING, "left_semi")
                .withColumn(Schemas.RETRY_NUMBER, F.lit(self.config.retry_number).cast("long"))
            )
        else:
            return (
                spark.read.parquet(JobsPaths.structuring_path(self.config))
                .withColumn(Schemas.RETRY_NUMBER, F.lit(self.config.retry_number).cast("long"))
            )
