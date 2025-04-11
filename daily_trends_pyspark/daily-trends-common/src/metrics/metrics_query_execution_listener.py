from pyspark import SparkContext
from pyspark.sql import SparkSession
from src.cli.job_config import JobConfig
from src.constants.jobs_paths import JobsPaths
from src.constants.metrics_definition import MetricsDefinition
from src.utils.json_fs_utils import JsonFsUtils
from src.cli.spark_session_wrapper import SparkSessionWrapper
import logging

# Initialisation de la session Spark
spark_wrapper = SparkSessionWrapper()
spark = spark_wrapper.get_spark_session()
spark_context = spark.sparkContext

class MetricsQueryExecutionListener:

    def __init__(self, config: JobConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def onApplicationStart(self, applicationStart):
        self.logger.info("Application started")

    def onApplicationEnd(self, applicationEnd):
        self.logger.info("Application ended")

    def onTaskEnd(self, taskEnd):
        self.logger.info(f"Task ended: {taskEnd}")

    def onStageCompleted(self, stageCompleted):
        self.logger.info(f"Stage completed: {stageCompleted}")

    def onJobStart(self, jobStart):
        self.logger.info(f"Job started: {jobStart}")

    def onJobEnd(self, jobEnd):
        self.logger.info(f"Job ended: {jobEnd}")

    def onSuccess(self, funcName: str, qe: 'QueryExecution', durationNs: int):
        """
        This method is called when the query succeeds.
        """
        observed_metrics = qe.observedMetrics()

        # Filter for the metrics we care about
        metrics = {k: v for k, v in observed_metrics.items() if k in [
            MetricsDefinition.RAW_DATA_METRICS,
            MetricsDefinition.USEFUL_RAW_DATA_METRICS,
            MetricsDefinition.FINAL_RAW_DATA_METRICS
        ]}

        # Flatten the metrics and write them to a file using JsonFsUtils
        flattened_metrics = {}
        for key, row in metrics.items():
            flattened_metrics.update({field: row[field] for field in row.schema.fieldNames})

        # Overwrite the metrics JSON at the specified path
        JsonFsUtils.overwrite(JobsPaths.structuring_rejects_summary_path(self.config), flattened_metrics)

        # Unregister the listener
        spark_context._jsc.sc().listenerBus().removeListener(self)

    def onFailure(self, funcName: str, qe: 'QueryExecution', exception: Exception):
        """
        This method is called when the query fails.
        """
        # Unregister the listener in case of failure
        spark_context._jsc.sc().listenerBus().removeListener(self)