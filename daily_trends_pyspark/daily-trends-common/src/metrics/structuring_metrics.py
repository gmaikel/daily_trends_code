from src.cli.job_config import JobConfig
from src.metrics.metrics_query_execution_listener import MetricsQueryExecutionListener
from src.cli.spark_session_wrapper import SparkSessionWrapper

class StructuringMetrics:

    @staticmethod
    def register_structuring_listener_metrics(config: JobConfig):
        """
        Register the MetricsQueryExecutionListener with the Spark session.
        """
        spark_wrapper = SparkSessionWrapper
        spark = spark_wrapper.get_spark_session()
        spark_context = spark.sparkContext
        listener = MetricsQueryExecutionListener(config)
        spark_context._jsc.sc().listenerBus().addListener(listener)
