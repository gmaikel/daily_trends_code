import logging
import argparse
import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.utils import AnalysisException

from spark_session_wrapper import SparkSessionWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobConfig:
    def __init__(self, local: bool = False):
        self.local = local

def parse_cli_options(args):
    parser = argparse.ArgumentParser(description="CLI options parser")
    parser.add_argument("--local", action="store_true", help="Run Spark in local mode")
    parsed_args = parser.parse_args(args)
    return JobConfig(local=parsed_args.local)

class DefaultSparkBatch:
    def __init__(self, config: JobConfig):
        self.config = config

    def run(self):
        raise NotImplementedError("Subclasses should implement this method")

    @classmethod
    def run_batch(cls, batch_cls, args):
        logger.info(f"Starting batch {batch_cls.__name__}")
        batch = batch_cls(parse_cli_options(args))

        logger.info("Parsing CLI Options")
        for attr, value in vars(batch.config).items():
            logger.info(f"\t - {attr} : {value}")

        if not SparkSessionWrapper.get_spark_session().sparkContext.appName:
            SparkSessionWrapper.get_spark_session().sparkContext.setJobDescription(batch_cls.__name__)

        if batch.config.local:
            logger.info("Setting Spark Cluster to standalone")
            cores = max(1, len(os.sched_getaffinity(0)) - 2)
            SparkConf().setMaster(f"local[{cores}]")

        logger.info(f"Running job {batch_cls.__name__}")
        start_time = time.time()

        try:
            batch.run()
        finally:
            duration = time.time() - start_time
            logger.info(f"Job {batch_cls.__name__} finished in {duration:.2f} seconds")
            SparkSessionWrapper.description("Job completed")
