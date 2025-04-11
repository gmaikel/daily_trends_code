from pyspark.sql import functions as F
from src.constants.metrics_definition import MetricsDefinition
from src.schemas.scehmas import Schemas
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType

# TODO: la méthode observe est lié à un listener personnalisé en scala mais qui ne marche pas très bien en pyspark, une autre solution plus simple est de faire un print simple des metrique
class MetricsTransformation:

    @staticmethod
    def observe_raw_data(df: DataFrame) -> DataFrame:
        # TODO: Metriques simples au lieu du observe
        raw_data_count = df.count()
        invalid_raw_data_count = df.filter(F.col(Schemas.CORRUPT_RECORD).isNotNull()).count()
        # Fin - Metriques simples au lieu du observe

        df_cleaned = (
            df.observe(
                MetricsDefinition.RAW_DATA_METRICS,
                F.count(F.lit(1)).alias(MetricsDefinition.RAW_DATA),
                F.sum(F.col(Schemas.CORRUPT_RECORD).cast(LongType())).alias(MetricsDefinition.INVALID_RAW_DATA)
            )
            .filter(F.col(Schemas.CORRUPT_RECORD).isNull())
            .drop(Schemas.CORRUPT_RECORD)
        )

        return df_cleaned

    @staticmethod
    def observe_useful_data(df):
        # Metrique simple au lieu du observe
        useful_raw_data_count = df.count()
        # Fin - Metrique simple au lieu du observe
        return df.observe(
            MetricsDefinition.USEFUL_RAW_DATA_METRICS,
            F.count(F.lit(1)).alias(MetricsDefinition.USEFUL_RAW_DATA)
        )

    @staticmethod
    def observe_raw_and_useful_data(df):
        """
        Observes both raw data and useful data metrics.
        """
        return MetricsTransformation.observe_raw_data(df).transform(MetricsTransformation.observe_useful_data)

    @staticmethod
    def observe_rejects_structuring(categories, df):
        from pyspark.sql.functions import col, lit, sum as spark_sum, when

        condition = lit(False)
        for category in categories:
            condition = condition | col(category.errorColName)

        final_raw_data = df.select(spark_sum(when(~condition, 1).otherwise(0))).collect()[0][0]
        print(f"FINAL_RAW_DATA: {final_raw_data}")

        return df
