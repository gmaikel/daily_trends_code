from snowflake.snowpark import functions as F
from snowflake.snowpark.types import LongType
from common.src.constants.metrics_definition import MetricsDefinition
from common.src.constants.category_definition import CategoryDefinition
from common.src.schemas.scehmas import Schemas
from conn_snowpark import SessionManager

class MetricsTransformation:

    @staticmethod
    def observe_raw_data(df):
        # TODO : pour metriques
        raw_data_count = df.count()
        invalid_raw_data_count = df.filter(F.col(Schemas.CORRUPT_RECORD).isNotNull()).count()
        print(f"Raw data count: {raw_data_count}")
        print(f"Invalid raw data count: {invalid_raw_data_count}")
        # FIN TODO

        df_cleaned = (
            df.filter(F.col(Schemas.CORRUPT_RECORD).isNull())
            .drop(Schemas.CORRUPT_RECORD)
        )

        return df_cleaned

    @staticmethod
    def observe_useful_data(df):
        # Metrique simple au lieu du observe
        useful_raw_data_count = df.count()
        # Affichage de la métrique simple
        print(f"Useful raw data count: {useful_raw_data_count}")

        # Calcul de la métrique utile
        df_transformed = df.select(
            F.count(F.lit(1)).alias(MetricsDefinition.USEFUL_RAW_DATA)
        )

        return df_transformed

    @staticmethod
    def observe_raw_and_useful_data(df):
        """
        Observe les métriques des données brutes et utiles.
        """
        df_raw = MetricsTransformation.observe_raw_data(df)
        df_useful = MetricsTransformation.observe_useful_data(df)

        df_combined = df_raw.join(df_useful, on=None, how="cross")

        return df_combined

    @staticmethod
    def observe_rejects_structuring(categories, df):
        # Calcul des rejets avec Snowpark
        condition = F.lit(False)
        for category in categories:
            condition = condition | F.col(category.errorColName)

        # Calcul du nombre de rejets
        final_raw_data = df.select(F.sum(F.when(~condition, 1).otherwise(0))).collect()[0][0]
        print(f"FINAL_RAW_DATA: {final_raw_data}")  # Simule `observe()`

        return df
