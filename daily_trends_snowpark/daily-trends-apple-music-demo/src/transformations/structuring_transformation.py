from pyspark.sql import functions as F
from pyspark.sql import DataFrame

def keep_real_listeners_and_deduplicate(df: DataFrame) -> DataFrame:
    """
    Cette fonction filtre les lignes où les colonnes 'STREAMS' et 'LISTENERS' ne sont pas nulles
    et supprime les doublons du DataFrame.
    """
    return (df
            .filter(F.col("STREAMS").isNotNull() & F.col("LISTENERS").isNotNull())
            .dropDuplicates())
