from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# TODO: Importation des noms de colonnes
LISTENERS = 'listeners'
STREAMS = 'streams'

def keep_real_listeners_and_deduplicate(df: DataFrame) -> DataFrame:
    return (df
            .filter(F.col(STREAMS).isNotNull() & F.col(LISTENERS).isNotNull())
            .dropDuplicates())
