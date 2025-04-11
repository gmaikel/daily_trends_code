from typing import Callable, Any

from src.schemas.youtube_cms_technical_names import YoutubeCmsTechnicalNames
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType

def map_obsolete_country(country: str):
    mapping = {
        "UK": "GB", "EN": "GB", "GG": "GB", "IM": "GB", "JE": "GB",
        "SP": "ES", "BL": "GP", "FX": "FR", "ME": "CS", "RS": "CS",
        "SX": "NL", "BQ": "NL", "SS": "SD"
    }
    return udf(
        mapping.get(country, "ZZ" if not country or country.isspace() else country),
        StringType()
    )

def format_isrc(isrc):
    return F.when(
        F.length(isrc) == 12,
        F.concat_ws(
            "-",
            isrc.substr(1, 2),
            isrc.substr(3, 3),
            isrc.substr(6, 2),
            isrc.substr(8, 5)
        )
    ).otherwise(isrc)

def format_upc(upc):
    return F.lpad(upc, 13, "0")

def extract_grid_id_track(grid):
    regex = "^(A1)-?(0320W)-?(T)(\\d{9})-?([0-9A-Z])$"
    return F.regexp_extract(grid, regex, 4).cast(IntegerType())

def extract_grid_id_album(grid):
    regex = "^(A1)-?(0320W)-?(A)(\\d{9})-?([0-9A-Z])$"
    return F.regexp_extract(grid, regex, 4).cast(IntegerType())

def map_old_youtube_cms_names(source_cms_name: str):
    mapping = {
        YoutubeCmsTechnicalNames.OLD_BELIEVE_CMS: YoutubeCmsTechnicalNames.BELIEVE_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_KIDS_CMS: YoutubeCmsTechnicalNames.BELIEVE_KIDS_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_KIDS_MANAGED_CMS: YoutubeCmsTechnicalNames.BELIEVE_KIDS_MANAGED_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_NEWS_FRANCE_CMS: YoutubeCmsTechnicalNames.BELIEVE_NEWS_FRANCE_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_NEWS_FRANCE_MANAGED_CMS: YoutubeCmsTechnicalNames.BELIEVE_NEWS_FRANCE_MANAGED_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_NEWS_SPAIN_CMS: YoutubeCmsTechnicalNames.BELIEVE_NEWS_SPAIN_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_SAS_CMS: YoutubeCmsTechnicalNames.BELIEVE_SAS_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_SAS_MANAGED_CMS: YoutubeCmsTechnicalNames.BELIEVE_SAS_MANAGED_CMS,
        YoutubeCmsTechnicalNames.OLD_BELIEVE_TUNECORE_CMS: YoutubeCmsTechnicalNames.BELIEVE_TUNECORE_CMS,
        YoutubeCmsTechnicalNames.OLD_MUSICAST_CMS: YoutubeCmsTechnicalNames.MUSICAST_CMS
    }
    return udf(
        mapping.get(source_cms_name, source_cms_name),
        StringType()
    )

