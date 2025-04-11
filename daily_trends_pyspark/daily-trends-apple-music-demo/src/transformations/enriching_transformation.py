from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# TODO: use AppleMusicDemoSchema to import 'PKID_DSP_DEMOGRAPHIC_RAW'
#from schemas.apple_music_demo_schema import AppleMusicDemoSchema
from src.schemas.scehmas import Schemas

class EnrichingTransformation:
    @staticmethod
    def generate_pkid_dsp_demographic_raw(df):
        return df.withColumn(
            'PKID_DSP_DEMOGRAPHIC_RAW',
            F.xxhash64(
                F.col(Schemas.FKID_DSP),
                F.col(Schemas.FKID_DISTRIBUTOR),
                F.col(Schemas.FKID_DP_PRODUCT),
                F.col(Schemas.STREAM_DATE_DSP),
                F.col(Schemas.STREAM_COUNTRY_DSP),
                F.col(Schemas.CLIENT_AGE_BAND_DSP),
                F.col(Schemas.CLIENT_GENDER_DSP),
                F.col(Schemas.DSP_OFFER_DSP)
            ).cast(StringType())
        )
