from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, BooleanType, ArrayType, TimestampType, IntegerType
from src.schemas.scehmas import Schemas
from src.constants.category_definition import Categories


class AppleMusicDemoSchema:
    REPORT_DATE = "Report Date"
    APPLE_IDENTIFIER = "Apple Identifier"
    STOREFRONT_NAME = "Storefront Name"
    MEMBERSHIP_TYPE = "Membership Type"
    MEMBERSHIP_MODE = "Membership Mode"
    SUBSCRIPTION_TYPE = "Subscription Type"
    SUBSCRIPTION_MODE = "Subscription Mode"
    ACTION_TYPE = "Action Type"
    GENDER = "Gender"
    AGE_BAND = "Age Band"
    AUDIO_FORMAT = "Audio Format"
    LISTENERS = "Listeners"
    STREAMS = "Streams"
    LISTENERS_LAST_SEVEN_DAYS = "Listeners Last 7 Days"
    STREAMS_LAST_SEVEN_DAYS = "Streams Last 7 Days"

    PKID_DSP_DEMOGRAPHIC_RAW = "PKID_DSP_DEMOGRAPHIC_RAW"
    UNIQUE_LISTENER = "UNIQUE_LISTENER"

    # StructType for MAINSTREAMS_UNTIL_20230423
    MAINSTREAMS_UNTIL_20230423 = StructType([
        StructField(REPORT_DATE, DateType()),
        StructField(APPLE_IDENTIFIER, StringType()),
        StructField(STOREFRONT_NAME, StringType()),
        StructField(MEMBERSHIP_TYPE, StringType()),
        StructField(MEMBERSHIP_MODE, StringType()),
        StructField(ACTION_TYPE, LongType()),
        StructField(GENDER, StringType()),
        StructField(AGE_BAND, StringType()),
        StructField(LISTENERS, LongType()),
        StructField(STREAMS, LongType()),
        StructField(LISTENERS_LAST_SEVEN_DAYS, LongType()),
        StructField(STREAMS_LAST_SEVEN_DAYS, LongType()),
        StructField(Schemas.CORRUPT_RECORD, StringType(), True)
    ])

    # StructType for MAINSTREAMS
    MAINSTREAMS = StructType([
        StructField(REPORT_DATE, DateType()),
        StructField(APPLE_IDENTIFIER, StringType()),
        StructField(STOREFRONT_NAME, StringType()),
        StructField(SUBSCRIPTION_TYPE, StringType()),
        StructField(SUBSCRIPTION_MODE, StringType()),
        StructField(ACTION_TYPE, LongType()),
        StructField(GENDER, StringType()),
        StructField(AGE_BAND, StringType()),
        StructField(AUDIO_FORMAT, StringType()),
        StructField(LISTENERS, LongType()),
        StructField(STREAMS, LongType()),
        StructField(LISTENERS_LAST_SEVEN_DAYS, LongType()),
        StructField(STREAMS_LAST_SEVEN_DAYS, LongType()),
        StructField(Schemas.CORRUPT_RECORD, StringType(), True)
    ])

    # RAW_MANDATORY_COLUMNS_UNTIL_20230423
    RAW_MANDATORY_COLUMNS_UNTIL_20230423 = [REPORT_DATE, APPLE_IDENTIFIER, MEMBERSHIP_MODE, STREAMS, LISTENERS]

    # RAW_MANDATORY_COLUMNS
    RAW_MANDATORY_COLUMNS = [REPORT_DATE, APPLE_IDENTIFIER, SUBSCRIPTION_MODE, STREAMS, LISTENERS]

    # StructType for STRUCTURING
    STRUCTURING = StructType([
        StructField(Schemas.ID_REPORT, LongType(), False),
        StructField(Schemas.FKID_DISTRIBUTOR, StringType(), False),
        StructField(Schemas.FKID_DSP, StringType(), False),
        StructField(Schemas.STREAM_DATE_DSP, DateType(), False),
        StructField(Schemas.STREAM_COUNTRY_DSP, StringType(), True),
        StructField(Schemas.TRACK_STORE_ID_DSP, StringType(), True),
        StructField(Schemas.CLIENT_AGE_BAND_DSP, StringType(), True),
        StructField(Schemas.CLIENT_GENDER_DSP, StringType(), True),
        StructField(Schemas.CLIENT_OFFER_DSP, StringType(), False),
        StructField(Schemas.DSP_OFFER_DSP, StringType(), True),
        StructField(Schemas.QUANTITY, LongType(), True),
        StructField(UNIQUE_LISTENER, LongType(), False),
        StructField(Categories.INGESTION_CATEGORY.error_col_name, BooleanType(), False),
        StructField(Categories.INGESTION_CATEGORY.error_message_name, ArrayType(StringType()), True),
        StructField(Categories.RAW_DATA_CATEGORY.error_col_name, BooleanType(), False),
        StructField(Categories.RAW_DATA_CATEGORY.error_message_name, ArrayType(StringType()), True),
        StructField(Schemas.ID_STRUCTURING, LongType(), False)
    ])

    # StructType for PIVOT
    PIVOT = StructType([
        StructField(PKID_DSP_DEMOGRAPHIC_RAW, StringType(), False),
        StructField(Schemas.FKID_DP_PRODUCT, LongType(), False),
        StructField(Schemas.ID_REPORT, LongType(), False),
        StructField(Schemas.FKID_DISTRIBUTOR, StringType(), False),
        StructField(Schemas.FKID_DSP, StringType(), False),
        StructField(Schemas.STREAM_DATE_DSP, DateType(), False),
        StructField(Schemas.FKID_STREAM_DATE, IntegerType(), False),
        StructField(Schemas.STREAM_DAY, IntegerType(), False),
        StructField(Schemas.STREAM_MONTH, IntegerType(), False),
        StructField(Schemas.STREAM_YEAR, IntegerType(), False),
        StructField(Schemas.STREAM_COUNTRY_DSP, StringType(), True),
        StructField(Schemas.FKID_STREAM_COUNTRY, IntegerType(), True),
        StructField(Schemas.TRACK_STORE_ID_DSP, StringType(), True),
        StructField(Schemas.CLIENT_AGE_BAND_DSP, StringType(), True),
        StructField(Schemas.FKID_CLIENT_AGE_BAND, IntegerType(), True),
        StructField(Schemas.CLIENT_GENDER_DSP, StringType(), True),
        StructField(Schemas.FKID_CLIENT_GENDER, IntegerType(), True),
        StructField(Schemas.CLIENT_OFFER_DSP, StringType(), True),
        StructField(Schemas.FKID_CLIENT_OFFER, IntegerType(), True),
        StructField(Schemas.DSP_OFFER_DSP, StringType(), True),
        StructField(Schemas.FKID_DSP_OFFER, IntegerType(), True),
        StructField(Schemas.QUANTITY, LongType(), False),
        StructField(UNIQUE_LISTENER, LongType(), False),
        StructField(Schemas.INSERT_TIMESTAMP, TimestampType(), True),
        StructField(Schemas.BLV_ERROR_CATEGORY, StringType(), True),
        StructField(Schemas.BLV_MESSAGES, ArrayType(StringType()), True),
        StructField(Schemas.BLV_ERROR, BooleanType(), True),
        StructField(Schemas.ID_STRUCTURING, LongType(), False),
        StructField(Schemas.RETRY_NUMBER, LongType(), False)
    ])
