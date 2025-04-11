from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, BooleanType, DateType, ArrayType

class ProductReferenceSchema:
    PKID_DP_PRODUCT = "PKID_DP_PRODUCT"
    START_DATE_VALIDITY = "START_DATE_VALIDITY"
    END_DATE_VALIDITY = "END_DATE_VALIDITY"
    IS_LAST_FLAG = "IS_LAST_FLAG"
    IS_DELETED = "IS_DELETED"
    ID_TRACK = "ID_TRACK"
    ID_ALBUM = "ID_ALBUM"
    ID_PRODUCER = "ID_PRODUCER"
    ISRC = "ISRC"
    UPC = "UPC"
    DURATION = "DURATION"
    REF_TYPE = "REF_TYPE"
    CONTENT_TYPE = "CONTENT_TYPE"
    STORE_ID_APPLE = "STORE_ID_APPLE"
    STORE_ID_SPOTIFY_TC = "STORE_ID_SPOTIFY_TC"
    STORE_ID_SPOTIFY = "STORE_ID_SPOTIFY"
    STORE_ID_DEEZER_TC = "STORE_ID_DEEZER_TC"
    STORE_ID_DEEZER = "STORE_ID_DEEZER"
    STORE_ID_APPLE_TC = "STORE_ID_APPLE_TC"
    STORE_ID_YOUTUBE = "STORE_ID_YOUTUBE"

    ALIAS_STORE_ID = "ALIAS_STORE_ID"

    COMPILATION_BELIEVE = "COMPILATION_BELIEVE"
    COMPILATION_TRACK_LIST = "COMPILATION_TRACK_LIST"

    # TUNECORE SPECIFIC
    IS_TUNECORE = "IS_TUNECORE"
    ISRC_SPOTIFY = "ISRC_SPOTIFY"
    ISRC_APPLE = "ISRC_APPLE"
    ISRC_DEEZER = "ISRC_DEEZER"
    ISRC_NAPSTER = "ISRC_NAPSTER"
    UPC_SPOTIFY = "UPC_SPOTIFY"
    UPC_APPLE = "UPC_APPLE"
    UPC_DEEZER = "UPC_DEEZER"
    UPC_NAPSTER = "UPC_NAPSTER"

    # YOUTUBE SPECIFIC
    ID_SONG = "idSong"
    ID_YOUTUBE = "idYoutube"
    DELIVERY_TYPE = "type"
    CUSTOM_ID = "CUSTOM_ID"
    VIDEO_DELIVERY_TYPE = "video"
    ASSET_DELIVERY_TYPE = "asset"

    PRODUCT_REFERENCE = StructType([
        StructField(PKID_DP_PRODUCT, LongType(), True),
        StructField(START_DATE_VALIDITY, DateType(), True),
        StructField(END_DATE_VALIDITY, DateType(), True),
        StructField(IS_LAST_FLAG, BooleanType(), True),
        StructField(IS_DELETED, BooleanType(), True),
        StructField(ID_TRACK, IntegerType(), True),
        StructField(ID_ALBUM, IntegerType(), True),
        StructField(ID_PRODUCER, LongType(), True),
        StructField(ISRC, StringType(), True),
        StructField(UPC, StringType(), True),
        StructField(DURATION, IntegerType(), True),
        StructField(REF_TYPE, StringType(), True),
        StructField(CONTENT_TYPE, StringType(), True),
        StructField(STORE_ID_APPLE, StringType(), True),
        StructField(STORE_ID_SPOTIFY_TC, StringType(), True),
        StructField(STORE_ID_SPOTIFY, StringType(), True),
        StructField(STORE_ID_DEEZER_TC, StringType(), True),
        StructField(STORE_ID_DEEZER, StringType(), True),
        StructField(STORE_ID_APPLE_TC, StringType(), True),
        StructField(STORE_ID_YOUTUBE, StringType(), True),
        StructField(COMPILATION_BELIEVE, BooleanType(), True),
        StructField(COMPILATION_TRACK_LIST, ArrayType(LongType()), True),
    ])
