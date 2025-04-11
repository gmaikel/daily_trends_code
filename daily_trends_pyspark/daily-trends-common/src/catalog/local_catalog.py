from src.catalog.db_common_datas import DBCommonDatas
from src.catalog.db_data_enrichment import DBDataEnrichment
from src.catalog.db_product_believe import DBProductBelieve
from src.catalog.db_dictionnaries import DBDictionnaries
from src.catalog.db_dimensions import DBDimensions
from src.cli.job_config import JobConfig
from src.cli.spark_session_wrapper import SparkSessionWrapper
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalCatalog:
    @staticmethod
    def register_databases(config: JobConfig):
        logger.info("Register Local Database")
        path = config.local_catalog
        spark = SparkSessionWrapper.get_spark_session()
        catalog = spark.catalog

        if not path:
            raise ValueError("In Local Mode you should set --local-catalog")

        spark.sql(f"create database if not exists {DBDimensions.DB_DIMENSION}")
        catalog.setCurrentDatabase(DBDimensions.DB_DIMENSION)

        LocalCatalog.register_table(DBDimensions.T_DATE_DIMENSION, f"{path}/data_repository/technical/date_time_dimensions/t_date_dimension.parquet", False)
        LocalCatalog.register_table(DBDimensions.T_TIME_DIMENSION, f"{path}/data_repository/technical/date_time_dimensions/t_time_dimension.parquet", False)

        spark.sql(f"create database if not exists {DBDictionnaries.DB_DAILY_TRENDS}")
        catalog.setCurrentDatabase(DBDictionnaries.DB_DAILY_TRENDS)

        LocalCatalog.register_table(DBDictionnaries.T_ACTION_TYPE_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_ACTION_TYPE_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_AGE_BAND_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_AGE_BAND_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_CACHED_PLAY_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_CACHED_PLAY_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_DEVICE_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_DEVICE_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_GENDER_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_GENDER_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_OFFER_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_OFFER_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_OS_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_OS_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_ISO_COUNTRY_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_ISO_COUNTRY_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_ISO_CURRENCY_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_ISO_CURRENCY_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_MEDIA_TYPE_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_MEDIA_TYPE_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_RETAILER_OFFER_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_RETAILER_OFFER_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CLIENT_PARTNER_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CLIENT_PARTNER_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_STREAM_REVENUE, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_STREAM_REVENUE}")
        LocalCatalog.register_table(DBDictionnaries.T_ORIGIN_SOURCE_STREAM_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_ORIGIN_SOURCE_STREAM_MAPPING}")
        LocalCatalog.register_table(DBDictionnaries.T_CONTENT_SUBTYPE_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_CONTENT_SUBTYPE_MAPPING}/")
        LocalCatalog.register_table(DBDictionnaries.T_AUDIO_FORMAT_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_AUDIO_FORMAT_MAPPING}/")
        LocalCatalog.register_table(DBDictionnaries.T_MUSIC_CONTAINER_TYPE_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_MUSIC_CONTAINER_TYPE_MAPPING}/")
        LocalCatalog.register_table(DBDictionnaries.T_MUSIC_CONTAINER_SUBTYPE_NEW_MAPPING, f"{path}/data_repository/technical/daily_trends/{DBDictionnaries.T_MUSIC_CONTAINER_SUBTYPE_NEW_MAPPING}/")

        spark.sql(f"create database if not exists {DBDataEnrichment.DB_DATA_ENRICHMENT}")
        catalog.setCurrentDatabase(DBDataEnrichment.DB_DATA_ENRICHMENT)
        LocalCatalog.register_table(DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE, f"{path}/data_repository/data_enrichment/product_identification_believe.parquet", partitioned=False)
        LocalCatalog.register_table(DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE, f"{path}/data_repository/data_enrichment/product_identification_tunecore.parquet", partitioned=False)

        spark.sql(f"create database if not exists {DBCommonDatas.DB_COMMON_DATAS}")
        catalog.setCurrentDatabase(DBCommonDatas.DB_COMMON_DATAS)
        LocalCatalog.register_table(DBCommonDatas.T_EXCHANGE_AVERAGE, f"{path}/data_repository/technical/commonDatas/{DBCommonDatas.T_EXCHANGE_AVERAGE}/")

        spark.sql(f"create database if not exists {DBProductBelieve.DB_PRODUCT_BELIEVE}")
        catalog.setCurrentDatabase(DBProductBelieve.DB_PRODUCT_BELIEVE)
        LocalCatalog.register_table(DBProductBelieve.T_YOUTUBE_DELIVERIES, f"{path}/data_repository/product/believe/tYoutubeDeliveryId/")

        # ERROR : data doesn't exist in AWS S3
        """
        spark.sql(f"create database if not exists {DB_RIGHT_MGMT}")
        catalog.setCurrentDatabase(DB_RIGHT_MGMT)
        LocalCatalog.register_table(T_YOUTUBE_ASSETS, f"{path}/data_repository/youtube_referential/asset.delta", partitioned=False, source="delta")
        LocalCatalog.register_table(T_YOUTUBE_VIDEOS, f"{path}/data_repository/youtube_referential/video.delta", partitioned=False, source="delta")
        """

        catalog.setCurrentDatabase(DBDimensions.DB_DIMENSION)

    @staticmethod
    def register_table(table_name: str, path: str, partitioned: bool = True, source: str = "parquet"):
        spark = SparkSessionWrapper.get_spark_session()

        logger.info(f"Creating table if not exists {table_name}")

        if not spark.catalog.tableExists(table_name):
            spark.catalog.createTable(table_name, path, source)

            if partitioned:
                spark.catalog.recoverPartitions(table_name)
