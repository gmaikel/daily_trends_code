from common.src.catalog.db_common_datas import DBCommonDatas
from common.src.catalog.db_data_enrichment import DBDataEnrichment
from common.src.catalog.db_product_believe import DBProductBelieve
from common.src.catalog.db_external_data import DBExternalData
from common.src.catalog.db_dictionnaries import DBDictionnaries
from common.src.catalog.db_dimensions import DBDimensions
from common.src.cli.job_config import JobConfig
import logging
from conn_snowpark import SessionManager
import pandas as pd
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalCatalog:
    @staticmethod
    def register_dataframe(session, db: str, table_name: str, path: str):
        if path.endswith('.parquet'):
            df_pandas = pd.read_parquet(path)
        else:
            parquet_files = get_parquet_files(path=path)
            df_list = [pd.read_parquet(file) for file in parquet_files]
            df_pandas = pd.concat(df_list, ignore_index=True)

        df_snowpark = session.create_dataframe(df_pandas)

        table_name_with_prefix = f"{db}_{table_name}"
        logger.info(f"Creating temporary table {table_name_with_prefix}")

        df_snowpark.write.save_as_table(table_name_with_prefix, mode="overwrite", table_type="temporary")

    @staticmethod
    def register_dataframes(config: JobConfig):
        logger.info("Register Temporary Tables")
        session = SessionManager.get_session()
        path = config.local_catalog

        if not path:
            raise ValueError("In Local Mode you should set --local-catalog")

        # Register temporary tables
        LocalCatalog.register_dataframe(
            session=session,
            db=DBDimensions.DB_DIMENSION,
            table_name=DBDimensions.T_DATE_DIMENSION,
            path= f"{path}/data_repository/technical/date_time_dimensions/t_date_dimension.parquet")
        LocalCatalog.register_dataframe(
            session=session,
            db=DBDimensions.DB_DIMENSION,
            table_name=DBDimensions.T_TIME_DIMENSION,
            path=f"{path}/data_repository/technical/date_time_dimensions/t_time_dimension.parquet")

        tables = [
            DBDictionnaries.T_ACTION_TYPE_MAPPING,
            DBDictionnaries.T_CLIENT_AGE_BAND_MAPPING,
            DBDictionnaries.T_CLIENT_CACHED_PLAY_MAPPING,
            DBDictionnaries.T_CLIENT_DEVICE_MAPPING,
            DBDictionnaries.T_CLIENT_GENDER_MAPPING,
            DBDictionnaries.T_CLIENT_OFFER_MAPPING,
            DBDictionnaries.T_CLIENT_OS_MAPPING,
            DBDictionnaries.T_ISO_COUNTRY_MAPPING,
            DBDictionnaries.T_ISO_CURRENCY_MAPPING,
            DBDictionnaries.T_MEDIA_TYPE_MAPPING,
            DBDictionnaries.T_RETAILER_OFFER_MAPPING,
            DBDictionnaries.T_CLIENT_PARTNER_MAPPING,
            DBDictionnaries.T_STREAM_REVENUE,
            DBDictionnaries.T_ORIGIN_SOURCE_STREAM_MAPPING,
            DBDictionnaries.T_CONTENT_SUBTYPE_MAPPING,
            DBDictionnaries.T_AUDIO_FORMAT_MAPPING,
            DBDictionnaries.T_MUSIC_CONTAINER_TYPE_MAPPING,
            DBDictionnaries.T_MUSIC_CONTAINER_SUBTYPE_NEW_MAPPING,
        ]

        for table in tables:
            LocalCatalog.register_dataframe(
                session=session,
                db=DBDictionnaries.DB_DAILY_TRENDS,
                table_name=table,
                path=f"{path}/data_repository/technical/daily_trends/{table}")

        LocalCatalog.register_dataframe(
            session=session,
            db=DBDataEnrichment.DB_DATA_ENRICHMENT,
            table_name=DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE,
            path=f"{path}/data_repository/data_enrichment/product_identification_believe.parquet")
        LocalCatalog.register_dataframe(
            session=session,
            db=DBDataEnrichment.DB_DATA_ENRICHMENT,
            table_name=DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE,
            path=f"{path}/data_repository/data_enrichment/product_identification_tunecore.parquet")

        LocalCatalog.register_dataframe(
            session=session,
            db=DBCommonDatas.DB_COMMON_DATAS,
            table_name=DBCommonDatas.T_EXCHANGE_AVERAGE,
            path=f"{path}/data_repository/technical/commonDatas/{DBCommonDatas.T_EXCHANGE_AVERAGE}")

        LocalCatalog.register_dataframe(
            session=session,
            db=DBProductBelieve.DB_PRODUCT_BELIEVE,
            table_name=DBProductBelieve.T_YOUTUBE_DELIVERIES,
            path=f"{path}/data_repository/product/believe/tYoutubeDeliveryId/")




def get_parquet_files(path):
    parquet_files = []
    for root, dirs, files in os.walk(path):  
        for file in files:
            if file.endswith(".parquet"):  
                parquet_files.append(os.path.join(root, file))  
    return parquet_files