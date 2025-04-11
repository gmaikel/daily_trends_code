from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from src.catalog.db_dictionnaries import DBDictionnaries
from src.catalog.db_dimensions import DBDimensions
from src.exceptions.duplicate_dictionnary_entry_exception import DuplicateDictionaryEntryException
from src.schemas.scehmas import Schemas
from src.schemas.audit_status import AuditStatus
import logging
from pyspark.sql.functions import col, lit, lower, trim

logger = logging.getLogger(__name__)
from src.cli.spark_session_wrapper import SparkSessionWrapper
spark_wrapper = SparkSessionWrapper
spark = spark_wrapper.get_spark_session()

class MappingTransformations:

    REGEX_PARTITION = "year=\\d{4}/month=\\d{2}/day=\\d{2}"
    DATE_DIFF = "date_diff"
    ROW_NUMBER = "row_number"

    @staticmethod
    def map_dictionnary(dsp_code: str,
                        table_name: str,
                        column_id: str,
                        column_dsp: str,
                        column_fkid: str,
                        contains_comparison: bool = False,
                        value: str = DBDictionnaries.VALUE,
                        fallback_value: str = None, df=None):
        spark.catalog.setCurrentDatabase(DBDictionnaries.DB_DAILY_TRENDS)

        dictionnary_df = spark.table(table_name).where(
            (col(DBDictionnaries.DSP_CODE) == lit(dsp_code)) &
            (col(DBDictionnaries.AUDIT_STATUS) == AuditStatus.SET)
        )

        if fallback_value:
            dictionnary = dictionnary_df.select(
                col(column_id).alias(column_fkid),
                lower(trim(col(value))).alias(value),
                lower(trim(col(fallback_value))).alias(DBDictionnaries.FAMILY_VALUE)
            )
        else:
            dictionnary = dictionnary_df.select(
                col(column_id).alias(column_fkid),
                lower(trim(col(value))).alias(value),
                lit(None).alias(DBDictionnaries.FAMILY_VALUE)
            )

        MappingTransformations.check_unicity(dictionnary, table_name, value)

        join_condition = (
            lower(trim(col(column_dsp))).contains(dictionnary[value])
            if contains_comparison else
            lower(trim(col(column_dsp))) == dictionnary[value]
        )

        joined_df = df.join(
            dictionnary.drop(DBDictionnaries.FAMILY_VALUE).hint("broadcast"),
            join_condition,
            "left_outer"
        )

        if fallback_value == DBDictionnaries.FAMILY_VALUE:
            unmatched_df = joined_df.filter(col(column_fkid).isNull) \
                .drop(column_fkid) \
                .join(
                dictionnary.drop(value)
                .filter(col(DBDictionnaries.FAMILY_VALUE) != lit(""))
                .distinct()
                .hint("broadcast"),
                lower(trim(col(column_dsp))).contains(col(DBDictionnaries.FAMILY_VALUE)),
                "left_outer"
            )

            matched_df = joined_df.filter(col(column_fkid).isNotNull)

            final_df = matched_df.unionByName(unmatched_df, allowMissingColumns=True) \
                .drop(DBDictionnaries.FAMILY_VALUE) \
                .drop(value)
        else:
            final_df = joined_df.drop(value)

        return final_df

    @staticmethod
    def map_client_age_band(dsp_code: str, df=None):
        return MappingTransformations.map_dictionnary(
            dsp_code=dsp_code,
            table_name=DBDictionnaries.T_CLIENT_AGE_BAND_MAPPING,
            column_id=DBDictionnaries.REF_ID,
            column_dsp=Schemas.CLIENT_AGE_BAND_DSP,
            column_fkid=Schemas.FKID_CLIENT_AGE_BAND,
            df=df
        )

    @staticmethod
    def map_retailer_offer(dsp_code: str, df=None):
        if dsp_code == "amazon-stream":
            return MappingTransformations.map_dictionnary(
                dsp_code=dsp_code,
                table_name=DBDictionnaries.T_RETAILER_OFFER_MAPPING,
                column_id=DBDictionnaries.REF_ID,
                column_dsp=Schemas.DSP_OFFER_DSP,
                column_fkid=Schemas.FKID_DSP_OFFER,
                contains_comparison=False,
                value=DBDictionnaries.VALUE,
                fallback_value=DBDictionnaries.FAMILY_VALUE,
                df=df
            )
        else:
            return MappingTransformations.map_dictionnary(
                dsp_code=dsp_code,
                table_name=DBDictionnaries.T_RETAILER_OFFER_MAPPING,
                column_id=DBDictionnaries.REF_ID,
                column_dsp=Schemas.DSP_OFFER_DSP,
                column_fkid=Schemas.FKID_DSP_OFFER,
                df=df
            )

    @staticmethod
    def map_stream_date(df, col_input="STREAM_DATE_DSP", col_output="FKID_STREAM_DATE"):
        spark.catalog.setCurrentDatabase(DBDimensions.DB_DIMENSION)

        date_column = "stream_date"
        pkid_date_column = "pkid_stream_date"

        date_dimension = spark.table(DBDimensions.T_DATE_DIMENSION) \
            .select(
            F.col(DBDimensions.PKID_DATE).alias(pkid_date_column),
            F.col(DBDimensions.DATE).alias(date_column)
        ).alias("stream_table")

        MappingTransformations.check_unicity(date_dimension, DBDimensions.T_DATE_DIMENSION, date_column)

        df_transformed = df.join(
            date_dimension.hint("broadcast"),
            date_dimension[date_column] == df[col_input],
            "left_outer"
        ).withColumn(
            col_output, date_dimension[pkid_date_column].cast(IntegerType())
        ).drop(
            date_dimension[date_column], date_dimension[pkid_date_column]
        )

        return df_transformed

    @staticmethod
    def map_client_gender(dsp_code: str, df=None):
        return MappingTransformations.map_dictionnary(
            dsp_code=dsp_code,
            table_name=DBDictionnaries.T_CLIENT_GENDER_MAPPING,
            column_id=DBDictionnaries.REF_ID,
            column_dsp=Schemas.CLIENT_GENDER_DSP,
            column_fkid=Schemas.FKID_CLIENT_GENDER,
            df=df
        )

    @staticmethod
    def map_client_cached(dsp_code: str, df=None):
        return MappingTransformations.map_dictionnary(
            dsp_code, DBDictionnaries.T_CLIENT_CACHED_PLAY_MAPPING, DBDictionnaries.REF_ID,
            Schemas.CLIENT_CACHED_PLAY_DSP, Schemas.FKID_CLIENT_CACHED_PLAY, df=df
        )

    @staticmethod
    def map_client_device(dsp_code: str, contains_comparison: bool = False, df=None):
        return MappingTransformations.map_dictionnary(
            dsp_code, DBDictionnaries.T_CLIENT_DEVICE_MAPPING, DBDictionnaries.REF_ID,
            Schemas.CLIENT_DEVICE_DSP, Schemas.FKID_CLIENT_DEVICE, contains_comparison=contains_comparison, df=df
        )

    @staticmethod
    def map_stream_country(df, dsp_code: str, col_input: str = 'STREAM_COUNTRY_DSP', col_output: str='FKID_STREAM_COUNTRY'):
        return MappingTransformations.map_dictionnary(
            df=df,
            dsp_code=dsp_code,
            table_name=DBDictionnaries.T_ISO_COUNTRY_MAPPING,
            column_id=DBDictionnaries.REF_ID,
            column_dsp=col_input,
            column_fkid=col_output
        )

    @staticmethod
    def map_client_offer(dsp_code: str, contains_comparison: bool = False, df=None):
        if dsp_code == "amazon-stream":
            return MappingTransformations.map_dictionnary(
                dsp_code=dsp_code,
                table_name=DBDictionnaries.T_CLIENT_OFFER_MAPPING,
                column_id=DBDictionnaries.REF_ID,
                column_dsp=Schemas.CLIENT_OFFER_DSP,
                column_fkid=Schemas.FKID_CLIENT_OFFER,
                contains_comparison=False,
                value=DBDictionnaries.VALUE,
                fallback_value=DBDictionnaries.FAMILY_VALUE,
                df=df
            )
        else:
            return MappingTransformations.map_dictionnary(
                dsp_code=dsp_code,
                table_name=DBDictionnaries.T_CLIENT_OFFER_MAPPING,
                column_id=DBDictionnaries.REF_ID,
                column_dsp=Schemas.CLIENT_OFFER_DSP,
                column_fkid=Schemas.FKID_CLIENT_OFFER,
                contains_comparison=contains_comparison,
                df=df
            )

    @staticmethod
    def check_unicity(df=None, table_name: str=None, col1: str=None, *cols: str) -> None:
        df_check = df.groupBy(col1, *cols).count().where(F.col("count") > 1)
        if df_check.count() > 0:
            message = f"Multiple values on dictionnary {table_name} detected this will cause cartesian product. Stopping Job"
            logger.error(message)
            df_check.show(truncate=False)
            raise DuplicateDictionaryEntryException(message)

    @staticmethod
    def hash_id_client_identifier(df=None):
        return df.withColumn(Schemas.ID_CLIENT_IDENTIFIER, F.hash(F.col(Schemas.ID_CLIENT_IDENTIFIER)).cast(StringType()))

    @staticmethod
    def check_unicity(df, table_name: str, col1: str, *cols: str):
        group_by_columns = [col1] + list(cols)
        df_check = df.groupBy(*group_by_columns).count().filter(F.col("count") > 1)

        if df_check.count() > 0:
            message = f"Multiple values on dictionary {table_name} detected, this will cause a cartesian product. Stopping Job"
            print(message)
            df_check.show(truncate=False)
            raise DuplicateDictionaryEntryException(message)
