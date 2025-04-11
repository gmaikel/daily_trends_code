from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import LongType, IntegerType, StringType, TimestampType, BooleanType

from src.constants.fkid_constant import FKIDConstant
from src.schemas.scehmas import Schemas


class PivotTransformations:

    @staticmethod
    def add_audit_columns(df: DataFrame) -> DataFrame:
        return df.withColumn(Schemas.INSERT_TIMESTAMP, current_timestamp()) \
            .withColumn(Schemas.UPDATE_TIMESTAMP, lit(None).cast(TimestampType())) \
            .withColumn(Schemas.IS_DELETED, lit(False).cast(BooleanType()))

    @staticmethod
    def add_partitions(df: DataFrame) -> DataFrame:
        return df.withColumn(Schemas.STREAM_YEAR, year(col(Schemas.STREAM_DATE_DSP))) \
            .withColumn(Schemas.STREAM_MONTH, month(col(Schemas.STREAM_DATE_DSP))) \
            .withColumn(Schemas.STREAM_DAY, dayofmonth(col(Schemas.STREAM_DATE_DSP)))

    @staticmethod
    def add_social_event_partitions(df: DataFrame) -> DataFrame:
        return df.withColumn(Schemas.SOCIAL_EVENT_YEAR, year(col(Schemas.SOCIAL_EVENT_DATE_DSP))) \
            .withColumn(Schemas.SOCIAL_EVENT_MONTH, month(col(Schemas.SOCIAL_EVENT_DATE_DSP))) \
            .withColumn(Schemas.SOCIAL_EVENT_DAY, dayofmonth(col(Schemas.SOCIAL_EVENT_DATE_DSP)))

    @staticmethod
    def map_fkid_to_not_available(df: DataFrame, schema=None) -> DataFrame:
        if schema is None:
            schema = Schemas.PIVOT

        field_names = PivotTransformations.extract_dictionnary_fields(
            [f.name for f in Schemas.PIVOT.fields],
            exclude=[Schemas.FKID_MUSIC_CONTAINER_ID, Schemas.MUSIC_CONTAINER_ID_DSP]
        )

        fkids = ["FKID_" + name for name in field_names] + [
            Schemas.FKID_CLIENT_ACTION_DATE, Schemas.FKID_CLIENT_ACTION_TIME
        ]

        present_fkids = [f for f in fkids if f in df.columns]
        missing_fkids = [f for f in fkids if f not in df.columns]

        for field in schema.fields:
            if field.name in missing_fkids:
                df = PivotTransformations.not_available(field, df)

        return df

    @staticmethod
    def undefined(field, df: DataFrame) -> DataFrame:
        return df.withColumn(
            field.name,
            when(col(field.name).isNull(), PivotTransformations._value(field, FKIDConstant.UNDEFINED))
            .otherwise(col(field.name))
        )

    @staticmethod
    def not_available(field, df: DataFrame) -> DataFrame:
        return df.withColumn(field.name, PivotTransformations._value(field, FKIDConstant.NOT_AVAILABLE))

    @staticmethod
    def _value(field, value):
        if isinstance(field.dataType, LongType):
            return lit(int(value)).cast(LongType())
        elif isinstance(field.dataType, IntegerType):
            return lit(int(value))
        elif isinstance(field.dataType, StringType):
            return lit(str(value))
        else:
            raise ValueError(f"Unexpected type {field.dataType} for field {field.name}")

    @staticmethod
    def extract_dictionnary_fields(field_names, exclude=None):
        if exclude is None:
            exclude = []

        filtered = [f for f in field_names if f not in exclude]
        extracted = []

        for name in filtered:
            if name.startswith("FKID_"):
                extracted.append(name.replace("FKID_", ""))
            elif name.endswith("_DSP"):
                extracted.append(name.replace("_DSP", ""))

        return [name for name in set(extracted) if extracted.count(name) == 2]
