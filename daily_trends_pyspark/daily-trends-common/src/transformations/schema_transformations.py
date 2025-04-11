from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType
import logging
from src.schemas.schemas_tools import SchemasTools


class SchemaTransformations:
    """
    Class to handle schema transformations such as adding missing columns, selecting specific columns,
    and enforcing column consistency.
    """

    @staticmethod
    def enforce_columns(schema: StructType, df: DataFrame) -> DataFrame:
        """
        Enforce Columns:
        - Add missing columns with null value
        - Select only columns present in the schema
        - Validate dataframe schema

        :param schema: The expected schema to validate against
        :param df: The DataFrame to transform
        :return: Transformed DataFrame
        """
        normalized_df = df.transform(lambda df: SchemaTransformations.add_missing_columns(schema, df)) \
            .transform(lambda df: SchemaTransformations.select_columns(schema, df))

        SchemasTools.validate_schema(normalized_df, schema)

        return normalized_df

    @staticmethod
    def select_columns(schema: StructType, df: DataFrame) -> DataFrame:
        """
        Select only columns present in the schema from the DataFrame.

        :param schema: The schema defining the expected columns
        :param df: The DataFrame to select columns from
        :return: DataFrame with only selected columns
        """
        columns = [col(field) for field in schema.fieldNames()]
        return df.select(*columns)

    @staticmethod
    def add_missing_columns(schema: StructType, df: DataFrame) -> DataFrame:
        """
        Add missing columns to the DataFrame based on the expected schema, with null values.

        :param schema: The schema that defines the expected columns
        :param df: The DataFrame to add missing columns to
        :return: DataFrame with missing columns added
        """
        missing_columns = list(set(schema.fieldNames()) - set(df.columns))
        final_df = df

        if missing_columns:
            logging.info(f"Adding missing columns in Pivot: {missing_columns}")
            for name in missing_columns:
                field = schema[name]
                final_df = final_df.withColumn(name, lit(None).cast(field.dataType))

        return final_df
