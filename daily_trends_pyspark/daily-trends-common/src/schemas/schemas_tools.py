import re
from pyspark.sql.types import StructType
from src.exceptions.validation_schemas_exception import ValidationSchemaException, ValidationSchemaExceptionUtil
from src.schemas.scehmas import Schemas
from pyspark.sql import DataFrame
from collections import Counter

class SchemasTools:
    """
    A utility class for schema validation and extracting dictionary fields.
    """

    @staticmethod
    def validate_schema(df: DataFrame, schema: StructType) -> None:
        """
        Validate schema (same column name, same type)
        :param df: DataFrame to validate
        :param schema: Expected schema to validate against
        :raise ValidationSchemaException: If the schema is invalid
        """
        names_diff = list(set(schema.fieldNames()) - set(df.schema.fieldNames()))

        if names_diff:
            message = ValidationSchemaExceptionUtil.buildMessage(
                f"Unknown column name {names_diff}", df.schema, Schemas.PIVOT
            )
            raise ValidationSchemaException(message)

        schema_diff = [
            field for field in schema if field.dataType != df.schema[field.name].dataType
        ]

        if schema_diff:
            errors = "Wrong datatype for:\n"
            errors += "\n".join(
                [f"- field '{field.name}' expected {field.dataType} (actual : {df.schema[field.name].dataType})" for field in schema_diff]
            )
            message = ValidationSchemaExceptionUtil.buildMessage(errors, df.schema, schema)
            raise ValidationSchemaException(message)

    @staticmethod
    def extract_dictionaries_fields(schema: StructType, filter_out=None):
        if filter_out is None:
            filter_out = []

        fkid_regex = re.compile(r"(FKID_)(.*)")
        dsp_regex = re.compile(r"(.*)(_DSP)")

        names = [
            match.group(2) if (match := fkid_regex.match(field)) else
            match.group(1) if (match := dsp_regex.match(field)) else
            None
            for field in schema.fieldNames()
            if field not in filter_out
        ]

        filtered_names = list(filter(None, names))

        name_counts = Counter(filtered_names)
        result = [name for name, count in name_counts.items() if count == 2]

        return result
