import re
from pyspark.sql.types import StructType
from src.exceptions.validation_schemas_exception import ValidationSchemaException, ValidationSchemaExceptionUtil
from src.schemas.scehmas import Schemas
from pyspark.sql import DataFrame

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
    def extract_dictionaries_fields(schema: StructType, filter_out: list = []) -> list:
        """
        Extracts all the dictionary field names from a schema.
        A field is considered a dictionary field when the schema has two columns:
        FKID_<dictionary_field> and <dictionary_field>_DSP.
        :param schema: The schema to extract dictionary fields from
        :param filter_out: A list of column names to exclude from the extraction
        :return: A list of dictionary field names
        """
        fkid_regex = "(FKID_)(.*)"
        dsp_regex = "(.*)(_DSP)"

        dict_fields = []

        for name in schema.fieldNames:
            if name not in filter_out:
                match_fkid = re.match(fkid_regex, name)
                match_dsp = re.match(dsp_regex, name)

                if match_fkid:
                    dict_fields.append(match_fkid.group(2))
                elif match_dsp:
                    dict_fields.append(match_dsp.group(1))

        return dict_fields
