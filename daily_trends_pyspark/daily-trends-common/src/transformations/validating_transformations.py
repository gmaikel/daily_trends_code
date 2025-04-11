from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType
from src.schemas.scehmas import Schemas
from src.schemas.schemas_tools import SchemasTools
from src.constants.category_definition import Categories
from pyspark.sql.functions import when, col, lit, concat


class ValidatingTransformations:

    @staticmethod
    def classify_error(columnsErrors: list, categoryDefinition, df):
        """
        Classify generic errors and add the error messages and corresponding category.
        """
        if not columnsErrors:
            return df.withColumn(
                categoryDefinition.error_message_name,
                F.array().cast(ArrayType(StringType()))
            ).withColumn(
                categoryDefinition.error_col_name,
                F.lit(False)
            )
        else:
            return df.withColumn(
                categoryDefinition.error_message_name,
                F.array(*columnsErrors)
            ).withColumn(
                categoryDefinition.error_message_name,
                F.expr(f"FILTER({categoryDefinition.error_message_name}, x -> x IS NOT NULL)")
            ).withColumn(
                categoryDefinition.error_col_name,
                F.size(F.col(categoryDefinition.error_message_name)) > 0
            )

    @staticmethod
    def check_mandatory_fields(column_names):
        """
        Check all non-nullable fields and return error messages for any null values.
        """
        mandatory_messages = [
            when(col(column_name).isNull(), lit(f"Column {column_name} is null"))
            .otherwise(None)
            .cast(StringType())
            .alias(f"{Schemas.BLV_MESSAGES}_{column_name}")
            for column_name in column_names
        ]

        return mandatory_messages

    @staticmethod
    def check_mandatory_fields_from_list(columnNames: list):
        """
        Check that all columns are not null.
        """
        mandatoryMessages = [
            F.when(F.col(columnName).isNull(), F.lit(f"Column {columnName} is null"))
            .otherwise(None).cast(StringType()).alias(f"{Schemas.BLV_MESSAGES}_{columnName}")
            for columnName in columnNames
        ]
        return mandatoryMessages

    @staticmethod
    def check_unmapped_dictionnary_fields(schema: StructType):
        """
        Check for unmapped values in dictionary fields.
        """
        dictionnaryMessages = [
            F.when(F.col(f"FKID_{name}").isNull(),
                   F.concat(
                       F.lit(f"No mapping found for ({name}_DSP, FKID_{name}), unknown value: '"),
                       F.when(F.col(f"{name}_DSP").isNull(), F.lit("null")).otherwise(F.col(f"{name}_DSP")),
                       F.lit("'"))
                   ).otherwise(None).cast(StringType()).alias(f"{Schemas.BLV_MESSAGES}_{name}")
            for name in SchemasTools.extract_dictionaries_fields(schema, filter_out=[Schemas.FKID_MUSIC_CONTAINER_ID, Schemas.MUSIC_CONTAINER_ID_DSP])
        ]
        return dictionnaryMessages

    @staticmethod
    def check_quantity_values(quantity_col):
        """
        Check for quantity values less than or equal to zero.
        """
        negative_or_zero_quantity  = F.when(quantity_col.isNull(), None).otherwise(
            F.when(quantity_col <= 0, F.lit(f"{quantity_col} shouldn't be less or equal to zero")).otherwise(None)
        )
        return [negative_or_zero_quantity .alias(f"{Schemas.BLV_MESSAGES}_Invalid_quantity")]

    @staticmethod
    def get_category():
        """
        Checks categories in order of priority to find the first not null category.
        """
        mapped_to_null = [
            when(col(cat.error_col_name) == True, lit(cat.category_name)).otherwise(lit(None))
            for cat in Categories.ordered
        ]
        return F.coalesce(*mapped_to_null)

    @staticmethod
    def get_messages():
        """
        Checks messages in order of priority for categories.
        """
        mapped_to_null = [
            when(col(cat.error_col_name) == True, col(cat.error_message_name)).otherwise(lit(None))
            for cat in Categories.ordered
        ]
        return F.coalesce(*mapped_to_null)

    @staticmethod
    def resolve_categories(df):
        """
        Resolve categories and add the necessary columns for errors and messages.
        """
        return df.withColumn(Schemas.BLV_ERROR, ValidatingTransformations.get_blv_error()) \
            .withColumn(Schemas.BLV_ERROR_CATEGORY, ValidatingTransformations.get_category()) \
            .withColumn(Schemas.BLV_MESSAGES, ValidatingTransformations.get_messages())

    @staticmethod
    def get_blv_error():
        """
        Check if any rejection exists, indicating an error.
        """
        blv_error = lit(False)
        for category in Categories.ordered:
            blv_error = (blv_error == True) | (col(category.error_col_name) == True)
        return blv_error

    @staticmethod
    def classify_rejects_structuring(rawData: list, df):
        """
        Handles checks for the structuring phase based on category priorities.
        """
        return (
            df.transform(lambda df: ValidatingTransformations.classify_error(rawData, Categories.RAW_DATA_CATEGORY, df))
            .transform(lambda df: ValidatingTransformations.classify_error(
                ValidatingTransformations.check_mandatory_fields(Schemas.INGESTION_MANDATORY_COLUMNS),
                Categories.INGESTION_CATEGORY,
                df
            ))
            # .transform(lambda df: MetricsTransformation.observe_rejects_structuring(
            #     Categories.RAW_DATA_CATEGORY, Categories.INGESTION_CATEGORY
            # ))
        )

    @staticmethod
    def check_unmapped_dictionary_fields(schema: StructType):
        dictionary_fields = SchemasTools.extract_dictionaries_fields(
            schema,
            filter_out=["FKID_MUSIC_CONTAINER_ID", "MUSIC_CONTAINER_ID_DSP"]
        )

        dictionary_messages = [
            when(
                col(f"FKID_{name}").isNull(),
                concat(
                    lit(f"No mapping found for ({name}_DSP, FKID_{name}), unknown value : '"),
                    when(col(f"{name}_DSP").isNull(), lit("null")).otherwise(col(f"{name}_DSP")),
                    lit("'")
                )
            )
            .otherwise(None)
            .cast(StringType())
            .alias(f"{Schemas.BLV_MESSAGES}_{name}")
            for name in dictionary_fields
        ]

        return dictionary_messages

    @staticmethod
    def classify_rejects_enriching(df, pivot_schema: StructType, product_column: str = Schemas.FKID_DP_PRODUCT):
        undefined = ValidatingTransformations.check_mandatory_fields(
            [f.name for f in pivot_schema.fields if not f.nullable]
        )

        if product_column in [f.name for f in pivot_schema.fields]:
            referential_error = ValidatingTransformations.check_mandatory_fields([product_column])
        else:
            referential_error = []

        return (
            df.transform(lambda df : ValidatingTransformations.classify_error(
                columnsErrors=referential_error,
                categoryDefinition=Categories.REFERENTIAL_CATEGORY,
                df=df
            )).transform(lambda df : ValidatingTransformations.classify_error(
                categoryDefinition=Categories.DICTIONARY_CATEGORY,
                columnsErrors=ValidatingTransformations.check_unmapped_dictionary_fields(schema=pivot_schema),
                df=df
            )).transform(lambda df : ValidatingTransformations.classify_error(
                columnsErrors=undefined,
                categoryDefinition=Categories.UNDEFINED_CATEGORY,
                df=df
            ))
        )

