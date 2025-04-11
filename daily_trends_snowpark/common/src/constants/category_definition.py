class CategoryDefinition:
    def __init__(self, category_name: str):
        self.category_name = category_name

    @property
    def error_col_name(self) -> str:
        return f"BLV_ERROR_{self.category_name}"

    @property
    def error_message_name(self) -> str:
        return f"BLV_MESSAGES_{self.category_name}"


class Categories:
    RAW_DATA_CATEGORY = CategoryDefinition("MISSING_RAW_DATA")
    INGESTION_CATEGORY = CategoryDefinition("INGESTION_ERROR_RUNTIME")
    REFERENTIAL_CATEGORY = CategoryDefinition("MISSING_REFERENTIAL")
    DICTIONARY_CATEGORY = CategoryDefinition("MISSING_DICTIONARY")
    UNDEFINED_CATEGORY = CategoryDefinition("UNDEFINED_REJECTION_REASON")

    ordered = [
        RAW_DATA_CATEGORY,
        INGESTION_CATEGORY,
        REFERENTIAL_CATEGORY,
        DICTIONARY_CATEGORY,
        UNDEFINED_CATEGORY
    ]
