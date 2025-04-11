from pyspark.sql.types import DataType
from typing import List, Optional

class ProductIdCol:
    def __init__(self, source_name: str, destination_name: str, data_type: DataType):
        self.source_name = source_name
        self.destination_name = destination_name
        self.data_type = data_type

class ProductIdColumns:
    def __init__(self, main_product_key: ProductIdCol, extra_product_keys: Optional[List[ProductIdCol]] = None):
        if extra_product_keys is None:
            extra_product_keys = []
        self.main_product_key = main_product_key
        self.extra_product_keys = extra_product_keys
