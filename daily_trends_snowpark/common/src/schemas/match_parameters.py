from src.catalog.db_data_enrichment import DBDataEnrichment
from src.schemas.product_reference_schema import ProductReferenceSchema

class MatchParameters:
    def __init__(self, product_referential: str):
        self.product_referential = product_referential

class IsrcUpcMatchParameters(MatchParameters):
    def __init__(self, isrc_col_name: str, upc_col_name: str, product_referential: str):
        super().__init__(product_referential)
        self.isrc_col_name = isrc_col_name
        self.upc_col_name = upc_col_name

class BaseStoreIdMatchParameters(MatchParameters):
    def __init__(self, product_referential: str, store_id_col: str):
        super().__init__(product_referential)
        self.store_id_col = store_id_col

class DistributorMatchParameters(IsrcUpcMatchParameters):
    pass

class DistributorMatchParametersIsTuneCore(IsrcUpcMatchParameters):
    def __init__(self):
        super().__init__(ProductReferenceSchema.ISRC, ProductReferenceSchema.UPC, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE)

class StoreIdMatchParameters(BaseStoreIdMatchParameters):
    pass

class SimpleMatchParameters(MatchParameters):
    pass