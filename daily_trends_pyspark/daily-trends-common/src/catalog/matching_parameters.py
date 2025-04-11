from src.catalog.db_data_enrichment import DBDataEnrichment
from src.schemas.distributor import Distributor
from src.schemas.dsp import Dsp
from src.schemas.product_reference_schema import ProductReferenceSchema
from src.schemas.product_reference_tuncore_schema import ProductReferenceTunecoreSchema
from src.schemas.match_parameters import DistributorMatchParameters, StoreIdMatchParameters

class MatchingParameters:
    def __init__(self, dsp_code: str, distributor_code: str):
        self.dsp_code = dsp_code
        self.distributor_code = distributor_code

    def get_distributor_match_parameters(self):
        match (self.distributor_code, self.dsp_code):
            case (Distributor.BELIEVE | Distributor.EMS, _):
                return DistributorMatchParameters(ProductReferenceSchema.ISRC, ProductReferenceSchema.UPC, DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE)
            case (Distributor.TUNECORE, Dsp.SPOTIFY):
                return DistributorMatchParameters(ProductReferenceSchema.ISRC_SPOTIFY, ProductReferenceSchema.UPC_SPOTIFY, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE)
            case (Distributor.TUNECORE, Dsp.DEEZER):
                return DistributorMatchParameters(ProductReferenceSchema.ISRC_DEEZER, ProductReferenceSchema.UPC_DEEZER, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE)
            case (Distributor.TUNECORE, Dsp.NAPSTER):
                return DistributorMatchParameters(ProductReferenceSchema.ISRC_NAPSTER, ProductReferenceSchema.UPC_NAPSTER, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE)
            case (Distributor.TUNECORE, Dsp.ITUNES | Dsp.APPLE_MUSIC):
                return DistributorMatchParameters(ProductReferenceSchema.ISRC_APPLE, ProductReferenceSchema.UPC_APPLE, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE)
            case _:
                raise ValueError(f"Distributor = {self.distributor_code} and DSP = {self.dsp_code} is not an accepted tuple")

    def get_store_id_match_parameters(self):
        match (self.distributor_code, self.dsp_code):
            case (Distributor.BELIEVE, Dsp.APPLE_MUSIC | Dsp.APPLE_MUSIC_LIBRARY | Dsp.APPLE_MUSIC_DEMO | Dsp.APPLE_MUSIC_CONTAINER):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE, ProductReferenceSchema.STORE_ID_APPLE)
            case (Distributor.TUNECORE, Dsp.APPLE_MUSIC):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE, ProductReferenceSchema.STORE_ID_APPLE)
            case (Distributor.BELIEVE, Dsp.DEEZER):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE, ProductReferenceSchema.STORE_ID_DEEZER)
            case (Distributor.TUNECORE, Dsp.DEEZER):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE, ProductReferenceSchema.STORE_ID_DEEZER)
            case (Distributor.BELIEVE | Distributor.EMS, Dsp.SPOTIFY):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE, ProductReferenceSchema.STORE_ID_SPOTIFY)
            case (Distributor.TUNECORE, Dsp.SPOTIFY):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE, ProductReferenceSchema.STORE_ID_SPOTIFY)
            case (Distributor.BELIEVE, Dsp.YOUTUBE):
                return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_BELIEVE, ProductReferenceSchema.STORE_ID_YOUTUBE)
            case _:
                raise ValueError(f"Distributor = {self.distributor_code} and DSP = {self.dsp_code} is not an accepted tuple")

    def get_distributor_match_parameters_tunecore(self):
        if self.distributor_code == Distributor.TUNECORE and self.dsp_code == Dsp.SPOTIFY:
            return DistributorMatchParameters(ProductReferenceTunecoreSchema.ISRC_TC, ProductReferenceTunecoreSchema.TC_UPC, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE_NEW_REF)

    def get_isrc_optional_parameters_tunecore(self):
        if self.distributor_code == Distributor.TUNECORE and self.dsp_code == Dsp.SPOTIFY:
            return DistributorMatchParameters(ProductReferenceTunecoreSchema.ISRC_OPTIONAL_TC, ProductReferenceTunecoreSchema.TC_UPC, DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE_NEW_REF)

    def get_store_id_match_parameters_tunecore(self):
        if self.distributor_code == Distributor.TUNECORE and self.dsp_code == Dsp.SPOTIFY:
            return StoreIdMatchParameters(DBDataEnrichment.T_PRODUCT_REFERENCE_TUNECORE_NEW_REF, ProductReferenceTunecoreSchema.ID_SONG_SPOTIFY)
