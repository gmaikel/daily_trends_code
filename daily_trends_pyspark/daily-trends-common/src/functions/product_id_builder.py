from pyspark.sql import DataFrame

class ProductIdBuilder:
    def __init__(self):
        self.matchers = []

    def match_by_track_store_id(self, reference: DataFrame) -> 'ProductIdBuilder':
        from src.functions.matcher.matcher_by_track_store_id import MatcherByTrackStoreId
        self.matchers.append(MatcherByTrackStoreId(reference))
        return self

    def reset_builder(self):
        self.matchers = []

    def build(self):
        from src.functions.product_id import ProductId
        product = ProductId(self.matchers)
        self.reset_builder()
        return product


