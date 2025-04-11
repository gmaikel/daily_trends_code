from pyspark.sql import DataFrame, Column
from abc import ABC, abstractmethod


class ProductMatcher(ABC):
    @abstractmethod
    def product_reference(self, table: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def join_condition(self) -> Column:
        pass

    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def product_type(self) -> str:
        pass
