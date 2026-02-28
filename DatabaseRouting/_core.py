from abc import ABC, abstractmethod
from pandas import DataFrame
from typing import Sequence

class Engine(ABC):
    
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def fetch(
        self,
        query: str,
    ) -> DataFrame:
        pass

    @abstractmethod
    def merge(
        self
        ,table: str
        ,schema: str
        ,pk_columns: Sequence[str]
        ,data: DataFrame
    ) -> int:
        pass