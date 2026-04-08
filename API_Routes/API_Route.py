from abc import ABC, abstractmethod
from pandas import DataFrame

class API_Route(ABC):
    
    @abstractmethod
    def get_data(self) -> DataFrame:
        pass