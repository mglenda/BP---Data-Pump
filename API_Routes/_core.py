from abc import ABC, abstractmethod
from typing import Any

class API_Route(ABC):
    
    @abstractmethod
    def get_data(self) -> Any:
        pass