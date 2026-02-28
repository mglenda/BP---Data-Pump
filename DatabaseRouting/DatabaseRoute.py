from __future__ import annotations

from typing import Any
from ._core import Engine,DataFrame
from DataPumps._core import RouteDefinition

class DatabaseRoute():
    _engine: Engine
    _definition: RouteDefinition

    def __init__(self, definition: RouteDefinition, engine_type: Engine, engine_params: dict = {}):
        self._engine = engine_type(**engine_params)
        self._definition = definition

    def __enter__(self) -> DatabaseRoute:
        self._engine.connect()
        return self
    
    def __exit__(self
                ,exc_type: Any
                ,exc_val: Any
                ,exc_tb: Any
        ) -> None:
        self._engine.close()

    def get_data(self) -> DataFrame:
        return self._engine.fetch(self._definition.query)
    
    def merge_data(self, data: DataFrame) -> int:
        return self._engine.merge(
            table=self._definition.table
            ,schema=self._definition.schema
            ,pk_columns=self._definition.pk_columns
            ,data=data
        )