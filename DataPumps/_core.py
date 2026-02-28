from typing import Sequence

class RouteDefinition:
    query: str
    pk_columns: Sequence[str]
    table: str
    schema: str

class PumpDefinition:
    route: RouteDefinition