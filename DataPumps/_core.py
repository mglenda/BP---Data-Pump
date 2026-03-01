from typing import Sequence

class RouteDefinition:
    query: str
    pk_columns: Sequence[str]
    table: str
    schema: str

class Route_WorldBankDefault(RouteDefinition):
    pk_columns = ["country_iso","year"]
    table: str = "fact_country_year_indicators"
    schema: str = "public"