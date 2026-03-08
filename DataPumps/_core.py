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

class Route_MainDataset(RouteDefinition):
    query: str = "SELECT * FROM public.fact_country_year_indicators"

class Route_RegionalDataSet(RouteDefinition):
    query: str = """SELECT cr.region_name,i.* FROM public.fact_country_year_indicators i
                    JOIN public.country_region cr ON cr.country_iso = i.country_iso"""

class Route_CorrelationIndicators(RouteDefinition):
    pk_columns = ['scope_type', 'scope_value', 'method', 'variable_x', 'variable_y']
    table:str = "fact_indicator_correlations"
    schema:str = "public"