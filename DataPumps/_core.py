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

class Route_CorrelationExplorer(RouteDefinition):
    query: str = """SELECT
                        c.scope_type,
                        c.scope_value,
                        c.method,
                        c.variable_x,
                        c.variable_y,
                        c.correlation_value,
                        c.abs_correlation_value,
                        c.strength_label,
                        c.direction,
                        c.p_value,
                        c.observation_count,
                        c.calculated_at
                    FROM public.fact_indicator_correlations c"""

class Route_CountryRegionLookup(RouteDefinition):
    query: str = """SELECT
                        cr.country_iso,
                        cr.region_name
                    FROM public.country_region cr"""
