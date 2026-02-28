from ._core import PumpDefinition
from ._core import RouteDefinition

class GDP_Route(RouteDefinition):
    query: str = "SELECT * FROM public.testing_table"
    pk_columns = ["my_col"]
    table: str = "testing_table"
    schema: str = "public"

class GDP_Pump(PumpDefinition):
    route = GDP_Route