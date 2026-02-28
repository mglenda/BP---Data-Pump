from DatabaseRouting.DatabaseRoute import DatabaseRoute,DataFrame
from DatabaseRouting.Engines import PostgreSQL
from DataPumps.Pump_GDP import GDP_Pump
import logging

logging.basicConfig(
    filename="app.log"
    ,level=logging.INFO
    ,format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
data = None
with DatabaseRoute(engine_type=PostgreSQL,definition=GDP_Pump.route) as dr:
    data = dr.get_data()
    print(data)
    data.loc[[1, 3, 5], "test_data"] = "K"
    data = dr.merge_data(data)
    print(dr.get_data())