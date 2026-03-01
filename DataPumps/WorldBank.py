from ._core import Route_WorldBankDefault
from DatabaseRouting.Engines import PostgreSQL
from DatabaseRouting.DatabaseRoute import DatabaseRoute
from API_Routes.WorldBankRoute import WorldBankRoute,DataFrame
from Constants import Constants
from pandas import notnull
import logging

LOG = logging.getLogger(__name__)

class WorldBankDataPump():

    @staticmethod
    def load_data(indicator: str, column_name: str, source: int = None, daterange: str = Constants.default_daterange, country_codes: str = Constants.country_codes):
        data: DataFrame = WorldBankRoute(indicator=indicator
                                         ,source=source
                                         ,daterange=daterange
                                         ,country_codes=country_codes
                                         ).get_data()
        data.rename(columns={"value": column_name}, inplace=True)
        data = DataFrame(data).astype(object)
        data = data.where(notnull(data), None)
        rc: int = 0
        with DatabaseRoute(engine_type=PostgreSQL,definition=Route_WorldBankDefault) as dr:
            rc = dr.merge_data(data)

        LOG.info(f"Data loaded [row_count]: {rc}, [indicators]: {indicator}")