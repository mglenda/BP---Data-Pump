from ._core import Route_CorrelationIndicators
from DatabaseRouting.Engines import PostgreSQL
from DatabaseRouting.DatabaseRoute import DatabaseRoute
from API_Routes.WorldBankRoute import DataFrame
import logging

LOG = logging.getLogger(__name__)

class CorrelationPump():

    @staticmethod
    def load_data(data: DataFrame):
        rc: int = 0
        with DatabaseRoute(engine_type=PostgreSQL,definition=Route_CorrelationIndicators) as dr:
            rc = dr.merge_data(data)

        LOG.info(f"Correlation data stored [row_count]: {rc}")