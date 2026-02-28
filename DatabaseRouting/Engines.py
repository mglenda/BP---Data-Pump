from __future__ import annotations

from typing import Any
from ._core import Engine,DataFrame,Sequence
from pandas import read_sql_query
from configparser import ConfigParser
import psycopg2
from psycopg2.extras import execute_values
import logging
from Constants import Constants
import warnings

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable"
)

LOG = logging.getLogger(__name__)

class PostgreSQL(Engine):
    _autocommit: bool
    _connection: psycopg2.extensions.connection | None = None
    _config: dict[str, str]
    _config_section: str

    def __init__(self
                ,config_section:str = "db_config"
                ,autocommit: bool = False
        ):
        self._autocommit = autocommit
        self._connection = None
        self._config_section = config_section
        self._config = self._load_config()
    
    def connect(self) -> psycopg2.extensions.connection:
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(**self._config)
            self._connection.autocommit = self._autocommit
        return self._connection

    def close(self):
        if self._connection is not None and not self._connection.closed:
            if not self._autocommit:
                self._connection.commit()
            else:
                self._connection.rollback()

            self._connection.close()

    def _load_config(self) -> dict[str, str]:
        if not Constants.config_path.exists():
            logging.error(f"Config file '{Constants.config_path}' was not found.")

        parser = ConfigParser(interpolation=None)
        parser.read(Constants.config_path, encoding="utf-8")

        if not parser.has_section(self._config_section):
            logging.error(f"Section '{self._config_section}' was not found in '{Constants.config_path}'.")
        else:
            return {key: value for key, value in parser.items(self._config_section)}
        return {}
    
    def fetch(
        self
        ,query: str
    ) -> DataFrame:
        return read_sql_query(query, self._connection)
    
    def merge(
        self
        ,table: str
        ,schema: str
        ,pk_columns: Sequence[str]
        ,data: DataFrame
    ) -> int:
        if not set(pk_columns).issubset(data.columns):
            logging.error(f"DataFrame doesn't contain PK ident columns. table: {schema}.{table}, DataFrame columns: {', '.join(data.columns)}")
            raise KeyError("DataFrame doesn't contain PK ident columns.")
        
        update_columns = set(data.columns) - set(pk_columns)
        if len(update_columns) <= 0:
            logging.error(f"DataFrame doesn't contain any value columns. table: {schema}.{table}, DataFrame columns: {', '.join(data.columns)}")
            raise KeyError("DataFrame doesn't contain any value columns.")
        
        columns = ", ".join(data.columns)
        conflict_cols = ", ".join(pk_columns)
        updates = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in update_columns
        )

        statement: str = (
            f"INSERT INTO {schema}.{table} ({columns}) "
            "VALUES %s "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        )

        rows = list(data.to_dict(orient="records"))
        if rows and len(rows) > 0:
            values = [tuple(row[column] for column in list(rows[0].keys())) for row in rows]
            execute_values(self._connection.cursor(), statement, values, page_size=1000)

        return len(rows)