from ._core import API_Route
import requests
from pandas import DataFrame
import time

class WorldBankRoute(API_Route):
    _indicator: str
    _countries: str
    _daterange: str
    _source: int

    def __init__(self, indicator: str, daterange: str, country_codes: str, source: int = None):
        self._countries = ";".join(country_codes)
        self._indicator = indicator
        self._daterange = daterange
        self._source = source
        
    def get_data(self) -> DataFrame:
        params: dict = {
            "date": self._daterange
            ,"format": "json"
            ,"per_page": 1000
        }

        if self._source is not None:
            params["source"] = self._source

        url: str = f"https://api.worldbank.org/v2/country/{self._countries}/indicator/{self._indicator}"

        page: int = 1
        result: list[dict] = []
        while True:
            params["page"] = page
            response = requests.get(url=url, params=params)
            response.raise_for_status()

            data = response.json()

            result.extend([
                {
                    "country_iso": r["countryiso3code"],
                    "year": int(r["date"]),
                    "value": r["value"]
                }
                for r in data[1]
            ])

            if page >= data[0]["pages"]:
                break
            page += 1
            time.sleep(0.1)

        return DataFrame(result)