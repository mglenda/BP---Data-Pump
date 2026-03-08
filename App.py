from DataPumps.WorldBank import WorldBankDataPump
from Analytics.Correlation import CorellationMethods
from DataPumps.Correlation import CorrelationPump
import Utilities.FileToolkits as FileToolkits 
import logging

logging.basicConfig(
    filename="app.log"
    ,level=logging.INFO
    ,format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

class App:
    pumps: list[dict]
    indicator_cols: list
    def __init__(self):
        self.pumps = FileToolkits.load_json("settings.json")
        self.indicator_cols = [p['column_name'] for p in self.pumps]

    def calculate_correlations(self):
        CorrelationPump.load_data(data=CorellationMethods.get_country_correlations(self.indicator_cols))
        CorrelationPump.load_data(data=CorellationMethods.get_global_correlations(self.indicator_cols))
        CorrelationPump.load_data(data=CorellationMethods.get_regional_correlations(self.indicator_cols))

    def reload_world_bank_data(self):
        for pump in self.pumps:
            params = {}
            mandatory_params = ['indicator','column_name']
            optional_params = ['source']
            for p in mandatory_params:
                params[p] = pump[p]
            #Optional
            for p in optional_params:
                try:
                    params[p] = pump[p]
                except KeyError as k:
                    params[k.args[0]] = None

            WorldBankDataPump.load_data(**params)

if __name__ == "__main__":
    app = App()
    app.reload_world_bank_data()
    app.calculate_correlations()