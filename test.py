import logging
from DataPumps.WorldBank import WorldBankDataPump

logging.basicConfig(
    filename="app.log"
    ,level=logging.INFO
    ,format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

WorldBankDataPump.load_data(indicator="NY.GDP.MKTP.KD",column_name="gdp_per_country")
WorldBankDataPump.load_data(indicator="NY.GDP.MKTP.KD.ZG",column_name="gdp_growth")
WorldBankDataPump.load_data(indicator="SP.POP.TOTL",column_name="population")
WorldBankDataPump.load_data(indicator="SP.URB.TOTL.IN.ZS",column_name="urbanisation")
WorldBankDataPump.load_data(indicator="EN.ATM.CO2E.PC",column_name="co_per_capita",source=75)
WorldBankDataPump.load_data(indicator="SH.XPD.CHEX.GD.ZS",column_name="healthcare_spendings_gdp_ratio")
WorldBankDataPump.load_data(indicator="SL.UEM.TOTL.ZS",column_name="unemployment")
WorldBankDataPump.load_data(indicator="SL.IND.EMPL.ZS",column_name="employment_industry")
WorldBankDataPump.load_data(indicator="EG.USE.PCAP.KG.OE",column_name="energy_consumption_per_capita")
WorldBankDataPump.load_data(indicator="FP.CPI.TOTL.ZG",column_name="inflation")
WorldBankDataPump.load_data(indicator="NY.GDP.PCAP.KD",column_name="gdp_per_capita")