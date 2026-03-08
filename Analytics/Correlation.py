from DatabaseRouting.DatabaseRoute import DatabaseRoute
from pandas import DataFrame,concat
from DatabaseRouting.Engines import PostgreSQL
from DataPumps._core import RouteDefinition,Route_MainDataset,Route_RegionalDataSet
from Constants import CorrelationTypes
import itertools
from scipy.stats import pearsonr, spearmanr
import logging


LOG = logging.getLogger(__name__)

class CorrelationMetrics:

    @staticmethod
    def get_data(definition: RouteDefinition) -> DataFrame:
        with DatabaseRoute(definition=definition,engine_type=PostgreSQL) as dr:
            return dr.get_data()

    @staticmethod
    def compute_correlations(data: DataFrame
                             ,variables: list[str]
                             ,method: str=CorrelationTypes.pearson
                             ,scope_type: str="global"
                             ,scope_value: str="ALL"
                            ) -> DataFrame:
        rows = []

        for var1, var2 in itertools.combinations(sorted(variables), 2):
            #Erase Null Values
            pair_df = data[[var1, var2]].dropna()

            n = len(pair_df)
            if n < 3:
                continue

            x = pair_df[var1]
            y = pair_df[var2]

            try:
                if method == CorrelationTypes.pearson:
                    corr, p_value = pearsonr(x, y)
                elif method == CorrelationTypes.spearman:
                    corr, p_value = spearmanr(x, y)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                rows.append({
                    "scope_type": scope_type,
                    "scope_value": scope_value,
                    "method": method,
                    "variable_x": var1,
                    "variable_y": var2,
                    "correlation_value": float(corr),
                    "p_value": float(p_value) if p_value is not None else None,
                    "observation_count": n,
                })
            except Exception as e:
                print(f"Skipping {var1} vs {var2}: {e}")

        LOG.info(f"Correlation calculation [scope_type]: {scope_type}, [scope_value]: {scope_value}, [method]: {method}, [row_count]: {len(rows)}")

        return DataFrame(rows)
    
class CorellationMethods:

    @staticmethod
    def get_global_correlations(indicator_cols: str) -> DataFrame:
        data: DataFrame = CorrelationMetrics.get_data(Route_MainDataset)
        return concat([CorrelationMetrics.compute_correlations(data=data,variables=indicator_cols)
                ,CorrelationMetrics.compute_correlations(data=data,variables=indicator_cols,method=CorrelationTypes.spearman)]
                , ignore_index=True)
    
    @staticmethod
    def get_country_correlations(indicator_cols: str) -> DataFrame:
        data: DataFrame = CorrelationMetrics.get_data(Route_MainDataset)
        result_data: list = []

        for country_iso, group in data.groupby("country_iso"):
            result_data.append(CorrelationMetrics.compute_correlations(data=group
                                                                       ,variables=indicator_cols
                                                                       ,scope_type='country'
                                                                       ,scope_value=country_iso
                                                                       ))
            
            result_data.append(CorrelationMetrics.compute_correlations(data=group
                                                                       ,variables=indicator_cols
                                                                       ,scope_type='country'
                                                                       ,scope_value=country_iso
                                                                       ,method=CorrelationTypes.spearman
                                                                       ))

        return concat(result_data, ignore_index=True)
    
    @staticmethod
    def get_regional_correlations(indicator_cols: str) -> DataFrame:
        data = CorrelationMetrics.get_data(Route_RegionalDataSet)

        result_data: list = []

        for region_name, group in data.groupby("region_name"):
            result_data.append(CorrelationMetrics.compute_correlations(data=group
                                                                       ,variables=indicator_cols
                                                                       ,scope_type='region'
                                                                       ,scope_value=region_name
                                                                       ))
            result_data.append(CorrelationMetrics.compute_correlations(data=group
                                                                       ,variables=indicator_cols
                                                                       ,scope_type='region'
                                                                       ,scope_value=region_name
                                                                       ,method=CorrelationTypes.spearman
                                                                       ))
        return concat(result_data, ignore_index=True)
        