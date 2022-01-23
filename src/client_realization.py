from typing import Dict,Union, List,Tuple
import pandas as pd
from bson.objectid import ObjectId
from .mongo_df_client import load, save

class BaseClient:
    _collection_name=""
    
    def save(self,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
        return save(self._collection_name, config,df)
    def load(self,query:Dict[str,str]={}, id:Union[str,ObjectId]="")->List[Tuple[Dict[str,str], pd.DataFrame]]:
        return load(self._collection_name,query,id)

def BuildIndicatorClient():
    _ret = BaseClient()
    _ret._collection_name = "indicator"
    return _ret

def BuildMetricClient():
    _ret = BaseClient()
    _ret._collection_name = "metric"
    return _ret

def BuildCorrelationClient():
    _ret = BaseClient()
    _ret._collection_name = "correlation"
    return _ret