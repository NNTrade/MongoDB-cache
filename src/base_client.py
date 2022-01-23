from typing import Dict,Union, List,Tuple
import pandas as pd
from bson.objectid import ObjectId
from .mongo_df_client import load, save,check_df_convert
from . import ConnectionConfig, DefaultConnectionConfig

class BaseClient:
    def __init__(self,collection_name:str, connection_config:ConnectionConfig=DefaultConnectionConfig) -> None:
        self._collection_name=collection_name
        self.connection_config = connection_config
    
    def save(self,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
        return save(self._collection_name, config,df,self.connection_config)
    def load(self,query:Dict[str,str]={}, id:Union[str,ObjectId]="")->List[Tuple[Dict[str,str], pd.DataFrame]]:
        return load(self._collection_name,query,id,self.connection_config)
    def check_df_convert(self,df:pd.DataFrame)->bool:
        return check_df_convert(df)
    