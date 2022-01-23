from typing import Dict,Union, List,Tuple
import pandas as pd
from bson.objectid import ObjectId
from .mongo_df_client import load, save,check_df_convert,replace,_create_connection
from . import ConnectionConfig, DefaultConnectionConfig
import logging

class BaseClient:
    def __init__(self,collection_name:str, connection_config:ConnectionConfig=DefaultConnectionConfig) -> None:
        self._logger = logging.getLogger(f"Client for {collection_name}")
        self._collection_name=collection_name
        self.connection_config = connection_config
    
    def get_id(self,config: Dict[str, str])->ObjectId:
        self._logger.info("Lock id for data")
        return save(self._collection_name, config,pd.NA,self.connection_config)            
    def save(self,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
        self._logger.info("save df to mongo collection")
        return save(self._collection_name, config,df,self.connection_config)
    def load(self,query:Dict[str,str]={}, id:Union[str,ObjectId]="")->List[Tuple[Dict[str,str], pd.DataFrame]]:
        self._logger.info("load df from mongo collection")
        return load(self._collection_name,query,id,self.connection_config)
    def check_df_convert(self,df:pd.DataFrame)->bool:
        return check_df_convert(df)
    def replace(self, id:ObjectId,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
        self._logger.info("replace df from mongo collection") 
        return replace(id,self._collection_name,config,df,self.connection_config)
    