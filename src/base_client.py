from typing import Dict,Union, List,Tuple
import pandas as pd
from bson.objectid import ObjectId
from .mongo_df_client import load, save,check_df_convert

class BaseClient:
    def __init__(self,collection_name:str) -> None:
        self.collection_name=collection_name
    
    def save(self,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
        return save(self._collection_name, config,df)
    def load(self,query:Dict[str,str]={}, id:Union[str,ObjectId]="")->List[Tuple[Dict[str,str], pd.DataFrame]]:
        return load(self._collection_name,query,id)
    def check_df_convert(self,df:pd.DataFrame)->bool:
        return check_df_convert(df)
    