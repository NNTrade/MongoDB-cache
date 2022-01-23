from typing import Dict, List, Tuple, Union
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from bson.objectid import ObjectId

HOST = "192.168.0.1"
PORT = 8080
USERNAME = "root"
PASSWORD = "root"
AUTHSOURCE = "nntrade"
DATABASE = "nntrade"

def _create_connection(collection_name: str) -> Tuple[MongoClient,Collection]:
    mng_client = MongoClient(
            host=HOST,
            port=PORT,
            username=USERNAME,
            password=PASSWORD,
            authSource=AUTHSOURCE)
    mng_db = mng_client[DATABASE]
    mng_collection = mng_db[collection_name]
    return mng_client, mng_collection

def _query_to_query_str(query:Dict[str,str])->Dict[str,str]:
    _query_config = {}
    for conf in query:
        _query_config[f"config.{conf}"] = query[conf]
    return _query_config

def save(collection_name: str,config: Dict[str, str], df: pd.DataFrame)->ObjectId:
    try:
        mng_client, mng_collection = _create_connection(collection_name)
        data = {
            "config": config,
            "payload": df.to_dict()
        }
        _id = mng_collection.insert_one(data)
    finally:
        mng_client.close()
    return _id.inserted_id
    
def load(collection_name: str,query:Dict[str,str]={}, id:Union[str,ObjectId]="")->List[Tuple[Dict[str,str], pd.DataFrame]]:
    _ret = []
    try:
        mng_client, mng_collection = _create_connection(collection_name)
        _query = _query_to_query_str(query)
        
        if isinstance(id, str) and id != "":
           id = ObjectId(id)
            
        if isinstance(id, ObjectId):
            _query["_id"] = id
            
        for doc in mng_collection.find(_query):
            _ret.append((doc["config"], pd.DataFrame.from_dict(doc["payload"])))
    finally:
        mng_client.close()
    return _ret