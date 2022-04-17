from asyncio.log import logger
from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Dict, List, Tuple, Union
from xmlrpc.client import Boolean
import pandas as pd
from pymongo import MongoClient
from pymongo.collection import Collection
from bson.objectid import ObjectId
from . import DefaultConnectionConfig, ConnectionConfig


class save_logic(Enum):
    ErrorOnDuplicateConfig = 1,
    ReplaceDuplicateConfig = 2,
    AddDuplicateConfig = 3


def _create_connection(collection_name: str, conn: ConnectionConfig = DefaultConnectionConfig) -> Tuple[MongoClient, Collection]:
    logger = logging.getLogger("Create connection")
    logger.info(
        f"Open connection to host: {conn.HOST} DB: {conn.DATABASE} collection: {collection_name}")
    mng_client = MongoClient(
        host=conn.HOST,
        port=conn.PORT,
        username=conn.USERNAME,
        password=conn.PASSWORD,
        authSource=conn.AUTHSOURCE)
    mng_db = mng_client[conn.DATABASE]
    mng_collection = mng_db[collection_name]
    return mng_client, mng_collection


def _query_to_query_str(query: Dict[str, str]) -> Dict[str, str]:
    _query_config = {}
    for conf in query:
        _query_config[f"config.{conf}"] = query[conf]
    return _query_config


def replace(collection_name: str, config: Dict[str, str], payload: pd.DataFrame, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig) -> ObjectId:
    try:
        mng_client, mng_collection = _create_connection(
            collection_name, connection_config)
        data = {
            "config": config,
            "payload": payload.to_dict()
        }
        if isinstance(id, str) and id != "":
            id = ObjectId(id)

        mng_collection.replace_one({"_id": id}, data)
    finally:
        mng_client.close()
    return id


def save(collection_name: str, config: Dict[str, str], payload: pd.DataFrame, connection_config: ConnectionConfig = DefaultConnectionConfig, on_duplicate_config: save_logic = save_logic.ReplaceDuplicateConfig) -> ObjectId:
    try:
        mng_client, mng_collection = _create_connection(
            collection_name, connection_config)
        data = {
            "config": config
        }
        if isinstance(payload, pd.DataFrame):
            data["payload"] = payload.to_dict()

        data["timestamp"] = datetime.now(
            timezone.utc).strftime("%Y-%m-%d, %H:%M:%S, %Z")

        ids = search_id(collection_name, config)
        if len(ids) > 0:
            if on_duplicate_config == save_logic.ErrorOnDuplicateConfig:
                raise Exception("Exist record with config")
            elif on_duplicate_config == save_logic.ReplaceDuplicateConfig:
                mng_collection.delete_many(_query_to_query_str(config))
        _id = mng_collection.insert_one(data)
    finally:
        mng_client.close()
    return _id.inserted_id


def _search_id(mng_collection: Collection,query: Dict[str, str])->List[ObjectId]:
    _ret = []
    _query = _query_to_query_str(query)

    for doc in mng_collection.find(_query):
        if "_id" in doc:
            _ret.append(ObjectId(doc["_id"]))
    return _ret

def search_id(collection_name: str, query: Dict[str, str], connection_config: ConnectionConfig = DefaultConnectionConfig) -> List[ObjectId]:
    try:
        mng_client, mng_collection = _create_connection(
            collection_name, connection_config)
        return _search_id(mng_collection, query)
    finally:
        mng_client.close()


def delete_dy_config(collection_name: str, query: Dict[str, str], connection_config: ConnectionConfig = DefaultConnectionConfig)->int:
    """
    delete records by query
    return delete count
    """
    try:
        mng_client, mng_collection = _create_connection(
            collection_name, connection_config)
        _query = _query_to_query_str(query)

        return mng_collection.delete_many(_query_to_query_str(_query))
    finally:
        mng_client.close()


def load(collection_name: str, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig) -> List[Tuple[Dict[str, str], pd.DataFrame]]:
    _ret = []
    try:
        mng_client, mng_collection = _create_connection(
            collection_name, connection_config)
        _query = _query_to_query_str(query)

        if isinstance(id, str) and id != "":
            id = ObjectId(id)

        if isinstance(id, ObjectId):
            _query["_id"] = id

        for doc in mng_collection.find(_query):
            if "payload" in doc:
                df = pd.DataFrame.from_dict(doc["payload"])
            else:
                df = pd.NA
            _ret.append((doc["config"], df))
    finally:
        mng_client.close()
    return _ret


def check_df_convert(df: pd.DataFrame) -> bool:
    data = df.to_dict()
    returned_df = pd.DataFrame.from_dict(data)
    return df.equals(returned_df)
