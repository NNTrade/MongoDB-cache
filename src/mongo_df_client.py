from ast import arg
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


def create_connection(collection_name: str, conn: ConnectionConfig = DefaultConnectionConfig) -> Tuple[MongoClient, Collection]:
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


def __build_payload(config: Dict[str,str], payload:str)->Dict[str, str]:
    data = config.copy()
    data["__payload"] = payload
    data["__timestamp"] = datetime.now(
        timezone.utc).strftime("%Y-%m-%d, %H:%M:%S, %Z")
    return data

def replace(collection_name: str, config: Dict[str, str], payload: pd.DataFrame, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig) -> ObjectId:
    try:
        mng_client, mng_collection = create_connection(
            collection_name, connection_config)
        data = __build_payload(config, payload.to_dict())

        if isinstance(id, str) and id != "":
            id = ObjectId(id)

        mng_collection.replace_one({"_id": id}, data)
    finally:
        mng_client.close()
    return id


def __save_payload(collection_name: str, config: Dict[str, str], payload: str, connection_config: ConnectionConfig = DefaultConnectionConfig, on_duplicate_config: save_logic = save_logic.ErrorOnDuplicateConfig) -> ObjectId:
    try:
        mng_client, mng_collection = create_connection(
            collection_name, connection_config)
        data = __build_payload(config, payload)

        ids = search_id(collection_name, config)
        if len(ids) > 0:
            if on_duplicate_config == save_logic.ErrorOnDuplicateConfig:
                raise Exception("Exist record with same config")
            elif on_duplicate_config == save_logic.ReplaceDuplicateConfig:
                mng_collection.delete_many(config)
        _id = mng_collection.insert_one(data)
    finally:
        mng_client.close()
    return _id.inserted_id


def save_df(collection_name: str, config: Dict[str, str], payload: pd.DataFrame, connection_config: ConnectionConfig = DefaultConnectionConfig, on_duplicate_config: save_logic = save_logic.ErrorOnDuplicateConfig, tuple_split: str = "|") -> ObjectId:
    if isinstance(payload, pd.DataFrame):
        payload_data = payload.to_dict()
    else:
        raise Exception("Payload should be DataFrame")
    return __save_payload(collection_name, config, payload_data, connection_config, on_duplicate_config)


def save_sr(collection_name: str, config: Dict[str, str], payload: pd.Series, connection_config: ConnectionConfig = DefaultConnectionConfig, on_duplicate_config: save_logic = save_logic.ErrorOnDuplicateConfig, tuple_split="|") -> ObjectId:
    if isinstance(payload, pd.Series):
        payload_data = payload.to_dict()
    else:
        raise Exception("Payload should be Series")
    return __save_payload(collection_name, config, payload_data, connection_config, on_duplicate_config)


def _search_id(mng_collection: Collection, query: Dict[str, str]) -> List[ObjectId]:
    _ret = []

    for doc in mng_collection.find(query):
        if "_id" in doc:
            _ret.append(ObjectId(doc["_id"]))
    return _ret


def search_id(collection_name: str, query: Dict[str, str], connection_config: ConnectionConfig = DefaultConnectionConfig) -> List[ObjectId]:
    try:
        mng_client, mng_collection = create_connection(
            collection_name, connection_config)
        return _search_id(mng_collection, query)
    finally:
        mng_client.close()


def delete_dy_config(collection_name: str, query: Dict[str, str], connection_config: ConnectionConfig = DefaultConnectionConfig) -> int:
    """
    delete records by query
    return delete count
    """
    try:
        mng_client, mng_collection = create_connection(
            collection_name, connection_config)

        return mng_collection.delete_many(query)
    finally:
        mng_client.close()


def __load_payload(collection_name: str, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig) -> List[Tuple[Dict[str, str], str]]:
    _ret = []
    try:
        mng_client, mng_collection = create_connection(
            collection_name, connection_config)
        _query = query.copy()

        if isinstance(id, str) and id != "":
            id = ObjectId(id)

        if isinstance(id, ObjectId):
            _query["_id"] = id

        for doc in mng_collection.find(_query):
            config = {}
            data = None
            for k, v in doc.items():
                if not ("_id" == k or "__" == k[:2]):
                    config[k] = v
                elif k == "__payload":
                    data = v
            _ret.append((config, data))
    finally:
        mng_client.close()
    return _ret


def load_df(collection_name: str, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig, tuple_split: str = "|") -> List[Tuple[Dict[str, str], pd.DataFrame]]:
    _tmp_ret: List[Tuple[Dict[str, str], str]] = __load_payload(
        collection_name, query, id, connection_config)
    _ret: List[Tuple[Dict[str, str], pd.DataFrame]] = []

    for (dict, payload) in _tmp_ret:
        _ret_df_tmp = pd.DataFrame.from_dict(payload)
        _ret.append((dict, _ret_df_tmp))
    return _ret


def load_sr(collection_name: str, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "", connection_config: ConnectionConfig = DefaultConnectionConfig, tuple_split: str = "|") -> List[Tuple[Dict[str, str], pd.Series]]:
    _tmp_ret: List[Tuple[Dict[str, str], str]] = __load_payload(
        collection_name, query, id, connection_config)
    _ret: List[Tuple[Dict[str, str], pd.DataFrame]] = []

    for (dict, payload) in _tmp_ret:
        _ret_sr_tmp = pd.Series(payload)
        _ret.append((dict, _ret_sr_tmp))
    return _ret


def check_df_convert(df: pd.DataFrame) -> bool:
    data = df.to_dict()
    returned_df = pd.DataFrame.from_dict(data)
    return df.equals(returned_df)


def check_sr_convert(sr: pd.Series) -> bool:
    data = sr.to_dict()
    returned_sr = pd.Series(data)
    return sr.equals(returned_sr)
