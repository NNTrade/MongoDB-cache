from typing import Dict, Union, List, Tuple
import pandas as pd
from bson.objectid import ObjectId
from .mongo_df_client import check_df_convert, replace, save_df, save_logic, search_id, load_df, load_sr, check_sr_convert, save_sr
from . import ConnectionConfig, DefaultConnectionConfig
import logging


class BaseClient:
    def __init__(self, collection_name: str, connection_config: ConnectionConfig = DefaultConnectionConfig) -> None:
        self._logger = logging.getLogger(f"Client for {collection_name}")
        self._collection_name = collection_name
        self.connection_config = connection_config

    def get_id(self, config: Dict[str, str]) -> List[ObjectId]:
        self._logger.info("Lock id for data")
        return search_id(self._collection_name, config, self.connection_config)

    def save_df(self, config: Dict[str, str], df: pd.DataFrame, on_duplicate_config: save_logic = save_logic.ErrorOnDuplicateConfig) -> ObjectId:
        self._logger.info("save DataFrame to mongo collection")
        return save_df(self._collection_name, config, df, self.connection_config, on_duplicate_config)

    def save_sr(self, config: Dict[str, str], sr: pd.Series, on_duplicate_config: save_logic = save_logic.ErrorOnDuplicateConfig) -> ObjectId:
        self._logger.info("save Series to mongo collection")
        return save_sr(self._collection_name, config, sr, self.connection_config, on_duplicate_config)

    def load_df(self, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "") -> List[Tuple[Dict[str, str], pd.DataFrame]]:
        self._logger.info("load DataFrame from mongo collection")
        return load_df(self._collection_name, query, id, self.connection_config)

    def load_sr(self, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "") -> List[Tuple[Dict[str, str], pd.DataFrame]]:
        self._logger.info("load Series from mongo collection")
        return load_sr(self._collection_name, query, id, self.connection_config)

    def check_df_convert(self, df: pd.DataFrame) -> bool:
        return check_df_convert(df)

    def check_sr_convert(self, sr: pd.Series) -> bool:
        return check_sr_convert(sr)

    def replace(self, id: ObjectId, config: Dict[str, str], df: pd.DataFrame) -> ObjectId:
        self._logger.info("replace df from mongo collection")
        return replace(self._collection_name, config, df, id, self.connection_config)
