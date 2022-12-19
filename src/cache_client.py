from typing import Dict, Union, List, Tuple
import pandas as pd
from bson.objectid import ObjectId
from .cache_client_func import check_df_convert, replace, save_df, search_id, load_df, load_sr, check_sr_convert, save_sr
from .constants import ConnectionConfig
from .save_logic import SaveLogic
import logging


class CacheClient:
    def __init__(self, collection_name: str, connection_config: ConnectionConfig) -> None:
        self._logger = logging.getLogger(f"Client for {collection_name}")
        self._collection_name = collection_name
        self.connection_config = connection_config

    def get_id(self, config: Dict[str, str]) -> List[ObjectId]:
        self._logger.info("Lock id for data")
        return search_id(self._collection_name, self.connection_config, config)

    def save_df(self, config: Dict[str, str], df: pd.DataFrame, on_duplicate_config: SaveLogic = SaveLogic.ErrorOnDuplicateConfig) -> ObjectId:
        self._logger.info("save DataFrame to mongo collection")
        return save_df(self._collection_name, self.connection_config, config, df, on_duplicate_config)

    def save_sr(self, config: Dict[str, str], sr: pd.Series, on_duplicate_config: SaveLogic = SaveLogic.ErrorOnDuplicateConfig) -> ObjectId:
        self._logger.info("save Series to mongo collection")
        return save_sr(self._collection_name, self.connection_config, config, sr, on_duplicate_config)

    def load_df(self, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "") -> List[Tuple[Dict[str, str], pd.DataFrame]]:
        self._logger.info("load DataFrame from mongo collection")
        return load_df(self._collection_name, self.connection_config, query, id)

    def load_sr(self, query: Dict[str, str] = {}, id: Union[str, ObjectId] = "") -> List[Tuple[Dict[str, str], pd.DataFrame]]:
        self._logger.info("load Series from mongo collection")
        return load_sr(self._collection_name, self.connection_config, query, id)

    def check_df_convert(self, df: pd.DataFrame) -> bool:
        return check_df_convert(df)

    def check_sr_convert(self, sr: pd.Series) -> bool:
        return check_sr_convert(sr)

    def replace(self, id: ObjectId, config: Dict[str, str], df: pd.DataFrame) -> ObjectId:
        self._logger.info("replace df from mongo collection")
        return replace(self._collection_name,self.connection_config, config, df, id)
