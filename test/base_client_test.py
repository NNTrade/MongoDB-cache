from datetime import datetime
import logging
import unittest

from pymongo import MongoClient
from src.base_client import BaseClient
from src.mongo_df_client import DefaultConnectionConfig, save_logic
import pandas as pd

from test.df_assert_equals import compare_df


class BaseClientTestCase(unittest.TestCase):
    collection_name = "BaseClientTestCase"
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    def setUp(self):
        DefaultConnectionConfig.HOST = "192.168.34.112"
        DefaultConnectionConfig.PORT = 9012
        DefaultConnectionConfig.USERNAME = "unittestbot"
        DefaultConnectionConfig.PASSWORD = "unittestbot"
        DefaultConnectionConfig.AUTHSOURCE = "nntrade"
        DefaultConnectionConfig.DATABASE = "nntrade_unittest"

    def tearDown(self):
        try:
            mng_client = MongoClient(
                host=DefaultConnectionConfig.HOST,
                port=DefaultConnectionConfig.PORT,
                username=DefaultConnectionConfig.USERNAME,
                password=DefaultConnectionConfig.PASSWORD,
                authSource=DefaultConnectionConfig.AUTHSOURCE)
            mng_db = mng_client[DefaultConnectionConfig.DATABASE]
            mng_collection = mng_db[self.collection_name]
            mng_collection.drop()
        finally:
            mng_client.close()

    def test_hello_world(self):
        self.assertEqual(1, 1)

    def test_convert_df(self):
        client = BaseClient(self.collection_name)

        expectedDF = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])

        self.assertTrue(client.check_df_convert(expectedDF))

    def test_save_with_same_config_and_add(self):
        client = BaseClient(self.collection_name)
        old_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        old_df.index = old_df.index.map(lambda el: str(el))
        client.save_df(
            {"p1": "v1"}, old_df)

        df = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        df.index = df.index.map(lambda idx: str(idx))

        expectId = client.save_df(
            {"p1": "v1"}, df, on_duplicate_config=save_logic.AddDuplicateConfig)

        assertId = client.get_id({"p1": "v1"})
        self.assertEqual(2, len(assertId))
        self.assertTrue(expectId in assertId)

    def test_save_with_same_config_and_replace(self):
        client = BaseClient(self.collection_name)
        old_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        old_df.index = old_df.index.map(lambda el: str(el))
        client.save_df(
            {"p1": "v1"}, old_df)

        df = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        df.index = df.index.map(lambda idx: str(idx))

        expectId = client.save_df(
            {"p1": "v1"}, df, on_duplicate_config=save_logic.ReplaceDuplicateConfig)

        assertId = client.get_id({"p1": "v1"})
        self.assertEqual(1, len(assertId))
        self.assertEqual(expectId, assertId[0])

    def test_objId_search_df(self):
        client = BaseClient(self.collection_name)

        df = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        df.index = df.index.map(lambda idx: str(idx))

        expectId = client.save_df(
            {"p1": "v1"}, df)

        assertId = client.get_id({"p1": "v1"})

        self.assertEqual(1, len(assertId))
        self.assertEqual(expectId, assertId[0])

    def test_return_equal_df(self):

        client = BaseClient(self.collection_name)

        expectedDf = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        expectedDf.index = expectedDf.index.map(lambda idx: str(idx))

        self.assertTrue(client.check_df_convert(expectedDf))

        objId = client.save_df({"p1": "v1"}, expectedDf)

        assertedDf = client.load_df(id=objId)[0][1]

        self.logger.info(expectedDf)
        self.logger.info(assertedDf)
        compare_df(self, expectedDf, assertedDf)

    def test_return_equal_sr(self):

        client = BaseClient(self.collection_name)

        expectedDf = pd.Series({"A": 1, "B": 3})
        expectedDf.index = expectedDf.index.map(lambda idx: str(idx))

        self.assertTrue(client.check_sr_convert(expectedDf))

        objId = client.save_sr({"p1": "v1"}, expectedDf)

        assertedDf = client.load_sr(id=objId)[0][1]

        self.logger.info(expectedDf)
        self.logger.info(assertedDf)
        self.assertTrue(expectedDf.equals(assertedDf))
