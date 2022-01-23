from sys import float_repr_style
from src import DefaultConnectionConfig
from src import mongo_df_client
from pymongo import MongoClient
import pandas as pd
import unittest
import uuid


class Mongo_DF_Client_TestCase(unittest.TestCase):
    collection_name = "Mongo_DF_Client_TestCase"

    def setUp(self):
        DefaultConnectionConfig.HOST = "192.168.34.2"
        DefaultConnectionConfig.PORT = 8001
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

    def test_save_load(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_config = {"f1": id, "f2": "test3"}
        mongo_df_client.save(self.collection_name,
                             expected_config, expected_df)

        asserted_list = mongo_df_client.load(self.collection_name, {"f1": id})

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_save_load_many(self):
        id = str(uuid.uuid4())
        expected_df1 = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df2 = pd.DataFrame(
            {"A1": [10, 20, 30], "B1": [40, 50, 60]}, index=[100, 110, 120])
        expected_df1.index = expected_df1.index.map(lambda el: str(el))
        expected_df2.index = expected_df2.index.map(lambda el: str(el))

        expected_config1 = {"f1": id, "f2": "test3"}
        expected_config2 = {"f1": id, "f2": "test4"}
        mongo_df_client.save(self.collection_name,
                             expected_config1, expected_df1)
        mongo_df_client.save(self.collection_name,
                             expected_config2, expected_df2)

        asserted_list = mongo_df_client.load(self.collection_name, {"f1": id})

        self.assertEqual(2, len(asserted_list))

        assert1 = False
        assert2 = False

        for asserted_tuple in asserted_list:
            asserted_config = asserted_tuple[0]
            asserted_df = asserted_tuple[1]
            if asserted_config["f2"] == expected_config1["f2"]:
                self.assertEqual(expected_config1, asserted_config)
                self.assertTrue(expected_df1.equals(asserted_df))
                assert1 = True

        for asserted_tuple in asserted_list:
            asserted_config = asserted_tuple[0]
            asserted_df = asserted_tuple[1]
            if asserted_config["f2"] == expected_config2["f2"]:
                self.assertEqual(expected_config2, asserted_config)
                self.assertTrue(expected_df2.equals(asserted_df))
                assert2 = True

        self.assertTrue(assert1)
        self.assertTrue(assert2)

    def test_load_by_object_id(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_config = {"f1": id, "f2": "test3"}
        object_id = mongo_df_client.save(
            self.collection_name, expected_config, expected_df)

        asserted_list = mongo_df_client.load(
            self.collection_name, id=object_id)

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_load_by_str_id(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_config = {"f1": id, "f2": "test3"}
        object_id = mongo_df_client.save(
            self.collection_name, expected_config, expected_df)
        str_id = str(object_id)
        asserted_list = mongo_df_client.load(self.collection_name, id=str_id)

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_get_id(self):
        id = str(uuid.uuid4())
        expected_config = {"f1": id, "f2": "test3"}
        object_id = mongo_df_client.save(
            self.collection_name, expected_config, pd.NA)
        str_id = str(object_id)
        asserted_list = mongo_df_client.load(self.collection_name, id=str_id)

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(pd.isna(asserted_df))

        expected_df2 = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df2.index = expected_df2.index.map(lambda el: str(el))
        expected_config2 = {"f1": id, "f2": "test4"}

        object_id2 = mongo_df_client.replace(
            self.collection_name, expected_config2, expected_df2, object_id)
        asserted_list2 = mongo_df_client.load(
            self.collection_name, id=object_id)
        self.assertEqual(1, len(asserted_list))

        asserted_config2 = asserted_list2[0][0]
        asserted_df2 = asserted_list2[0][1]
        self.assertEqual(expected_config2, asserted_config2)
        self.assertTrue(expected_df2.equals(asserted_df2))
        self.assertEqual(object_id2, object_id)
