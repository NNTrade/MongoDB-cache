from sys import float_repr_style
from src import cache_client_func
from src.cache_client_func import ConnectionConfig
from pymongo import MongoClient
import pandas as pd
import unittest
import uuid

from src.tools.multi_index_flatting import multiindex_to_index


class MongoDfClientTestCase(unittest.TestCase):
    collection_name = "Mongo_DF_Client_TestCase"

    def setUp(self):
        self.connection_config = ConnectionConfig("192.168.100.227",27017,"unittestbot","unittestbot","mongodb-cache-test","mongodb-cache-test")

    def tearDown(self):
        try:
            mng_client = MongoClient(
                host=self.connection_config.host,
                port=self.connection_config.port,
                username=self.connection_config.username,
                password=self.connection_config.password,
                authSource=self.connection_config.authsource)
            mng_db = mng_client[self.connection_config.database]
            mng_collection = mng_db[self.collection_name]
            mng_collection.drop()
        finally:
            mng_client.close()

    def test_save_load_df(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=["10", "11", "12"])
        print("expected_df")
        print(expected_df)
        expected_config = {"f1": id, "f2": "test3"}
        cache_client_func.save_df(self.collection_name,self.connection_config,
                                expected_config, expected_df)

        asserted_list = cache_client_func.load_df(
            self.collection_name,self.connection_config,{"f1": id})

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        print("asserted_df")
        print(asserted_df)

        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_save_load_multiindex_df(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {("A", "1"): [1, 2, 3], ("A", "2"): [4, 5, 6], ("B", "2"): [7, 8, 0]}, index=[("A", "10"), ("A", "11"), ("B", "12")])
        print("expected_df")
        print(expected_df)

        expected_df.index = multiindex_to_index(expected_df.index)
        expected_df.columns = multiindex_to_index(expected_df.columns)

        print("expected_df after flating")
        print(expected_df)

        expected_config = {"f1": id, "f2": "test3"}
        cache_client_func.save_df(self.collection_name,self.connection_config,
                                expected_config, expected_df)

        asserted_list = cache_client_func.load_df(
            self.collection_name,self.connection_config, {"f1": id})

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        print("asserted_df")
        print(asserted_df)
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_save_load_sr(self):
        id = str(uuid.uuid4())
        expected_df = pd.Series(
            {"A": 1, "B": 4})
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_config = {"f1": id, "f2": "test4"}
        print("expected_df")
        print(expected_df)
        cache_client_func.save_sr(self.collection_name,self.connection_config,
                                expected_config, expected_df)

        asserted_list = cache_client_func.load_sr(
            self.collection_name,self.connection_config, {"f1": id})

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        print("asserted_df")
        print(asserted_df)
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_save_load_many_df(self):
        id = str(uuid.uuid4())
        expected_df1 = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df2 = pd.DataFrame(
            {"A1": [10, 20, 30], "B1": [40, 50, 60]}, index=[100, 110, 120])
        expected_df1.index = expected_df1.index.map(lambda el: str(el))
        expected_df2.index = expected_df2.index.map(lambda el: str(el))

        expected_config1 = {"f1": id, "f2": "test3"}
        expected_config2 = {"f1": id, "f2": "test4"}
        cache_client_func.save_df(self.collection_name,self.connection_config,
                                expected_config1, expected_df1)
        cache_client_func.save_df(self.collection_name,self.connection_config,
                                expected_config2, expected_df2)

        asserted_list = cache_client_func.load_df(
            self.collection_name, self.connection_config,{"f1": id})

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

    def test_save_load_many_sr(self):
        id = str(uuid.uuid4())
        expected_df1 = pd.Series(
            {"A": 1, "B": 4})
        expected_df2 = pd.Series(
            {"A1": 10, "B1": 40})
        expected_df1.index = expected_df1.index.map(lambda el: str(el))
        expected_df2.index = expected_df2.index.map(lambda el: str(el))

        expected_config1 = {"f1": id, "f2": "test3"}
        expected_config2 = {"f1": id, "f2": "test4"}
        cache_client_func.save_sr(self.collection_name,self.connection_config,
                                expected_config1, expected_df1)
        cache_client_func.save_sr(self.collection_name,self.connection_config,
                                expected_config2, expected_df2)

        asserted_list = cache_client_func.load_sr(
            self.collection_name,self.connection_config, {"f1": id})

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
        object_id = cache_client_func.save_df(
            self.collection_name,self.connection_config, expected_config, expected_df)

        asserted_list = cache_client_func.load_df(
            self.collection_name,self.connection_config, id=object_id)

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
        object_id = cache_client_func.save_df(
            self.collection_name,self.connection_config, expected_config, expected_df)
        str_id = str(object_id)
        asserted_list = cache_client_func.load_df(
            self.collection_name,self.connection_config, id=str_id)

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_load_by_config(self):
        id = str(uuid.uuid4())
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_config = {"f1": id, "f2": "test4"}
        object_id = cache_client_func.save_df(
            self.collection_name,self.connection_config, expected_config, expected_df)
        str_id = str(object_id)
        asserted_list = cache_client_func.load_df(
            self.collection_name,self.connection_config, query=expected_config)

        self.assertEqual(1, len(asserted_list))

        asserted_config = asserted_list[0][0]
        asserted_df = asserted_list[0][1]
        self.assertEqual(expected_config, asserted_config)
        self.assertTrue(expected_df.equals(asserted_df))

    def test_get_id(self):
        id = str(uuid.uuid4())
        config = {"f1": id, "f2": "test3"}
        expected_df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6]}, index=[10, 11, 12])
        expected_df.index = expected_df.index.map(lambda el: str(el))
        expected_id = cache_client_func.save_df(
            self.collection_name,self.connection_config, config, expected_df)

        asserted_id = cache_client_func.search_id(self.collection_name,self.connection_config, config)

        self.assertEqual(1, len(asserted_id))
        self.assertEqual(expected_id, asserted_id[0])
