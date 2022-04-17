from datetime import datetime
import logging
import unittest
from src.base_client import BaseClient
from src.mongo_df_client import DefaultConnectionConfig
import pandas as pd


class BaseClientTestCase(unittest.TestCase):
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)

    def setUp(self):
        DefaultConnectionConfig.HOST = "192.168.34.2"
        DefaultConnectionConfig.PORT = 9012
        DefaultConnectionConfig.USERNAME = "unittestbot"
        DefaultConnectionConfig.PASSWORD = "unittestbot"
        DefaultConnectionConfig.AUTHSOURCE = "nntrade"
        DefaultConnectionConfig.DATABASE = "nntrade_unittest"

    def test_hello_world(self):
        self.assertEqual(1, 1)

    def test_convert_df(self):
        client = BaseClient("BaseClientTestCase")

        expectedDF = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])

        self.assertTrue(client.check_df_convert(expectedDF))

    def test_objId_search_df(self):
        client = BaseClient("BaseClientTestCase")

        df = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        df.index = df.index.map(lambda idx: str(idx))

        expectId = client.save({"p1": "v1"}, df)

        assertId = client.get_id({"p1": "v1"})

        self.assertEqual(expectId, assertId[0])

    def test_return_equal_df(self):

        client = BaseClient("BaseClientTestCase")

        expectedDf = pd.DataFrame({"A": [0, 1, 2, 3], "B": [1, 2, 3, 4]}, index=[datetime(
            2022, 1, 1), datetime(2022, 1, 2), datetime(2022, 1, 3), datetime(2022, 1, 4)])
        expectedDf.index = expectedDf.index.map(lambda idx: str(idx))

        self.assertTrue(client.check_df_convert(expectedDf))

        objId = client.save({"p1": "v1"}, expectedDf)

        assertedDf = client.load(id=objId)[0][1]

        self.logger.info(expectedDf)
        self.logger.info(assertedDf)
        self.assertTrue(expectedDf.equals(assertedDf))
