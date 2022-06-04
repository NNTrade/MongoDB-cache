from datetime import datetime
import unittest
import logging
import pandas as pd
from src.tools.column_convert_tools import datetime_to_save, datetime_from_save


class ColumnConvertTool_TestCase(unittest.TestCase):

  logger = logging.getLogger(__name__)
  logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                      datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

  def test_WHEN_conver_dt_sr_THEN_return_same_sr(self):
    # Array
    expected_sr = pd.Series([datetime(2020, 1, 1, 1, 1, 1), datetime(
        2020, 1, 1, 1, 1, 2), datetime(2020, 1, 1, 1, 1, 3)], name="dt")

    # Act
    temp_sr = datetime_to_save(expected_sr)
    asserted_sr = datetime_from_save(temp_sr)

    # Assert
    self.assertEqual(expected_sr, asserted_sr)
