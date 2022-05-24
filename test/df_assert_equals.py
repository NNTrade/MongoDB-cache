from numpy import NaN
import pandas as pd
import unittest
import numpy as np


def compare_df(testCase: unittest.TestCase, expected_df: pd.DataFrame, asserted_df: pd.DataFrame):
    if not expected_df.equals(asserted_df):
        testCase.assertEqual(len(expected_df.columns), len(
            asserted_df.columns), "Columns count does not equal")
        testCase.assertEqual(len(expected_df.index), len(
            asserted_df.index), "Index count does not equal")

        for i in expected_df.index:
            testCase.assertTrue(i in asserted_df.index,
                                f"Couldn't find expected index {i}")

        for c in expected_df.columns:
            testCase.assertTrue(c in asserted_df.columns,
                                f"Couldn't find expected column {c}")
            assCol = asserted_df[c]
            expCol = expected_df[c]
            for i in expected_df.index:
                assVal = assCol[i]
                expVal = expCol[i]
                if np.isnan(expVal):
                    testCase.assertTrue(np.isnan(assVal))
                else:
                    testCase.assertEqual(
                        expVal, assVal, f"Values of index {i} in col {c} doesn't equal (expect {expVal} != assert {assVal})")
