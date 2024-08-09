from unittest import TestCase

import pandas as pd

from environments.kubernetes.gke.collecting_results.results_dataframe import (
    add_column_to_dataframe,
)


class TestResultsDataframe(TestCase):
    def test_add_column_to_dataframe(self):

        dataframe = pd.DataFrame({"A": [1, 2, 3], "C": [4, 5, 6]})

        expected_result = pd.DataFrame({"A": [1, 2, 3], "B": [7, 7, 7], "C": [4, 5, 6]})

        returned_location = add_column_to_dataframe(
            dataframe=dataframe, column_name="B", column_value=7, location=1
        )

        self.assertTrue(dataframe.equals(expected_result))
        self.assertEqual(returned_location, 2)

    def test_add_column_to_dataframe_existing_column(self):

        dataframe = pd.DataFrame({"A": [1, 2, 3], "B": [7, 7, 7], "C": [4, 5, 6]})

        expected_result = dataframe.copy()

        returned_location = add_column_to_dataframe(
            dataframe=dataframe, column_name="B", column_value=7, location=1
        )

        self.assertTrue(dataframe.equals(expected_result))
        self.assertEqual(returned_location, 1)
