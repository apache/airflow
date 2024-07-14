import os
from unittest import TestCase, mock

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from environments.kubernetes.gke.collecting_results.metrics_dataframe import (
    get_column_order,
    join_metric_columns,
    join_resource_dataframes,
)

# fmt: off
from environments.kubernetes.gke.collecting_results.\
    monitoring_api.monitored_resources import (
        ResourceType,
    )
# fmt: on

DATAFRAMES_DIR = os.path.join(os.path.dirname(__file__), "dataframes")


class TestMetricsDataframe(TestCase):
    def setUp(self) -> None:

        self.start_date = "1970-01-01 00:00:01.0"

        self.resource_data = {
            ResourceType.CLUSTER: {
                "df": pd.read_csv(os.path.join(DATAFRAMES_DIR, f"{ResourceType.CLUSTER.value}.csv")),
                "resource_labels_set": {"project_id", "location", "cluster_name"},
                "metric_columns": ["cluster_metric_2", "cluster_metric_1"],
            },
            ResourceType.NODE: {
                "df": pd.read_csv(os.path.join(DATAFRAMES_DIR, f"{ResourceType.NODE.value}.csv")),
                "resource_labels_set": {
                    "project_id",
                    "location",
                    "cluster_name",
                    "node_name",
                    "non_existing_label_2",
                },
                "metric_columns": ["node_metric_2", "node_metric_1"],
            },
            ResourceType.POD: {
                "df": pd.read_csv(os.path.join(DATAFRAMES_DIR, f"{ResourceType.POD.value}.csv")),
                "resource_labels_set": {
                    "project_id",
                    "location",
                    "cluster_name",
                    "namespace_name",
                    "pod_name",
                    "non_existing_label_1",
                },
                "metric_columns": ["pod_metric_2", "pod_metric_1"],
            },
            ResourceType.CONTAINER: {
                "df": pd.read_csv(os.path.join(DATAFRAMES_DIR, f"{ResourceType.CONTAINER.value}.csv")),
                "resource_labels_set": {
                    "project_id",
                    "location",
                    "cluster_name",
                    "namespace_name",
                    "pod_name",
                    "non_existing_label_1",
                    "container_name",
                },
                "metric_columns": ["container_metric_2", "container_metric_1"],
            },
        }

    def test_join_metric_columns(self):

        df_1 = pd.DataFrame({"A": [1, np.nan, np.nan, 4]})
        df_2 = pd.DataFrame({"A": [np.nan, 2, np.nan, 4]})

        self.assertTrue(
            df_1["A"].combine(df_2["A"], func=join_metric_columns).equals(pd.Series([1.0, 2.0, np.nan, 4.0]))
        )

    def test_join_metric_columns_raises_exception_for_different_values_in_same_row(self):

        df_1 = pd.DataFrame({"A": [1, np.nan, np.nan, 4]})
        df_2 = pd.DataFrame({"A": [np.nan, 2, np.nan, 5]})
        with self.assertRaises(ValueError):

            df_1["A"].combine(df_2["A"], func=join_metric_columns)

    def test_get_column_order(self):

        expected_column_order = [
            "project_id",
            "location",
            "cluster_name",
            "node_name",
            "namespace_name",
            "pod_name",
            "container_name",
            "non_existing_label_1",
            "non_existing_label_2",
            "timestamp",
            "seconds_from_normalized_start",
            "cluster_metric_1",
            "cluster_metric_2",
            "node_metric_1",
            "node_metric_2",
            "pod_metric_1",
            "pod_metric_2",
            "container_metric_1",
            "container_metric_2",
        ]

        self.assertEqual(get_column_order(self.resource_data), expected_column_order)

    def test_join_resource_dataframes(self):

        expected_result = pd.read_csv(os.path.join(DATAFRAMES_DIR, "join_result.csv"))

        result = join_resource_dataframes(self.resource_data, self.start_date)

        # differences in index do not concern us
        assert_frame_equal(expected_result.reset_index(drop=True), result.reset_index(drop=True))

    def test_join_resource_dataframes_raises_exception_on_metric_column_duplicates(self):

        # we change the order so that CLUSTER type is first merged into both POD and CONTAINER types
        # causing cluster metric columns to duplicate when merging POD and CONTAINER
        with mock.patch(
            "performance_scripts.environments.kubernetes.gke.collecting_results.metrics_dataframe."
            "get_merging_order",
            return_value=list(ResourceType),
        ):
            with self.assertRaises(ValueError):
                join_resource_dataframes(self.resource_data, self.start_date)

    def test_join_resource_dataframes_raises_exception_on_empty_dict(self):

        with self.assertRaises(ValueError):

            join_resource_dataframes({}, self.start_date)
