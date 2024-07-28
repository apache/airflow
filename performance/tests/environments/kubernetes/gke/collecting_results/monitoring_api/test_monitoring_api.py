from unittest import TestCase, mock

from google.cloud.monitoring_v3.types import TimeInterval, TypedValue
from google.protobuf import timestamp_pb2

# fmt: off
from environments.kubernetes.gke.collecting_results.\
    monitoring_api.monitoring_api import (
        convert_to_full_minutes_timestamp,
        get_typed_value,
        MonitoringApi,
    )
from environments.kubernetes.gke.collecting_results.\
    monitoring_api.monitored_resources import (
        ResourceType,
    )
# fmt: on


PROJECT_ID = "project_id"


class TestMonitoringApi(TestCase):
    def setUp(self):
        self.start_date = "1970-01-01 00:00:01.10"
        self.end_date = "1970-01-01 00:01:01.20"

        with mock.patch(
            "environments.kubernetes.gke.collecting_results.monitoring_api."
            "monitoring_api.monitoring_v3.MetricServiceClient"
        ):
            self.api = MonitoringApi(project_id=PROJECT_ID)

    def test_convert_to_full_minutes_timestamp(self):

        self.assertEqual(convert_to_full_minutes_timestamp(self.start_date), 0)

    def test_convert_to_full_minutes_timestamp_round_up(self):

        self.assertEqual(convert_to_full_minutes_timestamp(self.start_date, round_up=True), 60)

    def test_get_typed_value_double_value(self):

        typed_value = TypedValue(double_value=1.0)

        self.assertEqual(get_typed_value(typed_value), 1.0)

    def test_get_typed_value_int_value(self):

        typed_value = TypedValue(int64_value=1)

        self.assertEqual(get_typed_value(typed_value), 1)

    def test_get_typed_value_string_value(self):

        typed_value = TypedValue(string_value="1")

        self.assertEqual(get_typed_value(typed_value), "1")

    def test_get_typed_value_bool_value(self):

        typed_value = TypedValue(bool_value=True)

        self.assertTrue(get_typed_value(typed_value))

    def test_get_typed_value_raises_exception_on_all_fields_empty(self):

        typed_value = TypedValue()

        with self.assertRaises(TypeError):
            get_typed_value(typed_value)

    def test_collect_time_series(self):

        resources_map = {
            ResourceType.NODE: {
                "resource_groups": [
                    {"resource_label_filters": {"label_1": "label_1_value"}},
                    {"resource_label_filters": {"label_2": "label_2_value"}},
                ],
                "metrics": [
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_1": "label_1_value"},
                    },
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_2": "label_2_value"},
                    },
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_3": "label_3_value"},
                    },
                    {
                        "metric_type": "metric_2",
                        "metric_label_filters": {"label_1": "label_1_value"},
                    },
                ],
            }
        }

        with mock.patch(
            "environments.kubernetes.gke.collecting_results.monitoring_api."
            "monitoring_api.MonitoringApi.list_time_series"
        ) as mock_list_time_series:
            self.api.collect_time_series(self.start_date, self.end_date, resources_map)
            # 2 * 3 calls for metric_1 plus 2 * 1 calls for metric_2
            self.assertEqual(mock_list_time_series.call_count, 8)

    def test_collect_time_series_empty_resource_label_filters_combinations(self):

        resources_map = {
            ResourceType.NODE: {
                "resource_groups": [{}],
                "metrics": [
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_1": "label_1_value"},
                    },
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_2": "label_2_value"},
                    },
                    {
                        "metric_type": "metric_1",
                        "metric_label_filters": {"label_3": "label_3_value"},
                    },
                ],
            }
        }

        with mock.patch(
            "environments.kubernetes.gke.collecting_results.monitoring_api."
            "monitoring_api.MonitoringApi.list_time_series"
        ) as mock_list_time_series:
            self.api.collect_time_series(self.start_date, self.end_date, resources_map)
            self.assertEqual(mock_list_time_series.call_count, 3)

    def test_collect_time_series_empty_metric_label_filters_combinations(self):

        resources_map = {
            ResourceType.NODE: {
                "resource_groups": [
                    {"resource_label_filters": {"label_1": "label_1_value"}},
                    {"resource_label_filters": {"label_2": "label_2_value"}},
                ],
                "metrics": [{"metric_type": "metric_1"}],
            }
        }

        with mock.patch(
            "environments.kubernetes.gke.collecting_results.monitoring_api."
            "monitoring_api.MonitoringApi.list_time_series"
        ) as mock_list_time_series:
            self.api.collect_time_series(self.start_date, self.end_date, resources_map)
            self.assertEqual(mock_list_time_series.call_count, 2)

    def test_get_time_interval(self):
        start_time = timestamp_pb2.Timestamp()
        start_time.seconds = 0
        start_time.nanos = 0

        end_time = timestamp_pb2.Timestamp()
        end_time.nanos = 0
        end_time.seconds = 120
        interval = TimeInterval(start_time=start_time, end_time=end_time)

        self.assertEqual(self.api.get_time_interval(self.start_date, self.end_date), interval)

    def test_get_filter(self):

        returned_filter = self.api.get_filter(
            metric_type="metric",
            resource_type="resource",
            metric_label_filters={"label_1_prefix": "label_1_value_prefix"},
            resource_label_filters={"label_2": "label_2_value"},
        )

        expected_filter = (
            'metric.type = "metric" '
            'AND resource.type = "resource" '
            'AND metric.label.label_1 : "label_1_value_prefix" '
            'AND resource.label.label_2 = "label_2_value"'
        )

        self.assertEqual(returned_filter, expected_filter)
