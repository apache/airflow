"""
Class and methods used to communicate with Google Cloud Monitoring API.
"""

import datetime
import itertools
from typing import Dict, List, Optional, Union

import pytz
from google.api.distribution_pb2 import Distribution
from google.api_core.page_iterator import GRPCIterator
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3.types import (
    Aggregation,
    TimeInterval,
    TimeSeries,
    TypedValue,
)


ALIGNMENT_PERIOD = {"seconds": 60}
METRIC_KINDS = ["GAUGE", "DELTA", "CUMULATIVE"]
VALUE_TYPES = [
    "bool_value",
    "distribution_value",
    "double_value",
    "int64_value",
    "string_value",
]
PREFIX_LABEL_INDICATOR = "_prefix"
# PAGE_SIZE controls maximal amount of time series points returned in one page of
# list_time_series response
# when creating pages, api tries to put full time series on a single page, adding additional time
# series to the page only if it can fully fit
# number of pages in the responses may have impact on number of merges we have to do to combine them
# if single time series are split between multiple pages
# PAGE_SIZE of 1440 should allow to store a 24 hour-span time series
# with points collected in 1 minute intervals in a single page
PAGE_SIZE = 1440


def convert_to_full_minutes_timestamp(date_string: str, round_up: bool = False) -> float:
    """
    Rounds the date_string to full minutes and converts it to seconds since January 1, 1970.

    :param date_string: string with date in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
    :type date_string: str
    :param round_up: if date_string should be rounded up instead of down.
    :type round_up: bool

    :return: number of seconds passed since January 1, 1970 until date_string.
    :rtype: float
    """

    date = pytz.utc.localize(datetime.datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S.%f"))

    equal_timestamp = int(date.timestamp())
    full_minutes_timestamp = int(equal_timestamp / 60) * 60

    if round_up and full_minutes_timestamp != equal_timestamp:
        full_minutes_timestamp += 60

    return full_minutes_timestamp


def convert_int_to_metric_kind(metric_kind_value: int) -> str:
    """
    Returns the kind of metric matching given int value.

    :param metric_kind_value: integer identifying the kind of Cloud Monitoring metric.
    :type metric_kind_value: int

    :return: metric kind matching the int value.
    :rtype: str

    :raises: TypeError: if the int value does not match any of known metric kinds.
    """

    for metric_kind in METRIC_KINDS:
        if metric_kind_value == getattr(TypedValue, metric_kind):
            return metric_kind

    raise TypeError("Unknown kind of metric returned in Cloud Monitoring response.")


def get_typed_value(typed_value: TypedValue) -> Union[bool, Distribution, float, int, str]:
    """
    Returns the value of provided TypedValue.

    :param typed_value: TypedValue value of which should be returned.
    :type typed_value: TypedValue

    :return: the value of the first type the TypedValue has field for.
    :rtype: Union[bool, Distribution, float, int, str]

    :raises: TypeError: if TypedValue does not have a field for any of the known value types.
    """

    for value_type in VALUE_TYPES:
        if typed_value.HasField(value_type):
            return getattr(typed_value, value_type)

    raise TypeError("Unknown type of value returned in Cloud Monitoring response.")


class MonitoringApi:
    """
    Class dedicated to communication Google Cloud Monitoring API.
    """

    def __init__(self, project_id: str) -> None:
        """
        Creates an instance of MonitoringApi.

        :param project_id: project_id that will be used when executing requests.
        :type project_id: str
        """

        self.client = monitoring_v3.MetricServiceClient()
        self.project_id = project_id

        self.metric_descriptors = []
        for descriptor in self.client.list_metric_descriptors(
            name=self.client.project_path(self.project_id),
            filter_='metric.type = starts_with("kubernetes.io")',
        ):
            self.metric_descriptors.append(descriptor)

    def collect_time_series(
        self, start_date: str, end_date: str, monitored_resources_map: Dict
    ) -> List[TimeSeries]:
        """
        Collects time series within provided time window
        for resource_type/metric_type pairs with filters specified in monitored_resources_map.

        :param start_date: start of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
        :type start_date: str
        :param end_date: end of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
        :type end_date: str
        :param monitored_resources_map: a dictionary that contains resource and metric information
            required to execute time series listing requests.
        :type monitored_resources_map: Dict

        :return: list of all collected TimeSeries instances.
        :rtype: List[TimeSeries]
        """

        interval = self.get_time_interval(start_date, end_date)

        collected_time_series = []

        for resource_type in monitored_resources_map:
            resource_groups = monitored_resources_map[resource_type]["resource_groups"]
            metrics = monitored_resources_map[resource_type]["metrics"]

            # we make a call for every resource group/metric
            for resource_group, metric in itertools.product(resource_groups, metrics):
                metric_type = metric["metric_type"]
                metric_label_filters = metric.get("metric_label_filters", {})
                resource_label_filters = resource_group.get("resource_label_filters", {})
                new_resource_labels = resource_group.get("new_resource_labels", {})

                if resource_group.get("aggregation", False):
                    aggregation = self.get_aggregation(metric_type)
                else:
                    aggregation = None

                iterator = self.list_time_series(
                    metric_type=metric_type,
                    resource_type=resource_type.value,
                    interval=interval,
                    metric_label_filters=metric_label_filters,
                    resource_label_filters=resource_label_filters,
                    aggregation=aggregation,
                )

                time_series = self.preprocess_time_series_iterator(
                    iterator=iterator,
                    new_resource_labels=new_resource_labels,
                    new_metric_labels=metric_label_filters,
                    aggregation=aggregation,
                )

                collected_time_series += time_series

        return collected_time_series

    @staticmethod
    def get_time_interval(start_date: str, end_date: str) -> TimeInterval:
        """
        Creates an instance of TimeInterval based on provided start and end dates.

        :param start_date: start of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
        :type start_date: str
        :param end_date: end of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
        :type end_date: str

        :return: an instance of TimeInterval that can be used
            to execute time series listing request.
        :rtype: TimeInterval
        """

        interval = TimeInterval()
        start_date_seconds = convert_to_full_minutes_timestamp(start_date)
        end_date_seconds = convert_to_full_minutes_timestamp(end_date, round_up=True)

        interval.start_time.seconds = int(start_date_seconds)
        interval.end_time.seconds = int(end_date_seconds)

        interval.start_time.nanos = int((start_date_seconds - interval.start_time.seconds) * 10**9)
        interval.end_time.nanos = int((end_date_seconds - interval.end_time.seconds) * 10**9)
        return interval

    def get_aggregation(self, metric_type: str) -> Aggregation:
        """
        Returns an Aggregation object based on the provided metric type.

        :param metric_type: metric time series of which are to be aggregated.
        :type metric_type: str

        :return: an object describing how to aggregate the time series of given metric.
        :rtype: Aggregation
        """

        metric_kind = self.get_metric_kind_of_metric_type(metric_type)

        # GAUGE metrics are aligned by calculating a mean of points belonging to the same
        # time window
        if metric_kind == "GAUGE":
            per_series_aligner = Aggregation.ALIGN_MEAN
        # CUMULATIVE AND DELTA metrics ale aligned by calculating the rate at which time series
        # points change their value
        else:
            per_series_aligner = Aggregation.ALIGN_RATE

        # aligned points for all metric kinds are aggregated by summing them
        aggregation = Aggregation(
            alignment_period=ALIGNMENT_PERIOD,
            per_series_aligner=per_series_aligner,
            cross_series_reducer=Aggregation.REDUCE_SUM,
        )

        return aggregation

    def get_metric_kind_of_metric_type(self, metric_type: str) -> str:
        """
        Checks and returns metric kind assigned to given metric type.

        :param metric_type: name of the metric, the kind of which should be returned.
        :type metric_type: str

        :return: metric kind represented by the metric type.
        :rtype: str

        :raises: ValueError: if the provided metric type does not have exactly one matching
            MetricDescriptor.
        """

        metric_kind_values = [
            metric.metric_kind for metric in self.metric_descriptors if metric.type == metric_type
        ]

        if len(metric_kind_values) != 1:
            raise ValueError(
                f"Could not find a metric kind for {metric_type} " f"amongst collected metric descriptors."
            )

        return convert_int_to_metric_kind(metric_kind_values[0])

    # pylint: disable=too-many-arguments
    def list_time_series(
        self,
        metric_type: str,
        resource_type: str,
        interval: TimeInterval,
        metric_label_filters: Optional[Dict] = None,
        resource_label_filters: Optional[Dict] = None,
        aggregation: Optional[Aggregation] = None,
    ) -> GRPCIterator:
        """
        Executes the time series listing request.

        :param metric_type: metric for which matching time series should be collected.
        :type metric_type: str
        :param resource_type: the resource for which matching time series should be collected.
        :type resource_type: str
        :param interval: the time interval for which results should be returned.
        :type interval: TimeInterval
        :param metric_label_filters: optional dictionary containing restraints on metric labels
            that, if provided, will be added to the monitoring_filter.
        :type metric_label_filters: Dict
        :param resource_label_filters: optional dictionary containing restraints on resource labels
            that, if provided, will be added to the monitoring_filter.
        :type resource_label_filters: Dict
        :param aggregation: optional Aggregation object explaining how to combine collected time
            series into a single one
        :type aggregation: Aggregation

        :return: a PageIterator instance which is an iterable of TimeSeries instances
            matching the filter.
        :rtype: GRPCIterator
        """

        results = self.client.list_time_series(
            name=self.client.project_path(self.project_id),
            filter_=self.get_filter(metric_type, resource_type, metric_label_filters, resource_label_filters),
            interval=interval,
            view=monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=aggregation,
            page_size=PAGE_SIZE,
        )

        return results

    # pylint: enable=too-many-arguments

    @staticmethod
    def get_filter(
        metric_type: str,
        resource_type: str,
        metric_label_filters: Optional[Dict] = None,
        resource_label_filters: Optional[Dict] = None,
    ) -> str:
        """
        Prepares a monitoring_filter based on provided resource and metric information.

        :param metric_type: metric for which matching time series should be collected.
        :type metric_type: str
        :param resource_type: the resource for which matching time series should be collected.
        :type resource_type: str
        :param metric_label_filters: optional dictionary containing restraints on metric labels
            that will be added to the filter. Labels ending with PREFIX_LABEL_INDICATOR
            will be used to specify a partial match at start of string.
        :type metric_label_filters: Dict
        :param resource_label_filters: optional dictionary containing restraints on resource labels
            that will be added to the filter. Labels ending with PREFIX_LABEL_INDICATOR
            will be used to specify a partial match at start of string.
        :type resource_label_filters: Dict

        :return: a monitoring_filter that can be used to execute time series listing request.
        :rtype: str
        """

        label_filters = []

        if metric_label_filters:

            for label, value in metric_label_filters.items():
                if label.endswith(PREFIX_LABEL_INDICATOR):
                    label_filters.append(f'metric.label.{label[:-(len(PREFIX_LABEL_INDICATOR))]} : "{value}"')
                else:
                    label_filters.append(f'metric.label.{label} = "{value}"')

        if resource_label_filters:

            for label, value in resource_label_filters.items():
                if label.endswith(PREFIX_LABEL_INDICATOR):
                    label_filters.append(
                        f'resource.label.{label[:-(len(PREFIX_LABEL_INDICATOR))]} : "{value}"'
                    )
                else:
                    label_filters.append(f'resource.label.{label} = "{value}"')

        return " AND ".join(
            [f'metric.type = "{metric_type}"'] + [f'resource.type = "{resource_type}"'] + label_filters
        )

    @staticmethod
    def preprocess_time_series_iterator(
        iterator: GRPCIterator,
        new_resource_labels: Optional[Dict],
        new_metric_labels: Optional[Dict],
        aggregation: Optional[Aggregation],
    ) -> List[TimeSeries]:
        """
        Preprocesses iterator containing collected time series.

        :param iterator: metric for which matching time series should be collected.
        :type iterator: GRPCIterator
        :param new_resource_labels: optional dictionary containing new resource labels and their
            values which should be added to every time series.
        :type new_resource_labels: Dict
        :param new_metric_labels: optional dictionary containing new metric labels and their
            values which should be added to every time series.
        :type new_metric_labels: Dict
        :param aggregation: optional Aggregation object explaining how the collected time
            series where aggregated.
        :type aggregation: Aggregation

        :return: a list with preprocessed TimeSeries objects.
        :rtype: List[TimeSeries]
        """

        time_series_list = []
        for page in iterator.pages:
            for time_series in page:
                # aggregated metrics loose all metric label information upon aggregation
                # so we add some label information manually
                for resource_label in new_resource_labels:
                    time_series.resource.labels[resource_label] = new_resource_labels[resource_label]
                for metric_label in new_metric_labels:
                    time_series.metric.labels[metric_label] = new_metric_labels[metric_label]
                # change the metric name for the metrics aggregated with ALIGN_RATE
                # to reflect the change in their meaning
                if aggregation:
                    if aggregation.per_series_aligner == Aggregation.ALIGN_RATE:
                        time_series.metric.type += "_per_second"

                time_series_list.append(time_series)

        return time_series_list
