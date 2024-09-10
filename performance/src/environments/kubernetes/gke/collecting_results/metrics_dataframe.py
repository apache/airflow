"""
Methods used to prepare pandas dataframe using time series data with metrics
collected from Cloud Monitoring API.
"""

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from environments.kubernetes.gke.collecting_results.monitoring_api.monitoring_api import (
    MonitoringApi,
    convert_int_to_metric_kind,
    get_typed_value,
    convert_to_full_minutes_timestamp,
)

# fmt: off
from environments.kubernetes.gke.collecting_results.\
    monitoring_api.monitored_resources import (
        get_monitored_resources_map,
        get_merging_order,
        ResourceType,
        RESOURCE_LABEL_HIERARCHY,
        RESOURCE_TYPE_LABELS,
    )
# fmt: on

TIMESTAMP_COLUMN_NAME = "timestamp"
SECONDS_FROM_START_COLUMN_NAME = "seconds_from_normalized_start"
COLUMN_SORTING_ORDER = [TIMESTAMP_COLUMN_NAME] + RESOURCE_LABEL_HIERARCHY

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def prepare_metrics_dataframe(
    project_id: str,
    cluster_id: str,
    airflow_namespace_prefix: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Collects Cloud Monitoring time series metrics in the specified time window
    for provided GKE cluster and stores them in a single pandas dataframe.

    :param project_id: Google Cloud project of GKE cluster.
    :type project_id: str
    :param cluster_id: id of GKE cluster metrics of which should be collected.
    :type cluster_id: str
    :param airflow_namespace_prefix: prefix of the namespace where airflow pods are located.
    :type airflow_namespace_prefix: str
    :param start_date: start of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
    :type start_date: str
    :param end_date: end of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
    :type end_date: str

    :return: pandas Dataframe with Cloud Monitoring metrics data.
    :rtype: pd.DataFrame
    """

    api = MonitoringApi(project_id=project_id)

    monitored_resources_map = get_monitored_resources_map(cluster_id, airflow_namespace_prefix)

    collected_time_series = api.collect_time_series(
        start_date=start_date,
        end_date=end_date,
        monitored_resources_map=monitored_resources_map,
    )

    resource_data = convert_time_series_to_dataframes(collected_time_series)

    metrics_df = join_resource_dataframes(resource_data, start_date)

    return metrics_df


def convert_time_series_to_dataframes(collected_time_series: List) -> Dict[ResourceType, Dict]:
    """
    Converts a list of time series returned from Cloud Monitoring API into dataframes (separate
    for every ResourceType) and stores them together with additional information in a dictionary.

    :param collected_time_series: list of TimeSeries instances.
    :type collected_time_series: List

    :return: a dictionary that for every ResourceType holds dataframe with its time series data,
        resource labels set and a list of metric columns.
    :rtype: Dict[ResourceType, Dict]
    """

    log.info("Converting TimeSeries into dataframes.")

    resource_data = {}

    for time_series in collected_time_series:
        resource_type = ResourceType(time_series.resource.type)
        labels_of_resource_type = RESOURCE_TYPE_LABELS[resource_type]
        metric_kind = convert_int_to_metric_kind(time_series.metric_kind)

        # sort values so resulting metric_column is not random
        metric_labels = sorted([f"{key}-{value}" for key, value in time_series.metric.labels.items()])
        metric_column = "__".join([resource_type.value, metric_kind, time_series.metric.type] + metric_labels)

        points = [
            (point.interval.end_time.seconds, get_typed_value(point.value)) for point in time_series.points
        ]

        current_df = pd.DataFrame(points, columns=[TIMESTAMP_COLUMN_NAME, metric_column])

        # add columns with resource labels to temporary df
        for label in labels_of_resource_type:
            value = time_series.resource.labels.get(label, "")
            current_df[label] = value

        if resource_type not in resource_data:
            resource_data[resource_type] = {
                "df": current_df,
                "resource_labels_set": set(labels_of_resource_type),
                "metric_columns": [metric_column],
            }
        else:
            columns_to_merge_on = [TIMESTAMP_COLUMN_NAME] + list(labels_of_resource_type)

            # if metric_column is already present in target dataframe,
            # then metric column will be duplicated upon merge
            # and we will have to combine them
            to_join_metric_columns = metric_column in resource_data[resource_type]["df"].columns

            resource_data[resource_type]["df"] = pd.merge(
                resource_data[resource_type]["df"],
                current_df,
                on=columns_to_merge_on,
                how="outer",
                sort=False,
            )

            if to_join_metric_columns:

                resource_data[resource_type]["df"][metric_column] = resource_data[resource_type]["df"][
                    f"{metric_column}_x"
                ].combine(
                    resource_data[resource_type]["df"][f"{metric_column}_y"],
                    func=join_metric_columns,
                )

                resource_data[resource_type]["df"].drop(
                    [f"{metric_column}_x", f"{metric_column}_y"], axis=1, inplace=True
                )
            else:
                resource_data[resource_type]["metric_columns"].append(metric_column)

    return resource_data


def join_metric_columns(value_x: pd.Series, value_y: pd.Series) -> pd.Series:
    """
    Function used to join two duplicated columns with the data of the same metric
    that were created as a result of merging dataframes.

    :raises: ValueError: if two columns have different non-nan values in one of the rows
    """
    if np.isnan(value_x):
        return value_y
    if np.isnan(value_y):
        return value_x
    if value_x != value_y:
        raise ValueError(
            "Failed to join metric columns. " "Metric columns have different non-nan values in the same row."
        )
    # if we have not returned by this point, then both columns have the same value in given row
    return value_x


def join_resource_dataframes(resource_data: Dict[ResourceType, Dict], start_date: str) -> pd.DataFrame:
    """
    Joins dataframes with time series data of separate ResourceTypes
    into a single, final metrics dataframe.

    :param resource_data: a dictionary that for every queried ResourceType holds dataframe
        with time series data, resource labels set and a list of metric columns.
    :type resource_data: Dict[ResourceType, Dict]
    :param start_date: start of the time window in "%Y-%m-%d %H:%M:%S.%f" format (UTC timezone).
        Used to prepare SECONDS_FROM_START_COLUMN_NAME column.
    :type start_date: str

    :return: pandas Dataframe with Cloud Monitoring metrics data.
    :rtype: pd.DataFrame

    :raises: ValueError: if resource_data dictionary is empty which indicates
        that no TimeSeries data was collected at all
    """

    if not resource_data:
        raise ValueError("It seems that no TimeSeries data was collected.")

    log.info("Joining resource dataframes into a single dataframe.")

    remaining_resources = list(resource_data.keys())

    resource_types_merging_order = get_merging_order()

    for resource_type in resource_types_merging_order:

        if resource_type not in resource_data:
            continue

        merged = False

        # for every resource we check, if its labels are a subset of any other resource's
        for merge_candidate in remaining_resources:

            if merge_candidate == resource_type:
                continue

            # if they are, then we can merge data of currently considered resource
            # into the merge_candidate
            if resource_data[resource_type]["resource_labels_set"].issubset(
                resource_data[merge_candidate]["resource_labels_set"]
            ):
                merged = True

                # when merging dataframes for different resource types we assume that:
                # - resource labels with the same names hold the same kind of information
                #   (so there is point on merging on them)
                # - metric column sets are disjoint for every resource type pair

                # TODO: if one of the resource types has more than one matching merge candidate
                #  then merging dataframes in the manner below may result in duplicated
                #  and/or redundant rows (by redundant row we mean a row that has a counterpart
                #  which differs from it only in empty columns) due to using outer join
                resource_data[merge_candidate]["df"] = pd.merge(
                    resource_data[merge_candidate]["df"],
                    resource_data[resource_type]["df"],
                    on=[TIMESTAMP_COLUMN_NAME] + list(resource_data[resource_type]["resource_labels_set"]),
                    how="outer",
                    sort=False,
                )

        # if we merged data of given resource type with at least one other type,
        # then we can exclude given resource type from further logic
        # as its data is already stored elsewhere
        if merged:
            remaining_resources.remove(resource_type)

    # any unmerged dataframes must be concatenated
    metrics_df = pd.concat(
        [resource_data[resource_type]["df"] for resource_type in remaining_resources],
        axis=0,
        ignore_index=True,
    )

    start_date_timestamp = float(convert_to_full_minutes_timestamp(start_date))

    metrics_df[SECONDS_FROM_START_COLUMN_NAME] = metrics_df[TIMESTAMP_COLUMN_NAME] - start_date_timestamp

    columns_order = get_column_order(resource_data)

    metrics_df = rearrange_metrics_df(metrics_df, columns_order)

    return metrics_df


def get_column_order(resource_data: Dict[ResourceType, Dict]) -> List[str]:
    """
    Gets the order of columns in metrics dataframe based on ResourceType hierarchy and
    RESOURCE_LABEL_HIERARCHY.

    :param resource_data: a dictionary that for every ResourceType holds dataframe
        with time series data, resource labels set and a list of metric columns.
    :type resource_data: Dict[ResourceType, Dict]

    :return: an ordered list of columns.
    :rtype: List[str]
    """

    metric_columns = []
    collected_resource_columns = set()

    for resource_type in ResourceType:
        if resource_type in resource_data:
            metric_columns += sorted(resource_data[resource_type]["metric_columns"])
            collected_resource_columns.update(resource_data[resource_type]["resource_labels_set"])

    # resource labels present in collected data have the same order
    # as in RESOURCE_LABEL_HIERARCHY
    resource_columns = [
        resource_label
        for resource_label in RESOURCE_LABEL_HIERARCHY
        if resource_label in collected_resource_columns
    ]

    # if collected data has resource labels that are missing from RESOURCE_LABEL_HIERARCHY,
    # then sort them and put them last
    missing_labels = [
        resource_label
        for resource_label in collected_resource_columns
        if resource_label not in RESOURCE_LABEL_HIERARCHY
    ]

    if missing_labels:
        log.warning(
            "Following resource labels were put as last in the column order "
            "as they are missing from RESOURCE_LABEL_HIERARCHY: %s",
            missing_labels,
        )
        resource_columns += sorted(missing_labels)

    column_order = resource_columns + [TIMESTAMP_COLUMN_NAME, SECONDS_FROM_START_COLUMN_NAME] + metric_columns

    return column_order


def rearrange_metrics_df(metrics_df: pd.DataFrame, columns_order: List[str]) -> pd.DataFrame:
    """
    Rearranges order of columns in metrics dataframe and sorts its data
    according to COLUMN_SORTING_ORDER.

    :param metrics_df: pandas dataframe to be rearranged.
    :type metrics_df: pd.DataFrame
    :param columns_order: list with order of columns.
    :type columns_order: List[str]

    :return: reordered and sorted dataframe.
    :rtype: pd.DataFrame

    :raises: ValueError: if columns_order does not contain the same set of columns as metrics_df
    """

    if not set(columns_order) == set(metrics_df.columns):
        raise ValueError(
            f"Provided columns order: {columns_order} does not contain the same set "
            f"of columns as metrics dataframe: {list(metrics_df.columns)}."
        )

    metrics_df = metrics_df[columns_order]

    sorting_order = [column for column in COLUMN_SORTING_ORDER if column in metrics_df.columns]

    if sorting_order:
        log.info("Sorting dataframes.")

        metrics_df.sort_values(by=sorting_order, inplace=True, na_position="last")

    return metrics_df
