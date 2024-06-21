import functools
import math
import os
from statistics import stdev
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import scipy.stats

from loguru import logger

PREFIX_LABEL_INDICATOR = "_prefix"
SUFFIX_LABEL_INDICATOR = "_suffix"
PRECISION = 2
MAX_TIMESTAMP_DIFFERENCE = 30
# value from range [0; 1) which controls how much test_duration of given attempt can differ from
# average in order not to be considered an anomaly. For example: 0.4 means test_duration can be
# between 0,6 to 1,4 of average test_duration
ANOMALY_FACTOR = 0.5

CONFIGURATION_COLUMNS = {
    "AIRFLOW__CORE__STORE_SERIALIZED_DAGS",
    "airflow_version",
    "composer_api_endpoint",
    "composer_api_version",
    "composer_version",
    "node_count",
    "scheduler_count",
    "drs_enabled",
    "elastic_dag_configuration_type",
    "environment_size",
    "environment_type",
    "gke_version",
    "private_ip_enabled",
    "python_version",
}


def convert_size(size_bytes: int) -> str:
    """
    Converts provided number of bytes to human readable format
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1000)))
    p = math.pow(1000, i)
    s = round(size_bytes / p, PRECISION)
    return "%s %s" % (s, size_name[i])


def assign_time_series_metrics(
    resources_dict: Dict, time_series_metrics: Set[str]
) -> Dict:
    """
    Assigns time series metrics to proper resource type and metric category

    :param resources_dict: dict containing information about analyzed resource types.
    :type resources_dict: Dict
    :param time_series_metrics: set of column names containing time series metrics data.
    :type time_series_metrics: Set[str]

    :return: resources_dict with assigned metrics
    :rtype: Dict
    """

    for resource_type in resources_dict:
        resources_dict[resource_type]["gauge_metrics"] = []
        resources_dict[resource_type]["cumulative_metrics"] = []

    # by iterating this way we make sure every metric ends up in only one of the resource types
    # and metric kinds
    for metric in time_series_metrics:

        metric_elements = metric.split("__")
        assigned = False

        for resource_type in sorted(resources_dict):

            if resource_type not in metric_elements:
                continue

            if "CUMULATIVE" in metric_elements:
                resources_dict[resource_type]["cumulative_metrics"].append(metric)
                assigned = True
                break

            if "GAUGE" in metric_elements:
                resources_dict[resource_type]["gauge_metrics"].append(metric)
                assigned = True
                break

        if not assigned:
            logger.debug(
                f"Metric {metric} does not match any of resource types or metric kinds."
            )
    return resources_dict


def analyze_results(
    results_table: pd.DataFrame,
    resources_dict: Dict,
    general_metrics: Set[str],
    remove_test_attempts_with_failed_dag_runs: bool = True,
    remove_anomalous_test_attempts: bool = True,
) -> Tuple[pd.DataFrame, Dict, pd.DataFrame, Dict]:
    """
    Analyzes configuration and results of provided results_table.

    :param results_table: pandas Dataframe with performance test results for single study component.
    :type results_table: pd.DataFrame
    :param resources_dict: dict containing information about time series metrics that should be
        collected for different resource types and optional filter that control resource aggregation
    :type resources_dict: Dict
    :param general_metrics: set with metrics referring to a whole test attempt. Average of each of
        these metrics will be calculated across all matching test attempts.
    :type general_metrics: Set[str]
    :param remove_test_attempts_with_failed_dag_runs: set to False if you want the test attempts
        with failed dag runs to be included in analysis.
    :type remove_test_attempts_with_failed_dag_runs: bool
    :param remove_anomalous_test_attempts: set to False if you want the anomalous test attempts
       to be included in analysis.
    :type remove_anomalous_test_attempts: bool

    :return: a tuple consisting of
        - an updated results_table with removed test ids that either contained failed dag runs
        or had anomalous test_duration (provided that corresponding flags were set)
        - a dictionary with averages of metrics across all uuids belonging to results_table
        - a pandas DataFrame that for every group of values of configuration columns from
        results_table contains a total number of test_attempts, test_attempts with failed dag runs
        and anomalous test attempts present in this group
        - a dictionary with time series metrics separately for every uuid from results_table
    :rtype: Tuple[pd.DataFrame, Dict, pd.DataFrame, Dict]
    """

    (
        configuration_information,
        all_test_ids_with_failed_dag_runs,
        all_anomalous_test_ids,
    ) = get_configuration_information(results_table)

    results_table = collect_test_attempts_for_analysis(
        results_table,
        all_test_ids_with_failed_dag_runs,
        all_anomalous_test_ids,
        remove_test_attempts_with_failed_dag_runs,
        remove_anomalous_test_attempts,
    )

    # this will work for aggregated resources as well, as they only have gauge metrics and a single
    # matching group (single matching node or pod name) per every uuid
    statistics = calculate_statistics(results_table, resources_dict, general_metrics)

    metrics = collect_metrics(results_table, resources_dict)

    return results_table, statistics, configuration_information, metrics


def compare_statistics(
    subject_statistics: Dict, baseline_statistics: Dict
) -> pd.DataFrame:
    """
    Returns a dataframe with comparison of statistics averages.

    :param subject_statistics: dict containing averages of metrics calculated for group of test
        attempts that are the subject of comparison
    :type subject_statistics: Dict
    :param baseline_statistics: dict containing averages of metrics calculated for group of test
        attempts that are the baseline of comparison
    :type baseline_statistics: Dict

    :return: pandas DataFrame containing average values of general and time series metrics for both
        subject_statistics and baseline_statistics as well as difference between.
    :rtype: pd.DataFrame
    """

    categories = []
    baseline_values = []
    subject_float_values = []
    subject_values = []
    differences = []
    difference_percentages = []

    all_categories = set(subject_statistics).union(set(baseline_statistics))

    for category in sorted(all_categories):

        difference = None
        change_sign = None

        if category not in baseline_statistics or category not in subject_statistics:

            baseline_value = baseline_statistics.get(category, np.nan)
            subject_value = subject_statistics.get(category, np.nan)
            # display nan in difference in case of category missing from one of dictionaries
            difference_str = np.nan
            percentage_change_str = np.nan

        else:
            # it should be impossible for categories present for both statistics to be np.nan
            baseline_value = baseline_statistics[category]
            subject_value = subject_statistics[category]

            # also covers the case when they are both zero
            if baseline_value == subject_value:
                difference_str = "0." + "0" * PRECISION
                percentage_change_str = "0." + "0" * PRECISION + "%"
            # if the first value is zero
            elif not baseline_value:
                difference_str = "+inf" if subject_value > 0 else "-inf"
                percentage_change_str = difference_str
            elif np.isnan(baseline_value) or np.isnan(subject_value):
                difference_str = np.nan
                percentage_change_str = np.nan
            else:
                difference = subject_value - baseline_value
                percentage_change = abs(difference / baseline_value * 100)
                change_sign = "+" if subject_value > baseline_value else "-"
                percentage_change_str = (
                    f"{change_sign}{percentage_change:.{PRECISION}f}%"
                )
        subject_float_value = subject_value

        if "_byte" in category:
            baseline_value = (
                convert_size(baseline_value) if not np.isnan(baseline_value) else np.nan
            )
            subject_value = (
                convert_size(subject_value) if not np.isnan(subject_value) else np.nan
            )
            difference_str = (
                f"{change_sign}{convert_size(abs(difference))}"
                if difference is not None
                else difference_str
            )

            if "_per_second" in category:
                baseline_value = (
                    f"{baseline_value}/s" if isinstance(baseline_value, str) else np.nan
                )
                subject_value = (
                    f"{subject_value}/s" if isinstance(subject_value, str) else np.nan
                )
                difference_str = (
                    f"{difference_str}/s" if difference is not None else difference_str
                )

        else:
            # use space as a thousands separator
            baseline_value = (
                f"{baseline_value:,.{PRECISION}f}".replace(",", " ")
                if not np.isnan(baseline_value)
                else np.nan
            )
            subject_value = (
                f"{subject_value:,.{PRECISION}f}".replace(",", " ")
                if not np.isnan(subject_value)
                else np.nan
            )
            difference_str = (
                change_sign + f"{abs(difference):,.{PRECISION}f}".replace(",", " ")
                if difference is not None
                else difference_str
            )

        categories.append(category)
        baseline_values.append(baseline_value)
        subject_float_values.append(subject_float_value)
        subject_values.append(subject_value)
        differences.append(difference_str)
        difference_percentages.append(percentage_change_str)

    statistics_comparison_df = pd.DataFrame(
        {
            "category": categories,
            "baseline": baseline_values,
            "subject_of_comparison_float": subject_float_values,
            "subject_of_comparison": subject_values,
            "difference": differences,
            "difference_percentage": difference_percentages,
        }
    )

    return statistics_comparison_df


def get_statistics_df(results_statistics: Dict) -> pd.DataFrame:
    """
    Returns a dataframe with statistics averages.

    :param results_statistics: dict containing averages of metrics calculated for group of test
        attempts
    :type results_statistics: Dict

    :return: pandas DataFrame containing average values of general and time series metrics
    :rtype: pd.DataFrame
    """

    statistics_df = compare_statistics(
        subject_statistics=results_statistics, baseline_statistics={}
    )

    statistics_df.drop(
        columns=["baseline", "difference", "difference_percentage"], inplace=True
    )
    statistics_df.rename(
        columns={
            "subject_of_comparison_float": "average_value_float",
            "subject_of_comparison": "average_value",
        }, inplace=True
    )

    return statistics_df


def collect_test_attempts_for_analysis(
    results_table: pd.DataFrame,
    all_test_ids_with_failed_dag_runs: List[str],
    all_anomalous_test_ids: List[str],
    remove_test_attempts_with_failed_dag_runs: bool = True,
    remove_anomalous_test_attempts: bool = True,
) -> pd.DataFrame:
    """
    Collects test attempts matching the provided flags.

    :param results_table: pandas Dataframe with performance test results for single study component.
    :type results_table: pd.DataFrame
    :param all_test_ids_with_failed_dag_runs: a list of all test ids that have failed dag runs
    :type all_test_ids_with_failed_dag_runs: List[str]
    :param all_anomalous_test_ids: a list of all test ids that are considered anomalous due to their
        test duration
    :type all_anomalous_test_ids: List[str]
    :param remove_test_attempts_with_failed_dag_runs: set to False if you want the test attempts
        with failed dag runs to be included in analysis.
    :type remove_test_attempts_with_failed_dag_runs: bool
    :param remove_anomalous_test_attempts: set to False if you want the anomalous test attempts
       to be included in analysis.
    :type remove_anomalous_test_attempts: bool

    :return: pandas DataFrame containing performance metrics for test attempts that match the
        provided requirements.
    :rtype: pd.DataFrame
    """

    if remove_test_attempts_with_failed_dag_runs:

        results_table = results_table[
            ~results_table["uuid"].isin(all_test_ids_with_failed_dag_runs)
        ]

        if all_test_ids_with_failed_dag_runs:
            logger.debug(
                f"Test attempts removed from analysis due to having failed runs: "
                f"{all_test_ids_with_failed_dag_runs}"
            )

    if remove_anomalous_test_attempts:

        results_table = results_table[
            ~results_table["uuid"].isin(all_anomalous_test_ids)
        ]

        if all_anomalous_test_ids:
            logger.debug(
                f"Test attempts removed from analysis due to having anomalous test duration: "
                f"{all_anomalous_test_ids}"
            )

    return results_table


def get_configuration_information(
    results_table: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """
    For provided results_table checks how many test_attempts belong to different set of values
    of configuration columns.

    :param results_table: pandas Dataframe with performance test results for single study component.
    :type results_table: pd.DataFrame

    :return: a tuple consisting of
        - pandas DataFrame that for every group of values of configuration columns from
        results_table contains a total number of test_attempts, test_attempts with failed dag runs
        and anomalous test attempts present in this group
        - a list of all test ids that have failed dag runs
        - a list of all test ids that are considered anomalous due to their test duration
    :rtype: pd.DataFrame
    """

    df_rows = []
    all_test_ids_with_failed_dag_runs = []
    all_anomalous_test_ids = []
    configuration_columns = sorted(list(CONFIGURATION_COLUMNS))
    for group_name, group in results_table.groupby(configuration_columns):

        grouped_by_test_ids = group.groupby("uuid")
        test_attempts_in_group = len(grouped_by_test_ids)

        dag_run_failed_counts = grouped_by_test_ids.dag_run_failed_count.first()
        test_ids_with_failed_dag_runs = [
            index for index, value in dag_run_failed_counts.items() if value != 0
        ]

        test_durations = grouped_by_test_ids.test_duration.first()
        test_durations_without_failed_dag_runs = [
            (index, value)
            for index, value in test_durations.items()
            if index not in test_ids_with_failed_dag_runs
        ]

        if test_durations_without_failed_dag_runs:
            mean_test_duration = np.mean(
                [pair[1] for pair in test_durations_without_failed_dag_runs]
            )
            max_test_duration = (1 + ANOMALY_FACTOR) * mean_test_duration
            min_test_duration = (1 - ANOMALY_FACTOR) * mean_test_duration

            anomalous_test_ids = [
                index
                for index, value in test_durations_without_failed_dag_runs
                if value > max_test_duration or value < min_test_duration
            ]
        else:
            anomalous_test_ids = []

        configuration_columns_values = list(group_name)

        df_rows.append(
            configuration_columns_values
            + [
                test_attempts_in_group,
                len(test_ids_with_failed_dag_runs),
                len(anomalous_test_ids),
            ]
        )
        all_test_ids_with_failed_dag_runs += test_ids_with_failed_dag_runs
        all_anomalous_test_ids += anomalous_test_ids

    configuration_information = pd.DataFrame(
        df_rows,
        columns=configuration_columns
        + [
            "total_test_attempts",
            "test_attempts_with_failed_dag_runs",
            "anomalous_test_attempts",
        ],
    )

    return (
        configuration_information,
        all_test_ids_with_failed_dag_runs,
        all_anomalous_test_ids,
    )


def calculate_statistics(
    results_table: pd.DataFrame, resources_dict: Dict, general_metrics: Set[str]
) -> Dict:
    """
    For provided results_table calculates average growth of cumulative metrics
    and average values of gauge and general metrics, grouping resources based on resource types and
    filters from resources_dict.

    :param results_table: pandas Dataframe with performance test results for single study component.
    :type results_table: pd.DataFrame
    :param resources_dict: dict containing information about time series metrics that should be
        analyzed for different resource types and optional filter that control resource aggregation.
    :type resources_dict: Dict
    :param general_metrics: set with metrics referring to a whole test attempt. Average of each of
        these metrics will be calculated across all test attempts from results_table.
    :type general_metrics: Set[str]

    :return: dictionary with averages across all uuids belonging to results_table.
    :rtype: Dict
    """

    uuid_results_dict = {}

    for resource_type in resources_dict:

        analyzed_columns = (
            resources_dict[resource_type]["group_by_columns"]
            + ["timestamp", "test_duration"]
            + resources_dict[resource_type]["cumulative_metrics"]
            + resources_dict[resource_type]["gauge_metrics"]
        )

        results_table_grouped = results_table[analyzed_columns].groupby(
            resources_dict[resource_type]["group_by_columns"]
        )

        uuid_index = resources_dict[resource_type]["group_by_columns"].index("uuid")

        group_filters = resources_dict[resource_type].get("group_filters")

        for group_name, single_resource_data in results_table_grouped:

            if group_filters:
                group_filter_name = check_if_group_filters_apply(
                    group_name,
                    group_filters,
                    resources_dict[resource_type]["group_by_columns"],
                )
                if group_filter_name is None:
                    continue
            else:
                group_filter_name = "all"

            uuid = group_name[uuid_index]
            test_duration = single_resource_data["test_duration"].iloc[0]

            if uuid not in uuid_results_dict:
                uuid_results_dict[uuid] = {}

            if group_filter_name not in uuid_results_dict[uuid]:
                uuid_results_dict[uuid][group_filter_name] = {}

            # in case of pod metrics it is possible to get duplicated entries
            # due to multiple containers on given pod, but with a correct
            # subset of columns we can simply remove duplicated rows;
            # after removing duplicates we should have a single row per
            # timestamp in every group
            single_resource_data = single_resource_data.sort_values(
                ["timestamp"]
            ).drop_duplicates()

            for cumulative_metric in resources_dict[resource_type][
                "cumulative_metrics"
            ]:
                # add _per_second suffix to cumulative metric names
                # to indicate the change in their meaning
                cumulative_metric_growth = f"{cumulative_metric}_per_second"

                if (
                    cumulative_metric_growth
                    not in uuid_results_dict[uuid][group_filter_name]
                ):
                    uuid_results_dict[uuid][group_filter_name][
                        cumulative_metric_growth
                    ] = []

                growth = process_cumulative_metric_for_single_resource(
                    single_resource_data,
                    cumulative_metric,
                    resources_dict[resource_type]["group_by_columns"],
                )

                average_growth = growth / test_duration

                uuid_results_dict[uuid][group_filter_name][
                    cumulative_metric_growth
                ].append(average_growth)

            for gauge_metric in resources_dict[resource_type]["gauge_metrics"]:
                if gauge_metric not in uuid_results_dict[uuid][group_filter_name]:
                    uuid_results_dict[uuid][group_filter_name][gauge_metric] = []

                average_metric_value = process_gauge_metric_for_single_resource(
                    single_resource_data,
                    gauge_metric,
                    resources_dict[resource_type]["group_by_columns"],
                )

                uuid_results_dict[uuid][group_filter_name][gauge_metric].append(
                    average_metric_value
                )

    final_results_dict = {}

    for uuid in uuid_results_dict:

        for group_filter_name in uuid_results_dict[uuid]:

            for metric_name in uuid_results_dict[uuid][group_filter_name]:

                # sum of metric values across resources of specific category
                # (for example airflow-worker containers)

                # TODO: using sum might produce misleading results if pods are restarted during
                #  the test, as there will be more pods/containers than anticipated. Add possibility
                #  to use average for aggregation.
                sum_of_values = np.sum(
                    uuid_results_dict[uuid][group_filter_name][metric_name]
                )

                uuid_results_dict[uuid][group_filter_name][metric_name] = sum_of_values

                if np.isnan(sum_of_values):
                    logger.debug(
                        f"Nan value encountered when calculating value of {metric_name} "
                        f"for {group_filter_name} category of resources for uuid {uuid}. "
                        f"Skipping this uuid in calculating mean value of this metric "
                        f"for this resource category."
                    )
                    continue

                category = "__".join([group_filter_name, metric_name])

                if category not in final_results_dict:
                    final_results_dict[category] = []

                final_results_dict[category].append(sum_of_values)

    # first row for every test attempt
    general_results = results_table.groupby(["uuid"]).first()

    # calculate minimal sample size
    for general_metric in general_metrics:
        general_metric_values = list(general_results[general_metric])
        if general_metric_values:
            final_results_dict[general_metric] = general_metric_values

    # calculate_minimal_sample_size_for_metric(final_results_dict)

    # finally, calculate mean of values across different test attempts (different uuids)
    for category in final_results_dict:
        final_results_dict[category] = np.mean(final_results_dict[category])

    final_results_dict["test_attempts"] = len(general_results)

    return final_results_dict


def calculate_minimal_sample_size_for_metric(
    final_results_dict: Dict[str, List]
) -> dict:
    """
    Calculates minimal sample size based on given sample using model with unknown standard deviation
    for population.

    :param final_results_dict: a dictionary that for different metrics contains list of their
        average values calculated for separate uuids belonging to given results_table.
    :type final_results_dict: Dict[str, List]
    """

    data = {}
    for category in final_results_dict:
        samples = final_results_dict[category]

        if not samples or len(samples) == 1:
            logger.debug(f"No samples for category {category}.")
            continue

        average = np.mean(samples)
        standard_deviation = stdev(samples)
        max_error = average * 0.05
        t_student = t(0.05, len(samples) - 1)
        optimal_number_of_samples = (
            int((t_student * standard_deviation / max_error) ** 2) + 1
        )
        data[category] = optimal_number_of_samples
    return data


def t(alpha, gl):  # pylint: disable=invalid-name
    """Returns value of T-Student for given alpha and gl"""
    return scipy.stats.t.ppf(1 - (alpha / 2), gl)


def check_if_group_filters_apply(
    group_name: Tuple[str, ...],
    group_filters: Dict[str, Dict],
    group_by_columns: List[str],
) -> Optional[str]:
    """
    Returns first of group_filters specified for the resource type that applies to given group_name
    or None if none of them applies to this group.

    :param group_name: tuple that identifies a single group created as a result of grouping
        results_table by group_by_columns.
    :type group_name: Tuple[str, ...]
    :param group_filters: a dictionary containing filters which control how groups of given resource
        type should be aggregated.
    :type group_filters: Dict[str, Dict]
    :param group_by_columns: list of columns by which results_table was grouped to calculate
        performance of given resource type.
    :type group_by_columns: List[str]

    :return: name of the first filter that fully applies to group_name or None
        if none of them applies.
    :rtype: Optional[str]

    :raises: ValueError: if one of group filters contains filter on column that is not
        amongst columns by which metrics of single test attempt were grouped.
    """

    for filter_name in group_filters:

        for column_filter in group_filters[filter_name]:

            if column_filter.endswith(PREFIX_LABEL_INDICATOR):
                column_name = column_filter[: -(len(PREFIX_LABEL_INDICATOR))]
                expected_column_value = group_filters[filter_name][column_filter]
                matching_function = column_value_starts_with
            elif column_filter.endswith(SUFFIX_LABEL_INDICATOR):
                column_name = column_filter[: -(len(SUFFIX_LABEL_INDICATOR))]
                expected_column_value = group_filters[filter_name][column_filter]
                matching_function = column_value_ends_with
            else:
                column_name = column_filter
                expected_column_value = group_filters[filter_name][column_filter]
                matching_function = column_value_equals

            if column_name not in group_by_columns:
                raise ValueError(
                    f"Group filter {filter_name} contains filter on column {column_name} "
                    f"that is not amongst columns "
                    f"by which this resource is grouped: {group_by_columns}."
                )

            index = group_by_columns.index(column_name)

            column_value = group_name[index]

            # if at least one column filter does not apply, then check next group of filters
            if not matching_function(column_value, expected_column_value):
                break

        # if all column_filter from given group_filter apply then we have found
        # a match
        else:
            return filter_name

    return None


def column_value_starts_with(column_value: str, expected_value: str) -> bool:
    """
    Returns True if column_value starts with expected_value and False otherwise.
    """
    return column_value.startswith(expected_value)


def column_value_ends_with(column_value: str, expected_value: str) -> bool:
    """
    Returns True if column_value ends with expected_value and False otherwise.
    """
    return column_value.endswith(expected_value)


def column_value_equals(column_value: str, expected_value: str) -> bool:
    """
    Returns True if column_value is equal to expected_value and False otherwise.
    """
    return column_value == expected_value


def process_gauge_metric_for_single_resource(
    single_resource_data: pd.DataFrame, metric: str, group_by_columns: List[str]
) -> float:
    """
    Calculates average value of gauge metric for single resource
    (node, pod or container) in given test attempt.

    :param single_resource_data: pandas DataFrame which contains time series data of single resource
        (single node, pod or container).
    :type single_resource_data: pd.DataFrame
    :param metric: name of gauge metric average of which should be calculated.
    :type metric: str
    :param group_by_columns: list of columns by which results_table was grouped to calculate
        performance of given resource type.
    :type group_by_columns: List[str]

    :return: float value with average of provided metric or np.nan if DataFrame did not contain a
        single value of that metric.
    :rtype: float
    """

    count = single_resource_data[metric].count()

    # count is 0 if metric column consists only of nans
    if count == 0:
        logger.debug(
            f"It seems that {metric} contains only nan values for "
            f"{identify_resource(single_resource_data, group_by_columns)}. Returning nan."
        )
        return np.nan

    return single_resource_data[metric].sum() / count


def process_cumulative_metric_for_single_resource(
    single_resource_data: pd.DataFrame, metric: str, group_by_columns: List[str]
) -> float:
    """
    Calculates growth of cumulative metric over time for single resource
    (node, pod or container) in given test attempt.

    :param single_resource_data: pandas DataFrame which contains time series data of single resource
        (single node, pod or container). Must be sorted by timestamp.
    :type single_resource_data: pd.DataFrame
    :param metric: name of cumulative metric average growth of which should be calculated.
    :type metric: str
    :param group_by_columns: list of columns by which results_table was grouped to calculate
        performance of given resource type.
    :type group_by_columns: List[str]

    :return: float value with growth per second of provided metric or np.nan if DataFrame
        contains less than two values of that metric.
    :rtype: float
    """

    min_value = np.nan

    for _, row in single_resource_data.iterrows():
        metric_value = row[metric]

        if metric_value is None or np.isnan(metric_value):
            continue

        if np.isnan(min_value):
            min_value = metric_value
            current_max_value = metric_value

            min_timestamp = row["timestamp"]
            current_max_timestamp = row["timestamp"]
            continue

        # the same value as in previous non-nan timestamp is allowed
        if metric_value < current_max_value:
            # TODO: this might happen if pods get restarted during the test - add possibility to
            #  display a warning and skip calculation for given pod/container instead
            logger.debug(
                f"Error when calculating {metric} for "
                f"{identify_resource(single_resource_data, group_by_columns)}. "
                f"The value in latter timestamp is smaller than in a previous one. "
                f"Returning nan."
            )
            return np.nan

        current_max_value = metric_value
        current_max_timestamp = row["timestamp"]

    if np.isnan(min_value):
        logger.debug(
            f"It seems that metric {metric} contains only nan values for "
            f"{identify_resource(single_resource_data, group_by_columns)}. Returning nan."
        )
        return np.nan

    if min_timestamp == current_max_timestamp:
        logger.debug(
            f"It seems that metric {metric} contains only a single non-nan value for "
            f"{identify_resource(single_resource_data, group_by_columns)}. and its growth "
            f"cannot be calculated. Returning nan."
        )
        return np.nan

    # growth = (current_max_value - min_value) / (current_max_timestamp - min_timestamp)

    growth = current_max_value - min_value

    return growth

    # in case of 'breaks' in rising of cumulative metric
    # (for example due to pod restart) we could probably take
    # the total increase of metric value divided by total timespan

    #
    # 7
    # 6       /
    # 5      /
    # 4     /
    # 3    /              /
    # 2   /              /
    # 1  /              /
    # 0  1  2  3  4  5  6  7  8

    # so in this case:
    # increase: 6 + 3
    # timespan: 2 + 1


def identify_resource(
    single_resource_data: pd.DataFrame, group_by_columns: List[str]
) -> str:
    """
    Returns a string identifying the resource data of which is stored in single_resource_data.

    :param single_resource_data: pandas DataFrame which contains time series data of single resource
        (single node, pod or container).
    :type single_resource_data: pd.DataFrame
    :param group_by_columns: list of columns by which results_table was grouped to calculate
        performance of this resource type.
    :type group_by_columns: List[str]

    :return: string identifying the resource.
    :rtype: str
    """
    string_parts = []

    for column in group_by_columns:
        string_parts.append(f"{column}: {single_resource_data[column].iloc[0]}")

    return ", ".join(string_parts)


def collect_metrics(results_table: pd.DataFrame, resources_dict: Dict) -> Dict:
    """
    For provided results_table returns time series of chosen metrics,
    grouping resources based on resource types and filters from resources_dict.

    :param results_table: pandas Dataframe with performance test results for single study component.
    :type results_table: pd.DataFrame
    :param resources_dict: dict containing information about time series metrics that should be
        collected for different resource types and optional filter that control resource aggregation
    :type resources_dict: Dict

    :return: a dictionary with time series of chosen metrics separately for every uuid from
        results_table.
    :rtype: Dict
    """
    # TODO: address this copy/paste code
    uuid_results_dict = {}

    for resource_type in resources_dict:

        analyzed_columns = (
            resources_dict[resource_type]["group_by_columns"]
            + ["seconds_from_normalized_start"]
            + resources_dict[resource_type]["cumulative_metrics"]
            + resources_dict[resource_type]["gauge_metrics"]
        )

        results_table_grouped = results_table[analyzed_columns].groupby(
            resources_dict[resource_type]["group_by_columns"]
        )

        uuid_index = resources_dict[resource_type]["group_by_columns"].index("uuid")

        group_filters = resources_dict[resource_type].get("group_filters")

        for group_name, single_resource_data in results_table_grouped:

            if group_filters:
                group_filter_name = check_if_group_filters_apply(
                    group_name,
                    group_filters,
                    resources_dict[resource_type]["group_by_columns"],
                )
                if group_filter_name is None:
                    continue
            else:
                group_filter_name = "all"

            uuid = group_name[uuid_index]

            if uuid not in uuid_results_dict:
                uuid_results_dict[uuid] = {}

            if group_filter_name not in uuid_results_dict[uuid]:
                uuid_results_dict[uuid][group_filter_name] = {}

            # in case of pod metrics it is possible to get duplicated entries
            # due to multiple containers on given pod, but with a correct
            # subset of columns we can simply remove duplicated rows;
            # after removing duplicates we should have a single row per
            # seconds_from_normalized_start in every group
            single_resource_data = single_resource_data.sort_values(
                ["seconds_from_normalized_start"]
            ).drop_duplicates()

            for metric in (
                resources_dict[resource_type]["cumulative_metrics"]
                + resources_dict[resource_type]["gauge_metrics"]
            ):

                if metric not in uuid_results_dict[uuid][group_filter_name]:
                    uuid_results_dict[uuid][group_filter_name][metric] = []

                metric_time_series = get_time_series_for_single_resource(
                    single_resource_data,
                    metric,
                    resources_dict[resource_type]["group_by_columns"],
                )

                uuid_results_dict[uuid][group_filter_name][metric].append(
                    metric_time_series
                )

    final_results_dict = {}

    for uuid in uuid_results_dict:

        for group_filter_name in uuid_results_dict[uuid]:

            for metric_name in uuid_results_dict[uuid][group_filter_name]:

                # sum of metric values across timestamps for resources belonging to same category
                # (for example airflow-worker containers)

                # TODO: using sum might produce misleading results if pods are restarted during
                #  the test, as there will be more pods/containers than anticipated. Add possibility
                #  to use average for aggregation.

                category = "__".join([group_filter_name, metric_name])

                summed_time_series = sum_values_of_time_series(
                    uuid_results_dict[uuid][group_filter_name][metric_name],
                    uuid,
                    category,
                )

                if uuid not in final_results_dict:
                    final_results_dict[uuid] = {}

                final_results_dict[uuid][category] = summed_time_series

    return final_results_dict


# pylint: disable=unused-argument
def get_time_series_for_single_resource(
    single_resource_data: pd.DataFrame, metric: str, group_by_columns: List[str]
) -> List[Tuple[int, float]]:
    """
    Gets time series of metric for single resource (node, pod or container) in given test attempt.

    :param single_resource_data: pandas DataFrame which contains time series data of single resource
        (single node, pod or container).
    :type single_resource_data: pd.DataFrame
    :param metric: name of gauge metric average of which should be calculated.
    :type metric: str
    :param group_by_columns: list of columns by which results_table was grouped to calculate
        performance of given resource type.
    :type group_by_columns: List[str]

    :return: list of pairs with first value being number of seconds since test start and second
        being metric value
    :rtype: List[Tuple[int, float]]
    """
    time_series = []

    for _, row in single_resource_data.iterrows():

        metric_value = row[metric]

        if metric_value is None or np.isnan(metric_value):
            continue

        seconds_from_normalized_start = int(row["seconds_from_normalized_start"])

        time_series.append((seconds_from_normalized_start, metric_value))

    return time_series


def sum_values_of_time_series(
    time_series_list: List[List[Tuple[int, float]]], uuid: str, category: str
) -> Optional[List[Tuple[int, float]]]:
    """
    Combines time series of multiple resources belonging to same category (for example:
    airflow-worker containers) by summing values at given timestamp.

    :param time_series_list: list of time series, each being a list of tuples
    :type time_series_list: List[List[Tuple[int, float]]]
    :param uuid: uuid of test attempt to which given list of time series belongs
    :type uuid: str
    :param category: category of given list of time series
    :type category: str

    :return: a single time series created by combining the input ones or None if an issue occured
        during calculations
    :rtype: List[Tuple[int, float]]
    """
    if len(time_series_list) == 1:
        return time_series_list[0]

    combined_time_series = []

    for index, time_series in enumerate(time_series_list):
        # sort time series by their seconds_from_normalized_start value in reveresed order
        time_series_list[index] = sorted(time_series, key=lambda x: x[0], reverse=True)

    # fix desynchronization at the beginning of the test
    first_indexes_to_remove = []
    while True:
        first_timestamps = [time_series[-1][0] for time_series in time_series_list]
        if max(first_timestamps) - min(first_timestamps) > MAX_TIMESTAMP_DIFFERENCE:
            min_index = first_timestamps.index(min(first_timestamps))
            if min_index in first_indexes_to_remove:
                logger.debug(
                    f"An issue occured when combining time series from category {category} for "
                    f"uuid {uuid}. Desynchronization at the beginning"
                )
                return None
            first_indexes_to_remove.append(min_index)
            time_series_list[min_index] = time_series_list[min_index][:-1]
        else:
            break

    # fix desynchronization at the end of the test
    last_indexes_to_remove = []
    while True:
        last_timestamps = [time_series[0][0] for time_series in time_series_list]
        if max(last_timestamps) - min(last_timestamps) > MAX_TIMESTAMP_DIFFERENCE:
            max_index = last_timestamps.index(max(last_timestamps))
            if max_index in last_indexes_to_remove:
                logger.debug(
                    f"An issue occured when combining time series from category {category} for "
                    f"uuid {uuid}. Desynchronization at the end"
                )
                return None
            last_indexes_to_remove.append(max_index)
            time_series_list[max_index] = time_series_list[max_index][1:]
        else:
            break

    time_series_lengths = [len(time_series) for time_series in time_series_list]

    if len(set(time_series_lengths)) != 1:
        logger.debug(
            f"An issue occured when combining time series from category {category} for uuid "
            f"{uuid} - time series appear are desynchronized even after an attempt to "
            f"desynchronize them. Returning nan."
        )
        return None

    # keep combining points from the same time stamp until an error occurs or you combine all points
    while True:

        # if all time series are empty, we can return the results. We check only the first one
        # because we previosuly checked that they are of the same lenght
        if not time_series_list[0]:
            return combined_time_series

        points = [time_series.pop() for time_series in time_series_list]

        timestamps = [point[0] for point in points]

        if max(timestamps) - min(timestamps) > MAX_TIMESTAMP_DIFFERENCE:
            logger.debug(
                f"An issue occured when combining time series from category {category} for uuid "
                f"{uuid} - the difference between next points from same category are not within "
                f"range of {MAX_TIMESTAMP_DIFFERENCE} seconds."
            )
            return None

        values = [point[1] for point in points]

        combined_time_series.append((int(np.mean(timestamps)), sum(values)))


cache_dir = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.path.pardir, ".reports_cache"
)


def cache_dataframe(func):
    """
    Caches dataframe to avoid
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if args:
            raise ValueError(
                "Using cache requires you to pass argument using keyword arguments."
            )
        cache_name = "__".join([f"{key}_{value}" for key, value in kwargs.items()])
        cache_file = os.path.join(cache_dir, f"cache_{cache_name}.csv")

        os.makedirs(cache_dir, exist_ok=True)
        if os.path.isfile(cache_file):
            data = pd.read_csv(cache_file)
            logger.info(f"Reusing cache. Data size: {data.size}")
            return data
        df: pd.DataFrame = func(**kwargs)
        df.to_csv(cache_file)
        return df

    return wrapper
