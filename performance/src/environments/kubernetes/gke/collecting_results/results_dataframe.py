"""
Methods used to combine information regarding performance test
collected from different sources into a single dataframe.
"""

import getpass
import logging
import uuid
from collections import OrderedDict
from typing import Iterable, Union

import pandas as pd

from environments.kubernetes.gke.collecting_results.metrics_dataframe import (
    prepare_metrics_dataframe,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def prepare_results_dataframe(
    project_id: str,
    cluster_id: str,
    airflow_namespace_prefix: str,
    environment_columns: OrderedDict,
    elastic_dag_columns: OrderedDict,
    airflow_configuration: OrderedDict,
    airflow_statistics: OrderedDict,
) -> pd.DataFrame:
    """
    Collects Cloud Monitoring metrics and combines it with information collected from other sources
    into a single dataframe.

    :param project_id: Google Cloud project where the resources metrics of which are collected
        are located in.
    :type project_id: str
    :param cluster_id: id of GKE cluster metrics of which should be collected.
    :type cluster_id: str
    :param airflow_namespace_prefix: prefix of the namespace where airflow pods are located.
    :type airflow_namespace_prefix: str
    :param environment_columns: a dict containing columns with test environment configuration.
    :type environment_columns: OrderedDict
    :param elastic_dag_columns: a dict containing columns with elastic dag environment variables.
    :type elastic_dag_columns: OrderedDict
    :param airflow_configuration: a dict containing columns with airflow configuration
        environment variables.
    :type airflow_configuration: OrderedDict
    :param airflow_statistics: a dict containing columns with airflow statistics
        regarding test Dag Runs execution.
    :type airflow_statistics: OrderedDict

    :return: pandas Dataframe containing combined information about performance test.
    :rtype: pd.DataFrame
    """

    name_to_hash = "__".join(
        [
            environment_columns["environment_name"],
            environment_columns["environment_type"],
            airflow_statistics["test_start_date"],
        ]
    )

    test_id = uuid.uuid5(uuid.NAMESPACE_DNS, name_to_hash)

    user = getpass.getuser()

    results_df = prepare_metrics_dataframe(
        project_id=project_id,
        cluster_id=cluster_id,
        airflow_namespace_prefix=airflow_namespace_prefix,
        start_date=airflow_statistics["test_start_date"],
        end_date=airflow_statistics["test_end_date"],
    )

    # new columns are added starting from the beginning of the metrics dataframe
    location = add_column_to_dataframe(
        dataframe=results_df, column_name="uuid", column_value=test_id, location=0
    )
    location = add_column_to_dataframe(
        dataframe=results_df, column_name="user", column_value=user, location=location
    )

    # columns from other sources are added in specific order
    for dictionary in [
        environment_columns,
        airflow_configuration,
        elastic_dag_columns,
        airflow_statistics,
    ]:
        for column_name in dictionary:
            location = add_column_to_dataframe(
                dataframe=results_df,
                column_name=column_name,
                column_value=dictionary[column_name],
                location=location,
            )

    return results_df


# pylint: enable=too-many-arguments
# pylint: enable=too-many-locals


def add_column_to_dataframe(
    dataframe: pd.DataFrame,
    column_name: str,
    column_value: Union[bool, int, float, str, uuid.UUID, pd.Series, Iterable],
    location: int,
) -> int:
    """
    Inserts a new column to the dataframe at specified location, provided said column
    is not already present in the dataframe.

    :param dataframe: pandas DataFrame to which a new column should be inserted.
    :type dataframe: pd.DataFrame
    :param column_name: name of column to be inserted.
    :type column_name: str
    :param column_value: a single value or an iterable of column values.
    :type column_value: Union[bool, int, float, str, uuid.UUID, pd.Series, Iterable]
    :param location: location in dataframe at which the new column should be inserted.
    :type location: int

    :return: a location incremented by 1 if column was inserted and the same location otherwise.
    :rtype: int
    """

    if column_name in dataframe.columns:
        log.warning(
            "Column '%s' is already present in results dataframe. " "Column was not overwritten.",
            column_name,
        )
        return location

    dataframe.insert(location, column_name, column_value)

    return location + 1
