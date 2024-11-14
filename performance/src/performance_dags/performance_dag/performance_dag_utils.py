#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import json
import logging
import os
import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta
from shutil import copyfile
from typing import Callable

import re2 as re

import airflow

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

MANDATORY_performance_DAG_VARIABLES = {
    "PERF_DAGS_COUNT",
    "PERF_TASKS_COUNT",
    "PERF_SHAPE",
    "PERF_START_DATE",
}

performance_DAG_VARIABLES_DEFAULT_VALUES = {
    "PERF_DAG_FILES_COUNT": "1",
    "PERF_DAG_PREFIX": "perf_scheduler",
    "PERF_START_AGO": "1h",
    "PERF_SCHEDULE_INTERVAL": "@once",
    "PERF_SLEEP_TIME": "0",
    "PERF_OPERATOR_TYPE": "bash",
    "PERF_MAX_RUNS": None,
    "PERF_START_PAUSED": "1",
}

ALLOWED_SHAPES = ("no_structure", "linear", "binary_tree", "star", "grid")

ALLOWED_OPERATOR_TYPES = ("bash", "big_query_insert_job", "python")

ALLOWED_TASKS_TRIGGER_RULES = ("all_success", "all_failed")

# "None" schedule interval is not supported for now so that dag runs are created automatically
ALLOWED_NON_REGEXP_SCHEDULE_INTERVALS = ("@once",)

DAG_IDS_NOT_ALLOWED_TO_MATCH_PREFIX = ("airflow_monitoring",)

RE_TIME_DELTA = re.compile(
    r"^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$"
)


def add_perf_start_date_env_to_conf(performance_dag_conf: dict[str, str]) -> None:
    """
    Calculate start date based on configuration.

    Calculates value for PERF_START_DATE environment variable and adds it to the performance_dag_conf
    if it is not already present there.

    :param performance_dag_conf: dict with environment variables as keys and their values as values
    """
    if "PERF_START_DATE" not in performance_dag_conf:
        start_ago = get_performance_dag_environment_variable(performance_dag_conf, "PERF_START_AGO")

        perf_start_date = airflow.utils.timezone.utcnow - check_and_parse_time_delta(
            "PERF_START_AGO", start_ago
        )

        performance_dag_conf["PERF_START_DATE"] = str(perf_start_date)


def validate_performance_dag_conf(performance_dag_conf: dict[str, str]) -> None:
    """
    Check `performance_dag_conf` contains a valid configuration for performance DAG.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :raises:
        TypeError: if performance_dag_conf is not a dict
        KeyError: if performance_dag_conf does not contain mandatory environment variables
        ValueError: if any value in performance_dag_conf is not a string
    """
    if not isinstance(performance_dag_conf, dict):
        raise TypeError(
            f"performance_dag configuration must be a dictionary containing at least following keys: "
            f"{MANDATORY_performance_DAG_VARIABLES}."
        )

    missing_variables = MANDATORY_performance_DAG_VARIABLES.difference(set(performance_dag_conf.keys()))

    if missing_variables:
        raise KeyError(
            f"Following mandatory environment variables are missing "
            f"from performance_dag configuration: {missing_variables}."
        )

    if not all(isinstance(env, str) for env in performance_dag_conf.values()):
        raise ValueError("All values of variables must be strings.")

    variable_to_validation_fun_map = {
        "PERF_DAGS_COUNT": check_positive_int_convertibility,
        "PERF_TASKS_COUNT": check_positive_int_convertibility,
        "PERF_START_DATE": check_datetime_convertibility,
        "PERF_DAG_FILES_COUNT": check_positive_int_convertibility,
        "PERF_DAG_PREFIX": check_dag_prefix,
        "PERF_START_AGO": check_and_parse_time_delta,
        "PERF_SCHEDULE_INTERVAL": check_schedule_interval,
        "PERF_SHAPE": get_check_allowed_values_function(ALLOWED_SHAPES),
        "PERF_SLEEP_TIME": check_non_negative_float_convertibility,
        "PERF_OPERATOR_TYPE": get_check_allowed_values_function(ALLOWED_OPERATOR_TYPES),
        "PERF_MAX_RUNS": check_positive_int_convertibility,
        "PERF_START_PAUSED": check_int_convertibility,
        "PERF_TASKS_TRIGGER_RULE": get_check_allowed_values_function(ALLOWED_TASKS_TRIGGER_RULES),
        "PERF_OPERATOR_EXTRA_KWARGS": check_valid_json,
    }

    # we do not need to validate default values of variables
    for env_name in variable_to_validation_fun_map:
        if env_name in performance_dag_conf:
            variable_to_validation_fun_map[env_name](env_name, performance_dag_conf[env_name])

    check_max_runs_and_schedule_interval_compatibility(performance_dag_conf)


def check_int_convertibility(env_name: str, env_value: str) -> None:
    """
    Check if value of provided environment variable is convertible to int value.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.

    :raises: ValueError: if env_value could not be converted to int value
    """
    try:
        int(env_value)
    except ValueError:
        raise ValueError(f"{env_name} value must be convertible to int. Received: '{env_value}'.")


def check_positive_int_convertibility(env_name: str, env_value: str) -> None:
    """
    Check if string value is a positive integer.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.

    :raises: ValueError: if env_value could not be converted to positive int value
    """
    try:
        converted_value = int(env_value)
        check_positive(converted_value)
    except ValueError:
        raise ValueError(f"{env_name} value must be convertible to positive int. Received: '{env_value}'.")


def check_positive(value: int | float) -> None:
    """Check if provided value is positive and raises ValueError otherwise."""
    if value <= 0:
        raise ValueError


def check_datetime_convertibility(env_name: str, env_value: str) -> None:
    """
    Check if value of provided environment variable is a date string in expected format.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.
    """
    try:
        datetime.strptime(env_value, "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        raise ValueError(
            f"Value '{env_value}' of {env_name} cannot be converted "
            f"to datetime object in '%Y-%m-%d %H:%M:%S.%f' format."
        )


def check_dag_prefix(env_name: str, env_value: str) -> None:
    """
    Validate dag prefix value.

    Checks if value of dag prefix env variable is a prefix for one of the forbidden dag ids
    (which would cause runs of corresponding DAGs to be collected alongside the real test Dag Runs).
    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.
    """
    # TODO: allow every environment type to specify its own "forbidden" matching dag ids

    safe_dag_prefix = safe_dag_id(env_value)

    matching_dag_ids = [
        dag_id for dag_id in DAG_IDS_NOT_ALLOWED_TO_MATCH_PREFIX if dag_id.startswith(safe_dag_prefix)
    ]

    if matching_dag_ids:
        raise ValueError(
            f"Value '{env_value}' of {env_name} is not allowed as {safe_dag_prefix} is a prefix "
            f"for the following forbidden dag ids: {matching_dag_ids}"
        )


def safe_dag_id(dag_id: str) -> str:
    """Remove characters that are invalid in dag id from provided string."""
    return re.sub("[^0-9a-zA-Z_]+", "_", dag_id)


def check_and_parse_time_delta(env_name: str, env_value: str) -> timedelta:
    """
    Validate and parse time delta value.

    Check if value of provided environment variable is a parsable time expression
    and returns timedelta object with duration.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.

    :return: a timedelta object with the duration specified in env_value string
    :rtype: timedelta

    :raises: ValueError: if env_value could not be parsed
    """
    parts = RE_TIME_DELTA.match(env_value)

    if parts is None:
        raise ValueError(
            f"Could not parse any time information from '{env_value}' value of {env_name}. "
            f"Examples of valid strings: '8h', '2d8h5m20s', '2m4s'"
        )

    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)


def check_schedule_interval(env_name: str, env_value: str) -> None:
    """
    Validate schedule_interval value.

    Checks if value of schedule_interval is a parsable time expression
    or within a specified set of non-parsable values.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.

    :raises: ValueError: if env_value is neither a parsable time expression
        nor one of allowed non-parsable values
    """
    try:
        check_and_parse_time_delta(env_name, env_value)
        return
    except ValueError as exception:
        error_message = str(exception)

    check_allowed_values = get_check_allowed_values_function(ALLOWED_NON_REGEXP_SCHEDULE_INTERVALS)

    try:
        check_allowed_values(env_name, env_value)
    except ValueError:
        log.error(error_message)
        raise ValueError(
            f"Value '{env_value}' of {env_name} is neither a parsable time expression "
            f"nor one of the following: {ALLOWED_NON_REGEXP_SCHEDULE_INTERVALS}."
        )


def get_check_allowed_values_function(
    values: tuple[str, ...],
) -> Callable[[str, str], None]:
    """
    Return function that validates environment variable value.

    Returns a function which will check if value of provided environment variable
    is within a specified set of values

    :param values: tuple of any length with allowed string values of environment variable

    :return: a function that checks if given environment variable's value is within the specified
        set of values and raises ValueError otherwise
    :rtype: Callable[[str, str], None]
    """

    def check_allowed_values(env_name: str, env_value: str) -> None:
        """
        Check if value of provided environment variable is within a specified set of values.

        :param env_name: name of the environment variable which is being checked.
        :param env_value: value of the variable.

        :raises: ValueError: if env_value is not within a specified set of values
        """
        if env_value not in values:
            raise ValueError(
                f"{env_name} value must be one of the following: {values}. Received: '{env_value}'."
            )

    return check_allowed_values


def check_non_negative_float_convertibility(env_name: str, env_value: str) -> None:
    """
    Validate if a string is parsable float.

    Checks if value of provided environment variable is convertible to non negative float value.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.

    :raises: ValueError: if env_value could not be converted to non negative float value
    """
    try:
        converted_value = float(env_value)
        check_non_negative(converted_value)
    except ValueError:
        raise ValueError(
            f"{env_name} value must be convertible to non negative float. Received: '{env_value}'."
        )


def check_non_negative(value: int | float) -> None:
    """Check if provided value is not negative and raises ValueError otherwise."""
    if value < 0:
        raise ValueError


def check_max_runs_and_schedule_interval_compatibility(
    performance_dag_conf: dict[str, str],
) -> None:
    """
    Validate max_runs value.

    Check if max_runs and schedule_interval values create a valid combination.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :raises: ValueError:
        if max_runs is specified when schedule_interval is not a duration time expression
        if max_runs is not specified when schedule_interval is a duration time expression
        if max_runs, schedule_interval and start_ago form a combination which causes end_date
            to be in the future
    """
    schedule_interval = get_performance_dag_environment_variable(
        performance_dag_conf, "PERF_SCHEDULE_INTERVAL"
    )
    max_runs = get_performance_dag_environment_variable(performance_dag_conf, "PERF_MAX_RUNS")
    start_ago = get_performance_dag_environment_variable(performance_dag_conf, "PERF_START_AGO")

    if schedule_interval == "@once":
        if max_runs is not None:
            raise ValueError(
                "PERF_MAX_RUNS is allowed only if PERF_SCHEDULE_INTERVAL is provided as a time expression."
            )
        # if dags are set to be scheduled once, we do not need to check end_date
        return

    if max_runs is None:
        raise ValueError(
            "PERF_MAX_RUNS must be specified if PERF_SCHEDULE_INTERVAL is provided as a time expression."
        )

    max_runs = int(max_runs)

    # make sure that the end_date does not occur in future
    current_date = datetime.now()

    start_date = current_date - check_and_parse_time_delta("PERF_START_AGO", start_ago)

    end_date = start_date + (
        check_and_parse_time_delta("PERF_SCHEDULE_INTERVAL", schedule_interval) * (max_runs - 1)
    )

    if current_date < end_date:
        raise ValueError(
            f"PERF_START_AGO ({start_ago}), "
            f"PERF_SCHEDULE_INTERVAL ({schedule_interval}) "
            f"and PERF_MAX_RUNS ({max_runs}) "
            f"must be specified in such a way that end_date does not occur in the future "
            f"(end_date with provided values: {end_date})."
        )


def check_valid_json(env_name: str, env_value: str) -> None:
    """
    Validate json string.

    Check if value of provided environment variable is a valid json.

    :param env_name: name of the environment variable which is being checked.
    :param env_value: value of the variable.
    """
    try:
        json.loads(env_value)
    except json.decoder.JSONDecodeError:
        raise ValueError(f"Value '{env_value}' of {env_name} cannot be json decoded.")


@contextmanager
def generate_copies_of_performance_dag(
    performance_dag_path: str, performance_dag_conf: dict[str, str]
) -> tuple[str, list[str]]:
    """
    Create context manager that creates copies of DAG.

    Contextmanager that creates copies of performance DAG inside temporary directory using the
    dag prefix env variable as a base for filenames.

    :param performance_dag_path: path to the performance DAG that should be copied.
    :param performance_dag_conf: dict with environment variables as keys and their values as values.

    :yields: a pair consisting of path to the temporary directory
        and a list with paths to copies of performance DAG
    :type: Tuple[str, List[str]]
    """
    dag_files_count = int(
        get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAG_FILES_COUNT")
    )

    safe_dag_prefix = get_dag_prefix(performance_dag_conf)

    with tempfile.TemporaryDirectory() as temp_dir:
        performance_dag_copies = []

        for i in range(1, dag_files_count + 1):
            destination_filename = f"{safe_dag_prefix}_{i}.py"
            destination_path = os.path.join(temp_dir, destination_filename)

            copyfile(performance_dag_path, destination_path)
            performance_dag_copies.append(destination_path)

        yield temp_dir, performance_dag_copies


def get_dag_prefix(performance_dag_conf: dict[str, str]) -> str:
    """
    Return DAG prefix.

    Returns prefix that will be assigned to DAGs created with given performance DAG configuration.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :return: final form of prefix after substituting inappropriate characters
    :rtype: str
    """
    dag_prefix = get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAG_PREFIX")

    safe_dag_prefix = safe_dag_id(dag_prefix)

    return safe_dag_prefix


def get_dags_count(performance_dag_conf: dict[str, str]) -> int:
    """
    Return the number of test DAGs based on given performance DAG configuration.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :return: number of test DAGs
    :rtype: int
    """
    dag_files_count = int(
        get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAG_FILES_COUNT")
    )

    dags_per_dag_file = int(get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAGS_COUNT"))

    return dag_files_count * dags_per_dag_file


def calculate_number_of_dag_runs(performance_dag_conf: dict[str, str]) -> int:
    """
    Calculate how many Dag Runs will be created with given performance DAG configuration.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :return: total number of Dag Runs
    :rtype: int
    """
    max_runs = get_performance_dag_environment_variable(performance_dag_conf, "PERF_MAX_RUNS")

    total_dags_count = get_dags_count(performance_dag_conf)

    # if PERF_MAX_RUNS is missing from the configuration,
    # it means that PERF_SCHEDULE_INTERVAL must be '@once'
    if max_runs is None:
        return total_dags_count

    return int(max_runs) * total_dags_count


def prepare_performance_dag_columns(
    performance_dag_conf: dict[str, str],
) -> OrderedDict:
    """
    Prepare dict containing DAG env variables.

    Prepare an OrderedDict containing chosen performance dag environment variables
    that will serve as columns for the results dataframe.

    :param performance_dag_conf: dict with environment variables as keys and their values as values

    :return: a dict with a subset of environment variables
        in order in which they should appear in the results dataframe
    :rtype: OrderedDict
    """
    max_runs = get_performance_dag_environment_variable(performance_dag_conf, "PERF_MAX_RUNS")

    # TODO: if PERF_MAX_RUNS is missing from configuration, then PERF_SCHEDULE_INTERVAL must
    #  be '@once'; this is an equivalent of PERF_MAX_RUNS being '1', which will be the default value
    #  once PERF_START_AGO and PERF_SCHEDULE_INTERVAL are removed

    # TODO: we should not ban PERF_SCHEDULE_INTERVAL completely because we will make it impossible
    #  to run time-based tests (where you run dags constantly for 1h for example). I think we should
    #  allow setting of only one of them.
    #  If PERF_MAX_RUNS is set, then PERF_SCHEDULE_INTERVAL should be ignored - default value of 1h
    #  should be used combined with PERF_START_AGO so that expected number of runs can be created immediately
    #  If PERF_SCHEDULE_INTERVAL is set and PERF_MAX_RUNS is not, then PERF_START_AGO should be set
    #  to current date so that dag runs start creating now instead of creating multiple runs from the
    #  past - but it will be rather hard taking into account time of environment creation. Wasn't
    #  there some dag option to NOT create past runs? -> catchup
    #  ALSO either PERF_MAX_RUNS or PERF_SCHEDULE_INTERVAL OR both should be included in results file
    if max_runs is None:
        max_runs = 1
    else:
        max_runs = int(max_runs)

    performance_dag_columns = OrderedDict(
        [
            (
                "PERF_DAG_FILES_COUNT",
                int(get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAG_FILES_COUNT")),
            ),
            (
                "PERF_DAGS_COUNT",
                int(get_performance_dag_environment_variable(performance_dag_conf, "PERF_DAGS_COUNT")),
            ),
            (
                "PERF_TASKS_COUNT",
                int(get_performance_dag_environment_variable(performance_dag_conf, "PERF_TASKS_COUNT")),
            ),
            ("PERF_MAX_RUNS", max_runs),
            (
                "PERF_SCHEDULE_INTERVAL",
                get_performance_dag_environment_variable(performance_dag_conf, "PERF_SCHEDULE_INTERVAL"),
            ),
            (
                "PERF_SHAPE",
                get_performance_dag_environment_variable(performance_dag_conf, "PERF_SHAPE"),
            ),
            (
                "PERF_SLEEP_TIME",
                float(get_performance_dag_environment_variable(performance_dag_conf, "PERF_SLEEP_TIME")),
            ),
            (
                "PERF_OPERATOR_TYPE",
                get_performance_dag_environment_variable(performance_dag_conf, "PERF_OPERATOR_TYPE"),
            ),
        ]
    )

    add_performance_dag_configuration_type(performance_dag_columns)

    return performance_dag_columns


def get_performance_dag_environment_variable(performance_dag_conf: dict[str, str], env_name: str) -> str:
    """
    Get env variable value.

    Returns value of environment variable with given env_name based on provided `performance_dag_conf`.

    :param performance_dag_conf: dict with environment variables as keys and their values as values
    :param env_name: name of the environment variable value of which should be returned.

    :return: value of environment variable taken from performance_dag_conf or its default value, if it
        was not present in the dictionary (if applicable)
    :rtype: str

    :raises: ValueError:
        if env_name is a mandatory environment variable but it is missing from performance_dag_conf
        if env_name is not a valid name of an performance dag environment variable
    """
    if env_name in MANDATORY_performance_DAG_VARIABLES:
        if env_name not in performance_dag_conf:
            raise ValueError(
                f"Mandatory environment variable '{env_name}' "
                f"is missing from performance dag configuration."
            )
        return performance_dag_conf[env_name]

    if env_name not in performance_DAG_VARIABLES_DEFAULT_VALUES:
        raise ValueError(
            f"Provided environment variable '{env_name}' is not a valid performance dag"
            f"configuration variable."
        )

    return performance_dag_conf.get(env_name, performance_DAG_VARIABLES_DEFAULT_VALUES[env_name])


def add_performance_dag_configuration_type(
    performance_dag_columns: OrderedDict,
) -> None:
    """
    Add a key with type of given performance dag configuration to the columns dict.

    :param performance_dag_columns: a dict with columns containing performance dag configuration
    """
    performance_dag_configuration_type = "__".join(
        [
            f"{performance_dag_columns['PERF_SHAPE']}",
            f"{performance_dag_columns['PERF_DAG_FILES_COUNT']}_dag_files",
            f"{performance_dag_columns['PERF_DAGS_COUNT']}_dags",
            f"{performance_dag_columns['PERF_TASKS_COUNT']}_tasks",
            f"{performance_dag_columns['PERF_MAX_RUNS']}_dag_runs",
            f"{performance_dag_columns['PERF_SLEEP_TIME']}_sleep",
            f"{performance_dag_columns['PERF_OPERATOR_TYPE']}_operator",
        ]
    )

    performance_dag_columns.update({"performance_dag_configuration_type": performance_dag_configuration_type})

    # move the type key to the beginning of dict
    performance_dag_columns.move_to_end("performance_dag_configuration_type", last=False)


def parse_time_delta(time_str):
    # type: (str) -> datetime.timedelta
    """
    Parse a time string e.g. (2h13m) into a timedelta object.

    :param time_str: A string identifying a duration.  (eg. 2h13m)
    :return datetime.timedelta: A datetime.timedelta object or "@once"
    """
    parts = RE_TIME_DELTA.match(time_str)

    if parts is None:
        raise ValueError(
            f"Could not parse any time information from '{time_str}'. "
            "Examples of valid strings: '8h', '2d8h5m20s', '2m4s'"
        )

    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)  # type: ignore


def parse_start_date(date, start_ago):
    """
    Parse date or relative distance to current time.

    Returns the start date for the performance DAGs and string to be used as part of their ids.

    :return Tuple[datetime.datetime, str]: A tuple of datetime.datetime object to be used
        as a start_date and a string that should be used as part of the dag_id.
    """
    if date:
        start_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
        dag_id_component = str(int(start_date.timestamp()))
    else:
        start_date = datetime.now() - parse_time_delta(start_ago)
        dag_id_component = start_ago
    return start_date, dag_id_component


def parse_schedule_interval(time_str):
    # type: (str) -> datetime.timedelta
    """
    Parse a schedule interval string e.g. (2h13m) or "@once".

    :param time_str: A string identifying a schedule interval.  (eg. 2h13m, None, @once)
    :return datetime.timedelta: A datetime.timedelta object or "@once" or None
    """
    if time_str == "None":
        return None

    if time_str == "@once":
        return "@once"

    return parse_time_delta(time_str)
