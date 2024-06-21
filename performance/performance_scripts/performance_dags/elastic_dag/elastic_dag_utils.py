"""
Module containing functions used for validation of elastic dag configuration
and collecting information about it's resulting DAGs' arrangement
"""

import json
import logging
import os
import re
import tempfile
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta
from shutil import copyfile
from typing import Callable, Dict, List, Tuple, Union

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

MANDATORY_ELASTIC_DAG_VARIABLES = {
    "PERF_DAGS_COUNT",
    "PERF_TASKS_COUNT",
    "PERF_SHAPE",
    "PERF_START_DATE",
}

ELASTIC_DAG_VARIABLES_DEFAULT_VALUES = {
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


def add_perf_start_date_env_to_conf(elastic_dag_conf: Dict[str, str]) -> None:
    """
    Calculates value for PERF_START_DATE environment variable and adds it to the elastic_dag_conf
    if it is not already present there.

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]
    """

    if "PERF_START_DATE" not in elastic_dag_conf:
        start_ago = get_elastic_dag_environment_variable(
            elastic_dag_conf, "PERF_START_AGO"
        )

        perf_start_date = datetime.utcnow() - check_and_parse_time_delta(
            "PERF_START_AGO", start_ago
        )

        elastic_dag_conf["PERF_START_DATE"] = str(perf_start_date)


def validate_elastic_dag_conf(elastic_dag_conf: Dict[str, str]) -> None:
    """
    Checks if elastic_dag_conf contains a valid configuration for elastic DAG

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :raises:
        TypeError: if elastic_dag_conf is not a Dict
        KeyError: if elastic_dag_conf does not contain mandatory environment variables
        ValueError: if any value in elastic_dag_conf is not a string
    """

    if not isinstance(elastic_dag_conf, Dict):
        raise TypeError(
            f"elastic_dag configuration must be a dictionary containing at least following keys: "
            f"{MANDATORY_ELASTIC_DAG_VARIABLES}."
        )

    missing_variables = MANDATORY_ELASTIC_DAG_VARIABLES.difference(
        set(elastic_dag_conf.keys())
    )

    if missing_variables:
        raise KeyError(
            f"Following mandatory environment variables are missing "
            f"from elastic_dag configuration: {missing_variables}."
        )

    if not all(isinstance(env, str) for env in elastic_dag_conf.values()):
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
        if env_name in elastic_dag_conf:
            variable_to_validation_fun_map[env_name](
                env_name, elastic_dag_conf[env_name]
            )

    check_max_runs_and_schedule_interval_compatibility(elastic_dag_conf)


def check_int_convertibility(env_name: str, env_value: str) -> None:
    """
    Checks if value of provided environment variable is convertible to int value.

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str

    :raises: ValueError: if env_value could not be converted to int value
    """
    try:
        int(env_value)
    except ValueError:
        raise ValueError(
            f"{env_name} value must be convertible to int. Received: '{env_value}'."
        )


def check_positive_int_convertibility(env_name: str, env_value: str) -> None:
    """
    Checks if value of provided environment variable is convertible to positive int value

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str

    :raises: ValueError: if env_value could not be converted to positive int value
    """
    try:
        converted_value = int(env_value)
        check_positive(converted_value)
    except ValueError:
        raise ValueError(
            f"{env_name} value must be convertible to positive int. Received: '{env_value}'."
        )


def check_positive(value: Union[int, float]) -> None:
    """
    Checks if provided value is positive and raises ValueError otherwise
    """
    if value <= 0:
        raise ValueError


def check_datetime_convertibility(env_name: str, env_value: str) -> None:
    """
    Checks if value of provided environment variable is a date string in expected format.

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str
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
    Checks if value of dag prefix env variable is a prefix for one of the forbidden dag ids
    (which would cause runs of corresponding DAGs to be collected alongside the real test Dag Runs).
    """
    # TODO: allow every environment type to specify its own "forbidden" matching dag ids

    safe_dag_prefix = safe_dag_id(env_value)

    matching_dag_ids = [
        dag_id
        for dag_id in DAG_IDS_NOT_ALLOWED_TO_MATCH_PREFIX
        if dag_id.startswith(safe_dag_prefix)
    ]

    if matching_dag_ids:
        raise ValueError(
            f"Value '{env_value}' of {env_name} is not allowed as {safe_dag_prefix} is a prefix "
            f"for the following forbidden dag ids: {matching_dag_ids}"
        )


def safe_dag_id(dag_id: str) -> str:
    """
    Removes characters that are invalid in dag id from provided string.
    """
    return re.sub("[^0-9a-zA-Z_]+", "_", dag_id)


def check_and_parse_time_delta(env_name: str, env_value: str) -> timedelta:
    """
    Checks if value of provided environment variable is a parsable time expression
    and returns timedelta object with duration.

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str

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

    time_params = {
        name: float(param) for name, param in parts.groupdict().items() if param
    }
    return timedelta(**time_params)


def check_schedule_interval(env_name: str, env_value: str) -> None:
    """
    Checks if value of schedule_interval is a parsable time expression
    or within a specified set of non-parsable values.

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str

    :raises: ValueError: if env_value is neither a parsable time expression
        nor one of allowed non-parsable values
    """

    try:
        check_and_parse_time_delta(env_name, env_value)
        return
    except ValueError as exception:
        error_message = str(exception)

    check_allowed_values = get_check_allowed_values_function(
        ALLOWED_NON_REGEXP_SCHEDULE_INTERVALS
    )

    try:
        check_allowed_values(env_name, env_value)
    except ValueError:
        log.error(error_message)
        raise ValueError(
            f"Value '{env_value}' of {env_name} is neither a parsable time expression "
            f"nor one of the following: {ALLOWED_NON_REGEXP_SCHEDULE_INTERVALS}."
        )


def get_check_allowed_values_function(
    values: Tuple[str, ...]
) -> Callable[[str, str], None]:
    """
    Returns a function which will check if value of provided environment variable
    is within a specified set of values

    :param values: tuple of any length with allowed string values of environment variable
    :type values: Tuple[str, ...]

    :return: a function that checks if given environment variable's value is within the specified
        set of values and raises ValueError otherwise
    :rtype: Callable[[str, str], None]
    """

    def check_allowed_values(env_name: str, env_value: str) -> None:
        """
        Checks if value of provided environment variable is within a specified set of values

        :param env_name: name of the environment variable which is being checked.
        :type env_name: str
        :param env_value: value of the variable.
        :type env_value: str

        :raises: ValueError: if env_value is not within a specified set of values
        """

        if env_value not in values:
            raise ValueError(
                f"{env_name} value must be one of the following: {values}. Received: '{env_value}'."
            )

    return check_allowed_values


def check_non_negative_float_convertibility(env_name: str, env_value: str) -> None:
    """
    Checks if value of provided environment variable is convertible to non negative float value

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str

    :raises: ValueError: if env_value could not be converted to non negative float value
    """
    try:
        converted_value = float(env_value)
        check_non_negative(converted_value)
    except ValueError:
        raise ValueError(
            f"{env_name} value must be convertible to non negative float. Received: '{env_value}'."
        )


def check_non_negative(value: Union[int, float]) -> None:
    """
    Checks if provided value is not negative and raises ValueError otherwise
    """
    if value < 0:
        raise ValueError


def check_max_runs_and_schedule_interval_compatibility(
    elastic_dag_conf: Dict[str, str]
) -> None:
    """
    Checks if max_runs and schedule_interval values create a valid combination

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :raises: ValueError:
        if max_runs is specified when schedule_interval is not a duration time expression
        if max_runs is not specified when schedule_interval is a duration time expression
        if max_runs, schedule_interval and start_ago form a combination which causes end_date
            to be in the future
    """

    schedule_interval = get_elastic_dag_environment_variable(
        elastic_dag_conf, "PERF_SCHEDULE_INTERVAL"
    )
    max_runs = get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_MAX_RUNS")
    start_ago = get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_START_AGO")

    if schedule_interval == "@once":

        if max_runs is not None:
            raise ValueError(
                "PERF_MAX_RUNS is allowed only if PERF_SCHEDULE_INTERVAL is "
                "provided as a time expression."
            )
        # if dags are set to be scheduled once, we do not need to check end_date
        return

    if max_runs is None:
        raise ValueError(
            "PERF_MAX_RUNS must be specified if PERF_SCHEDULE_INTERVAL is "
            "provided as a time expression."
        )

    max_runs = int(max_runs)

    # make sure that the end_date does not occur in future
    current_date = datetime.now()

    start_date = current_date - check_and_parse_time_delta("PERF_START_AGO", start_ago)

    end_date = start_date + (
        check_and_parse_time_delta("PERF_SCHEDULE_INTERVAL", schedule_interval)
        * (max_runs - 1)
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
    Checks if value of provided environment variable is a valid json.

    :param env_name: name of the environment variable which is being checked.
    :type env_name: str
    :param env_value: value of the variable.
    :type env_value: str
    """

    try:
        json.loads(env_value)
    except json.decoder.JSONDecodeError:
        raise ValueError(
            f"Value '{env_value}' of {env_name} cannot be json decoded.")


@contextmanager
def generate_copies_of_elastic_dag(
    elastic_dag_path: str, elastic_dag_conf: Dict[str, str]
) -> Tuple[str, List[str]]:
    """
    Contextmanager that creates copies of elastic DAG inside temporary directory using the
    dag prefix env variable as a base for filenames.

    :param elastic_dag_path: path to the elastic DAG that should be copied.
    :type elastic_dag_path: str
    :param elastic_dag_conf: dict with environment variables as keys and their values as values.
    :type elastic_dag_conf: Dict[str, str]

    :yields: a pair consisting of path to the temporary directory
        and a list with paths to copies of elastic DAG
    :type: Tuple[str, List[str]]
    """

    dag_files_count = int(
        get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_DAG_FILES_COUNT")
    )

    safe_dag_prefix = get_dag_prefix(elastic_dag_conf)

    with tempfile.TemporaryDirectory() as temp_dir:

        elastic_dag_copies = []

        for i in range(1, dag_files_count + 1):

            destination_filename = f"{safe_dag_prefix}_{i}.py"
            destination_path = os.path.join(temp_dir, destination_filename)

            copyfile(elastic_dag_path, destination_path)
            elastic_dag_copies.append(destination_path)

        yield temp_dir, elastic_dag_copies


def get_dag_prefix(elastic_dag_conf: Dict[str, str]) -> str:
    """
    Returns prefix that will be assigned to DAGs created with given elastic DAG configuration

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :return: final form of prefix after substituting inappropriate characters
    :rtype: str
    """

    dag_prefix = get_elastic_dag_environment_variable(
        elastic_dag_conf, "PERF_DAG_PREFIX"
    )

    safe_dag_prefix = safe_dag_id(dag_prefix)

    return safe_dag_prefix


def get_dags_count(elastic_dag_conf: Dict[str, str]) -> int:
    """
    Returns the number of test DAGs based on given elastic DAG configuration.

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :return: number of test DAGs
    :rtype: int
    """

    dag_files_count = int(
        get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_DAG_FILES_COUNT")
    )

    dags_per_dag_file = int(
        get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_DAGS_COUNT")
    )

    return dag_files_count * dags_per_dag_file


def calculate_number_of_dag_runs(elastic_dag_conf: Dict[str, str]) -> int:
    """
    Calculates how many Dag Runs will be created with given elastic DAG configuration

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :return: total number of Dag Runs
    :rtype: int
    """
    max_runs = get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_MAX_RUNS")

    total_dags_count = get_dags_count(elastic_dag_conf)

    # if PERF_MAX_RUNS is missing from the configuration,
    # it means that PERF_SCHEDULE_INTERVAL must be '@once'
    if max_runs is None:
        return total_dags_count

    return int(max_runs) * total_dags_count


def prepare_elastic_dag_columns(elastic_dag_conf: Dict[str, str]) -> OrderedDict:
    """
    Prepares an OrderedDict containing chosen elastic dag environment variables
    that will serve as columns for the results dataframe

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]

    :return: a dict with a subset of environment variables
        in order in which they should appear in the results dataframe
    :rtype: OrderedDict
    """

    max_runs = get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_MAX_RUNS")

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

    elastic_dag_columns = OrderedDict(
        [
            (
                "PERF_DAG_FILES_COUNT",
                int(
                    get_elastic_dag_environment_variable(
                        elastic_dag_conf, "PERF_DAG_FILES_COUNT"
                    )
                ),
            ),
            (
                "PERF_DAGS_COUNT",
                int(
                    get_elastic_dag_environment_variable(
                        elastic_dag_conf, "PERF_DAGS_COUNT"
                    )
                ),
            ),
            (
                "PERF_TASKS_COUNT",
                int(
                    get_elastic_dag_environment_variable(
                        elastic_dag_conf, "PERF_TASKS_COUNT"
                    )
                ),
            ),
            ("PERF_MAX_RUNS", max_runs),
            (
                "PERF_SCHEDULE_INTERVAL",
                get_elastic_dag_environment_variable(
                    elastic_dag_conf, "PERF_SCHEDULE_INTERVAL"
                ),
            ),
            (
                "PERF_SHAPE",
                get_elastic_dag_environment_variable(elastic_dag_conf, "PERF_SHAPE"),
            ),
            (
                "PERF_SLEEP_TIME",
                float(
                    get_elastic_dag_environment_variable(
                        elastic_dag_conf, "PERF_SLEEP_TIME"
                    )
                ),
            ),
            (
                "PERF_OPERATOR_TYPE",
                get_elastic_dag_environment_variable(
                    elastic_dag_conf, "PERF_OPERATOR_TYPE"
                ),
            ),
        ]
    )

    add_elastic_dag_configuration_type(elastic_dag_columns)

    return elastic_dag_columns


def get_elastic_dag_environment_variable(
    elastic_dag_conf: Dict[str, str], env_name: str
) -> str:
    """
    Returns value of environment variable with given env_name based on provided elastic_dag_conf

    :param elastic_dag_conf: dict with environment variables as keys and their values as values
    :type elastic_dag_conf: Dict[str, str]
    :param env_name: name of the environment variable value of which should be returned.
    :type env_name: str

    :return: value of environment variable taken from elastic_dag_conf or its default value, if it
        was not present in the dictionary (if applicable)
    :rtype: str

    :raises: ValueError:
        if env_name is a mandatory environment variable but it is missing from elastic_dag_conf
        if env_name is not a valid name of an elastic dag environment variable
    """
    if env_name in MANDATORY_ELASTIC_DAG_VARIABLES:
        if env_name not in elastic_dag_conf:
            raise ValueError(
                f"Mandatory environment variable '{env_name}' "
                f"is missing from elastic dag configuration."
            )
        return elastic_dag_conf[env_name]

    if env_name not in ELASTIC_DAG_VARIABLES_DEFAULT_VALUES:
        raise ValueError(
            f"Provided environment variable '{env_name}' is not a valid elastic dag"
            f"configuration variable."
        )

    return elastic_dag_conf.get(
        env_name, ELASTIC_DAG_VARIABLES_DEFAULT_VALUES[env_name]
    )


def add_elastic_dag_configuration_type(elastic_dag_columns: OrderedDict) -> None:
    """
    Adds a key with type of given elastic dag configuration to the columns dict

    :param elastic_dag_columns: a dict with columns containing elastic dag configuration
    :type elastic_dag_columns: OrderedDict
    """

    elastic_dag_configuration_type = "__".join(
        [
            f"{elastic_dag_columns['PERF_SHAPE']}",
            f"{elastic_dag_columns['PERF_DAG_FILES_COUNT']}_dag_files",
            f"{elastic_dag_columns['PERF_DAGS_COUNT']}_dags",
            f"{elastic_dag_columns['PERF_TASKS_COUNT']}_tasks",
            f"{elastic_dag_columns['PERF_MAX_RUNS']}_dag_runs",
            f"{elastic_dag_columns['PERF_SLEEP_TIME']}_sleep",
            f"{elastic_dag_columns['PERF_OPERATOR_TYPE']}_operator",
        ]
    )

    elastic_dag_columns.update(
        {"elastic_dag_configuration_type": elastic_dag_configuration_type}
    )

    # move the type key to the beginning of dict
    elastic_dag_columns.move_to_end("elastic_dag_configuration_type", last=False)
