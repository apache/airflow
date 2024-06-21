"""
This module contains payload that can be executed on pods of GKE cluster. These payloads must be
compatible with both python 2 and 3.
"""
from typing import List


# pylint: disable=import-outside-toplevel
def get_dags_count(dag_id_prefix):
    # type: (str) -> int
    """
    Returns number of DAGs the dag_id of which begins with dag_id_prefix present on
    the instance.

    :param dag_id_prefix: prefix of the dag_id of the DAGs to check.
    :type dag_id_prefix: str

    :return: number of DAGs found in instance's database.
    :rtype: int
    """
    from airflow.models import DagModel
    from airflow.utils.db import create_session
    from sqlalchemy import func

    with create_session() as session:
        query = session.query(func.count(DagModel.dag_id)).filter(
            DagModel.dag_id.startswith(dag_id_prefix)
        )
        dags_count = query.scalar()

    return dags_count


def unpause_dags(dag_id_prefix):
    # type: (str) -> None
    """
    Finds DAGs the dag_id of which begins with provided prefix and unpauses them.

    :param dag_id_prefix: prefix of the dag_id of the DAGs to unpause.
    :type dag_id_prefix: str
    """

    from airflow.models import DagModel
    from airflow.utils.db import create_session

    with create_session() as session:
        query = session.query(DagModel).filter(
            DagModel.dag_id.startswith(dag_id_prefix)
        )
        query.update({DagModel.is_paused: False}, synchronize_session="fetch")


def get_dag_runs_count(dag_id_prefix, states=None):
    # type: (str, List[str]) -> int
    """
    Finds Dag Runs fulfilling conditions and return their count

    :param dag_id_prefix: prefix of the dag_id of the searched Dag Runs
    :type dag_id_prefix: str
    :param states: list with allowed states of the searched Dag Runs
    :type states: List[str]

    :return: number of found Dag Runs
    :rtype: int
    """
    from airflow.models import DagRun
    from airflow.utils.db import create_session
    from sqlalchemy import func

    with create_session() as session:
        query = session.query(func.count(DagRun.dag_id)).filter(
            DagRun.dag_id.startswith(dag_id_prefix)
        )

        if states:
            query = query.filter(DagRun.state.in_(states))

        dag_runs_count = query.scalar()

    return dag_runs_count


# pylint: disable=too-many-locals
def collect_dag_run_statistics(dag_id_prefix, states=None):
    # type: (str, List[str]) -> str
    """
    Collects statistics about Dag Runs fulfilling conditions. Statistics are presented
    as a list of pairs so it is easy to initialize an OrderedDict using their dumped json.

    :param dag_id_prefix: prefix of the dag_id of the searched Dag Runs
    :type dag_id_prefix: str
    :param states: list with allowed states of the searched Dag Runs
    :type states: List[str]

    :return: dumped json with different statistics regarding found Dag Runs.
    :rtype: str
    """
    import warnings

    from airflow.models import DagRun, TaskInstance
    from airflow.utils import timezone
    from airflow.utils.db import create_session

    with create_session() as session:
        query = session.query(DagRun).filter(DagRun.dag_id.startswith(dag_id_prefix))

        if states:
            query = query.filter(DagRun.state.in_(states))

        results = query.all()

    dag_run_total_count = len(results)
    finished_with_state_count = []

    if states:
        for state in states:
            finished_with_state_count.append(
                (
                    "dag_run_{}_count".format(state),
                    len([dag_run for dag_run in results if dag_run.state == state]),
                )
            )

    dag_run_start_dates = [
        dag_run.start_date for dag_run in results if dag_run.start_date is not None
    ]

    if not dag_run_start_dates:
        raise ValueError("None of the collected Dag Runs has a start_date.")

    if len(dag_run_start_dates) != dag_run_total_count:
        warnings.warn("Some of the dag runs collected are missing start_date.")

    test_start_date = timezone.make_naive(
        timezone.convert_to_utc(min(dag_run_start_dates))
    )

    dag_run_end_dates = [
        dag_run.end_date for dag_run in results if dag_run.end_date is not None
    ]

    if not dag_run_end_dates:
        raise ValueError("None of the collected Dag Runs has an end_date.")

    if len(dag_run_end_dates) != dag_run_total_count:
        warnings.warn("Some of the dag runs collected are missing end_date.")

    test_end_date = timezone.make_naive(timezone.convert_to_utc(max(dag_run_end_dates)))

    test_duration = round((test_end_date - test_start_date).total_seconds(), 4)

    dag_run_duration = [
        (dag_run.end_date - dag_run.start_date).total_seconds()
        for dag_run in results
        if dag_run.start_date is not None and dag_run.end_date is not None
    ]
    average_dag_run_duration = round(
        sum(dag_run_duration) / len(dag_run_duration) if dag_run_duration else 0.0, 4
    )

    execution_dates = [dag_run.execution_date for dag_run in results]

    # collecting metrics related to task_instances
    with create_session() as session:
        task_instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id.startswith(dag_id_prefix))
            .filter(TaskInstance.execution_date.in_(execution_dates))
        ).all()

    task_instance_total_count = len(task_instances)
    # if task instance finished with a failure, its duration may be missing
    task_instance_duration = [
        task_instance.duration
        for task_instance in task_instances
        if task_instance.duration is not None
    ]

    if len(task_instance_duration) != task_instance_total_count:
        warnings.warn("Some of the task instances collected are missing duration.")

    average_task_instance_duration = round(
        sum(task_instance_duration) / len(task_instance_duration)
        if task_instance_duration
        else 0.0,
        4,
    )

    # response has such a structure so that the order of metrics in returned string is not random
    # and it can be used to create an OrderedDict
    response = (
        [
            ("test_start_date", str(test_start_date)),
            ("test_end_date", str(test_end_date)),
            ("test_duration", test_duration),
            ("dag_run_total_count", dag_run_total_count),
        ]
        + finished_with_state_count
        + [
            ("dag_run_average_duration", average_dag_run_duration),
            (
                "dag_run_min_duration",
                min(dag_run_duration) if dag_run_duration else 0.0,
            ),
            (
                "dag_run_max_duration",
                max(dag_run_duration) if dag_run_duration else 0.0,
            ),
            ("task_instance_total_count", task_instance_total_count),
            ("task_instance_average_duration", average_task_instance_duration),
            (
                "task_instance_min_duration",
                min(task_instance_duration) if task_instance_duration else 0.0,
            ),
            (
                "task_instance_max_duration",
                max(task_instance_duration) if task_instance_duration else 0.0,
            ),
        ]
    )

    return str(response)


# pylint: enable=too-many-locals


def get_python_version():
    # type: () -> str
    """
    Returns full version of python used on pod.
    """
    import platform

    return platform.python_version()


def get_airflow_version():
    # type: () -> str
    """
    Returns version of installed airflow.
    """
    from airflow import version

    return version.version


def get_airflow_configuration():
    # type: () -> str
    """
    Collects a subset of airflow configuration options regarding parallelism. These options are
    stored as a list of pairs (Airflow environment variable name - value) so it is easy
    to initialize an OrderedDict using their dumped json.

    :return: dumped json with collected configuration options.
    :rtype: str
    """

    import airflow.configuration
    from airflow.exceptions import AirflowConfigException

    store_serialized_dags_default_value = "False"
    worker_autoscale_default_value = None

    # tuples in format: config section, section setting
    configuration_options_to_collect = [
        ("core", "dag_concurrency"),
        ("core", "dagbag_import_timeout"),
        ("core", "max_active_runs_per_dag"),
        ("core", "parallelism"),
        ("core", "store_serialized_dags"),
        ("celery", "worker_autoscale"),
        ("celery", "worker_concurrency"),
        ("scheduler", "max_threads"),
    ]

    airflow_configuration = []

    for configuration_option in configuration_options_to_collect:
        section = configuration_option[0]
        key = configuration_option[1]
        # pylint: disable=protected-access
        option_name = airflow.configuration.AirflowConfigParser()._env_var_name(
            section, key
        )
        # pylint: enable=protected-access
        try:
            option_value = airflow.configuration.get(section, key)
        # this try/except clause is solely for the purpose of store_serialized_dags option
        # which will be missing from config of non-composer environment prior to Airflow <1.10.7
        except AirflowConfigException as exception:
            if section == "core" and key == "store_serialized_dags":
                option_value = store_serialized_dags_default_value
            elif section == "celery" and key == "worker_autoscale":
                option_value = worker_autoscale_default_value
            elif section == "scheduler" and key == "max_threads":
                # option's key changed since Airflow 1.10.14, we keep the old name for now so that
                # results table schema remains the same
                option_value = airflow.configuration.get(
                    "scheduler", "parsing_processes"
                )
            else:
                raise exception

        airflow_configuration.append((option_name, option_value))

    return str(airflow_configuration)


# pylint: enable=import-outside-toplevel
