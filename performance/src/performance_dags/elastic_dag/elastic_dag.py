"""
Elastic dag copied from apache/airflow master and adjusted to work on python2 and with Airflow
versions prior to 2.0
"""

import enum
import json
import os
import re
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Union, cast

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


# DAG File used in performance tests. Its shape can be configured by environment variables.
RE_TIME_DELTA = re.compile(
    r"^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$"
)


def parse_time_delta(time_str):
    # type: (str) -> datetime.timedelta
    """
    Parse a time string e.g. (2h13m) into a timedelta object.

    :param time_str: A string identifying a duration.  (eg. 2h13m)
    :return datetime.timedelta: A datetime.timedelta object or "@once"
    """
    parts = RE_TIME_DELTA.match(time_str)

    assert parts is not None, (
        "Could not parse any time information from '{time_str}'. "
        "Examples of valid strings: '8h', '2d8h5m20s', '2m4s'".format(time_str=time_str)
    )

    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)  # type: ignore


def parse_start_date(date, start_ago):
    """
    Returns the start date for the elastic DAGs and string to be used as part of their ids.
    :return Tuple[datetime.datetime, str]: A tuple of datetime.datetime object to be used
        as a start_date and a string that should be used as part of the dag_id.
    """

    if date:
        start_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
        dag_id_component = int(start_date.timestamp())
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


def safe_dag_id(dag_id):
    # type: (str) -> str
    """
    Remove invalid characters for dag_id
    """
    return re.sub("[^0-9a-zA-Z_]+", "_", dag_id)


def get_task_list(dag_object, operator_type_str, task_count, trigger_rule, sleep_time, operator_extra_kwargs):
    # type: (DAG, str, int, float, int, dict) -> List[BaseOperator]
    """
    Return list of tasks of test dag

    :param dag_object: A DAG object the tasks should be assigned to.
    :param operator_type_str: A string identifying the type of operator
    :param task_count: An integer specifying the number of tasks to create.
    :param trigger_rule: A string identifying the rule by which dependencies are applied
    for the tasks to get triggered
    :param sleep_time: A non-negative float value specifying the time of sleep occurring
    when each task is executed
    :param operator_extra_kwargs: A dictionary with extra kwargs for operator
    :return List[BaseOperator]: a list of tasks
    """
    if operator_type_str == "bash":
        task_list = [
            BashOperator(
                task_id="__".join(["tasks", "{i}_of_{task_count}".format(i=i, task_count=task_count)]),
                bash_command="sleep {sleep_time}; echo test".format(sleep_time=sleep_time),
                dag=dag_object,
                trigger_rule=trigger_rule,
                **operator_extra_kwargs,
            )
            for i in range(1, task_count + 1)
        ]
    elif operator_type_str == "python":

        def sleep_function():
            time.sleep(sleep_time)
            print("test")

        task_list = [
            PythonOperator(
                task_id="__".join(["tasks", "{i}_of_{task_count}".format(i=i, task_count=task_count)]),
                python_callable=sleep_function,
                dag=dag_object,
                trigger_rule=trigger_rule,
                **operator_extra_kwargs,
            )
            for i in range(1, task_count + 1)
        ]
    else:
        raise ValueError(f"Unsupported operator type: {operator_type_str}.")
    return task_list


def chain(*tasks):
    # type: (Union[BaseOperator, List[BaseOperator]]) -> None
    r"""
    Given a number of tasks, builds a dependency chain.
    Support mix airflow.models.BaseOperator and List[airflow.models.BaseOperator].
    If you want to chain between two List[airflow.models.BaseOperator], have to
    make sure they have same length.
    .. code-block:: python
         chain(t1, [t2, t3], [t4, t5], t6)
    is equivalent to::
         / -> t2 -> t4 \
       t1               -> t6
         \ -> t3 -> t5 /
    .. code-block:: python
        t1.set_downstream(t2)
        t1.set_downstream(t3)
        t2.set_downstream(t4)
        t3.set_downstream(t5)
        t4.set_downstream(t6)
        t5.set_downstream(t6)
    :param tasks: List of tasks or List[airflow.models.BaseOperator] to set dependencies
    :type tasks: List[airflow.models.BaseOperator] or airflow.models.BaseOperator
    """
    for index, up_task in enumerate(tasks[:-1]):
        down_task = tasks[index + 1]
        if isinstance(up_task, BaseOperator):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, BaseOperator):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, list) or not isinstance(down_task, list):
            raise TypeError(
                "Chain not supported between instances of {up_type} and {down_type}".format(
                    up_type=type(up_task), down_type=type(down_task)
                )
            )
        up_task_list = cast(List[BaseOperator], up_task)
        down_task_list = cast(List[BaseOperator], down_task)
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                "Chain not supported different length Iterable "
                "but get {} and {}".format(len(up_task), len(down_task))
            )
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)


def chain_as_binary_tree(*tasks):
    # type: (BaseOperator) -> None
    r"""
    Chain tasks as a binary tree where task i is child of task (i - 1) // 2 :

        t0 -> t1 -> t3 -> t7
          |    \
          |      -> t4 -> t8
          |
           -> t2 -> t5 -> t9
               \
                 -> t6
    """
    for i in range(1, len(tasks)):
        tasks[i].set_upstream(tasks[(i - 1) // 2])


def chain_as_grid(*tasks):
    # type: (BaseOperator) -> None
    """
    Chain tasks as a grid:

     t0 -> t1 -> t2 -> t3
      |     |     |
      v     v     v
     t4 -> t5 -> t6
      |     |
      v     v
     t7 -> t8
      |
      v
     t9
    """
    if len(tasks) > 100 * 99 / 2:
        raise ValueError("Cannot generate grid DAGs with lateral size larger than 100 tasks.")
    grid_size = min([n for n in range(100) if n * (n + 1) / 2 >= len(tasks)])

    def index(i, j):
        """
        Return the index of node (i, j) on the grid.
        """
        return int(grid_size * i - i * (i - 1) / 2 + j)

    for i in range(grid_size - 1):
        for j in range(grid_size - i - 1):
            if index(i + 1, j) < len(tasks):
                tasks[index(i + 1, j)].set_downstream(tasks[index(i, j)])
            if index(i, j + 1) < len(tasks):
                tasks[index(i, j + 1)].set_downstream(tasks[index(i, j)])


def chain_as_star(*tasks):
    # type: (BaseOperator) -> None
    """
    Chain tasks as a star (all tasks are children of task 0)

     t0 -> t1
      | -> t2
      | -> t3
      | -> t4
      | -> t5
    """
    tasks[0].set_downstream(list(tasks[1:]))


@enum.unique
class DagShape(Enum):
    """
    Define shape of the Dag that will be used for testing.
    """

    NO_STRUCTURE = "no_structure"
    LINEAR = "linear"
    BINARY_TREE = "binary_tree"
    STAR = "star"
    GRID = "grid"


DAG_COUNT = int(os.environ["PERF_DAGS_COUNT"])
TASKS_COUNT = int(os.environ["PERF_TASKS_COUNT"])
START_DATE, DAG_ID_START_DATE = parse_start_date(
    os.environ["PERF_START_DATE"], os.environ.get("PERF_START_AGO", "1h")
)
SCHEDULE_INTERVAL_ENV = os.environ.get("PERF_SCHEDULE_INTERVAL", "@once")
SCHEDULE_INTERVAL = parse_schedule_interval(SCHEDULE_INTERVAL_ENV)
SHAPE = DagShape(os.environ["PERF_SHAPE"])
SLEEP_TIME = float(os.environ.get("PERF_SLEEP_TIME", "0"))
OPERATOR_TYPE = os.environ.get("PERF_OPERATOR_TYPE", "bash")
START_PAUSED = bool(int(os.environ.get("PERF_START_PAUSED", "1")))
TASKS_TRIGGER_RULE = os.environ.get("PERF_TASKS_TRIGGER_RULE", TriggerRule.ALL_SUCCESS)
OPERATOR_EXTRA_KWARGS = json.loads(os.environ.get("PERF_OPERATOR_EXTRA_KWARGS", "{}"))

args = {"owner": "airflow", "start_date": START_DATE}

if "PERF_MAX_RUNS" in os.environ:
    if isinstance(SCHEDULE_INTERVAL, str):
        raise ValueError("Can't set max runs with string-based schedule_interval")
    if "PERF_START_DATE" not in os.environ:
        raise ValueError(
            "When using 'PERF_MAX_RUNS', please provide the start date as a date string in "
            "'%Y-%m-%d %H:%M:%S.%f' format via 'PERF_START_DATE' environment variable."
        )
    num_runs = int(os.environ["PERF_MAX_RUNS"])
    args["end_date"] = START_DATE + (SCHEDULE_INTERVAL * (num_runs - 1))

for dag_no in range(1, DAG_COUNT + 1):
    dag = DAG(
        dag_id=safe_dag_id(
            "__".join(
                [
                    os.path.splitext(os.path.basename(__file__))[0],
                    "SHAPE={}".format(SHAPE.name.lower()),
                    "DAGS_COUNT={}_of_{}".format(dag_no, DAG_COUNT),
                    "TASKS_COUNT=${}".format(TASKS_COUNT),
                    "START_DATE=${}".format(DAG_ID_START_DATE),
                    "SCHEDULE_INTERVAL=${}".format(SCHEDULE_INTERVAL_ENV),
                ]
            )
        ),
        default_args=args,
        schedule_interval=SCHEDULE_INTERVAL,
        is_paused_upon_creation=START_PAUSED,
        catchup=True,
    )

    elastic_dag_tasks = get_task_list(
        dag, OPERATOR_TYPE, TASKS_COUNT, TASKS_TRIGGER_RULE, SLEEP_TIME, OPERATOR_EXTRA_KWARGS
    )

    shape_function_map = {
        DagShape.LINEAR: chain,
        DagShape.BINARY_TREE: chain_as_binary_tree,
        DagShape.STAR: chain_as_star,
        DagShape.GRID: chain_as_grid,
    }
    if SHAPE != DagShape.NO_STRUCTURE:
        shape_function_map[SHAPE](*elastic_dag_tasks)

    globals()["dag_{dag_no}".format(dag_no=dag_no)] = dag
