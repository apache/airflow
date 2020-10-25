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

import enum
import os
import re
from datetime import datetime, timedelta
from enum import Enum

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

# DAG File used in performance tests. Its shape can be configured by environment variables.
RE_TIME_DELTA = re.compile(
    r"^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$"
)


def parse_time_delta(time_str: str):
    """
    Parse a time string e.g. (2h13m) into a timedelta object.

    :param time_str: A string identifying a duration.  (eg. 2h13m)
    :return datetime.timedelta: A datetime.timedelta object or "@once"
    """
    parts = RE_TIME_DELTA.match(time_str)

    # pylint: disable=do-not-use-asserts
    assert parts is not None, (
        f"Could not parse any time information from '{time_str}'. "
        f"Examples of valid strings: '8h', '2d8h5m20s', '2m4s'"
    )
    # pylint: enable=do-not-use-asserts

    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)  # type: ignore


def parse_start_date():
    """
    Returns the start date for the elastic DAGs and string to be used as part of their ids.

    :return Tuple[datetime.datetime, str]: A tuple of datetime.datetime object to be used
        as a start_date and a string that should be used as part of the dag_id.
    """

    if "PERF_START_DATE" in os.environ:
        if "PERF_START_AGO" in os.environ:
            raise ValueError(
                "'PERF_START_DATE' and 'PERF_START_AGO' environment variables are mutually "
                "exclusive. Please provide only one variable to specify DAGs' start date."
            )
        start_date = datetime.strptime(os.environ["PERF_START_DATE"], "%Y-%m-%d %H:%M:%S.%f")
        dag_id_component = int(start_date.timestamp())
    else:
        start_ago = os.environ.get("PERF_START_AGO", "1h")
        start_date = datetime.now() - parse_time_delta(start_ago)
        dag_id_component = start_ago
    return start_date, dag_id_component


def parse_schedule_interval(time_str: str):
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


def safe_dag_id(s: str) -> str:
    """
    Remove invalid characters for dag_id
    """
    return re.sub('[^0-9a-zA-Z_]+', '_', s)


def chain_as_binary_tree(*tasks: BashOperator):
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
        tasks[i].set_downstream(tasks[(i - 1) // 2])


def chain_as_grid(*tasks: BashOperator):
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
        raise ValueError('Cannot generate grid DAGs with lateral size larger than 100 tasks.')
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


def chain_as_star(*tasks: BashOperator):
    """
    Chain tasks as a star (all tasks are children of task 0)

     t0 -> t1
      | -> t2
      | -> t3
      | -> t4
      | -> t5
    """
    tasks[0].set_upstream(list(tasks[1:]))


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


DAG_PREFIX = os.environ.get("PERF_DAG_PREFIX", "perf_scheduler")
DAG_COUNT = int(os.environ["PERF_DAGS_COUNT"])
TASKS_COUNT = int(os.environ["PERF_TASKS_COUNT"])
START_DATE, DAG_ID_START_DATE = parse_start_date()
SCHEDULE_INTERVAL_ENV = os.environ.get("PERF_SCHEDULE_INTERVAL", "@once")
SCHEDULE_INTERVAL = parse_schedule_interval(SCHEDULE_INTERVAL_ENV)

SHAPE = DagShape(os.environ["PERF_SHAPE"])

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
    args['end_date'] = START_DATE + (SCHEDULE_INTERVAL * (num_runs - 1))

for dag_no in range(1, DAG_COUNT + 1):
    dag = DAG(
        dag_id=safe_dag_id(
            "__".join(
                [
                    DAG_PREFIX,
                    f"SHAPE={SHAPE.name.lower()}",
                    f"DAGS_COUNT={dag_no}_of_{DAG_COUNT}",
                    f"TASKS_COUNT=${TASKS_COUNT}",
                    f"START_DATE=${DAG_ID_START_DATE}",
                    f"SCHEDULE_INTERVAL=${SCHEDULE_INTERVAL_ENV}",
                ]
            )
        ),
        is_paused_upon_creation=False,
        default_args=args,
        schedule_interval=SCHEDULE_INTERVAL,
    )

    elastic_dag_tasks = [
        BashOperator(task_id="__".join(["tasks", f"{i}_of_{TASKS_COUNT}"]), bash_command='echo test', dag=dag)
        for i in range(1, TASKS_COUNT + 1)
    ]

    shape_function_map = {
        DagShape.LINEAR: chain,
        DagShape.BINARY_TREE: chain_as_binary_tree,
        DagShape.STAR: chain_as_star,
        DagShape.GRID: chain_as_grid,
    }
    if SHAPE != DagShape.NO_STRUCTURE:
        shape_function_map[SHAPE](*elastic_dag_tasks)

    globals()[f"dag_{dag_no}"] = dag
