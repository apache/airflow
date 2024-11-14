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

"""
Module generates number DAGs for the purpose of performance testing.

The number of DAGs,
number, types of tasks in each DAG and the shape of the DAG are controlled through
environment variables:

- `PERF_DAGS_COUNT` - number of DAGs to generate
- `PERF_TASKS_COUNT` - number of tasks in each DAG
- `PERF_START_DATE` - if not provided current time - `PERF_START_AGO` applies
- `PERF_START_AGO` - start time relative to current time used if PERF_START_DATE is not provided. Default `1h`
- `SCHEDULE_INTERVAL_ENV` - Schedule interval. Default `@once`
- `PERF_SHAPE` - shape of DAG. See `DagShape`. Default `NO_STRUCTURE`
- `PERF_SLEEP_TIME` - A non-negative float value specifying the time of sleep occurring
    when each task is executed. Default `0`
- `PERF_OPERATOR_TYPE` - A string identifying the type of operator. Default `bash`
- `PERF_START_PAUSED` - Is DAG paused upon creation. Default  `1`
- `PERF_TASKS_TRIGGER_RULE` - A string identifying the rule by which dependencies are applied
    for the tasks to get triggered. Default `TriggerRule.ALL_SUCCESS`)
- `PERF_OPERATOR_EXTRA_KWARGS` - A dictionary with extra kwargs for operator

"""

from __future__ import annotations

import enum
import json
import os
import time
from enum import Enum

import re2 as re
from performance_dags.performance_dag.performance_dag_utils import (
    parse_schedule_interval,
    parse_start_date,
)

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# DAG File used in performance tests. Its shape can be configured by environment variables.


def safe_dag_id(dag_id):
    # type: (str) -> str
    """Remove invalid characters for dag_id."""
    return re.sub("[^0-9a-zA-Z_]+", "_", dag_id)


def get_task_list(
    dag_object,
    operator_type_str,
    task_count,
    trigger_rule,
    sleep_time,
    operator_extra_kwargs,
):
    # type: (DAG, str, int, float, int, dict) -> list[BaseOperator]
    """
    Return list of tasks of test dag.

    :param dag_object: A DAG object the tasks should be assigned to.
    :param operator_type_str: A string identifying the type of operator
    :param task_count: An integer specifying the number of tasks to create.
    :param trigger_rule: A string identifying the rule by which dependencies are applied
    for the tasks to get triggered
    :param sleep_time: A non-negative float value specifying the time of sleep occurring
    when each task is executed
    :param operator_extra_kwargs: A dictionary with extra kwargs for operator
    :return list[BaseOperator]: a list of tasks
    """
    if operator_type_str == "bash":
        task_list = [
            BashOperator(
                task_id="__".join(["tasks", f"{i}_of_{task_count}"]),
                bash_command=f"sleep {sleep_time}; echo test",
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
                task_id="__".join(["tasks", f"{i}_of_{task_count}"]),
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


def chain_as_binary_tree(*tasks):
    # type: (BaseOperator) -> None
    """
    Chain tasks as a binary tree where task i is child of task (i - 1) // 2.

    Example:
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
    Chain tasks as a grid.

    Example:
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
        """Return the index of node (i, j) on the grid."""
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
    Chain tasks as a star (all tasks are children of task 0).

    Example:
     t0 -> t1
      | -> t2
      | -> t3
      | -> t4
      | -> t5
    """
    tasks[0].set_downstream(list(tasks[1:]))


@enum.unique
class DagShape(Enum):
    """Define shape of the Dag that will be used for testing."""

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
                    f"SHAPE={SHAPE.name.lower()}",
                    f"DAGS_COUNT={dag_no}_of_{DAG_COUNT}",
                    f"TASKS_COUNT=${TASKS_COUNT}",
                    f"START_DATE=${DAG_ID_START_DATE}",
                    f"SCHEDULE_INTERVAL=${SCHEDULE_INTERVAL_ENV}",
                ]
            )
        ),
        default_args=args,
        schedule_interval=SCHEDULE_INTERVAL,
        is_paused_upon_creation=START_PAUSED,
        catchup=True,
    )

    performance_dag_tasks = get_task_list(
        dag,
        OPERATOR_TYPE,
        TASKS_COUNT,
        TASKS_TRIGGER_RULE,
        SLEEP_TIME,
        OPERATOR_EXTRA_KWARGS,
    )

    shape_function_map = {
        DagShape.LINEAR: chain,
        DagShape.BINARY_TREE: chain_as_binary_tree,
        DagShape.STAR: chain_as_star,
        DagShape.GRID: chain_as_grid,
    }
    if SHAPE != DagShape.NO_STRUCTURE:
        shape_function_map[SHAPE](*performance_dag_tasks)

    globals()[f"dag_{dag_no}"] = dag
