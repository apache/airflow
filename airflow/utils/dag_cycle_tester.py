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
"""DAG Cycle tester."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowDagCycleException

if TYPE_CHECKING:
    from airflow.models.dag import DAG

CYCLE_NEW = 0
CYCLE_IN_PROGRESS = 1
CYCLE_DONE = 2


def check_cycle(dag: DAG) -> None:
    """
    Check to see if there are any cycles in the DAG.

    :raises AirflowDagCycleException: If cycle is found in the DAG.
    """
    # default of int is 0 which corresponds to CYCLE_NEW
    visited: dict[str, int] = defaultdict(int)
    path_stack: deque[str] = deque()
    task_dict = dag.task_dict

    def _check_adjacent_tasks(task_id, current_task):
        """Return first untraversed child task, else None if all tasks traversed."""
        for adjacent_task in current_task.get_direct_relative_ids():
            if visited[adjacent_task] == CYCLE_IN_PROGRESS:
                msg = f"Cycle detected in DAG: {dag.dag_id}. Faulty task: {task_id}"
                raise AirflowDagCycleException(msg)
            elif visited[adjacent_task] == CYCLE_NEW:
                return adjacent_task
        return None

    for dag_task_id in dag.task_dict.keys():
        if visited[dag_task_id] == CYCLE_DONE:
            continue
        path_stack.append(dag_task_id)
        while path_stack:
            current_task_id = path_stack[-1]
            if visited[current_task_id] == CYCLE_NEW:
                visited[current_task_id] = CYCLE_IN_PROGRESS
            task = task_dict[current_task_id]
            child_to_check = _check_adjacent_tasks(current_task_id, task)
            if not child_to_check:
                visited[current_task_id] = CYCLE_DONE
                path_stack.pop()
            else:
                path_stack.append(child_to_check)
