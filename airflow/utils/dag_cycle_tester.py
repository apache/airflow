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
DAG Cycle tester
"""
from collections import defaultdict, deque
from typing import Dict

from airflow.exceptions import AirflowDagCycleException

CYCLE_NEW = 0
CYCLE_IN_PROGRESS = 1
CYCLE_DONE = 2


def test_cycle(dag):
    """
    Check to see if there are any cycles in the DAG. Returns False if no cycle found,
    otherwise raises exception.
    """
    # default of int is 0 which corresponds to CYCLE_NEW
    dag_visit_map: Dict[str, int] = defaultdict(int)
    for dag_task_id in dag.task_dict.keys():
        if dag_visit_map[dag_task_id] == CYCLE_NEW:
            _test_cycle_helper(dag_visit_map, dag_task_id, dag.task_dict)
    return False


def _test_cycle_helper(visit_map, task_id, task_dict):
    """
    Checks if a cycle exists from the input task using DFS traversal
    """
    path, visited = deque([task_id]), deque()

    while path:
        current_task_id = path.pop()
        if visit_map[current_task_id] == CYCLE_DONE:
            continue
        if visit_map[current_task_id] == CYCLE_IN_PROGRESS:
            msg = "Cycle detected in DAG. Faulty task: {}".format(
                current_task_id)
            raise AirflowDagCycleException(msg)
        visit_map[current_task_id] = CYCLE_IN_PROGRESS
        task = task_dict[current_task_id]
        path.extend(task.get_direct_relative_ids())
        if current_task_id in path:
            continue
        visited.append(current_task_id)

    for current_task_id in visited:
        visit_map[current_task_id] = CYCLE_DONE
