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
"""Utility functions for computing upstream map indexes in the Task SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.sdk.execution_time.comms import GetTICount, TICount

if TYPE_CHECKING:
    from airflow.sdk import BaseOperator
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup


def _find_common_ancestor_mapped_group(node1: BaseOperator, node2: BaseOperator) -> MappedTaskGroup | None:
    """
    Given two operators, find their innermost common mapped task group.

    :param node1: First operator
    :param node2: Second operator
    :return: The common mapped task group, or None if they don't share one
    """
    try:
        dag1 = node1.dag
        dag2 = node2.dag
    except RuntimeError:
        # Operator not assigned to a DAG
        return None

    if dag1 is None or dag2 is None or node1.dag_id != node2.dag_id:
        return None
    parent_group_ids = {g.group_id for g in node1.iter_mapped_task_groups()}
    common_groups = (g for g in node2.iter_mapped_task_groups() if g.group_id in parent_group_ids)
    return next(common_groups, None)


def _is_further_mapped_inside(operator: BaseOperator, container: TaskGroup) -> bool:
    """
    Whether given operator is *further* mapped inside a task group.

    :param operator: The operator to check
    :param container: The container task group
    :return: True if the operator is further mapped inside the container
    """
    # Use getattr for compatibility with both SDK and serialized operators
    if getattr(operator, "is_mapped", False):
        return True
    task_group = operator.task_group
    while task_group is not None and task_group.group_id != container.group_id:
        if getattr(task_group, "is_mapped", False):
            return True
        task_group = task_group.parent_group
    return False


def get_ti_count_for_task(task_id: str, dag_id: str, run_id: str) -> int:
    """
    Query TI count for a specific task.

    :param task_id: The task ID
    :param dag_id: The DAG ID
    :param run_id: The run ID
    :return: The count of task instances for the task
    """
    # Import here because SUPERVISOR_COMMS is set at runtime, not import time
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(GetTICount(dag_id=dag_id, task_ids=[task_id], run_ids=[run_id]))
    if not isinstance(response, TICount):
        raise RuntimeError(f"Unexpected response type: {type(response)}")
    return response.count


def get_relevant_map_indexes(
    task: BaseOperator,
    run_id: str,
    map_index: int,
    ti_count: int,
    relative: BaseOperator,
    dag_id: str,
) -> int | range | None:
    """
    Determine map indexes for XCom aggregation.

    This is used to figure out which specific map indexes of an upstream task
    are relevant when resolving XCom values for a task in a mapped task group.

    :param task: The current task
    :param run_id: The current run ID
    :param map_index: The map index of the current task instance
    :param ti_count: The total count of task instances for the current task
    :param relative: The upstream/downstream task to find relevant map indexes for
    :param dag_id: The DAG ID
    :return: None (use entire value), int (single index), or range (subset of indexes)
    """
    if not ti_count:
        return None

    common_ancestor = _find_common_ancestor_mapped_group(task, relative)
    if common_ancestor is None or common_ancestor.group_id is None:
        return None  # Different mapping contexts → use whole value

    # Query TI count using the current task, which is in the mapped task group.
    # This gives us the number of expansion iterations, not total TIs in the group.
    ancestor_ti_count = get_ti_count_for_task(task.task_id, dag_id, run_id)
    if not ancestor_ti_count:
        return None

    ancestor_map_index = map_index * ancestor_ti_count // ti_count

    if not _is_further_mapped_inside(relative, common_ancestor):
        return ancestor_map_index  # Single index

    # Partial aggregation for selected TIs
    further_count = ti_count // ancestor_ti_count
    map_index_start = ancestor_map_index * further_count
    return range(map_index_start, map_index_start + further_count)
