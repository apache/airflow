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

import operator
from functools import cache

from typing_extensions import Any

from airflow import DAG
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import (
    state_priority,
)
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridTaskInstanceSummary,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models import MappedOperator
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskmap import TaskMap
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import MappedTaskGroup, TaskGroup


@cache
def get_task_group_children_getter() -> operator.methodcaller:
    """Get the Task Group Children Getter for the DAG."""
    sort_order = conf.get("webserver", "grid_view_sorting_order", fallback="topological")
    if sort_order == "topological":
        return operator.methodcaller("topological_sort")
    if sort_order == "hierarchical_alphabetical":
        return operator.methodcaller("hierarchical_alphabetical_sort")
    raise AirflowConfigException(f"Unsupported grid_view_sorting_order: {sort_order}")


def get_task_group_map(dag: DAG) -> dict[str, dict[str, Any]]:
    """
    Get the Task Group Map for the DAG.

    :param dag: DAG

    :return: Task Group Map
    """
    task_nodes: dict[str, dict[str, Any]] = {}

    def _is_task_node_mapped_task_group(task_node: BaseOperator | MappedTaskGroup | TaskMap | None) -> bool:
        """Check if the Task Node is a Mapped Task Group."""
        return type(task_node) is MappedTaskGroup

    def _append_child_task_count_to_parent(
        child_task_count: int | MappedTaskGroup | TaskMap | MappedOperator | None,
        parent_node: BaseOperator | MappedTaskGroup | TaskMap | None,
    ):
        """
        Append the Child Task Count to the Parent.

        This method should only be used for Mapped Models.
        """
        if isinstance(parent_node, TaskGroup):
            # Remove the regular task counted in parent_node
            task_nodes[parent_node.node_id]["task_count"].append(-1)
            # Add the mapped task to the parent_node
            task_nodes[parent_node.node_id]["task_count"].append(child_task_count)

    def _fill_task_group_map(
        task_node: BaseOperator | MappedTaskGroup | TaskMap | None,
        parent_node: BaseOperator | MappedTaskGroup | TaskMap | None,
    ):
        """Recursively fill the Task Group Map."""
        if task_node is None:
            return
        if isinstance(task_node, MappedOperator):
            task_nodes[task_node.node_id] = {
                "is_group": False,
                "parent_id": parent_node.node_id if parent_node else None,
                "task_count": [task_node],
            }
            # Add the Task Count to the Parent Node because parent node is a Task Group
            _append_child_task_count_to_parent(child_task_count=task_node, parent_node=parent_node)
            return
        elif isinstance(task_node, TaskGroup):
            task_count = (
                task_node
                if _is_task_node_mapped_task_group(task_node)
                else len([child for child in get_task_group_children_getter()(task_node)])
            )
            task_nodes[task_node.node_id] = {
                "is_group": True,
                "parent_id": parent_node.node_id if parent_node else None,
                "task_count": [task_count],
            }
            return [
                _fill_task_group_map(task_node=child, parent_node=task_node)
                for child in get_task_group_children_getter()(task_node)
            ]
        elif isinstance(task_node, BaseOperator):
            task_nodes[task_node.task_id] = {
                "is_group": False,
                "parent_id": parent_node.node_id if parent_node else None,
                "task_count": task_nodes[parent_node.node_id]["task_count"]
                if _is_task_node_mapped_task_group(parent_node) and parent_node
                else [1],
            }
            # No Need to Add the Task Count to the Parent Node, these are already counted in Add the Parent
            return

    for node in [child for child in get_task_group_children_getter()(dag.task_group)]:
        _fill_task_group_map(task_node=node, parent_node=None)

    return task_nodes


def get_child_task_map(parent_task_id: str, task_node_map: dict[str, dict[str, Any]]):
    """Get the Child Task Map for the Parent Task ID."""
    return [task_id for task_id, task_map in task_node_map.items() if task_map["parent_id"] == parent_task_id]


def _get_total_task_count(
    run_id: str, task_count: list[int | MappedTaskGroup | MappedOperator], session: SessionDep
) -> int:
    return sum(
        node
        if isinstance(node, int)
        else (
            node.get_mapped_ti_count(run_id=run_id, session=session)
            if isinstance(node, (MappedTaskGroup, MappedOperator))
            else node
        )
        for node in task_count
    )


def fill_task_instance_summaries(
    grouped_task_instances: dict[tuple[str, str], list],
    task_instance_summaries_to_fill: dict[str, list],
    task_node_map: dict[str, dict[str, Any]],
    session: SessionDep,
) -> None:
    """
    Fill the Task Instance Summaries for the Grouped Task Instances.

    :param grouped_task_instances: Grouped Task Instances
    :param task_instance_summaries_to_fill: Task Instance Summaries to fill
    :param task_node_map: Task Node Map
    :param session: Session

    :return: None
    """
    # Additional logic to calculate the overall states to cascade recursive task states
    overall_states: dict[tuple[str, str], str] = {
        (task_id, run_id): next(
            (
                str(state.value)
                for state in state_priority
                for ti in tis
                if state is not None and ti.state == state
            ),
            "no_status",
        )
        for (task_id, run_id), tis in grouped_task_instances.items()
    }
    for (task_id, run_id), tis in grouped_task_instances.items():
        ti_try_number = max([ti.try_number for ti in tis])
        ti_start_date = min([ti.start_date for ti in tis if ti.start_date], default=None)
        ti_end_date = max([ti.end_date for ti in tis if ti.end_date], default=None)
        ti_queued_dttm = min([ti.queued_dttm for ti in tis if ti.queued_dttm], default=None)
        ti_note = min([ti.note for ti in tis if ti.note], default=None)

        # Calculate the child states for the task
        # Initialize the child states with 0
        child_states = {"no_status" if state is None else state.name.lower(): 0 for state in state_priority}
        # Update Task States for non-grouped tasks
        child_states.update(
            {
                "no_status" if state is None else state.name.lower(): len(
                    [ti for ti in tis if ti.state == state]
                    if not task_node_map[task_id]["is_group"]
                    else [
                        ti
                        for ti in tis
                        if ti.state == state and ti.task_id in get_child_task_map(task_id, task_node_map)
                    ]
                )
                for state in state_priority
            }
        )
        # Update Nested Task Group States by aggregating the child states
        child_states.update(
            {
                overall_states[(task_node_id, run_id)].lower(): child_states.get(
                    overall_states[(task_node_id, run_id)].lower(), 0
                )
                + 1
                for task_node_id in get_child_task_map(task_id, task_node_map)
                if task_node_map[task_node_id]["is_group"]
            }
        )

        # Get the overall state for the task
        overall_ti_state = next(
            (
                state
                for state in state_priority
                for state_name, state_count in child_states.items()
                if state_count > 0 and state_name == state
            ),
            "no_status",
        )

        # Task Count is either integer or a TaskGroup to get the task count
        task_instance_summaries_to_fill[run_id].append(
            GridTaskInstanceSummary(
                task_id=task_id,
                try_number=ti_try_number,
                start_date=ti_start_date,
                end_date=ti_end_date,
                queued_dttm=ti_queued_dttm,
                child_states=child_states,
                task_count=_get_total_task_count(run_id, task_node_map[task_id]["task_count"], session),
                state=TaskInstanceState[overall_ti_state.upper()]
                if overall_ti_state != "no_status"
                else None,
                note=ti_note,
            )
        )
