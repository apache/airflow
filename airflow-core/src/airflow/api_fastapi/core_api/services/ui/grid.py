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

import contextlib
from collections import Counter
from collections.abc import Iterable
from uuid import UUID

import structlog
from sqlalchemy import select
from typing_extensions import Any

from airflow import DAG
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.parameters import (
    state_priority,
)
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    GridTaskInstanceSummary,
)
from airflow.api_fastapi.core_api.datamodels.ui.structure import (
    StructureDataResponse,
)
from airflow.models.baseoperator import BaseOperator as DBBaseOperator
from airflow.models.dag_version import DagVersion
from airflow.models.taskmap import TaskMap
from airflow.sdk import BaseOperator
from airflow.sdk.definitions._internal.abstractoperator import NotMapped
from airflow.sdk.definitions._internal.expandinput import NotFullyPopulated
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import get_task_group_children_getter, task_group_to_dict

log = structlog.get_logger(logger_name=__name__)


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
    ) -> None:
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

        if isinstance(task_node, TaskGroup):
            task_count = task_node if _is_task_node_mapped_task_group(task_node) else len(task_node.children)
            task_nodes[task_node.node_id] = {
                "is_group": True,
                "parent_id": parent_node.node_id if parent_node else None,
                "task_count": [task_count],
            }
            for child in get_task_group_children_getter()(task_node):
                _fill_task_group_map(task_node=child, parent_node=task_node)
            return

        if isinstance(task_node, BaseOperator):
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


def _count_tis(node: int | MappedTaskGroup | MappedOperator, run_id: str, session: SessionDep) -> int:
    if not isinstance(node, (MappedTaskGroup, MappedOperator)):
        return node
    with contextlib.suppress(NotFullyPopulated, NotMapped):
        return DBBaseOperator.get_mapped_ti_count(node, run_id=run_id, session=session)
    # If the downstream is not actually mapped, or we don't have information to
    # determine the length yet, simply return 1 to represent the stand-in ti.
    return 1


def fill_task_instance_summaries(
    grouped_task_instances: dict[tuple[str, str], list],
    task_instance_summaries_to_fill: dict[str, list],
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

    serdag_cache: dict[UUID, SerializedDAG] = {}
    task_group_map_cache: dict[UUID, dict[str, dict[str, Any]]] = {}

    for (task_id, run_id), tis in grouped_task_instances.items():
        if not tis:
            continue

        sdm = _get_serdag(tis[0], session)
        serdag_cache[sdm.id] = serdag_cache.get(sdm.id) or sdm.dag
        dag = serdag_cache[sdm.id]
        task_group_map_cache[sdm.id] = task_group_map_cache.get(sdm.id) or get_task_group_map(dag=dag)
        task_node_map = task_group_map_cache[sdm.id]
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
                if task_node_map[task_node_id]["is_group"] and (task_node_id, run_id) in overall_states
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
                task_count=sum(_count_tis(n, run_id, session) for n in task_node_map[task_id]["task_count"]),
                state=TaskInstanceState[overall_ti_state.upper()]
                if overall_ti_state != "no_status"
                else None,
                note=ti_note,
            )
        )


def get_structure_from_dag(dag: DAG) -> StructureDataResponse:
    """If we do not have TIs, we just get the structure from the DAG."""
    nodes = [task_group_to_dict(child) for child in get_task_group_children_getter()(dag.task_group)]
    return StructureDataResponse(nodes=nodes, edges=[])


def _get_serdag(ti, session):
    dag_version = ti.dag_version
    if not dag_version:
        dag_version = session.scalar(
            select(DagVersion)
            .where(
                DagVersion.dag_id == ti.dag_id,
            )
            .order_by(DagVersion.id)  # ascending cus this is mostly for pre-3.0 upgrade
            .limit(1)
        )
    if not dag_version:
        raise RuntimeError("No dag_version object could be found.")
    if not dag_version.serialized_dag:
        log.error(
            "No serialized dag found",
            dag_id=dag_version.dag_id,
            version_id=dag_version.id,
            version_number=dag_version.version_number,
        )
    return dag_version.serialized_dag


def get_combined_structure(task_instances, session):
    """Given task instances with varying DAG versions, get a combined structure."""
    merged_nodes = []
    # we dedup with serdag, as serdag.dag varies somehow?
    serdags = {_get_serdag(ti, session) for ti in task_instances}
    dags = []
    for serdag in serdags:
        if serdag:
            dags.append(serdag.dag)
    for dag in dags:
        nodes = [task_group_to_dict(child) for child in get_task_group_children_getter()(dag.task_group)]
        _merge_node_dicts(merged_nodes, nodes)

    return StructureDataResponse(nodes=merged_nodes, edges=[])


def _merge_node_dicts(current, new) -> None:
    current_ids = {node["id"] for node in current}
    for node in new:
        if node["id"] in current_ids:
            current_node = _get_node_by_id(current, node["id"])
            # if we have children, merge those as well
            if current_node.get("children"):
                _merge_node_dicts(current_node["children"], node["children"])
        else:
            current.append(node)


def _get_node_by_id(nodes, node_id):
    for node in nodes:
        if node["id"] == node_id:
            return node
    return {}


def _is_task_node_mapped_task_group(task_node: BaseOperator | MappedTaskGroup | TaskMap | None) -> bool:
    """Check if the Task Node is a Mapped Task Group."""
    return type(task_node) is MappedTaskGroup


def agg_state(states):
    states = Counter(states)
    for state in state_priority:
        if state in states:
            return state
    return None


def _find_aggregates(
    node: TaskGroup | BaseOperator | MappedTaskGroup | TaskMap,
    parent_node: TaskGroup | BaseOperator | MappedTaskGroup | TaskMap | None,
    ti_states: dict[str, list[str]],
) -> Iterable[dict]:
    """Recursively fill the Task Group Map."""
    node_id = node.node_id
    parent_id = parent_node.node_id if parent_node else None

    if node is None:
        return

    if isinstance(node, MappedOperator):
        yield {
            "task_id": node_id,
            "type": "mapped_task",
            "parent_id": parent_id,
            "state": agg_state(ti_states[node_id]),
        }

        return
    if isinstance(node, TaskGroup):
        states = []
        for child in get_task_group_children_getter()(node):
            for child_node in _find_aggregates(node=child, parent_node=node, ti_states=ti_states):
                states.append(child_node["state"])
                yield child_node
        if node_id:
            yield {
                "task_id": node_id,
                "type": "group",
                "parent_id": parent_id,
                "state": agg_state(states),
            }
        return
    if isinstance(node, BaseOperator):
        yield {
            "task_id": node_id,
            "type": "task",
            "parent_id": parent_id,
            "state": agg_state(ti_states[node_id]),
        }
        return
