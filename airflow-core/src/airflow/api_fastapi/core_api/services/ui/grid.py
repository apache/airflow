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

import collections
from collections import Counter
from collections.abc import Iterable
from typing import TYPE_CHECKING

import structlog
from sqlalchemy import select

from airflow.api_fastapi.common.parameters import state_priority
from airflow.api_fastapi.core_api.services.ui.task_group import get_task_group_children_getter
from airflow.models.dag_version import DagVersion
from airflow.models.mappedoperator import MappedOperator
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.serialization.definitions.taskgroup import SerializedTaskGroup
from airflow.serialization.serialized_objects import SerializedBaseOperator

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

log = structlog.get_logger(logger_name=__name__)


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


def agg_state(states):
    states = Counter(states)
    for state in state_priority:
        if state in states:
            return state
    return None


def _get_aggs_for_node(detail):
    states = [x["state"] for x in detail]
    try:
        min_start_date = min(x["start_date"] for x in detail if x["start_date"])
    except ValueError:
        min_start_date = None
    try:
        max_end_date = max(x["end_date"] for x in detail if x["end_date"])
    except ValueError:
        max_end_date = None
    return {
        "state": agg_state(states),
        "min_start_date": min_start_date,
        "max_end_date": max_end_date,
        "child_states": dict(Counter(states)),
    }


def _find_aggregates(
    node: SerializedTaskGroup | SerializedBaseOperator | TaskMap,
    parent_node: SerializedTaskGroup | SerializedBaseOperator | TaskMap | None,
    ti_details: dict[str, list],
) -> Iterable[dict]:
    """Recursively fill the Task Group Map."""
    node_id = node.node_id
    parent_id = parent_node.node_id if parent_node else None
    # Do not mutate ti_details by accidental key creation
    details = ti_details.get(node_id, [])

    if node is None:
        return
    if isinstance(node, MappedOperator):
        # For unmapped tasks, reflect a single None state so UI shows one square
        mapped_details = details or [{"state": None, "start_date": None, "end_date": None}]
        yield {
            "task_id": node_id,
            "type": "mapped_task",
            "parent_id": parent_id,
            **_get_aggs_for_node(mapped_details),
        }

        return
    if isinstance(node, SerializedTaskGroup):
        children = []
        for child in get_task_group_children_getter()(node):
            for child_node in _find_aggregates(node=child, parent_node=node, ti_details=ti_details):
                if child_node["parent_id"] == node_id:
                    children.append(
                        {
                            "state": child_node["state"],
                            "start_date": child_node["min_start_date"],
                            "end_date": child_node["max_end_date"],
                        }
                    )
                yield child_node
        if node_id:
            yield {
                "task_id": node_id,
                "type": "group",
                "parent_id": parent_id,
                **_get_aggs_for_node(children),
            }
        return
    if isinstance(node, SerializedBaseOperator):
        yield {
            "task_id": node_id,
            "type": "task",
            "parent_id": parent_id,
            **_get_aggs_for_node(details),
        }
        return


def get_batch_ti_summaries(
    dag_id: str,
    run_ids: list[str],
    session: Session,
) -> dict:
    """
    Fetch task instance summaries for multiple runs in a single query.

    This is much more efficient than the N+1 query pattern of calling
    get_grid_ti_summaries multiple times.

    Returns a dict with structure:
    {
        "dag_id": str,
        "summaries": [GridTISummaries, ...]
    }
    """
    if not run_ids:
        return {"dag_id": dag_id, "summaries": []}

    # Single query to fetch ALL task instances for ALL runs
    tis_query = (
        select(
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.state,
            TaskInstance.dag_version_id,
            TaskInstance.start_date,
            TaskInstance.end_date,
        )
        .where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id.in_(run_ids),
        )
        .order_by(TaskInstance.run_id, TaskInstance.task_id)
    )

    task_instances = list(session.execute(tis_query))

    # Group by run_id and collect unique dag_version_ids
    tis_by_run = collections.defaultdict(list)
    dag_version_ids = set()
    for ti in task_instances:
        tis_by_run[ti.run_id].append(ti)
        if ti.dag_version_id:
            dag_version_ids.add(ti.dag_version_id)

    # Fetch all needed serialized DAGs in one query
    serdags_by_version = {}
    if dag_version_ids:
        serdags = session.scalars(
            select(SerializedDagModel)
            .join(DagVersion, SerializedDagModel.dag_version_id == DagVersion.id)
            .where(DagVersion.id.in_(dag_version_ids))
        )
        for serdag in serdags:
            if serdag.dag_version_id:
                serdags_by_version[serdag.dag_version_id] = serdag

    # Process each run
    summaries = []
    for run_id in run_ids:
        tis = tis_by_run.get(run_id, [])
        if not tis:
            continue

        # Build ti_details structure
        ti_details = collections.defaultdict(list)
        for ti in tis:
            ti_details[ti.task_id].append(
                {
                    "state": ti.state,
                    "start_date": ti.start_date,
                    "end_date": ti.end_date,
                }
            )

        # Get the appropriate serdag
        dag_version_id = tis[0].dag_version_id if tis else None
        serdag = serdags_by_version.get(dag_version_id) if dag_version_id else None

        if not serdag:
            log.warning(
                "No serialized dag found for run",
                dag_id=dag_id,
                run_id=run_id,
                dag_version_id=dag_version_id,
            )
            continue

        # Helper function to generate node summaries
        def get_node_summaries():
            yielded_task_ids: set[str] = set()

            # Yield all nodes discoverable from the serialized DAG structure
            for node in _find_aggregates(
                node=serdag.dag.task_group,
                parent_node=None,
                ti_details=ti_details,
            ):
                if node["type"] in {"task", "mapped_task"}:
                    yielded_task_ids.add(node["task_id"])
                    if node["type"] == "task":
                        node["child_states"] = None
                        node["min_start_date"] = None
                        node["max_end_date"] = None
                yield node

            # For good history: add synthetic leaf nodes for task_ids that have TIs in this run
            # but are not present in the current DAG structure (e.g. removed tasks)
            missing_task_ids = set(ti_details.keys()) - yielded_task_ids
            for task_id in sorted(missing_task_ids):
                detail = ti_details[task_id]
                # Create a leaf task node with aggregated state from its TIs
                agg = _get_aggs_for_node(detail)
                yield {
                    "task_id": task_id,
                    "type": "task",
                    "parent_id": None,
                    **agg,
                    # Align with leaf behavior
                    "child_states": None,
                    "min_start_date": None,
                    "max_end_date": None,
                }

        task_instances_list = list(get_node_summaries())

        # If a group id and a task id collide, prefer the group record
        group_ids = {n.get("task_id") for n in task_instances_list if n.get("type") == "group"}
        filtered = [
            n for n in task_instances_list if not (n.get("type") == "task" and n.get("task_id") in group_ids)
        ]

        summaries.append(
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "task_instances": filtered,
            }
        )

    return {"dag_id": dag_id, "summaries": summaries}
