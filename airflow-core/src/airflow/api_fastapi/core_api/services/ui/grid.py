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

from collections import Counter
from collections.abc import Iterable

import structlog

from airflow.api_fastapi.common.parameters import state_priority
from airflow.models.taskmap import TaskMap
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.task_group import get_task_group_children_getter

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
    node: TaskGroup | MappedTaskGroup | SerializedBaseOperator | TaskMap,
    parent_node: TaskGroup | MappedTaskGroup | SerializedBaseOperator | TaskMap | None,
    ti_details: dict[str, list],
) -> Iterable[dict]:
    """Recursively fill the Task Group Map."""
    node_id = node.node_id
    parent_id = parent_node.node_id if parent_node else None
    details = ti_details[node_id]

    if node is None:
        return
    if isinstance(node, MappedOperator):
        yield {
            "task_id": node_id,
            "type": "mapped_task",
            "parent_id": parent_id,
            **_get_aggs_for_node(details),
        }

        return
    if isinstance(node, TaskGroup):
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
