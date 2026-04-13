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
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

from airflow.api_fastapi.common.parameters import state_priority
from airflow.api_fastapi.core_api.services.ui.task_group import get_task_group_children_getter
from airflow.models.taskmap import TaskMap
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.definitions.taskgroup import SerializedTaskGroup

log = structlog.get_logger(logger_name=__name__)


@dataclass
class GridNodeAgg:
    """Compact task instance summary used to aggregate grid state without keeping TI details."""

    child_states: Counter[Any] = field(default_factory=Counter)
    min_start_date: datetime | None = None
    max_end_date: datetime | None = None
    dag_version_number: int | None = None

    def add_ti(
        self,
        *,
        state: Any,
        start_date: datetime | None,
        end_date: datetime | None,
        dag_version_number: int | None,
    ) -> None:
        """Merge one task instance row into the summary."""
        self.child_states[state] += 1
        if start_date is not None and (self.min_start_date is None or start_date < self.min_start_date):
            self.min_start_date = start_date
        if end_date is not None and (self.max_end_date is None or end_date > self.max_end_date):
            self.max_end_date = end_date
        if dag_version_number is not None and (
            self.dag_version_number is None or dag_version_number > self.dag_version_number
        ):
            self.dag_version_number = dag_version_number

    def merge(self, other: GridNodeAgg) -> None:
        """Merge another summary into this one."""
        self.child_states.update(other.child_states)
        if other.min_start_date is not None and (
            self.min_start_date is None or other.min_start_date < self.min_start_date
        ):
            self.min_start_date = other.min_start_date
        if other.max_end_date is not None and (
            self.max_end_date is None or other.max_end_date > self.max_end_date
        ):
            self.max_end_date = other.max_end_date
        if other.dag_version_number is not None and (
            self.dag_version_number is None or other.dag_version_number > self.dag_version_number
        ):
            self.dag_version_number = other.dag_version_number

    def with_placeholder_state(self) -> GridNodeAgg:
        """Represent mapped tasks without rows as a single no-status square in the grid."""
        if self.child_states:
            return self
        placeholder = GridNodeAgg(dag_version_number=self.dag_version_number)
        placeholder.add_ti(
            state=None,
            start_date=None,
            end_date=None,
            dag_version_number=self.dag_version_number,
        )
        return placeholder


def _merge_node_dicts(current: list[dict[str, Any]], new: list[dict[str, Any]] | None) -> None:
    """Merge node dictionaries from different DAG versions, handling structure changes."""
    # Handle None case - can occur when merging old DAG versions
    # where a TaskGroup was converted to a task or vice versa
    if new is None:
        return

    current_nodes_by_id = {node["id"]: node for node in current}
    for node in new:
        node_id = node["id"]
        current_node = current_nodes_by_id.get(node_id)
        if current_node is not None:
            # Only merge children if current node already has children
            # This preserves the structure of the latest DAG version
            if current_node.get("children") is not None:
                _merge_node_dicts(current_node["children"], node.get("children"))
        else:
            current.append(node)
            current_nodes_by_id[node_id] = node


def agg_state(states):
    state_counts = states if isinstance(states, Counter) else Counter(states)
    for state in state_priority:
        if state in state_counts:
            return state
    return None


def _get_aggs_for_node(summary: GridNodeAgg) -> dict[str, Any]:
    return {
        "state": agg_state(summary.child_states),
        "min_start_date": summary.min_start_date,
        "max_end_date": summary.max_end_date,
        "child_states": dict(summary.child_states),
        "dag_version_number": summary.dag_version_number,
    }


def _find_aggregates(
    node: SerializedTaskGroup | SerializedBaseOperator | TaskMap,
    parent_node: SerializedTaskGroup | SerializedBaseOperator | TaskMap | None,
    ti_details: Mapping[str, GridNodeAgg],
) -> Iterable[tuple[dict[str, Any], GridNodeAgg]]:
    """Recursively fill the Task Group Map."""
    node_id = node.node_id
    parent_id = parent_node.node_id if parent_node else None
    # Do not mutate ti_details by accidental key creation
    summary = ti_details.get(node_id)
    if summary is None:
        summary = GridNodeAgg()

    if node is None:
        return
    if isinstance(node, SerializedMappedOperator):
        mapped_summary = summary.with_placeholder_state()
        yield (
            {
                "task_id": node_id,
                "task_display_name": node.task_display_name,
                "type": "mapped_task",
                "parent_id": parent_id,
                **_get_aggs_for_node(mapped_summary),
            },
            mapped_summary,
        )

        return
    if isinstance(node, SerializedTaskGroup):
        children_summary = GridNodeAgg()
        for child in get_task_group_children_getter()(node):
            for child_node, child_summary in _find_aggregates(
                node=child, parent_node=node, ti_details=ti_details
            ):
                if child_node["parent_id"] == node_id:
                    children_summary.merge(child_summary)
                yield child_node, child_summary
        if node_id:
            yield (
                {
                    "task_id": node_id,
                    "task_display_name": node_id,
                    "type": "group",
                    "parent_id": parent_id,
                    **_get_aggs_for_node(children_summary),
                },
                children_summary,
            )
        return
    if isinstance(node, SerializedBaseOperator):
        yield (
            {
                "task_id": node_id,
                "task_display_name": node.task_display_name,
                "type": "task",
                "parent_id": parent_id,
                **_get_aggs_for_node(summary),
            },
            summary,
        )
        return
