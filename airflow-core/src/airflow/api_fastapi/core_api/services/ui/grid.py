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

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

import structlog
from fastapi import HTTPException
from sqlalchemy import select

from airflow.api_fastapi.common.parameters import state_priority
from airflow.api_fastapi.core_api.datamodels.ui.grid import (
    LoopInvocationResponse,
    LoopIterationSummary,
    LoopSummaryResponse,
)
from airflow.api_fastapi.core_api.services.ui.task_group import get_task_group_children_getter
from airflow.models.dynamic_region import DynamicRegion, loop_position
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup, SerializedTaskGroup
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag
    from airflow.models.dagrun import DagRun

log = structlog.get_logger(logger_name=__name__)


def has_started(state: Any, start_date: datetime | None) -> bool:
    """Tell whether a task instance has run, so an iteration holding only unstarted rows is not counted."""
    return start_date is not None or state in (TaskInstanceState.FAILED, TaskInstanceState.SUCCESS)


@dataclass
class GridNodeAgg:
    """Compact task instance summary used to aggregate grid state without keeping TI details."""

    child_states: Counter[Any] = field(default_factory=Counter)
    min_start_date: datetime | None = None
    max_end_date: datetime | None = None
    dag_version_number: int | None = None
    has_note: bool = False
    loop_iterations: dict[str, set[tuple[UUID, int]]] = field(default_factory=dict)
    latest_region: tuple[UUID, int] | None = None

    def add_ti(
        self,
        *,
        state: Any,
        start_date: datetime | None,
        end_date: datetime | None,
        dag_version_number: int | None,
        has_note: bool = False,
        loop_positions: dict[str, tuple[UUID, int]] | None = None,
        count_state: bool = True,
        region: tuple[UUID, int] | None = None,
    ) -> None:
        """
        Merge one task instance row into the summary.

        ``count_state`` is false for a superseded loop iteration: its position still counts
        towards the iterations run, but its state no longer describes where the task stands.
        """
        if count_state:
            self.child_states[state] += 1
            # A looped task has one row per iteration, so a link without a coordinate cannot say
            # which one it means. Carry the newest so the grid can address it.
            if region is not None:
                self.latest_region = region
        if has_started(state, start_date):
            for loop_id, position in (loop_positions or {}).items():
                self.loop_iterations.setdefault(loop_id, set()).add(position)
        if start_date is not None and (self.min_start_date is None or start_date < self.min_start_date):
            self.min_start_date = start_date
        if end_date is not None and (self.max_end_date is None or end_date > self.max_end_date):
            self.max_end_date = end_date
        if dag_version_number is not None and (
            self.dag_version_number is None or dag_version_number > self.dag_version_number
        ):
            self.dag_version_number = dag_version_number
        self.has_note = self.has_note or has_note

    def merge(self, other: GridNodeAgg) -> None:
        """Merge another summary into this one."""
        self.child_states.update(other.child_states)
        for loop_id, positions in other.loop_iterations.items():
            self.loop_iterations.setdefault(loop_id, set()).update(positions)
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
        self.has_note = self.has_note or other.has_note

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
    """Merge node dictionaries from different Dag versions, handling structure changes."""
    # Handle None case - can occur when merging old Dag versions
    # where a TaskGroup was converted to a task or vice versa
    if new is None:
        return

    current_nodes_by_id = {node["id"]: node for node in current}
    for node in new:
        node_id = node["id"]
        current_node = current_nodes_by_id.get(node_id)
        if current_node is not None:
            # Only merge children if current node already has children
            # This preserves the structure of the latest Dag version
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


def _serialize_child_states(child_states: Counter[Any]) -> dict[str, int]:
    return {state if state is not None else "none": count for state, count in child_states.items()}


def _get_aggs_for_node(summary: GridNodeAgg) -> dict[str, Any]:
    return {
        "state": agg_state(summary.child_states),
        "min_start_date": summary.min_start_date,
        "max_end_date": summary.max_end_date,
        "child_states": _serialize_child_states(summary.child_states),
        "dag_version_number": summary.dag_version_number,
        "has_note": summary.has_note,
        "latest_region_id": None if summary.latest_region is None else summary.latest_region[0],
        "latest_region_index": None if summary.latest_region is None else summary.latest_region[1],
    }


def _find_aggregates(
    node: SerializedTaskGroup | SerializedBaseOperator,
    parent_node: SerializedTaskGroup | SerializedBaseOperator | None,
    ti_details: Mapping[str, GridNodeAgg],
    group_dict: dict[str | None, SerializedTaskGroup] | None = None,
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
        if group_dict is None:
            group_dict = node.dag.task_group.get_task_group_dict()
        children_summary = GridNodeAgg()
        for child in get_task_group_children_getter()(node, group_dict):
            for child_node, child_summary in _find_aggregates(
                node=child, parent_node=node, ti_details=ti_details, group_dict=group_dict
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
                    **(
                        {"loop_iterations_count": len(children_summary.loop_iterations.get(node_id, set()))}
                        if isinstance(node, SerializedLoopTaskGroup)
                        else {}
                    ),
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


@dataclass
class LoopRunInputs:
    """The definitions and live task instances one Dag run contributes to a loop summary."""

    run: DagRun
    members: list[Any] = field(default_factory=list)
    group: SerializedLoopTaskGroup | None = None
    loop_definitions: dict[UUID | None, SerializedLoopTaskGroup] = field(default_factory=dict)
    regions: dict[UUID, DynamicRegion] = field(default_factory=dict)
    error: HTTPException | None = None


def _find_loop_definition(
    run: DagRun, group_id: str, *, session: Session, dag_bag: DBDagBag
) -> SerializedLoopTaskGroup:
    if run.created_dag_version_id is None:
        raise HTTPException(404, "Pinned DAG definition not found")
    dag = dag_bag.get_dag(run.created_dag_version_id, session=session)
    definition = dag.task_group.get_task_group_dict().get(group_id) if dag else None
    if definition is None:
        raise HTTPException(404, "Task group not found")
    if not isinstance(definition, SerializedLoopTaskGroup):
        raise HTTPException(422, "Task group is not a loop")
    return definition


def load_loop_run_inputs(
    runs: Sequence[DagRun], group_id: str, *, session: Session, dag_bag: DBDagBag
) -> dict[str, LoopRunInputs]:
    """Load a loop's task instances and regions for several runs of one Dag in a fixed number of queries."""
    if not runs:
        return {}
    dag_id = runs[0].dag_id
    live = (
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id.in_([run.run_id for run in runs]),
        TaskInstance.working_set.is_(True),
    )
    version_ids_by_run: dict[str, set[UUID | None]] = defaultdict(set)
    for run_id, version_id in session.execute(
        select(TaskInstance.run_id, TaskInstance.dag_version_id).where(*live).distinct()
    ):
        version_ids_by_run[run_id].add(version_id)
    inputs = {run.run_id: LoopRunInputs(run=run) for run in runs}
    found_by_run: dict[str, dict[UUID | None, SerializedLoopTaskGroup]] = {}
    for run in runs:
        found: dict[UUID | None, SerializedLoopTaskGroup] = {}
        try:
            for version_id in version_ids_by_run[run.run_id]:
                pinned_id = version_id or run.created_dag_version_id
                if pinned_id is None:
                    raise HTTPException(404, "Pinned DAG definition not found")
                dag = dag_bag.get_dag(pinned_id, session=session)
                definition = dag.task_group.get_task_group_dict().get(group_id) if dag else None
                if isinstance(definition, SerializedLoopTaskGroup):
                    found[version_id] = definition
        except HTTPException as error:
            inputs[run.run_id].error = error
        else:
            found_by_run[run.run_id] = found
    task_ids_by_run = {
        run_id: {
            version_id: {task.task_id for task in definition.iter_tasks()}
            for version_id, definition in found.items()
        }
        for run_id, found in found_by_run.items()
    }
    wanted_task_ids = set().union(
        *(ids for versions in task_ids_by_run.values() for ids in versions.values())
    )
    if wanted_task_ids:
        member_rows = session.execute(
            select(
                TaskInstance.run_id,
                TaskInstance.task_id,
                TaskInstance.region_id,
                TaskInstance.region_index,
                TaskInstance.state,
                TaskInstance.start_date,
                TaskInstance.end_date,
                TaskInstance.dag_version_id,
            )
            .where(*live, TaskInstance.task_id.in_(wanted_task_ids))
            .order_by(TaskInstance.task_id, TaskInstance.region_id, TaskInstance.region_index)
        )
        for ti in member_rows:
            if ti.task_id in task_ids_by_run.get(ti.run_id, {}).get(ti.dag_version_id, ()):
                inputs[ti.run_id].members.append(ti)
    regions: dict[UUID, DynamicRegion] = {}
    if any(run_inputs.members for run_inputs in inputs.values()):
        regions = {
            region.id: region
            for region in session.scalars(
                select(DynamicRegion).where(
                    DynamicRegion.dag_id == dag_id,
                    DynamicRegion.run_id.in_([run.run_id for run in runs]),
                )
            )
        }
    for run_id, run_inputs in inputs.items():
        if run_inputs.error is not None:
            continue
        found = found_by_run[run_id]
        run_inputs.loop_definitions = {
            ti.dag_version_id: found[ti.dag_version_id] for ti in run_inputs.members
        }
        run_inputs.group = next(iter(run_inputs.loop_definitions.values()), None)
        if run_inputs.group is None:
            try:
                run_inputs.group = _find_loop_definition(
                    run_inputs.run, group_id, session=session, dag_bag=dag_bag
                )
            except HTTPException as error:
                run_inputs.error = error
                continue
        run_inputs.regions = regions
    return inputs


def summarize_loop_run(
    inputs: LoopRunInputs, group_id: str, *, loop_region_id: UUID | None = None
) -> list[LoopSummaryResponse]:
    """Read each invocation of a loop separately, keyed by its fork family."""
    if inputs.error is not None:
        raise inputs.error
    run, members, regions, loop_definitions = (
        inputs.run,
        inputs.members,
        inputs.regions,
        inputs.loop_definitions,
    )
    group = inputs.group
    if TYPE_CHECKING:
        assert group is not None
    invocations: dict[UUID, dict[int, list[Any]]] = defaultdict(lambda: defaultdict(list))
    for ti in members:
        if position := loop_position(regions, ti.region_id, ti.region_index, group_id):
            family, index = position
            invocations[family][index].append(ti)
    families = sorted(invocations, key=str)
    if loop_region_id is not None and loop_region_id not in invocations:
        raise HTTPException(404, "Loop invocation not found in this Dag run")
    selected: list[UUID | None] = (
        [loop_region_id] if loop_region_id is not None else [*families] if families else [None]
    )
    region_options = [LoopInvocationResponse(region_id=family) for family in families]
    summaries = []
    for selected_family in selected:
        iterations = invocations[selected_family] if selected_family is not None else {}
        gate_rows = [
            ti
            for tasks in iterations.values()
            for ti in tasks
            if ti.task_id == loop_definitions[ti.dag_version_id].gate_task_id
        ]
        if gate_rows:
            group = loop_definitions[max(gate_rows, key=lambda ti: ti.region_index).dag_version_id]
        gate = group.dag.get_task(group.gate_task_id) if group.has_until else None
        rows = []
        failed = None
        reason_task = None
        for index, tasks in sorted(iterations.items()):
            states = [ti.state for ti in tasks]
            starts = [ti.start_date for ti in tasks if ti.start_date is not None]
            ends = [ti.end_date for ti in tasks if ti.end_date is not None]
            failures = [ti for ti in tasks if ti.state == TaskInstanceState.FAILED]
            if failures and failed is None:
                failed = index
                reason_task = failures[0].task_id
            rows.append(
                LoopIterationSummary(
                    index=index,
                    task_count=len(tasks),
                    state=agg_state(states),
                    start_date=min(starts) if starts else None,
                    end_date=max(ends) if ends and all(s in State.finished for s in states) else None,
                )
            )
        latest = max(iterations, default=-1)
        last_gate = next((ti for ti in iterations.get(latest, []) if ti.task_id == group.gate_task_id), None)
        active = any(ti.state not in State.finished for tasks in iterations.values() for ti in tasks)
        status: Literal["running", "stopped_early", "ran_to_cap", "failed", "skipped", "removed"] = "running"
        stopped = None
        reason: Literal["cap_reached", "iteration_failed"] | None = None
        if failed is not None:
            status, reason = "failed", "iteration_failed"
        elif not active and last_gate is not None:
            if last_gate.state == TaskInstanceState.SKIPPED:
                status = "skipped"
            elif last_gate.state == TaskInstanceState.REMOVED:
                status = "removed"
            elif last_gate.state == TaskInstanceState.UPSTREAM_FAILED:
                status = "failed"
            elif last_gate.state == TaskInstanceState.SUCCESS:
                if latest + 1 == group.max_iterations:
                    status = "ran_to_cap"
                    if not group.has_until:
                        reason = "cap_reached"
                else:
                    status, stopped = "stopped_early", latest
        summaries.append(
            LoopSummaryResponse(
                dag_id=run.dag_id,
                run_id=run.run_id,
                group_id=group_id,
                max_iterations=group.max_iterations,
                iterations_ran=sum(
                    any(has_started(ti.state, ti.start_date) for ti in tasks) for tasks in iterations.values()
                ),
                status=status,
                stopped_at_iteration=stopped,
                failed_at_iteration=failed,
                exit_task_id=group.gate_task_id,
                exit_criteria_name=group.gate_task_id.rpartition(".")[2] if gate is not None else None,
                exit_criteria_doc=gate.doc_md if gate is not None else None,
                reason=reason,
                reason_task_id=reason_task,
                loop_region_id=selected_family,
                loop_regions=region_options,
                iterations=rows,
            )
        )
    return summaries


def loop_run_summaries(
    run: DagRun,
    group_id: str,
    *,
    session: Session,
    dag_bag: DBDagBag,
    loop_region_id: UUID | None = None,
) -> list[LoopSummaryResponse]:
    """Summarize one run's loop invocations, loading its inputs through the batch loader."""
    inputs = load_loop_run_inputs([run], group_id, session=session, dag_bag=dag_bag)[run.run_id]
    return summarize_loop_run(inputs, group_id, loop_region_id=loop_region_id)
