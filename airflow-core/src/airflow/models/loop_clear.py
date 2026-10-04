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

from collections.abc import Collection, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from sqlalchemy import exists, select, tuple_

from airflow.exceptions import AirflowClearRunningTaskException
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import (
    SENTINEL_REGION_ID,
    DynamicRegion,
    load_region_ancestry,
    loop_position,
)
from airflow.models.task_coordinates import TaskCoordinateResolver, enclosing_loop
from airflow.models.taskinstance import TaskInstance, _get_relevant_map_indexes, clear_task_instances
from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup


@dataclass(frozen=True)
class LoopClearScope:
    """Live executions to retry and generated executions to archive."""

    retry_ids: frozenset[UUID]
    archive_ids: frozenset[UUID]


def select_loop_clear_scope(
    selected: Collection[TaskInstance],
    *,
    whole_expansion_ids: Collection[UUID] = (),
    upstream: bool = False,
    downstream: bool = True,
    later_loop_iterations: bool = True,
    session: Session,
) -> LoopClearScope:
    """Select loop clear executions; mutation callers must hold the DagRun lock."""
    if not selected:
        return LoopClearScope(frozenset(), frozenset())
    runs = {(ti.dag_id, ti.run_id) for ti in selected}
    if len(runs) != 1:
        raise ValueError("Loop clear selection must belong to one DagRun")
    dag_id, run_id = runs.pop()
    live = {
        ti.id: ti
        for ti in session.scalars(
            select(TaskInstance)
            .where(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.working_set.is_(True),
            )
            .execution_options(populate_existing=True)
        )
    }
    selected_ids = {ti.id for ti in selected}
    if not selected_ids <= live.keys():
        raise ValueError("Loop clear selection contains executions that are no longer live")
    if not set(whole_expansion_ids) <= selected_ids:
        raise ValueError("Whole-task selection requires an explicitly selected execution")
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    regions = load_region_ancestry(
        {ti.region_id for ti in live.values()}, dag_id=dag_id, run_id=run_id, session=session
    )
    retry = {ti_id: live[ti_id] for ti_id in selected_ids}
    for ti in tuple(retry.values()):
        task = resolver.get_task(dag_id, run_id, ti.task_id, dag_version_id=ti.dag_version_id)
        group = enclosing_loop(task)
        if group is not None and loop_position(regions, ti.region_id, ti.region_index, group.node_id) is None:
            raise ValueError("Loop clear selection requires an execution inside its pinned loop")
        if ti.id in whole_expansion_ids:
            if not task.get_needs_expansion():
                raise ValueError("Whole-expansion selection requires a mapped task")
            retry.update(
                (other.id, other)
                for other in resolver.resolve(dag_id=dag_id, run_id=run_id, task_id=ti.task_id, caller=ti)
            )
    for is_upstream, enabled in ((True, upstream), (False, downstream)):
        if not enabled:
            continue
        for ti in tuple(retry.values()):
            task = resolver.get_task(dag_id, run_id, ti.task_id, dag_version_id=ti.dag_version_id)
            contexts = resolver.producer_contexts(ti)
            count = (
                get_mapped_ti_count(task, run_id, session=session, producer_contexts=contexts)
                if task.get_needs_expansion() and ti.region_index >= 0
                else None
            )
            for relative in task.get_flat_relatives(upstream=is_upstream):
                indexes = _get_relevant_map_indexes(
                    task=task,
                    run_id=run_id,
                    map_index=resolver.public_map_index(ti),
                    relative=relative,
                    ti_count=count,
                    session=session,
                    producer_contexts=contexts,
                )
                relative_loop = enclosing_loop(relative)
                task_loop = enclosing_loop(task)
                matches: Collection[TaskInstance]
                if relative_loop is not None and (
                    task_loop is None or task_loop.group_id != relative_loop.group_id
                ):
                    matches = [
                        other
                        for other in live.values()
                        if other.task_id == relative.task_id
                        and (
                            indexes is None
                            or (isinstance(indexes, int) and resolver.public_map_index(other) == indexes)
                            or (isinstance(indexes, range) and resolver.public_map_index(other) in indexes)
                        )
                    ]
                else:
                    matches = resolver.resolve(
                        dag_id=dag_id,
                        run_id=run_id,
                        task_id=relative.task_id,
                        caller=ti,
                        map_indexes=indexes,
                    )
                retry.update((other.id, other) for other in matches)
    archived: set[UUID] = set()
    if later_loop_iterations:
        for ti in retry.values():
            task = resolver.get_task(dag_id, run_id, ti.task_id, dag_version_id=ti.dag_version_id)
            group = enclosing_loop(task)
            if group is None or ti.task_id != group.gate_task_id:
                continue
            position = loop_position(regions, ti.region_id, ti.region_index, group.node_id)
            if position is None:
                raise ValueError("Gate coordinates do not belong to its pinned loop")
            for other in live.values():
                other_position = loop_position(regions, other.region_id, other.region_index, group.node_id)
                if (
                    other_position is not None
                    and other_position[0] == position[0]
                    and other_position[1] > position[1]
                ):
                    archived.add(other.id)
    return LoopClearScope(frozenset(retry.keys() - archived), frozenset(archived))


def _regions_for_run(ti: TaskInstance, session: Session) -> dict[UUID, DynamicRegion]:
    return {
        region.id: region
        for region in session.scalars(
            select(DynamicRegion).where(DynamicRegion.dag_id == ti.dag_id, DynamicRegion.run_id == ti.run_id)
        )
    }


def _physical_loop_coordinate(
    region_id: UUID, index: int, node_id: str, regions: dict[UUID, DynamicRegion]
) -> tuple[UUID, int] | None:
    while region_id in regions:
        region = regions[region_id]
        if region.node_id == node_id:
            return region_id, index
        if region.parent_region_id is None or region.parent_region_index is None:
            return None
        region_id, index = region.parent_region_id, region.parent_region_index
    return None


def _forks_after(region_id: UUID, regions: dict[UUID, DynamicRegion]) -> Iterator[DynamicRegion]:
    successors = {region.forked_from_region_id: region for region in regions.values()}
    while region_id in successors:
        successor = successors[region_id]
        yield successor
        region_id = successor.id


def loop_execution_is_superseded(ti: TaskInstance, *, session: Session) -> bool:
    """Identify a archiving loop coordinate for termination acknowledgement."""
    return loop_coordinate_is_superseded(ti.region_id, ti.region_index, _regions_for_run(ti, session))


def loop_coordinate_is_superseded(region_id: UUID, index: int, regions: dict[UUID, DynamicRegion]) -> bool:
    """Test whether a coordinate or its parent lies beyond a durable fork cut."""
    while region_id in regions:
        if any(region.resumes_from_index <= index for region in _forks_after(region_id, regions)):
            return True
        region = regions[region_id]
        if region.parent_region_id is None or region.parent_region_index is None:
            break
        region_id, index = region.parent_region_id, region.parent_region_index
    return False


def loop_gate_waits_for_archival(gate: TaskInstance, *, session: Session) -> bool:
    """Hold a rerun gate until superseded executions of its later passes finish termination."""
    if gate.operator != "LoopGateOperator" or gate.region_id == SENTINEL_REGION_ID:
        return False
    if not session.scalar(
        select(
            exists().where(
                DynamicRegion.dag_id == gate.dag_id,
                DynamicRegion.run_id == gate.run_id,
                DynamicRegion.forked_from_region_id.is_not(None),
            )
        )
    ):
        return False
    regions = _regions_for_run(gate, session)
    gate_region = regions.get(gate.region_id)
    if gate_region is None:
        return False
    node_id = gate_region.node_id
    gate_position = loop_position(regions, gate.region_id, gate.region_index, node_id)
    if gate_position is None:
        raise ValueError("Gate coordinates do not belong to its pinned loop")
    for region_id, region_index in session.execute(
        select(TaskInstance.region_id, TaskInstance.region_index).where(
            TaskInstance.dag_id == gate.dag_id,
            TaskInstance.run_id == gate.run_id,
            TaskInstance.working_set.is_(True),
            TaskInstance.state == TaskInstanceState.RESTARTING,
        )
    ):
        position = loop_position(regions, region_id, region_index, node_id)
        if position is None or position[0] != gate_position[0] or position[1] <= gate_position[1]:
            continue
        coordinate = _physical_loop_coordinate(region_id, region_index, node_id, regions)
        if coordinate is not None and any(
            region.resumes_from_index <= coordinate[1] for region in _forks_after(coordinate[0], regions)
        ):
            return True
    return False


def loop_gate_has_later_pass(gate: TaskInstance, group: SerializedLoopTaskGroup, *, session: Session) -> bool:
    """Find a live gate of a later pass in the same loop family, including passes in forked regions."""
    regions = _regions_for_run(gate, session)
    position = loop_position(regions, gate.region_id, gate.region_index, group.node_id)
    if position is None:
        raise ValueError("Gate coordinates do not belong to its pinned loop")
    for region_id, region_index in session.execute(
        select(TaskInstance.region_id, TaskInstance.region_index).where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == gate.dag_id,
            TaskInstance.run_id == gate.run_id,
            TaskInstance.task_id == gate.task_id,
            TaskInstance.region_index > gate.region_index,
        )
    ):
        other = loop_position(regions, region_id, region_index, group.node_id)
        if other is not None and other[0] == position[0]:
            return True
    return False


def loop_successor_region(gate: TaskInstance, *, session: Session) -> UUID:
    """Choose the region for a newly generated successor pass."""
    region_id = gate.region_id
    for fork in _forks_after(gate.region_id, _regions_for_run(gate, session)):
        if fork.resumes_from_index <= gate.region_index + 1:
            region_id = fork.id
    return region_id


def clear_loop_task_instances(
    selected: Collection[TaskInstance],
    *,
    whole_expansion_ids: Collection[UUID] = (),
    upstream: bool = False,
    downstream: bool = True,
    later_loop_iterations: bool = True,
    session: Session,
    dag_run_state: DagRunState | Literal[False] = DagRunState.QUEUED,
    run_on_latest_version: bool = False,
    prevent_running_task: bool | None = None,
    whole_task_keys: Collection[tuple[str, str, str]] = (),
) -> LoopClearScope:
    """Clear selected loop tries and archive their replaced generated suffixes."""
    from airflow.models.dagrun import DagRun

    if not selected:
        return LoopClearScope(frozenset(), frozenset())
    run_keys = {(ti.dag_id, ti.run_id) for ti in selected}
    if len(run_keys) != 1:
        raise ValueError("Loop clear selection must belong to one DagRun")
    dag_id, run_id = run_keys.pop()
    session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id).with_for_update())
    scope = select_loop_clear_scope(
        selected,
        whole_expansion_ids=whole_expansion_ids,
        upstream=upstream,
        downstream=downstream,
        later_loop_iterations=later_loop_iterations,
        session=session,
    )
    apply_loop_clear_scope(
        scope,
        later_loop_iterations=later_loop_iterations,
        session=session,
        dag_run_state=dag_run_state,
        run_on_latest_version=run_on_latest_version,
        prevent_running_task=prevent_running_task,
        whole_task_keys=whole_task_keys,
    )
    return scope


def clear_task_instances_for_runs(
    tis: Collection[TaskInstance],
    *,
    session: Session,
    dag_run_state: DagRunState | Literal[False] = DagRunState.QUEUED,
    run_on_latest_version: bool = False,
    prevent_running_task: bool | None = None,
    whole_task_keys: Collection[tuple[str, str, str]] = (),
    later_loop_iterations: bool = True,
) -> None:
    """
    Clear exactly the given executions, archiving the later passes of any loop gate among them.

    Runs without a selected gate are retried in place as an ordinary clear.
    """
    from airflow.models.dagrun import DagRun

    if not tis:
        return
    run_keys = sorted({(ti.dag_id, ti.run_id) for ti in tis})
    session.scalars(
        select(DagRun.id)
        .where(tuple_(DagRun.dag_id, DagRun.run_id).in_(run_keys))
        .order_by(DagRun.dag_id, DagRun.run_id)
        .with_for_update()
    ).all()
    gate_runs = {(ti.dag_id, ti.run_id) for ti in tis if ti.operator == "LoopGateOperator"}
    clear_task_instances(
        [ti for ti in tis if (ti.dag_id, ti.run_id) not in gate_runs],
        session=session,
        dag_run_state=dag_run_state,
        run_on_latest_version=run_on_latest_version,
        prevent_running_task=prevent_running_task,
        whole_task_keys=whole_task_keys,
    )
    for run_key in sorted(gate_runs):
        clear_loop_task_instances(
            [ti for ti in tis if (ti.dag_id, ti.run_id) == run_key],
            downstream=False,
            later_loop_iterations=later_loop_iterations,
            session=session,
            dag_run_state=dag_run_state,
            run_on_latest_version=run_on_latest_version,
            prevent_running_task=prevent_running_task,
            whole_task_keys=whole_task_keys,
        )


def apply_loop_clear_scope(
    scope: LoopClearScope,
    *,
    later_loop_iterations: bool = True,
    session: Session,
    dag_run_state: DagRunState | Literal[False] = DagRunState.QUEUED,
    run_on_latest_version: bool = False,
    prevent_running_task: bool | None = None,
    whole_task_keys: Collection[tuple[str, str, str]] = (),
) -> list[TaskInstance]:
    """Apply a planned clear while the caller holds its DagRun lock."""
    selected_ids = scope.retry_ids | scope.archive_ids
    if not selected_ids:
        return []
    tis = list(
        session.scalars(
            select(TaskInstance)
            .where(TaskInstance.id.in_(selected_ids), TaskInstance.working_set.is_(True))
            .execution_options(populate_existing=True)
        )
    )
    if {ti.id for ti in tis} != selected_ids:
        raise ValueError("Clear selection contains executions that are no longer live")
    run_keys = {(ti.dag_id, ti.run_id) for ti in tis}
    if len(run_keys) != 1:
        raise ValueError("Clear selection must belong to one DagRun")
    dag_id, run_id = run_keys.pop()
    if prevent_running_task and any(
        ti.state in (TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING) for ti in tis
    ):
        raise AirflowClearRunningTaskException("Cannot clear running task instances")
    if later_loop_iterations:
        resolver = TaskCoordinateResolver(DBDagBag(), session)
        regions = _regions_for_run(tis[0], session)
        cuts: dict[UUID, int] = {}
        for ti in tis:
            if ti.id not in scope.retry_ids or ti.operator != "LoopGateOperator":
                continue
            task = resolver.get_task(dag_id, run_id, ti.task_id, dag_version_id=ti.dag_version_id)
            group = enclosing_loop(task)
            if group is None or group.gate_task_id != ti.task_id:
                raise ValueError("Gate coordinates require a pinned loop definition")
            family = loop_position(regions, ti.region_id, ti.region_index, group.node_id)
            if family is None:
                raise ValueError("Gate coordinates do not belong to its pinned loop")
            cuts[family[0]] = min(cuts.get(family[0], ti.region_index + 1), ti.region_index + 1)
        for family_id, index in cuts.items():
            leaf = regions[family_id]
            for successor in _forks_after(family_id, regions):
                leaf = successor
            session.add(
                DynamicRegion(
                    dag_id=dag_id,
                    run_id=run_id,
                    node_id=leaf.node_id,
                    parent_region_id=leaf.parent_region_id,
                    parent_region_index=leaf.parent_region_index,
                    forked_from_region_id=leaf.id,
                    resumes_from_index=index,
                )
            )
        session.flush()
    return clear_task_instances(
        tis,
        session=session,
        superseded_ti_ids=scope.archive_ids,
        dag_run_state=dag_run_state,
        run_on_latest_version=run_on_latest_version,
        prevent_running_task=prevent_running_task,
        whole_task_keys=whole_task_keys,
    )
