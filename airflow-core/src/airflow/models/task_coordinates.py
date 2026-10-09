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

from collections import defaultdict
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

import attrs
from sqlalchemy import FromClause, case, or_, select, tuple_

from airflow.exceptions import TaskNotFound
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import (
    SENTINEL_REGION_ID,
    AmbiguousProducerError,
    DynamicRegion,
    ProducerContext,
    load_region_ancestry,
    loop_position,
    resolve_current_producers,
    select_current_producer_ids,
)
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.definitions.mappedoperator import is_mapped
from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup, SerializedMappedTaskGroup

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from sqlalchemy import Select
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.serialization.definitions.dag import SerializedDAG, SerializedOperator


LOOP_GATE_OPERATOR = "LoopGateOperator"
"""Operator class name the Task SDK gives the gate task of a loop."""


@attrs.frozen(kw_only=True)
class _ProducerRequest:
    dag_id: str
    run_id: str
    task_id: str
    is_mapped: bool
    context: ProducerContext | None
    map_indexes: int | Collection[int] | None
    region_id: UUID | None
    region_index: int | None


class TaskCoordinate(Protocol):
    """Stored task identity shared by live and retained task data."""

    dag_id: str
    run_id: str
    task_id: str
    region_id: UUID
    region_index: int
    dag_version_id: UUID | None


def enclosing_loop(task: SerializedOperator) -> SerializedLoopTaskGroup | None:
    group = task.task_group
    while group is not None:
        if isinstance(group, SerializedLoopTaskGroup):
            return group
        group = group.parent_group
    return None


def mapped_region_expression(model) -> ColumnElement[bool]:
    """Match rows whose region is the task's own mapped expansion."""
    columns = model.c if isinstance(model, FromClause) else model
    return (
        select(DynamicRegion.id)
        .where(DynamicRegion.id == columns.region_id, DynamicRegion.node_id == columns.task_id)
        .correlate(model)
        .exists()
    )


def public_map_index_expression(model) -> ColumnElement[int]:
    """Build the public map index for an ORM entity or a Table (tables skip the current-rows filter)."""
    columns = model.c if isinstance(model, FromClause) else model
    return case(
        (or_(columns.region_id == SENTINEL_REGION_ID, mapped_region_expression(model)), columns.region_index),
        else_=-1,
    )


def get_public_map_index(task_instance: TaskInstance, *, session: Session) -> int:
    """Return the map index a client sees for a persisted task instance; only a regional one costs a query."""
    if task_instance.region_id == SENTINEL_REGION_ID:
        return task_instance.region_index
    return session.execute(
        select(public_map_index_expression(TaskInstance)).where(TaskInstance.id == task_instance.id)
    ).scalar_one()


def is_plain_expansion(task_instance: TaskInstance, *, session: Session) -> bool:
    """Tell whether a task instance sits in the top-level expansion of its own task."""
    return bool(
        session.scalar(
            select(
                select(DynamicRegion.id)
                .where(
                    DynamicRegion.id == task_instance.region_id,
                    DynamicRegion.node_id == task_instance.task_id,
                    DynamicRegion.parent_region_id.is_(None),
                )
                .exists()
            )
        )
    )


def get_public_region(region_id: UUID, region_index: int) -> tuple[UUID | None, int | None]:
    """Report the region a client sees: ``None`` for a task instance that lives in no dynamic region."""
    if region_id == SENTINEL_REGION_ID:
        return None, None
    return region_id, region_index


def build_coordinate_filters(
    model, *, map_index: int | None, region_id: UUID | None = None, region_index: int | None = None
) -> list[ColumnElement[bool]]:
    """Match task instances by explicit region coordinates, or by their public map index when none are given."""
    if region_index is not None and region_id is None:
        raise ValueError("region_index requires region_id")
    if region_id is None:
        return [] if map_index is None else [public_map_index_expression(model) == map_index]
    filters = [model.region_id == region_id]
    if region_index is not None:
        filters.append(model.region_index == region_index)
    return filters


@attrs.define
class TaskCoordinateResolver:
    """Resolve task coordinates against the definitions pinned to their executions."""

    dag_bag: DBDagBag
    session: Session
    _dags: dict[UUID, SerializedDAG] = attrs.field(factory=dict, init=False)
    _region_nodes: dict[UUID, str | None] = attrs.field(factory=dict, init=False)
    _regional_tasks: dict[tuple[str, str | None, str], bool] = attrs.field(factory=dict, init=False)

    @classmethod
    def for_dag(cls, dag: SerializedDAG | None, session: Session) -> TaskCoordinateResolver:
        """Build a resolver that reads from the Dag the caller already holds, loading others on demand."""
        resolver = cls(DBDagBag(load_op_links=False), session)
        resolver.adopt_dag(dag)
        return resolver

    def adopt_dag(self, dag: SerializedDAG | None) -> None:
        if dag is not None and dag.dag_version_id is not None:
            self._dags.setdefault(dag.dag_version_id, dag)

    def loop_context(self, ti: TaskCoordinate) -> tuple[SerializedLoopTaskGroup, int] | None:
        if ti.region_id == SENTINEL_REGION_ID:
            return None
        task = self.get_task(ti.dag_id, ti.run_id, ti.task_id, dag_version_id=ti.dag_version_id)
        group = enclosing_loop(task)
        if group is None:
            return None
        regions = load_region_ancestry(
            [ti.region_id], dag_id=ti.dag_id, run_id=ti.run_id, session=self.session
        )
        position = loop_position(regions, ti.region_id, ti.region_index, group.node_id)
        if position is None:
            raise ValueError("Task coordinates do not belong to the pinned loop")
        return group, position[1]

    def get_task(
        self, dag_id: str, run_id: str, task_id: str, *, dag_version_id: UUID | None = None
    ) -> SerializedOperator:
        if dag_version_id is None:
            return self._producer_task(dag_id, run_id, task_id)
        dag = self.get_dag(dag_version_id)
        if dag is None or dag.dag_id != dag_id:
            raise ValueError(f"Pinned Dag for {dag_id}/{run_id} not found")
        return dag.get_task(task_id)

    def find_task(self, ti: TaskInstance) -> SerializedOperator | None:
        """Return the task of ``ti`` from its pinned Dag, or None when that Dag or the task is gone."""
        if ti.dag_version_id is None or (dag := self.get_dag(ti.dag_version_id)) is None:
            return None
        try:
            return dag.get_task(ti.task_id)
        except TaskNotFound:
            return None

    def get_dag(self, dag_version_id: UUID) -> SerializedDAG | None:
        if dag_version_id not in self._dags:
            if (dag := self.dag_bag.get_dag(dag_version_id, session=self.session)) is None:
                return None
            self._dags[dag_version_id] = dag
        return self._dags[dag_version_id]

    def _producer_task(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        *,
        region_id: UUID | None = None,
        region_index: int | None = None,
    ) -> SerializedOperator:
        query = select(TaskInstance.dag_version_id).where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
        )
        if region_id is not None:
            query = query.where(TaskInstance.region_id == region_id)
        if region_index is not None:
            query = query.where(TaskInstance.region_index == region_index)
        versions = set(self.session.scalars(query.distinct()))
        if not versions or None in versions:
            versions.discard(None)
            version = self.session.scalar(
                select(DagRun.created_dag_version_id).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
            )
            if version is None:
                raise ValueError(f"Pinned Dag for {dag_id}/{run_id} not found")
            versions.add(version)
        tasks = [self.get_task(dag_id, run_id, task_id, dag_version_id=version) for version in versions]
        classifications = {
            (task.get_needs_expansion(), loop.group_id if (loop := enclosing_loop(task)) else None)
            for task in tasks
        }
        if len(classifications) != 1:
            raise AmbiguousProducerError(
                "Producer definitions differ between the live task instances of the requested task"
            )
        return tasks[0]

    def public_map_index(self, ti: TaskCoordinate, *, dag_version_id: UUID | None = None) -> int:
        if ti.region_id == SENTINEL_REGION_ID:
            return ti.region_index
        version = dag_version_id or getattr(ti, "dag_version_id", None)
        try:
            task = (
                self.get_task(ti.dag_id, ti.run_id, ti.task_id, dag_version_id=version)
                if version is not None
                else self._producer_task(
                    ti.dag_id, ti.run_id, ti.task_id, region_id=ti.region_id, region_index=ti.region_index
                )
            )
        except (TaskNotFound, ValueError):
            return ti.region_index if self._is_mapped_region(ti) else -1
        return ti.region_index if task.get_needs_expansion() else -1

    def _is_mapped_region(self, ti: TaskCoordinate) -> bool:
        """Tell from stored region data alone whether a task's own expansion holds this coordinate."""
        if ti.region_id not in self._region_nodes:
            self._region_nodes[ti.region_id] = self.session.scalar(
                select(DynamicRegion.node_id).where(DynamicRegion.id == ti.region_id)
            )
        return self._region_nodes[ti.region_id] == ti.task_id

    def prefetch_regional_tasks(self, dag_id: str, run_id: str, task_ids: Collection[str]) -> None:
        """Learn in one query which of ``task_ids`` have live task instances in a dynamic region."""
        if not task_ids:
            return
        regional = set(
            self.session.scalars(
                select(TaskInstance.task_id)
                .where(
                    TaskInstance.working_set.is_(True),
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.task_id.in_(task_ids),
                    TaskInstance.region_id != SENTINEL_REGION_ID,
                )
                .distinct()
            )
        )
        for task_id in task_ids:
            self._regional_tasks[dag_id, run_id, task_id] = task_id in regional

    def select_legacy_task_ids(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_ids: Collection[str],
        slots: Collection[tuple[str, int]],
    ) -> Select[tuple[UUID]]:
        """Select, in one statement, the region-less task instances that the named tasks or slots address."""
        targets = []
        if task_ids:
            targets.append(TaskInstance.task_id.in_(task_ids))
        if slots:
            targets.append(tuple_(TaskInstance.task_id, TaskInstance.region_index).in_(list(slots)))
        return select(TaskInstance.id).where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.region_id == SENTINEL_REGION_ID,
            or_(*targets),
        )

    def get_loop_passes(self, tis: Sequence[TaskCoordinate]) -> list[int | None]:
        """
        Return the iteration of the loop enclosing each of ``tis``, ``None`` outside any loop.

        The region ancestry of every Dag run is loaded once for all of its task instances.
        """
        passes: list[int | None] = [None] * len(tis)
        loop_ids: dict[tuple[str, str, str, UUID | None], str | None] = {}
        looped: dict[int, str] = {}
        regions_by_run: dict[tuple[str, str], set[UUID]] = defaultdict(set)
        for position, ti in enumerate(tis):
            if ti.region_id == SENTINEL_REGION_ID:
                continue
            version = getattr(ti, "dag_version_id", None)
            key = (ti.dag_id, ti.run_id, ti.task_id, version)
            if key not in loop_ids:
                try:
                    loop = enclosing_loop(
                        self.get_task(ti.dag_id, ti.run_id, ti.task_id, dag_version_id=version)
                    )
                except (TaskNotFound, ValueError):
                    loop = None
                loop_ids[key] = None if loop is None else loop.group_id
            if (loop_id := loop_ids[key]) is not None:
                looped[position] = loop_id
                regions_by_run[ti.dag_id, ti.run_id].add(ti.region_id)
        ancestry = {
            run: load_region_ancestry(region_ids, dag_id=run[0], run_id=run[1], session=self.session)
            for run, region_ids in regions_by_run.items()
        }
        for position, loop_id in looped.items():
            ti = tis[position]
            found = loop_position(ancestry[ti.dag_id, ti.run_id], ti.region_id, ti.region_index, loop_id)
            passes[position] = None if found is None else found[1]
        return passes

    def producer_contexts(
        self,
        caller: TaskInstance,
        producer_task_ids: Collection[str] | None = None,
    ) -> dict[str, ProducerContext]:
        task = caller.task or self.get_task(
            caller.dag_id, caller.run_id, caller.task_id, dag_version_id=caller.dag_version_id
        )
        if producer_task_ids is None:
            producer_task_ids = (
                {op.task_id for op in task.iter_mapped_dependencies()} if is_mapped(task) else set()
            )
            group = task.task_group
            while group is not None:
                if isinstance(group, SerializedMappedTaskGroup):
                    producer_task_ids.update(op.task_id for op in group.iter_mapped_dependencies())
                group = group.parent_group
        caller_loop = enclosing_loop(task)
        contexts = {}
        for task_id in producer_task_ids:
            producer = (
                task.dag.get_task(task_id)
                if caller.task is not None and task.dag is not None
                else self.get_task(
                    caller.dag_id, caller.run_id, task_id, dag_version_id=caller.dag_version_id
                )
            )
            loop = enclosing_loop(producer)
            if loop is None:
                continue
            if caller_loop is None or caller_loop.group_id != loop.group_id:
                raise ValueError("A loop producer requires a consumer inside the loop")
            contexts[task_id] = ProducerContext(caller.region_id, caller.region_index, loop.group_id)
        return contexts

    def has_regions(self, dag_id: str, run_id: str | None, task_id: str) -> bool:
        if (known := self._regional_tasks.get((dag_id, run_id, task_id))) is not None:
            return known
        query = select(TaskInstance.id).where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dag_id,
            TaskInstance.task_id == task_id,
            TaskInstance.region_id != SENTINEL_REGION_ID,
        )
        if run_id is not None:
            query = query.where(TaskInstance.run_id == run_id)
        return bool(self.session.scalar(select(query.exists())))

    def resolve_dependency(self, caller: TaskInstance, task_id: str) -> tuple[TaskInstance, ...]:
        try:
            producer = self.get_task(
                caller.dag_id, caller.run_id, task_id, dag_version_id=caller.dag_version_id
            )
        except TaskNotFound:
            return self.resolve(dag_id=caller.dag_id, run_id=caller.run_id, task_id=task_id, caller=caller)
        loop = enclosing_loop(producer)
        caller_task = self.get_task(
            caller.dag_id, caller.run_id, caller.task_id, dag_version_id=caller.dag_version_id
        )
        caller_loop = enclosing_loop(caller_task)
        if (
            loop is not None
            and loop.gate_task_id == task_id
            and (caller_loop is None or caller_loop.group_id != loop.group_id)
        ):
            gates = self.session.scalars(
                select(TaskInstance)
                .join(DynamicRegion, DynamicRegion.id == TaskInstance.region_id)
                .where(
                    TaskInstance.working_set.is_(True),
                    TaskInstance.dag_id == caller.dag_id,
                    TaskInstance.run_id == caller.run_id,
                    TaskInstance.task_id == task_id,
                    DynamicRegion.node_id == loop.group_id,
                )
                .order_by(TaskInstance.region_index.desc())
                .limit(2)
            ).all()
            if len(gates) == 2 and gates[0].region_index == gates[1].region_index:
                raise AmbiguousProducerError(f"Multiple live loop gates at pass {gates[0].region_index}")
            return tuple(gates[:1])
        return self.resolve(dag_id=caller.dag_id, run_id=caller.run_id, task_id=task_id, caller=caller)

    @staticmethod
    def _filter_region_indexes(
        query: Select, region_index: int | None, map_indexes: int | Collection[int] | None
    ) -> Select:
        if region_index is not None:
            query = query.where(TaskInstance.region_index == region_index)
        if isinstance(map_indexes, int):
            query = query.where(TaskInstance.region_index == map_indexes)
        elif map_indexes is not None:
            query = query.where(TaskInstance.region_index.in_(map_indexes))
        return query

    def _filter_legacy_producers(
        self,
        query: Select,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        region_index: int | None,
        map_indexes: int | Collection[int] | None,
    ) -> Select:
        query = query.where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
            TaskInstance.region_id == SENTINEL_REGION_ID,
        )
        return self._filter_region_indexes(query, region_index, map_indexes)

    def _filter_removed_task_producers(
        self,
        query: Select,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        region_id: UUID | None,
        region_index: int | None,
        map_indexes: int | Collection[int] | None,
    ) -> Select:
        """Find live rows of a task whose definition is gone, using only its own expansion regions."""
        query = query.join(DynamicRegion, DynamicRegion.id == TaskInstance.region_id).where(
            TaskInstance.working_set.is_(True),
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
            DynamicRegion.node_id == task_id,
        )
        if region_id is not None:
            query = query.where(TaskInstance.region_id == region_id)
        return self._filter_region_indexes(query, region_index, map_indexes)

    def _is_legacy_lookup(
        self, dag_id: str, run_id: str, task_id: str, region_id: UUID | None, previous_iteration: bool
    ) -> bool:
        return not previous_iteration and (
            region_id == SENTINEL_REGION_ID
            or (region_id is None and not self.has_regions(dag_id, run_id, task_id))
        )

    def _build_producer_request(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        caller: TaskInstance | None,
        region_id: UUID | None,
        region_index: int | None,
        map_indexes: int | Collection[int] | None,
        previous_iteration: bool,
    ) -> _ProducerRequest | None:
        shared_run = caller is not None and (caller.dag_id, caller.run_id) == (dag_id, run_id)
        try:
            task = (
                self.get_task(dag_id, run_id, task_id, dag_version_id=caller.dag_version_id)
                if shared_run and caller is not None and region_id is None
                else self._producer_task(
                    dag_id, run_id, task_id, region_id=region_id, region_index=region_index
                )
            )
        except TaskNotFound:
            return None
        loop = enclosing_loop(task)
        context = None
        if region_id is None and loop is not None:
            if caller is None or (caller.dag_id, caller.run_id) != (dag_id, run_id):
                raise ValueError("A loop producer requires an explicit scope or a consumer inside the loop")
            caller_loop = enclosing_loop(
                self.get_task(dag_id, run_id, caller.task_id, dag_version_id=caller.dag_version_id)
            )
            if caller_loop is None or caller_loop.group_id != loop.group_id:
                raise ValueError("A loop producer requires an explicit scope or a consumer inside the loop")
            context = ProducerContext(
                caller.region_id, caller.region_index, loop.group_id, previous_iteration
            )
        if previous_iteration and context is None:
            raise ValueError("Previous-iteration lookup requires a shared loop scope")
        return _ProducerRequest(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            is_mapped=task.get_needs_expansion(),
            context=context,
            map_indexes=map_indexes,
            region_id=region_id,
            region_index=region_index,
        )

    def resolve(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        caller: TaskInstance | None = None,
        region_id: UUID | None = None,
        region_index: int | None = None,
        map_indexes: int | Collection[int] | None = None,
        previous_iteration: bool = False,
    ) -> tuple[TaskInstance, ...]:
        if region_index is not None and region_id is None:
            raise ValueError("region_index requires an explicit producer region_id")
        if self._is_legacy_lookup(dag_id, run_id, task_id, region_id, previous_iteration):
            query = self._filter_legacy_producers(
                select(TaskInstance),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                region_index=region_index,
                map_indexes=map_indexes,
            )
            return tuple(self.session.scalars(query.order_by(TaskInstance.region_index)))
        request = self._build_producer_request(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            caller=caller,
            region_id=region_id,
            region_index=region_index,
            map_indexes=map_indexes,
            previous_iteration=previous_iteration,
        )
        if request is None:
            return tuple(
                self.session.scalars(
                    self._filter_removed_task_producers(
                        select(TaskInstance),
                        dag_id=dag_id,
                        run_id=run_id,
                        task_id=task_id,
                        region_id=region_id,
                        region_index=region_index,
                        map_indexes=map_indexes,
                    ).order_by(TaskInstance.region_index)
                )
            )
        return resolve_current_producers(
            dag_id=request.dag_id,
            run_id=request.run_id,
            task_id=request.task_id,
            is_mapped=request.is_mapped,
            context=request.context,
            map_indexes=request.map_indexes,
            region_id=request.region_id,
            region_index=request.region_index,
            session=self.session,
        )

    def select_skip_target_ids(
        self, *, caller: TaskInstance, task_id: str, map_indexes: int | None = None
    ) -> Select[tuple[UUID]]:
        """
        Select the task instance ids a skip request from ``caller`` reaches for ``task_id``.

        A caller outside the loop that encloses ``task_id`` reaches every live pass and region of it.
        """
        try:
            task = self.get_task(caller.dag_id, caller.run_id, task_id, dag_version_id=caller.dag_version_id)
        except TaskNotFound:
            task = None
        loop = enclosing_loop(task) if task is not None else None
        if loop is not None:
            caller_loop = enclosing_loop(
                self.get_task(
                    caller.dag_id, caller.run_id, caller.task_id, dag_version_id=caller.dag_version_id
                )
            )
            if caller_loop is None or caller_loop.group_id != loop.group_id:
                query = select(TaskInstance.id).where(
                    TaskInstance.working_set.is_(True),
                    TaskInstance.dag_id == caller.dag_id,
                    TaskInstance.run_id == caller.run_id,
                    TaskInstance.task_id == task_id,
                )
                if map_indexes is not None:
                    query = query.where(public_map_index_expression(TaskInstance) == map_indexes)
                return query
        return self.select_producer_ids(
            dag_id=caller.dag_id,
            run_id=caller.run_id,
            task_id=task_id,
            caller=caller,
            map_indexes=map_indexes,
        )

    def select_producer_ids(
        self,
        *,
        dag_id: str,
        run_id: str,
        task_id: str,
        caller: TaskInstance | None = None,
        region_id: UUID | None = None,
        region_index: int | None = None,
        map_indexes: int | Collection[int] | None = None,
        previous_iteration: bool = False,
    ) -> Select[tuple[UUID]]:
        """Select the task instance ids :meth:`resolve` returns, without loading the producers."""
        if region_index is not None and region_id is None:
            raise ValueError("region_index requires an explicit producer region_id")
        if self._is_legacy_lookup(dag_id, run_id, task_id, region_id, previous_iteration):
            return self._filter_legacy_producers(
                select(TaskInstance.id),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                region_index=region_index,
                map_indexes=map_indexes,
            )
        request = self._build_producer_request(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            caller=caller,
            region_id=region_id,
            region_index=region_index,
            map_indexes=map_indexes,
            previous_iteration=previous_iteration,
        )
        if request is None:
            return self._filter_removed_task_producers(
                select(TaskInstance.id),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                region_id=region_id,
                region_index=region_index,
                map_indexes=map_indexes,
            )
        return select_current_producer_ids(
            dag_id=request.dag_id,
            run_id=request.run_id,
            task_id=request.task_id,
            is_mapped=request.is_mapped,
            context=request.context,
            map_indexes=request.map_indexes,
            region_id=request.region_id,
            region_index=request.region_index,
            session=self.session,
        )
