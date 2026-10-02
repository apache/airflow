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

from collections.abc import Collection
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING
from uuid import UUID

import attrs
import uuid6
from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    UniqueConstraint,
    and_,
    false,
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

SENTINEL_REGION_ID = UUID(int=0)
LOOP_XCOM_PREFIX = "_airflow_loop_"
LOOP_DECISION_KEY = f"{LOOP_XCOM_PREFIX}decision"

if TYPE_CHECKING:
    from sqlalchemy import Select
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.models.taskinstance import TaskInstance


@attrs.define(frozen=True)
class ProducerContext:
    """Caller coordinates and the producer's shared loop context from the pinned graph."""

    region_id: UUID
    region_index: int
    loop_node_id: str | None = None
    previous_iteration: bool = False


class AmbiguousProducerError(ValueError):
    """Multiple live executions occupy the requested producer slot."""


class DynamicRegion(Base):
    """
    One execution of a construct that creates task instances at run time, within a Dag run.

    That is a loop, or the expansion of a mapped task or task group. Task instances created by the
    execution carry its ``id`` as ``region_id``, so rows that share dag, task, run and index but come
    from different executions can coexist. A region can sit inside another one (``parent_region_*``).
    When a clear replaces an execution, the successor records the region it was forked from and where
    it resumes. Rows never change after they are created; everything that does change lives on the
    task instances.
    """

    __tablename__ = "dynamic_region"

    id: Mapped[UUID] = mapped_column(CompactUUID(), primary_key=True, default=uuid6.uuid7)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    # The id of the task group (a loop) or mapped task that this region executes.
    node_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    parent_region_id: Mapped[UUID | None] = mapped_column(CompactUUID(), nullable=True)
    parent_region_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    forked_from_region_id: Mapped[UUID | None] = mapped_column(CompactUUID(), nullable=True)
    resumes_from_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False, default=timezone.utcnow)

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, run_id],
            ["dag_run.dag_id", "dag_run.run_id"],
            name="dynamic_region_dag_run_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            [parent_region_id],
            ["dynamic_region.id"],
            name="dynamic_region_parent_region_id_fkey",
            ondelete="CASCADE",
        ),
        UniqueConstraint("forked_from_region_id", name="dynamic_region_forked_from_region_id_uq"),
        CheckConstraint(
            "(parent_region_id IS NULL AND parent_region_index IS NULL) OR "
            "(parent_region_id IS NOT NULL AND parent_region_index IS NOT NULL)",
            name="parent_coordinates_paired",
        ),
        CheckConstraint("resumes_from_index >= 0", name="resumes_from_index_nonnegative"),
        Index("idx_dynamic_region_slot", dag_id, run_id, node_id, parent_region_id, parent_region_index),
        Index("idx_dynamic_region_parent_region_id", parent_region_id),
    )


def public_region_filter(
    model,
    *,
    dag_ids: str | Collection[str] | None = None,
    run_ids: str | Collection[str] | None = None,
    task_ids: str | Collection[str] | None = None,
) -> ColumnElement[bool]:
    """
    Match rows addressed by public coordinates: legacy rows and a task's own top-level region.

    Pass the dag, run and task the caller is looking up so the regions that can match are
    resolved once from ``dynamic_region`` instead of being checked per row. The row filter is then an
    equality-or-IN on ``region_id`` that the unique key serves, where a per-row check has to read
    every row of the task.
    """
    own_region = (
        select(DynamicRegion.id)
        .where(
            DynamicRegion.id == model.region_id,
            DynamicRegion.node_id == model.task_id,
            DynamicRegion.parent_region_id.is_(None),
        )
        .correlate(model)
        .exists()
    )
    exact = or_(model.region_id == SENTINEL_REGION_ID, own_region)
    if dag_ids is None or run_ids is None or task_ids is None:
        return exact
    candidates = union_all(
        select(literal(SENTINEL_REGION_ID, CompactUUID()).label("id")),
        select(DynamicRegion.id.label("id")).where(
            _match_any(DynamicRegion.dag_id, dag_ids),
            _match_any(DynamicRegion.run_id, run_ids),
            _match_any(DynamicRegion.node_id, task_ids),
            DynamicRegion.parent_region_id.is_(None),
        ),
    ).subquery()
    narrowed = model.region_id.in_(select(candidates.c.id))
    if isinstance(dag_ids, str) and isinstance(run_ids, str) and isinstance(task_ids, str):
        return narrowed
    return and_(narrowed, exact)


def _match_any(column, value: str | Collection[str]) -> ColumnElement[bool]:
    return column == value if isinstance(value, str) else column.in_(value)


def load_region_ancestry(
    region_ids: Collection[UUID], *, dag_id: str, run_id: str, session: Session
) -> dict[UUID, DynamicRegion]:
    """Load parent and fork ancestry in batches within one DagRun."""
    regions: dict[UUID, DynamicRegion] = {}
    pending = set(region_ids) - {SENTINEL_REGION_ID}
    while pending:
        rows = session.scalars(
            select(DynamicRegion).where(
                DynamicRegion.dag_id == dag_id,
                DynamicRegion.run_id == run_id,
                DynamicRegion.id.in_(pending),
            )
        ).all()
        if {row.id for row in rows} != pending:
            raise ValueError("Region context does not belong to the requested DagRun")
        regions.update((row.id, row) for row in rows)
        pending = {
            ref
            for row in rows
            for ref in (row.parent_region_id, row.forked_from_region_id)
            if ref is not None and ref not in regions
        }
    return regions


def loop_position(
    regions: dict[UUID, DynamicRegion], coordinate_id: UUID, index: int, loop_node_id: str
) -> tuple[UUID, int] | None:
    """Return the enclosing loop's fork family and iteration."""
    seen: set[UUID] = set()
    while coordinate_id != SENTINEL_REGION_ID:
        if coordinate_id in seen:
            raise ValueError("Cyclic region ancestry")
        seen.add(coordinate_id)
        region = regions[coordinate_id]
        if region.node_id == loop_node_id:
            family = region
            lineage: set[UUID] = set()
            while family.forked_from_region_id is not None:
                if family.id in lineage:
                    raise ValueError("Cyclic region lineage")
                lineage.add(family.id)
                family = regions[family.forked_from_region_id]
            return family.id, index
        if region.parent_region_id is None:
            break
        if TYPE_CHECKING:
            assert region.parent_region_index is not None
        coordinate_id, index = region.parent_region_id, region.parent_region_index
    return None


def _filter_producers(
    query: Select,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    is_mapped: bool,
    map_indexes: int | Collection[int] | None,
    region_id: UUID | None,
    region_index: int | None,
    top_level_only: bool,
) -> Select:
    """Push every coordinate predicate that does not need loop ancestry into SQL."""
    from airflow.models.taskinstance import TaskInstance

    query = query.where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.working_set.is_(True),
    )
    if region_id is not None:
        query = query.where(TaskInstance.region_id == region_id)
    if region_index is not None:
        query = query.where(TaskInstance.region_index == region_index)
    if top_level_only:
        if is_mapped:
            top_level_region = (
                select(DynamicRegion.id)
                .where(DynamicRegion.id == TaskInstance.region_id, DynamicRegion.parent_region_id.is_(None))
                .correlate(TaskInstance)
                .exists()
            )
            query = query.where(or_(TaskInstance.region_id == SENTINEL_REGION_ID, top_level_region))
        else:
            query = query.where(TaskInstance.region_id == SENTINEL_REGION_ID, TaskInstance.region_index == -1)
    if map_indexes is None:
        return query
    if not is_mapped:
        wanted = map_indexes == -1 if isinstance(map_indexes, int) else -1 in map_indexes
        return query if wanted else query.where(false())
    if isinstance(map_indexes, int):
        return query.where(TaskInstance.region_index == map_indexes)
    if isinstance(map_indexes, range) and map_indexes.step == 1:
        return query.where(
            TaskInstance.region_index >= map_indexes.start, TaskInstance.region_index < map_indexes.stop
        )
    return query.where(TaskInstance.region_index.in_(list(map_indexes)))


def _validate_producer_request(
    *, context: ProducerContext | None, region_id: UUID | None, region_index: int | None
) -> None:
    if region_index is not None and region_id is None:
        raise ValueError("region_index requires an explicit producer region_id")
    if context and context.previous_iteration and context.loop_node_id is None:
        raise ValueError("Previous-iteration lookup requires a loop context")


def resolve_current_producers(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    is_mapped: bool,
    context: ProducerContext | None = None,
    map_indexes: int | Collection[int] | None = None,
    region_id: UUID | None = None,
    region_index: int | None = None,
    session: Session,
) -> tuple[TaskInstance, ...]:
    """Resolve the live producer task instances whose data the caller reads by task instance UUID."""
    from airflow.models.taskinstance import TaskInstance

    _validate_producer_request(context=context, region_id=region_id, region_index=region_index)
    regions: dict[UUID, DynamicRegion] = {}
    position = None
    if context and region_id is None:
        regions = load_region_ancestry(
            {context.region_id} - {SENTINEL_REGION_ID}, dag_id=dag_id, run_id=run_id, session=session
        )
        if context.loop_node_id is not None:
            position = loop_position(regions, context.region_id, context.region_index, context.loop_node_id)
            if position is None:
                raise ValueError("Caller is not inside the requested loop")
            if context.previous_iteration:
                position = position[0], position[1] - 1
                if position[1] < 0:
                    return ()

    candidates = session.scalars(
        _filter_producers(
            select(TaskInstance),
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            is_mapped=is_mapped,
            map_indexes=map_indexes,
            region_id=region_id,
            region_index=region_index,
            top_level_only=region_id is None and position is None,
        )
    ).all()
    if position is not None:
        if TYPE_CHECKING:
            assert context is not None
            assert context.loop_node_id is not None
        regions.update(
            load_region_ancestry(
                {ti.region_id for ti in candidates} - set(regions) - {SENTINEL_REGION_ID},
                dag_id=dag_id,
                run_id=run_id,
                session=session,
            )
        )
        candidates = [
            ti
            for ti in candidates
            if loop_position(regions, ti.region_id, ti.region_index, context.loop_node_id) == position
        ]

    selected: dict[int, TaskInstance] = {}
    for ti in candidates:
        public_index = ti.region_index if is_mapped else -1
        if public_index in selected:
            raise AmbiguousProducerError(
                f"Multiple live producers for {dag_id}/{run_id}/{task_id} index {public_index}"
            )
        selected[public_index] = ti
    return tuple(selected[index] for index in sorted(selected))


def select_current_producer_ids(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    is_mapped: bool,
    context: ProducerContext | None = None,
    map_indexes: int | Collection[int] | None = None,
    region_id: UUID | None = None,
    region_index: int | None = None,
    session: Session,
) -> Select[tuple[UUID]]:
    """
    Select the ids of the live producers :func:`resolve_current_producers` would return.

    A task whose live executions all share one region cannot have two producers at one index, so
    that common case stays a pure SQL selection whose cost does not depend on the number of
    mapped instances. Everything else resolves the rows and pins their ids.
    """
    from airflow.models.taskinstance import TaskInstance

    _validate_producer_request(context=context, region_id=region_id, region_index=region_index)
    filter_producers = partial(
        _filter_producers,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        is_mapped=is_mapped,
        map_indexes=map_indexes,
        region_id=region_id,
        region_index=region_index,
        top_level_only=region_id is None,
    )
    query = filter_producers(select(TaskInstance.id))
    if region_id is None:
        if context is not None:
            single_region = False
        else:
            first_region = filter_producers(select(TaskInstance.region_id)).limit(1)
            other_region = filter_producers(select(TaskInstance.id)).where(
                TaskInstance.region_id != first_region.scalar_subquery()
            )
            single_region = not session.scalar(select(other_region.exists()))
        if not single_region:
            producers = resolve_current_producers(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                is_mapped=is_mapped,
                context=context,
                map_indexes=map_indexes,
                session=session,
            )
            return select(TaskInstance.id).where(TaskInstance.id.in_([ti.id for ti in producers]))
    return query
