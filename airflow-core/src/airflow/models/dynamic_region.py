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
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.orm import Mapped, aliased, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

SENTINEL_REGION_ID = UUID(int=0)

if TYPE_CHECKING:
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


@attrs.define(frozen=True)
class _LoopPassRegions:
    """The regions that hold one iteration of a loop: its fork family and everything nested under it."""

    family_ids: tuple[UUID, ...]
    nested_ids: tuple[UUID, ...]
    iteration: int


def _load_loop_pass_regions(position: tuple[UUID, int], *, session: Session) -> _LoopPassRegions:
    """
    Resolve, in one statement, the regions that hold the loop iteration ``position`` names.

    Both recursive CTEs run against ``dynamic_region`` only. The ``task_instance`` query then takes the
    ids as literals, which every backend turns into an index lookup on ``task_instance_current_key``;
    a subquery in that position leaves MySQL and Postgres scanning every row of the task.
    """
    family_root, iteration = position
    family = (
        select(DynamicRegion.id).where(DynamicRegion.id == family_root).cte("loop_family", recursive=True)
    )
    forked = aliased(DynamicRegion)
    family = family.union(select(forked.id).where(forked.forked_from_region_id == family.c.id))
    nested = (
        select(DynamicRegion.id)
        .where(
            DynamicRegion.parent_region_id.in_(select(family.c.id)),
            DynamicRegion.parent_region_index == iteration,
        )
        .cte("loop_iteration_regions", recursive=True)
    )
    child = aliased(DynamicRegion)
    nested = nested.union(select(child.id).where(child.parent_region_id == nested.c.id))
    rows = session.execute(
        union_all(
            select(family.c.id, literal(0).label("is_nested")),
            select(nested.c.id, literal(1).label("is_nested")),
        )
    ).all()
    return _LoopPassRegions(
        family_ids=tuple(region_id for region_id, is_nested in rows if not is_nested),
        nested_ids=tuple(region_id for region_id, is_nested in rows if is_nested),
        iteration=iteration,
    )


def _build_loop_pass_filter(regions: _LoopPassRegions) -> ColumnElement[bool]:
    from airflow.models.taskinstance import TaskInstance

    return or_(
        and_(TaskInstance.region_id.in_(regions.family_ids), TaskInstance.region_index == regions.iteration),
        TaskInstance.region_id.in_(regions.nested_ids),
    )


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

    if region_index is not None and region_id is None:
        raise ValueError("region_index requires an explicit producer region_id")
    if context and context.previous_iteration and context.loop_node_id is None:
        raise ValueError("Previous-iteration lookup requires a loop context")
    regions: dict[UUID, DynamicRegion] = {}
    position = None
    if context and region_id is None:
        regions = load_region_ancestry({context.region_id}, dag_id=dag_id, run_id=run_id, session=session)
        if context.loop_node_id is not None:
            position = loop_position(regions, context.region_id, context.region_index, context.loop_node_id)
            if position is None:
                raise ValueError("Caller is not inside the requested loop")
            if context.previous_iteration:
                position = position[0], position[1] - 1
                if position[1] < 0:
                    return ()
    query = select(TaskInstance).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.working_set.is_(True),
    )
    if region_id is not None:
        query = query.where(TaskInstance.region_id == region_id)
    if region_index is not None:
        query = query.where(TaskInstance.region_index == region_index)
    if position is not None:
        query = query.where(_build_loop_pass_filter(_load_loop_pass_regions(position, session=session)))
    candidates = session.scalars(query).all()
    if region_id is None and position is None:
        regions.update(
            load_region_ancestry(
                {ti.region_id for ti in candidates} - set(regions),
                dag_id=dag_id,
                run_id=run_id,
                session=session,
            )
        )

    selected: dict[int, TaskInstance] = {}
    for ti in candidates:
        if region_id is None and position is None:
            if ti.region_id != SENTINEL_REGION_ID:
                if not is_mapped or regions[ti.region_id].parent_region_id is not None:
                    continue
            elif not is_mapped and ti.region_index != -1:
                continue
        public_index = ti.region_index if is_mapped else -1
        if isinstance(map_indexes, int):
            if public_index != map_indexes:
                continue
        elif map_indexes is not None and public_index not in map_indexes:
            continue
        if public_index in selected:
            raise AmbiguousProducerError(
                f"Multiple live producers for {dag_id}/{run_id}/{task_id} index {public_index}"
            )
        selected[public_index] = ti
    return tuple(selected[index] for index in sorted(selected))
