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

import hashlib
import struct
from collections.abc import Collection, Iterable, Iterator
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any
from uuid import UUID

import attrs
import uuid6
from sqlalchemy import (
    BINARY,
    CheckConstraint,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    UniqueConstraint,
    and_,
    event,
    false,
    func,
    literal,
    or_,
    select,
    union_all,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, aliased, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

SENTINEL_REGION_ID = UUID(int=0)
LOOP_XCOM_PREFIX = "_airflow_loop_"
LOOP_DECISION_KEY = f"{LOOP_XCOM_PREFIX}decision"

if TYPE_CHECKING:
    from sqlalchemy import Select
    from sqlalchemy.engine import Connection
    from sqlalchemy.orm import Mapper, Session
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.models.taskinstance import TaskInstance


def _build_slot_key(
    dag_id: str, run_id: str, node_id: str, parent_region_id: UUID | None, parent_region_index: int | None
) -> bytes:
    """
    Hash the columns that identify a slot into the value of ``DynamicRegion.slot_key``.

    The strings are length-prefixed so that no two different slots encode to the same bytes, and a missing
    parent encodes as the sentinel id and index ``-1``.
    """
    digest = hashlib.sha256()
    for part in (dag_id, run_id, node_id):
        encoded = part.encode()
        digest.update(struct.pack(">I", len(encoded)))
        digest.update(encoded)
    digest.update((parent_region_id or SENTINEL_REGION_ID).bytes)
    digest.update(struct.pack(">i", -1 if parent_region_index is None else parent_region_index))
    return digest.digest()


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

    A *slot* is the place one region fills: ``(dag_id, run_id, node_id, parent_region_id,
    parent_region_index)``, for example the expansion of mapped task ``t`` in a run, or of ``t`` inside
    iteration 3 of a loop. Each slot holds one original region, and forks of it.

    ``slot_key`` guarantees one original region per slot. Without it, two creators of the same slot at
    once (two schedulers expanding the same placeholder, or a clear of only the new tasks racing a
    scheduler) would each insert a region with a fresh id and both commits would succeed, leaving the
    task with two live expansions that later lookups reject as ambiguous. Before regions existed the
    second writer collided on ``task_instance_current_key`` and rolled back; a fresh region id removed
    that collision. The key is a SHA-256 of the slot columns, set when an original region is inserted
    and left NULL for a fork, and it is unique, so the loser's insert fails and
    :meth:`DynamicRegion.get_or_create` reuses the winner's region.

    The key is a hash because a unique key over the slot columns cannot work: the parent columns of a
    top-level region are NULL, and NULLs never collide in a unique key; ``dag_id``, ``run_id`` and
    ``node_id`` alone take up to about 3000 bytes with utf8mb4 ids, close to MySQL's 3072-byte key
    limit; and a partial index is not portable to MySQL. A fork repeats the slot coordinates of the
    region it forks, so it takes no key; a chain of forks stays linear because
    ``forked_from_region_id`` is unique, which gives a region at most one successor.

    It does not check that a fork's coordinates match its source's. Regions inserted without it, such as
    rows from before the column existed or a raw insert that bypasses the ORM, have a NULL key and are
    not guarded: :meth:`DynamicRegion.get_or_create` finds those by their slot columns, and a raw insert
    must supply the key itself.
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
    # Hash of the slot, set on original regions and NULL on forks; see the class docstring.
    slot_key: Mapped[bytes | None] = mapped_column(
        LargeBinary(32).with_variant(BINARY(32), "mysql", "mariadb"), nullable=True
    )
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
        UniqueConstraint("slot_key", name="dynamic_region_slot_key_uq"),
        CheckConstraint(
            "(parent_region_id IS NULL AND parent_region_index IS NULL) OR "
            "(parent_region_id IS NOT NULL AND parent_region_index IS NOT NULL)",
            name="parent_coordinates_paired",
        ),
        CheckConstraint("resumes_from_index >= 0", name="resumes_from_index_nonnegative"),
        Index("idx_dynamic_region_slot", dag_id, run_id, node_id, parent_region_id, parent_region_index),
        Index("idx_dynamic_region_parent_region_id", parent_region_id),
    )

    @classmethod
    def get_or_create(
        cls,
        *,
        dag_id: str,
        run_id: str,
        node_id: str,
        parent_region_id: UUID | None = None,
        parent_region_index: int | None = None,
        session: Session,
    ) -> DynamicRegion:
        """
        Return the region that first filled the slot, creating it when no other writer has.

        Two transactions racing to create the same slot collide on ``slot_key``; the loser reuses the
        winner's region. Regions created before slot keys existed have none and are found by their columns.
        """
        existing = cls._find_original(
            dag_id=dag_id,
            run_id=run_id,
            node_id=node_id,
            parent_region_id=parent_region_id,
            parent_region_index=parent_region_index,
            session=session,
        )
        if existing is not None:
            return existing
        region = cls(
            dag_id=dag_id,
            run_id=run_id,
            node_id=node_id,
            parent_region_id=parent_region_id,
            parent_region_index=parent_region_index,
        )
        try:
            with session.begin_nested():
                session.add(region)
        except IntegrityError:
            existing = cls._find_original(
                dag_id=dag_id,
                run_id=run_id,
                node_id=node_id,
                parent_region_id=parent_region_id,
                parent_region_index=parent_region_index,
                session=session,
            )
            if existing is None:
                raise
            return existing
        return region

    @classmethod
    def _find_original(
        cls,
        *,
        dag_id: str,
        run_id: str,
        node_id: str,
        parent_region_id: UUID | None,
        parent_region_index: int | None,
        session: Session,
    ) -> DynamicRegion | None:
        """Find the oldest non-fork region of a slot by its columns, which also covers rows without a slot key."""
        return session.scalars(
            select(cls)
            .where(
                cls.dag_id == dag_id,
                cls.run_id == run_id,
                cls.node_id == node_id,
                cls.parent_region_id.is_(None)
                if parent_region_id is None
                else cls.parent_region_id == parent_region_id,
                cls.parent_region_index.is_(None)
                if parent_region_index is None
                else cls.parent_region_index == parent_region_index,
                cls.forked_from_region_id.is_(None),
            )
            .order_by(cls.id)
            .limit(1)
        ).first()

    @classmethod
    def get_or_create_many(
        cls,
        *,
        dag_id: str,
        run_id: str,
        node_ids: Iterable[str],
        parent_region_id: UUID | None = None,
        parent_region_index: int | None = None,
        session: Session,
    ) -> dict[str, DynamicRegion]:
        """
        Return the region that first filled each slot, creating the missing ones in one statement.

        The batch is inserted in a savepoint, which keeps the common case to a single INSERT. When any slot
        is already taken the batch is rolled back and each slot is resolved on its own.
        """
        slots = {
            node_id: cls(
                dag_id=dag_id,
                run_id=run_id,
                node_id=node_id,
                parent_region_id=parent_region_id,
                parent_region_index=parent_region_index,
            )
            for node_id in node_ids
        }
        if not slots:
            return {}
        try:
            with session.begin_nested():
                session.add_all(slots.values())
        except IntegrityError:
            return {
                node_id: cls.get_or_create(
                    dag_id=dag_id,
                    run_id=run_id,
                    node_id=node_id,
                    parent_region_id=parent_region_id,
                    parent_region_index=parent_region_index,
                    session=session,
                )
                for node_id in slots
            }
        return slots

    @classmethod
    def load_for_run(cls, dag_id: str, run_id: str, *, session: Session) -> dict[UUID, DynamicRegion]:
        """Load every region of a Dag run, keyed by id."""
        return {
            region.id: region
            for region in session.scalars(select(cls).where(cls.dag_id == dag_id, cls.run_id == run_id))
        }

    @classmethod
    def get_forks_after(cls, region_id: UUID, regions: dict[UUID, DynamicRegion]) -> Iterator[DynamicRegion]:
        """Yield the successive forks of a region, oldest first."""
        successors = {region.forked_from_region_id: region for region in regions.values()}
        while region_id in successors:
            successor = successors[region_id]
            yield successor
            region_id = successor.id

    @classmethod
    def find_physical_coordinate(
        cls, region_id: UUID, index: int, node_id: str, regions: dict[UUID, DynamicRegion]
    ) -> tuple[UUID, int] | None:
        """Walk up the parents of a coordinate to the region executing ``node_id``, without following forks."""
        while region_id in regions:
            region = regions[region_id]
            if region.node_id == node_id:
                return region_id, index
            if region.parent_region_id is None or region.parent_region_index is None:
                return None
            region_id, index = region.parent_region_id, region.parent_region_index
        return None

    @classmethod
    def is_coordinate_superseded(
        cls, region_id: UUID, index: int, regions: dict[UUID, DynamicRegion]
    ) -> bool:
        """Test whether a coordinate or its parent lies beyond a durable fork cut."""
        while region_id in regions:
            if any(region.resumes_from_index <= index for region in cls.get_forks_after(region_id, regions)):
                return True
            region = regions[region_id]
            if region.parent_region_id is None or region.parent_region_index is None:
                break
            region_id, index = region.parent_region_id, region.parent_region_index
        return False

    @classmethod
    def get_successor_region_id(
        cls, region_id: UUID, next_index: int, regions: dict[UUID, DynamicRegion]
    ) -> UUID:
        """Choose the region that holds ``next_index`` of the pass family ``region_id`` belongs to."""
        successor_id = region_id
        for fork in cls.get_forks_after(region_id, regions):
            if fork.resumes_from_index <= next_index:
                successor_id = fork.id
        return successor_id

    @classmethod
    def fork_family(
        cls, family_id: UUID, resumes_from_index: int, regions: dict[UUID, DynamicRegion], *, session: Session
    ) -> DynamicRegion:
        """Fork the newest region of a family so that it resumes from ``resumes_from_index``."""
        leaf = regions[family_id]
        for successor in cls.get_forks_after(family_id, regions):
            leaf = successor
        fork = cls(
            dag_id=leaf.dag_id,
            run_id=leaf.run_id,
            node_id=leaf.node_id,
            parent_region_id=leaf.parent_region_id,
            parent_region_index=leaf.parent_region_index,
            forked_from_region_id=leaf.id,
            resumes_from_index=resumes_from_index,
        )
        session.add(fork)
        return fork


@event.listens_for(DynamicRegion, "before_insert")
def _set_slot_key(mapper: Mapper, connection: Connection, region: DynamicRegion) -> None:
    if region.slot_key is None and region.forked_from_region_id is None:
        region.slot_key = _build_slot_key(
            region.dag_id,
            region.run_id,
            region.node_id,
            region.parent_region_id,
            region.parent_region_index,
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
    loop_pass: _LoopPassRegions | None = None,
) -> Select:
    """Push every coordinate predicate into SQL."""
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
    if loop_pass is not None:
        query = query.where(_build_loop_pass_filter(loop_pass))
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


def _build_candidate_query(
    query: Select[Any],
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    is_mapped: bool,
    context: ProducerContext | None,
    map_indexes: int | Collection[int] | None,
    region_id: UUID | None,
    region_index: int | None,
    session: Session,
) -> Select[Any] | None:
    """Narrow ``query`` to the candidate producers of the caller's scope, or ``None`` if there are none."""
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
                    return None

    return _filter_producers(
        query,
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        is_mapped=is_mapped,
        map_indexes=map_indexes,
        region_id=region_id,
        region_index=region_index,
        top_level_only=region_id is None and position is None,
        loop_pass=_load_loop_pass_regions(position, session=session) if position is not None else None,
    )


def _check_producers_unambiguous(
    region_indexes: Iterable[int], *, dag_id: str, run_id: str, task_id: str, is_mapped: bool
) -> None:
    seen: set[int] = set()
    for region_index in region_indexes:
        public_index = region_index if is_mapped else -1
        if public_index in seen:
            raise AmbiguousProducerError(
                f"Multiple live producers for {dag_id}/{run_id}/{task_id} index {public_index}"
            )
        seen.add(public_index)


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
    query = _build_candidate_query(
        select(TaskInstance),
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        is_mapped=is_mapped,
        context=context,
        map_indexes=map_indexes,
        region_id=region_id,
        region_index=region_index,
        session=session,
    )
    if query is None:
        return ()
    candidates = session.scalars(query).all()
    _check_producers_unambiguous(
        (ti.region_index for ti in candidates),
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        is_mapped=is_mapped,
    )
    return tuple(sorted(candidates, key=lambda ti: ti.region_index if is_mapped else -1))


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
            candidate_query = _build_candidate_query(
                select(TaskInstance.id, TaskInstance.region_index),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                is_mapped=is_mapped,
                context=context,
                map_indexes=map_indexes,
                region_id=None,
                region_index=None,
                session=session,
            )
            candidates = session.execute(candidate_query).all() if candidate_query is not None else []
            _check_producers_unambiguous(
                (candidate.region_index for candidate in candidates),
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                is_mapped=is_mapped,
            )
            return select(TaskInstance.id).where(
                TaskInstance.id.in_([candidate.id for candidate in candidates])
            )
    return query


def _load_loop_pass_owners(
    *, dag_id: str, run_id: str, loop_node_id: str, session: Session
) -> list[tuple[UUID, int | None]]:
    """
    Return each region that holds passes of a loop with the first pass it no longer owns.

    A fork takes over every pass from its ``resumes_from_index``, so a region owns only the passes
    before the earliest fork that follows it. The newest region owns every pass from its own start.
    """
    regions = session.scalars(
        select(DynamicRegion).where(
            DynamicRegion.dag_id == dag_id,
            DynamicRegion.run_id == run_id,
            DynamicRegion.node_id == loop_node_id,
        )
    ).all()
    originals = [region for region in regions if region.forked_from_region_id is None]
    if len(originals) > 1:
        raise ValueError(f"Loop {loop_node_id!r} has several executions in Dag run {run_id!r}")
    successors = {region.forked_from_region_id: region for region in regions}
    chain = originals[:]
    while chain and chain[-1].id in successors:
        chain.append(successors[chain[-1].id])
    owners: list[tuple[UUID, int | None]] = []
    cut: int | None = None
    for region in reversed(chain):
        owners.append((region.id, cut))
        cut = region.resumes_from_index if cut is None else min(cut, region.resumes_from_index)
    return owners


def _build_pass_ownership_filter(
    region_column, index_column, owners: Iterable[tuple[UUID, int | None]]
) -> ColumnElement[bool]:
    return or_(
        *(
            region_column == region_id
            if cut is None
            else and_(region_column == region_id, index_column < cut)
            for region_id, cut in owners
        )
    )


def select_loop_producer_ids(
    *, dag_id: str, run_id: str, loop_node_id: str, task_id: str, is_mapped: bool, session: Session
) -> Select[tuple[UUID]]:
    """
    Select every live execution of ``task_id`` across all passes of the loop ``loop_node_id``.

    That is what a task outside the loop sees of a loop task. A pass that a fork replaced does not count
    even while its task instances are still live, because the fork's task instances stand in for it.
    Order the executions with :func:`build_loop_sequence_order`.
    """
    from airflow.models.taskinstance import TaskInstance

    query = select(TaskInstance.id).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == run_id,
        TaskInstance.task_id == task_id,
        TaskInstance.working_set.is_(True),
    )
    owners = _load_loop_pass_owners(dag_id=dag_id, run_id=run_id, loop_node_id=loop_node_id, session=session)
    if not owners:
        return query.where(false())
    if not is_mapped:
        return query.where(
            _build_pass_ownership_filter(TaskInstance.region_id, TaskInstance.region_index, owners)
        )
    pass_region = (
        select(DynamicRegion.id)
        .where(
            DynamicRegion.id == TaskInstance.region_id,
            _build_pass_ownership_filter(
                DynamicRegion.parent_region_id, DynamicRegion.parent_region_index, owners
            ),
        )
        .correlate(TaskInstance)
        .exists()
    )
    return query.where(pass_region)


def build_loop_sequence_order(region_id_column, region_index_column) -> tuple[ColumnElement[int], ...]:
    """
    Order the rows :func:`select_loop_producer_ids` selects depth-first: by pass, then by map index.

    An unmapped task sits in the loop's own region, whose ``region_index`` is the pass. A mapped task sits
    in a region nested under the pass, whose parent index is the pass and whose rows carry the map index.
    """
    nested_pass = (
        select(DynamicRegion.parent_region_index)
        .where(DynamicRegion.id == region_id_column)
        .scalar_subquery()
    )
    return func.coalesce(nested_pass, region_index_column), region_index_column
