#
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
"""
Internal classes for management of dag backfills.

:meta private:
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, NamedTuple

import sqlalchemy as sa
import structlog
from sqlalchemy import (
    Boolean,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    and_,
    case,
    delete,
    func,
    select,
    text,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates

from airflow._shared.timezones import timezone
from airflow.exceptions import AirflowException, DagNotFound, DagRunTypeNotAllowed
from airflow.models.base import Base, StringID
from airflow.utils.db import get_dialect_name
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import (
    UtcDateTime,
    is_lock_not_available_error,
    with_row_locks,
)
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.models.dagrun import DagRun
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.timetables.base import DagRunInfo

log = structlog.get_logger(__name__)


class AlreadyRunningBackfill(AirflowException):
    """
    Raised when attempting to create backfill and one already active.

    :meta private:
    """


class DagNonPeriodicScheduleException(AirflowException):
    """
    Raised when attempting to backfill a Dag whose schedule is fundamentally incompatible with backfills.

    This covers the following timetable types:
    - NullTimetable
    - OnceTimetable
    - ContinuousTimetable
    - AssetTriggeredTimetable
    - PartitionedAssetTimetable

    :meta private:
    """


class ConsumerBackfillNotSupported(DagNonPeriodicScheduleException):
    """
    Raised when a Dag scheduled on partitioned assets cannot be backfilled.

    :meta private:
    """


class InvalidBackfillDirection(AirflowException):
    """
    Raised when backfill is attempted in reverse order with tasks that depend on past runs.

    :meta private:
    """


class InvalidReprocessBehavior(AirflowException):
    """
    Raised when a backfill cannot be completed because the reprocess behavior is not valid.

    :meta private:
    """


class InvalidBackfillDate(AirflowException):
    """
    Raised when a backfill is requested for future date.

    :meta private:
    """


class InvalidBackfillDateRange(AirflowException):
    """
    Raised when from_date is after to_date in a backfill request.

    :meta private:
    """


class TooManyBackfillPartitions(InvalidBackfillDateRange):
    """
    Raised when a backfill of a Dag scheduled on partitioned assets selects too many partitions.

    :meta private:
    """


class InvalidBackfillPartitionKeys(InvalidBackfillDateRange):
    """
    Raised when the partition keys of a backfill request are not valid.

    :meta private:
    """


class InvalidBackfillConf(AirflowException):
    """
    Raised when the provided ``dag_run_conf`` fails validation against the DAG's params.

    :meta private:
    """


class NoBackfillRunsToCreate(ValueError):
    """
    Raised when a backfill request yields no Dag runs for the given date range.

    This happens when the from/to date range falls entirely outside the Dag's
    scheduled intervals (e.g. the range predates the first partition boundary).

    :meta private:
    """


class UnknownActiveBackfills(AirflowException):
    """
    Raised when the quantity of active backfills cannot be determined.

    :meta private:
    """

    def __init__(self, dag_id: str):
        super().__init__(f"Unable to determine the number of active backfills for DAG {dag_id}")


class ReprocessBehavior(str, Enum):
    """
    Internal enum for setting reprocess behavior in a backfill.

    :meta private:
    """

    FAILED = "failed"
    COMPLETED = "completed"
    NONE = "none"

    def __str__(self) -> str:
        return self.value


class Backfill(Base):
    """Model representing a backfill job."""

    __tablename__ = "backfill"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    from_date: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    to_date: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    dag_run_conf: Mapped[dict] = mapped_column(sa.JSON(), nullable=False, default={})
    is_paused: Mapped[bool | None] = mapped_column(Boolean, default=False, nullable=True)
    """
    Controls whether new dag runs will be created for this backfill.

    Does not pause existing dag runs.
    """
    reprocess_behavior: Mapped[str] = mapped_column(
        StringID(), nullable=False, default=ReprocessBehavior.NONE
    )
    max_active_runs: Mapped[int] = mapped_column(Integer, default=10, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )
    triggering_user_name: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )  # The user that triggered the Backfill, if applicable

    __table_args__ = (Index("idx_backfill_dag_id_completed_at", dag_id, completed_at),)

    backfill_dag_run_associations = relationship("BackfillDagRun", back_populates="backfill")

    dag_model = relationship(
        "DagModel",
        primaryjoin="DagModel.dag_id == Backfill.dag_id",
        viewonly=True,
        foreign_keys=[dag_id],
    )

    def __repr__(self):
        return f"Backfill({self.dag_id=}, {self.from_date=}, {self.to_date=})"

    @hybrid_property
    def duration(self) -> float | None:
        if self.completed_at and self.created_at:
            return (self.completed_at - self.created_at).total_seconds()
        return None

    @duration.expression  # type: ignore[no-redef]
    def duration(cls: type[Backfill]) -> ColumnElement[Any]:
        @provide_session
        def _get_dialect(*, session: Session = NEW_SESSION) -> str | None:
            return get_dialect_name(session=session)

        dialect_name = _get_dialect()

        duration_expr: Any
        if dialect_name == "mysql":
            duration_expr = func.timestampdiff(text("SECOND"), cls.created_at, cls.completed_at)
        elif dialect_name == "sqlite":
            duration_expr = (func.julianday(cls.completed_at) - func.julianday(cls.created_at)) * 86400
        else:
            duration_expr = func.extract("epoch", cls.completed_at - cls.created_at)

        when_condition = (
            cls.completed_at.isnot(None) & cls.created_at.isnot(None),
            duration_expr,
        )

        return case(when_condition, else_=None)


class BackfillDagRunExceptionReason(str, Enum):
    """
    Enum for storing reasons why Dag run not created.

    :meta private:
    """

    IN_FLIGHT = "in flight"
    ALREADY_EXISTS = "already exists"
    MISSING_INPUT = "missing input"
    NO_SINGLE_PARTITION_DATE = "no single partition date"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return self.value


BACKFILL_PLAN_RESULT_RUN = "run"


class BackfillDagRun(Base):
    """Mapping table between backfill run and Dag run."""

    __tablename__ = "backfill_dag_run"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    backfill_id: Mapped[int] = mapped_column(Integer, nullable=False)
    dag_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    exception_reason: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    logical_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    partition_key: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    sort_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)

    backfill = relationship("Backfill", back_populates="backfill_dag_run_associations")
    dag_run = relationship("DagRun")

    def __repr__(self):
        return f"BackfillDagRun(id={self.id}, backfill_id={self.backfill_id}, logical_date='{self.logical_date}')"

    __table_args__ = (
        UniqueConstraint("backfill_id", "dag_run_id", name="ix_bdr_backfill_id_dag_run_id"),
        ForeignKeyConstraint(
            [backfill_id],
            ["backfill.id"],
            name="bdr_backfill_fkey",
            ondelete="cascade",
        ),
        ForeignKeyConstraint(
            [dag_run_id],
            ["dag_run.id"],
            name="bdr_dag_run_fkey",
            ondelete="set null",
        ),
    )

    @validates("sort_ordinal")
    def validate_sort_ordinal(self, key: str, val: int) -> int:
        if val < 1:
            raise ValueError("sort_ordinal must be >= 1")
        return val


def _get_latest_dag_run_row_query(*, dag_id: str, info: DagRunInfo):
    from airflow.models import DagRun

    stmt = select(DagRun).where(DagRun.dag_id == dag_id)
    if info.partition_date is not None:
        # Filter the runs whose partition_date matches (the UTC instant of the partition
        # tick), not the partition_key string. partition_date is the canonical, format-
        # independent identity of a partition: a scheduled run and a backfill run for the
        # same tick always agree on it, even if partition_key is later formatted differently
        # (e.g. relabelled in the timetable timezone). Filtering by the string would let a
        # backfill create duplicate runs whenever the key format changed.
        stmt = stmt.where(DagRun.partition_date == info.partition_date)
    if info.logical_date is not None:
        stmt = stmt.where(DagRun.logical_date == info.logical_date)
    stmt = stmt.order_by(DagRun.start_date.is_(None), DagRun.start_date.desc())
    return stmt.limit(1)


def _get_dag_run_no_create_reason(dr, reprocess_behavior: ReprocessBehavior) -> str | None:
    non_create_reason = None
    if dr.state not in (DagRunState.SUCCESS, DagRunState.FAILED):
        non_create_reason = BackfillDagRunExceptionReason.IN_FLIGHT
    elif reprocess_behavior is ReprocessBehavior.NONE:
        non_create_reason = BackfillDagRunExceptionReason.ALREADY_EXISTS
    elif reprocess_behavior is ReprocessBehavior.FAILED:
        if dr.state != DagRunState.FAILED:
            non_create_reason = BackfillDagRunExceptionReason.ALREADY_EXISTS
    return non_create_reason


def _validate_backfill_params(
    dag: SerializedDAG,
    reverse: bool,
    from_date: datetime,
    to_date: datetime,
    reprocess_behavior: ReprocessBehavior | None,
    dag_run_conf: dict | None = None,
) -> None:

    if from_date > to_date:
        raise InvalidBackfillDateRange(
            f"from_date ({from_date.isoformat()}) must not be after to_date ({to_date.isoformat()})."
        )

    current_time = timezone.utcnow()
    if from_date >= current_time and to_date >= current_time:
        raise InvalidBackfillDate("Backfill cannot be executed for future dates.")

    depends_on_past = any(x.depends_on_past for x in dag.tasks)
    if depends_on_past:
        if reverse is True:
            raise InvalidBackfillDirection(
                "Backfill cannot be run in reverse when the Dag has tasks where depends_on_past=True."
            )
        if reprocess_behavior in (None, ReprocessBehavior.NONE):
            raise InvalidReprocessBehavior(
                "Dag has tasks for which depends_on_past=True. "
                "You must set reprocess behavior to reprocess completed or reprocess failed."
            )
    if dag_run_conf is not None:
        try:
            dag.params.deep_merge(dag_run_conf).validate()
        except ValueError as e:
            raise InvalidBackfillConf(str(e)) from e


def _is_asset_consumer(dag: SerializedDAG) -> bool:
    from airflow.timetables.simple import PartitionedAssetTimetable

    return isinstance(dag.timetable, PartitionedAssetTimetable)


def _check_backfillable(dag: SerializedDAG, dag_id: str) -> None:
    """Raise when the Dag's schedule cannot be backfilled."""
    from airflow.timetables.simple import PartitionedAssetTimetable

    timetable = dag.timetable
    if timetable.periodic:
        return
    if not isinstance(timetable, PartitionedAssetTimetable):
        raise DagNonPeriodicScheduleException(
            f"{dag_id} has a non-periodic schedule that does not support backfills"
        )
    from airflow.configuration import conf

    if conf.getboolean("core", "multi_team", fallback=False):
        # Which consumers may receive an event is decided when the event arrives, with data
        # that is not all stored. A backfill cannot repeat that decision for old events.
        raise ConsumerBackfillNotSupported(
            f"{dag_id} is scheduled on partitioned assets; such a Dag cannot be backfilled "
            "when [core] multi_team is enabled"
        )
    if reason := timetable.backfill_unsupported_reason():
        raise ConsumerBackfillNotSupported(
            f"{dag_id} is scheduled on partitioned assets and cannot be backfilled: {reason}"
        )


def _consumer_infos_for_keys(
    dag: SerializedDAG, partition_keys: list[str], session: Session
) -> list[DagRunInfo]:
    """Resolve explicit partition keys of a Dag scheduled on partitioned assets."""
    from airflow.timetables.simple import PartitionedAssetTimetable

    timetable = dag.timetable
    if not isinstance(timetable, PartitionedAssetTimetable):
        raise InvalidBackfillPartitionKeys(
            f"partition_keys is only accepted for a Dag scheduled on partitioned assets, not for {dag.dag_id}"
        )
    if timetable.backfill_selects_keys_from_events():
        return _consumer_infos_from_events(dag, session, partition_keys=partition_keys)
    if not partition_keys:
        raise InvalidBackfillPartitionKeys("partition_keys must not be empty")
    try:
        return _consumer_info_list(dag, timetable.partition_dagrun_infos_for_keys(partition_keys))
    except ValueError as e:
        raise InvalidBackfillPartitionKeys(str(e)) from e


def _resolve_bounds(
    dag: SerializedDAG,
    from_date: datetime | None,
    to_date: datetime | None,
    partition_keys: list[str] | None,
    session: Session,
) -> tuple[datetime, datetime]:
    """
    Return the date bounds of a backfill request.

    A request has a date range or partition keys. For partition keys, the bounds are the
    earliest and the latest partition date of the keys.
    """
    if partition_keys is not None:
        if from_date is not None or to_date is not None:
            raise InvalidBackfillPartitionKeys("Give a date range or partition_keys, not both.")
        infos = _consumer_infos_for_keys(dag, partition_keys, session)
        dates = [info.partition_date for info in infos if info.partition_date is not None]
        if not dates:
            raise InvalidBackfillPartitionKeys("None of the partition keys has a single partition date.")
        return min(dates), max(dates)
    if from_date is None or to_date is None:
        raise InvalidBackfillDateRange("from_date and to_date are required when partition_keys is not given.")
    return from_date, to_date


def _consumer_info_list(dag: SerializedDAG, infos: Iterable[DagRunInfo]) -> list[DagRunInfo]:
    """List the partitions of a Dag scheduled on partitioned assets, up to the configured limit."""
    from airflow.configuration import conf

    limit = conf.getint("scheduler", "max_consumer_backfill_partitions", fallback=1000)
    dagrun_info_list: list[DagRunInfo] = []
    try:
        for info in infos:
            if len(dagrun_info_list) >= limit:
                raise TooManyBackfillPartitions(
                    f"The range selects more than {limit} partitions of Dag {dag.dag_id}. "
                    "Use a shorter range."
                )
            dagrun_info_list.append(info)
    except ValueError as e:
        raise ConsumerBackfillNotSupported(
            f"{dag.dag_id} is scheduled on partitioned assets and cannot be backfilled: {e}"
        ) from e
    return dagrun_info_list


def _consumer_evidence(
    dag: SerializedDAG, partition_key: str, session: Session
) -> tuple[list[Any], list[tuple[str, str]]]:
    """
    Find the newest existing asset event for each input of a partition.

    Returns the events, and the (asset name, input key) pairs that have no event.
    """
    from airflow.models.asset import AssetEvent, AssetModel
    from airflow.partition_mappers.base import is_rollup

    timetable: Any = dag.timetable
    events: list[Any] = []
    missing: list[tuple[str, str]] = []
    for unique_key, _ in timetable.asset_condition.iter_assets():
        mapper = timetable.get_partition_mapper(name=unique_key.name, uri=unique_key.uri)
        asset_id = session.scalar(
            select(AssetModel.id).where(
                AssetModel.name == unique_key.name,
                AssetModel.uri == unique_key.uri,
                AssetModel.active.has(),
            )
        )
        input_keys = sorted(mapper.to_upstream(partition_key)) if is_rollup(mapper) else [partition_key]
        for input_key in input_keys:
            event = None
            if asset_id is not None:
                event = session.scalar(
                    select(AssetEvent)
                    .where(AssetEvent.asset_id == asset_id, AssetEvent.partition_key == input_key)
                    .order_by(AssetEvent.timestamp.desc(), AssetEvent.id.desc())
                    .limit(1)
                )
            if event is None:
                missing.append((unique_key.name, input_key))
            else:
                events.append(event)
    return events, missing


def _evidence_partition_date(events: list[Any], session: Session) -> datetime | None:
    """
    Return the partition date that the evidence gives a key with no date of its own.

    It is the partition date of the run that wrote each event, which live scheduling carries
    onto the consumer. There is no date when an event has no such run, when a run has no
    partition date, or when the events disagree.
    """
    from airflow.models.dagrun import DagRun

    dates: set[datetime | None] = set()
    for event in events:
        partition_date = None
        if event.source_dag_id and event.source_run_id:
            partition_date = session.scalar(
                select(DagRun.partition_date).where(
                    DagRun.dag_id == event.source_dag_id, DagRun.run_id == event.source_run_id
                )
            )
        dates.add(partition_date)
    return dates.pop() if len(dates) == 1 else None


def _consumer_infos_from_events(
    dag: SerializedDAG,
    session: Session,
    *,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    partition_keys: list[str] | None = None,
) -> list[DagRunInfo]:
    """
    List the partitions of a consumer whose inputs all use ``IdentityMapper``.

    For a date range, the keys are those of existing events whose source run has a partition
    date in the range. The producer is not read. A key with one partition date comes first,
    by date. A key with no single date comes after, and gets no run.
    """
    from airflow.configuration import conf
    from airflow.models.asset import AssetEvent, AssetModel
    from airflow.models.dagrun import DagRun
    from airflow.timetables.base import DagRunInfo

    limit = conf.getint("scheduler", "max_consumer_backfill_partitions", fallback=1000)
    timetable: Any = dag.timetable
    if partition_keys is None:
        asset_ids = [
            asset_id
            for unique_key, _ in timetable.asset_condition.iter_assets()
            if (
                asset_id := session.scalar(
                    select(AssetModel.id).where(
                        AssetModel.name == unique_key.name,
                        AssetModel.uri == unique_key.uri,
                        AssetModel.active.has(),
                    )
                )
            )
            is not None
        ]
        keys = list(
            session.scalars(
                select(AssetEvent.partition_key)
                .join(
                    DagRun,
                    and_(
                        DagRun.dag_id == AssetEvent.source_dag_id, DagRun.run_id == AssetEvent.source_run_id
                    ),
                )
                .where(
                    AssetEvent.asset_id.in_(asset_ids),
                    AssetEvent.partition_key.is_not(None),
                    DagRun.partition_date >= from_date,
                    DagRun.partition_date <= to_date,
                )
                .distinct()
                .limit(limit + 1)
            )
        )
    else:
        if not partition_keys:
            raise InvalidBackfillPartitionKeys("partition_keys must not be empty")
        if len(set(partition_keys)) != len(partition_keys):
            raise InvalidBackfillPartitionKeys("a partition key is listed more than once")
        keys = list(partition_keys)
    selected: list[str] = [key for key in keys if key is not None]
    if len(selected) > limit:
        raise TooManyBackfillPartitions(
            f"The request selects more than {limit} partitions of Dag {dag.dag_id}. Use a shorter range."
        )

    dated: list[tuple[datetime, str]] = []
    undated: list[str] = []
    for key in selected:
        events, _ = _consumer_evidence(dag, key, session)
        partition_date = _evidence_partition_date(events, session)
        if partition_date is None:
            undated.append(key)
        else:
            dated.append((partition_date, key))
    infos = []
    for partition_date, key in sorted(dated):
        run_after = timezone.coerce_datetime(partition_date)
        infos.append(
            DagRunInfo(run_after=run_after, data_interval=None, partition_date=run_after, partition_key=key)
        )
    now = timezone.coerce_datetime(timezone.utcnow())
    infos.extend(
        DagRunInfo(run_after=now, data_interval=None, partition_date=None, partition_key=key)
        for key in undated
    )
    return infos


def _link_consumer_evidence(dag: SerializedDAG, dr: DagRun, evidence: list[Any], session: Session) -> None:
    """Link the run to the events it reads, with the same rows that live scheduling writes."""
    from airflow.models.asset import AssetPartitionDagRun, PartitionedAssetKeyLog
    from airflow.timetables.base import compute_rollup_fingerprint

    dr.consumed_asset_events.extend(evidence)
    # The row is created already linked to the run, so it is never a pending row of live scheduling.
    linked = AssetPartitionDagRun(
        target_dag_id=dr.dag_id,
        partition_key=dr.partition_key,
        partition_date=dr.partition_date,
        created_dag_run_id=dr.id,
        rollup_fingerprint=compute_rollup_fingerprint(dag.timetable),
    )
    session.add(linked)
    session.flush()
    session.add_all(
        PartitionedAssetKeyLog(
            asset_id=event.asset_id,
            asset_event_id=event.id,
            asset_partition_dag_run_id=linked.id,
            source_partition_key=event.partition_key,
            target_dag_id=dr.dag_id,
            target_partition_key=dr.partition_key,
        )
        for event in evidence
    )


class BackfillPlanEntry(NamedTuple):
    """
    What a backfill would do for one Dag run.

    ``result`` is ``run``, or the reason why no run would be created. ``missing_inputs`` lists
    the (asset name, partition key) pairs that have no asset event, for the result ``missing input``.

    :meta private:
    """

    info: DagRunInfo
    result: str
    missing_inputs: list[tuple[str, str]]


def _plan_backfill(
    *,
    dag_id: str,
    from_date: datetime | None,
    to_date: datetime | None,
    reverse: bool,
    reprocess_behavior: ReprocessBehavior,
    session: Session,
    dag_run_conf: dict | None = None,
    partition_keys: list[str] | None = None,
) -> list[BackfillPlanEntry]:
    """List every Dag run that a backfill request selects, with what the backfill would do for it."""
    from airflow.models.serialized_dag import SerializedDagModel

    serdag = session.scalar(SerializedDagModel.latest_item_select_object(dag_id))
    if not serdag:
        raise DagNotFound(f"Could not find Dag {dag_id}")
    dag = serdag.dag

    _check_backfillable(dag, dag_id)
    if dag.allowed_run_types is not None and DagRunType.BACKFILL_JOB not in dag.allowed_run_types:
        raise DagRunTypeNotAllowed(f"Dag with dag_id: '{dag_id}' does not allow backfill runs")

    from_date, to_date = _resolve_bounds(dag, from_date, to_date, partition_keys, session)
    _validate_backfill_params(
        dag,
        reverse,
        from_date,
        to_date,
        reprocess_behavior,
        dag_run_conf,
    )
    dagrun_info_list = _get_info_list(
        dag=dag,
        from_date=from_date,
        to_date=to_date,
        reverse=reverse,
        partition_keys=partition_keys,
        session=session,
    )

    is_consumer = _is_asset_consumer(dag)
    plan: list[BackfillPlanEntry] = []
    for info in dagrun_info_list:
        if is_consumer and info.partition_date is None:
            plan.append(
                BackfillPlanEntry(info, BackfillDagRunExceptionReason.NO_SINGLE_PARTITION_DATE.value, [])
            )
            continue
        dr = session.scalar(
            statement=_get_latest_dag_run_row_query(dag_id=dag_id, info=info),
        )
        if dr and (reason := _get_dag_run_no_create_reason(dr, reprocess_behavior)):
            plan.append(BackfillPlanEntry(info, str(getattr(reason, "value", reason)), []))
            continue
        if is_consumer and info.partition_key:
            missing = _consumer_evidence(dag, info.partition_key, session)[1]
            if missing:
                plan.append(
                    BackfillPlanEntry(info, BackfillDagRunExceptionReason.MISSING_INPUT.value, missing)
                )
                continue
        plan.append(BackfillPlanEntry(info, BACKFILL_PLAN_RESULT_RUN, []))
    return plan


def _do_dry_run(
    *,
    dag_id: str,
    from_date: datetime | None,
    to_date: datetime | None,
    reverse: bool,
    reprocess_behavior: ReprocessBehavior,
    session: Session,
    dag_run_conf: dict | None = None,
    partition_keys: list[str] | None = None,
) -> Iterable[DagRunInfo]:
    for entry in _plan_backfill(
        dag_id=dag_id,
        from_date=from_date,
        to_date=to_date,
        reverse=reverse,
        reprocess_behavior=reprocess_behavior,
        session=session,
        dag_run_conf=dag_run_conf,
        partition_keys=partition_keys,
    ):
        if entry.result == BACKFILL_PLAN_RESULT_RUN:
            yield entry.info


def _create_backfill_dag_run_non_partitioned(
    *,
    dag: SerializedDAG,
    info: DagRunInfo,
    reprocess_behavior: ReprocessBehavior,
    backfill_id: int,
    dag_run_conf: dict | None,
    backfill_sort_ordinal: int,
    triggering_user_name: str | None,
    run_on_latest_version: bool,
    session: Session,
) -> None:
    from airflow.models.dagrun import DagRun

    with session.begin_nested() as nested:
        dr = session.scalar(_get_latest_dag_run_row_query(dag_id=dag.dag_id, info=info))
        if dr:
            non_create_reason = _get_dag_run_no_create_reason(dr, reprocess_behavior)
            if non_create_reason:
                session.add(
                    BackfillDagRun(
                        backfill_id=backfill_id,
                        dag_run_id=None,
                        logical_date=info.logical_date,
                        partition_key=info.partition_key,
                        exception_reason=non_create_reason,
                        sort_ordinal=backfill_sort_ordinal,
                    )
                )
                return
            lock = session.execute(
                with_row_locks(
                    query=select(DagRun).where(
                        DagRun.logical_date == info.logical_date,
                        DagRun.dag_id == dag.dag_id,
                    ),
                    session=session,
                    skip_locked=True,
                )
            )
            if lock:
                _handle_clear_run(
                    session=session,
                    dag=dag,
                    dr=dr,
                    info=info,
                    backfill_id=backfill_id,
                    sort_ordinal=backfill_sort_ordinal,
                    run_on_latest=run_on_latest_version,
                    dag_run_conf=dag_run_conf,
                )
            else:
                session.add(
                    BackfillDagRun(
                        backfill_id=backfill_id,
                        dag_run_id=None,
                        logical_date=info.logical_date,
                        partition_key=info.partition_key,
                        exception_reason=BackfillDagRunExceptionReason.IN_FLIGHT,
                        sort_ordinal=backfill_sort_ordinal,
                    )
                )
            return

        try:
            dr = dag.create_dagrun(
                run_id=DagRun.generate_run_id(
                    run_type=DagRunType.BACKFILL_JOB, logical_date=info.logical_date, run_after=info.run_after
                ),
                logical_date=info.logical_date,
                partition_key=info.partition_key,
                data_interval=info.data_interval if info.logical_date else None,
                run_after=info.run_after,
                conf=dag_run_conf,
                run_type=DagRunType.BACKFILL_JOB,
                triggered_by=DagRunTriggeredByType.BACKFILL,
                triggering_user_name=triggering_user_name,
                state=DagRunState.QUEUED,
                start_date=timezone.utcnow(),
                backfill_id=backfill_id,
                session=session,
            )
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=dr.id,
                    sort_ordinal=backfill_sort_ordinal,
                    logical_date=info.logical_date,
                    partition_key=info.partition_key,
                )
            )
        except IntegrityError:
            log.info(
                "Backfill Dag run already exists; skipping.",
                dag_id=dag.dag_id,
                backfill_id=backfill_id,
                logical_date=info.logical_date,
            )
            nested.rollback()

            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    partition_key=info.partition_key,
                    exception_reason=BackfillDagRunExceptionReason.IN_FLIGHT,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )


def _create_backfill_dag_run_partitioned(
    *,
    dag: SerializedDAG,
    info: DagRunInfo,
    reprocess_behavior: ReprocessBehavior,
    backfill_id: int,
    dag_run_conf: dict | None,
    backfill_sort_ordinal: int,
    triggering_user_name: str | None,
    session: Session,
) -> None:
    if info.partition_date is None and _is_asset_consumer(dag):
        # The key takes its date from its evidence, and the evidence gives no single date.
        # Without a date the run lookup below would match the runs of other keys.
        session.add(
            BackfillDagRun(
                backfill_id=backfill_id,
                dag_run_id=None,
                logical_date=info.logical_date,
                partition_key=info.partition_key,
                exception_reason=BackfillDagRunExceptionReason.NO_SINGLE_PARTITION_DATE,
                sort_ordinal=backfill_sort_ordinal,
            )
        )
        log.warning(
            "Skipping dag run creation.",
            non_create_reason=BackfillDagRunExceptionReason.NO_SINGLE_PARTITION_DATE,
            backfill_id=backfill_id,
        )
        return
    stmt = _get_latest_dag_run_row_query(dag_id=dag.dag_id, info=info)
    dr = session.scalar(stmt)
    if dr:
        non_create_reason = _get_dag_run_no_create_reason(dr, reprocess_behavior)
        if non_create_reason:
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    partition_key=info.partition_key,
                    exception_reason=non_create_reason,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )
            log.warning(
                "Skipping dag run creation.", non_create_reason=non_create_reason, backfill_id=backfill_id
            )
            return

    evidence: list[Any] = []
    if _is_asset_consumer(dag) and info.partition_key:
        evidence, missing = _consumer_evidence(dag, info.partition_key, session)
        if missing:
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    partition_key=info.partition_key,
                    exception_reason=BackfillDagRunExceptionReason.MISSING_INPUT,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )
            log.warning(
                "Skipping dag run creation.",
                non_create_reason=BackfillDagRunExceptionReason.MISSING_INPUT,
                missing_inputs=missing,
                backfill_id=backfill_id,
            )
            return

    # reprocess_behavior allows a retry: create a new run alongside the existing
    # one rather than clearing it. The prior run is kept as a historical record;
    # _get_latest_dag_run_row_query picks the newest run by start_date, so the
    # new backfill run becomes the active one going forward.
    dr = dag.create_dagrun(
        run_id=dag.timetable.generate_run_id(
            run_type=DagRunType.BACKFILL_JOB,
            data_interval=info.data_interval,
            partition_key=info.partition_key,
            run_after=info.run_after,
        ),
        logical_date=info.logical_date,
        partition_key=info.partition_key,
        partition_date=info.partition_date,
        data_interval=info.data_interval if info.logical_date else None,
        run_after=info.run_after,
        conf=dag_run_conf,
        run_type=DagRunType.BACKFILL_JOB,
        triggered_by=DagRunTriggeredByType.BACKFILL,
        triggering_user_name=triggering_user_name,
        state=DagRunState.QUEUED,
        start_date=timezone.utcnow(),
        backfill_id=backfill_id,
        session=session,
    )
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            sort_ordinal=backfill_sort_ordinal,
            logical_date=info.logical_date,
            partition_key=info.partition_key,
        )
    )
    if evidence:
        _link_consumer_evidence(dag, dr, evidence, session)


def _get_info_list(
    *,
    from_date: datetime,
    to_date: datetime,
    reverse: bool,
    dag: SerializedDAG,
    partition_keys: list[str] | None = None,
    session: Session | None = None,
) -> list[DagRunInfo]:
    timetable: Any = dag.timetable
    from_events = _is_asset_consumer(dag) and timetable.backfill_selects_keys_from_events()
    if partition_keys is not None or from_events:
        if session is None:
            raise RuntimeError("A session is needed to list the partitions of this Dag.")
        if partition_keys is not None:
            dagrun_info_list = _consumer_infos_for_keys(dag, partition_keys, session)
        else:
            dagrun_info_list = _consumer_infos_from_events(dag, session, from_date=from_date, to_date=to_date)
        return list(reversed(dagrun_info_list)) if reverse else dagrun_info_list
    infos = dag.iter_dagrun_infos_between(from_date, to_date)
    if not dag.timetable.partitioned:
        now = timezone.utcnow()
        dagrun_info_list = [x for x in infos if x.data_interval and x.data_interval.end < now]
    elif _is_asset_consumer(dag):
        dagrun_info_list = _consumer_info_list(dag, infos)
    else:
        dagrun_info_list = list(infos)
    if reverse:
        dagrun_info_list = list(reversed(dagrun_info_list))
    return dagrun_info_list


def _handle_clear_run(
    *,
    session: Session,
    dag: SerializedDAG,
    dr: DagRun,
    info: DagRunInfo,
    backfill_id: int,
    sort_ordinal: int,
    run_on_latest: bool = False,
    dag_run_conf: dict | None = None,
) -> None:
    """Clear the existing Dag run and update backfill metadata."""
    from sqlalchemy.sql import update

    from airflow.models import DagRun
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType

    dag.clear(
        run_id=dr.run_id,
        dag_run_state=DagRunState.QUEUED,
        session=session,
        dry_run=False,
        run_on_latest_version=run_on_latest,
    )

    # Update backfill_id, run_type, and optionally conf in DagRun table
    stmt = (
        update(DagRun)
        .where(DagRun.logical_date == info.logical_date, DagRun.dag_id == dag.dag_id)
        .values(
            backfill_id=backfill_id,
            run_type=DagRunType.BACKFILL_JOB,
            triggered_by=DagRunTriggeredByType.BACKFILL,
        )
    )
    if dag_run_conf is not None:
        stmt = stmt.values(conf=dag_run_conf)
    session.execute(stmt)
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            logical_date=info.logical_date,
            partition_key=info.partition_key,
            sort_ordinal=sort_ordinal,
        )
    )


def _create_backfill(
    *,
    dag_id: str,
    from_date: datetime | None,
    to_date: datetime | None,
    max_active_runs: int,
    reverse: bool,
    dag_run_conf: dict | None,
    triggering_user_name: str | None,
    reprocess_behavior: ReprocessBehavior | None = None,
    run_on_latest_version: bool = False,
    drain_dag: bool = False,
    partition_keys: list[str] | None = None,
) -> Backfill:
    from airflow.models import DagModel
    from airflow.models.serialized_dag import SerializedDagModel

    with create_session() as session:
        serdag = session.scalar(SerializedDagModel.latest_item_select_object(dag_id))
        if not serdag:
            raise DagNotFound(f"Could not find dag {dag_id}")

        dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id).limit(1))
        if dag_model:
            if (
                dag_model.allowed_run_types is not None
                and DagRunType.BACKFILL_JOB not in dag_model.allowed_run_types
            ):
                raise DagRunTypeNotAllowed(f"Dag with dag_id: '{dag_id}' does not allow backfill runs")

        dag = serdag.dag
        _check_backfillable(dag, dag_id)

        num_active = session.scalar(
            select(func.count()).where(
                Backfill.dag_id == dag_id,
                Backfill.completed_at.is_(None),
            )
        )
        if num_active is None:
            raise UnknownActiveBackfills(dag_id)
        if num_active > 0:
            raise AlreadyRunningBackfill(
                f"Another backfill is running for Dag {dag_id}. "
                f"There can be only one running backfill per Dag."
            )

        from_date, to_date = _resolve_bounds(dag, from_date, to_date, partition_keys, session)
        _validate_backfill_params(
            dag,
            reverse,
            from_date,
            to_date,
            reprocess_behavior,
            dag_run_conf,
        )

        dagrun_info_list = _get_info_list(
            from_date=from_date,
            to_date=to_date,
            reverse=reverse,
            dag=dag,
            partition_keys=partition_keys,
            session=session,
        )
        if not dagrun_info_list:
            raise NoBackfillRunsToCreate(
                f"No runs to create for Dag {dag_id} in the range [{from_date}, {to_date}]"
            )

        backfill = Backfill(
            dag_id=dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=max_active_runs,
            dag_run_conf=dag_run_conf,
            reprocess_behavior=reprocess_behavior,
            dag_model=dag,
            triggering_user_name=triggering_user_name,
        )
        session.add(backfill)
        # Commit immediately so the backfill is visible to concurrent requests
        # checking num_active backfills, preventing duplicate active backfills
        # for the same dag.
        session.commit()

        session.scalars(select(DagModel).where(DagModel.dag_id == dag_id)).one()

        first_info = dagrun_info_list[0]
        try:
            if first_info.partition_key:
                _create_runs_partitioned(
                    backfill=backfill,
                    dag=dag,
                    dagrun_info_list=dagrun_info_list,
                    session=session,
                )
            else:
                _create_runs_non_partitioned(
                    backfill=backfill,
                    dag=dag,
                    dagrun_info_list=dagrun_info_list,
                    run_on_latest_version=run_on_latest_version,
                    session=session,
                )
            if drain_dag:
                DagModel.start_drain(dag_id, session=session)
        except OperationalError as e:
            if is_lock_not_available_error(e):
                # Lock error: clean up the orphan so the user can retry. The
                # helper is best-effort; if it fails the original error still
                # surfaces and the route returns 503.
                _cleanup_partial_backfill(backfill, session)
            raise
    return backfill


def _cleanup_partial_backfill(backfill: Backfill, session: Session) -> None:
    """Best-effort removal of a partially-created backfill after a lock error."""
    from airflow.models.dagrun import DagRun

    try:
        session.rollback()
        session.execute(delete(BackfillDagRun).where(BackfillDagRun.backfill_id == backfill.id))
        session.execute(delete(DagRun).where(DagRun.backfill_id == backfill.id))
        session.delete(backfill)
        session.commit()
    except Exception:
        session.rollback()


def _create_runs_partitioned(
    *,
    backfill: Backfill,
    dag: SerializedDAG,
    dagrun_info_list: list[DagRunInfo],
    session: Session,
) -> None:
    for info in dagrun_info_list:
        if not info.partition_key:
            raise RuntimeError("Expected all Dag run infos to have partition key and no logical date.")
    for backfill_sort_ordinal, info in enumerate(dagrun_info_list, start=1):
        _create_backfill_dag_run_partitioned(
            dag=dag,
            info=info,
            backfill_id=backfill.id,
            dag_run_conf=backfill.dag_run_conf,
            reprocess_behavior=ReprocessBehavior(backfill.reprocess_behavior),
            backfill_sort_ordinal=backfill_sort_ordinal,
            triggering_user_name=backfill.triggering_user_name,
            session=session,
        )
        log.info(
            "Created backfill Dag run.",
            dag_id=dag.dag_id,
            backfill_id=backfill.id,
            logical_date=info.logical_date,
        )


def _create_runs_non_partitioned(
    *,
    backfill: Backfill,
    dag: SerializedDAG,
    dagrun_info_list: list[DagRunInfo],
    run_on_latest_version: bool,
    session: Session,
) -> None:
    for info in dagrun_info_list:
        if info.partition_key or not info.logical_date:
            raise RuntimeError("Expected all Dag run infos to have logical date and no partition key.")

    for backfill_sort_ordinal, info in enumerate(dagrun_info_list, start=1):
        _create_backfill_dag_run_non_partitioned(
            dag=dag,
            info=info,
            backfill_id=backfill.id,
            dag_run_conf=backfill.dag_run_conf,
            reprocess_behavior=ReprocessBehavior(backfill.reprocess_behavior),
            backfill_sort_ordinal=backfill_sort_ordinal,
            triggering_user_name=backfill.triggering_user_name,
            run_on_latest_version=run_on_latest_version,
            session=session,
        )
        log.info(
            "Created backfill Dag run.",
            dag_id=dag.dag_id,
            backfill_id=backfill.id,
            logical_date=info.logical_date,
        )
