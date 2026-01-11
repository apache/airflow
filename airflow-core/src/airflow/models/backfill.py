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

import logging
from collections.abc import Sequence
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    ForeignKeyConstraint,
    Integer,
    String,
    UniqueConstraint,
    desc,
    func,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapped, relationship, validates
from sqlalchemy_jsonfield import JSONField

from airflow._shared.timezones import timezone
from airflow.exceptions import AirflowException, DagNotFound
from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column, nulls_first, with_row_locks
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.timetables.base import DagRunInfo

log = logging.getLogger(__name__)


class AlreadyRunningBackfill(AirflowException):
    """
    Raised when attempting to create backfill and one already active.

    :meta private:
    """


class DagNoScheduleException(AirflowException):
    """
    Raised when attempting to create backfill for a DAG with no schedule.

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


class Backfill(Base):
    """Model representing a backfill job."""

    __tablename__ = "backfill"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    from_date: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    to_date: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
    dag_run_conf: Mapped[dict] = mapped_column(JSONField(json=json), nullable=False, default={})
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

    backfill_dag_run_associations = relationship("BackfillDagRun", back_populates="backfill")

    dag_model = relationship(
        "DagModel",
        primaryjoin="DagModel.dag_id == Backfill.dag_id",
        viewonly=True,
        foreign_keys=[dag_id],
    )

    def __repr__(self):
        return f"Backfill({self.dag_id=}, {self.from_date=}, {self.to_date=})"


class BackfillDagRunExceptionReason(str, Enum):
    """
    Enum for storing reasons why dag run not created.

    :meta private:
    """

    IN_FLIGHT = "in flight"
    ALREADY_EXISTS = "already exists"
    UNKNOWN = "unknown"


class BackfillDagRun(Base):
    """Mapping table between backfill run and dag run."""

    __tablename__ = "backfill_dag_run"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    backfill_id: Mapped[int] = mapped_column(Integer, nullable=False)
    dag_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    exception_reason: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    logical_date: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)
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


def _get_latest_dag_run_row_query(*, dag_id: str, info: DagRunInfo, session: Session):
    from airflow.models import DagRun

    return (
        select(DagRun)
        .where(
            DagRun.logical_date == info.logical_date,
            DagRun.dag_id == dag_id,
        )
        .order_by(nulls_first(desc(DagRun.start_date), session=session))
        .limit(1)
    )


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
) -> None:
    depends_on_past = any(x.depends_on_past for x in dag.tasks)
    if depends_on_past:
        if reverse is True:
            raise InvalidBackfillDirection(
                "Backfill cannot be run in reverse when the DAG has tasks where depends_on_past=True."
            )
        if reprocess_behavior in (None, ReprocessBehavior.NONE):
            raise InvalidReprocessBehavior(
                "DAG has tasks for which depends_on_past=True. "
                "You must set reprocess behavior to reprocess completed or reprocess failed."
            )
    current_time = timezone.utcnow()
    if from_date >= current_time and to_date >= current_time:
        raise InvalidBackfillDate("Backfill cannot be executed for future dates.")


def _do_dry_run(
    *,
    dag_id: str,
    from_date: datetime,
    to_date: datetime,
    reverse: bool,
    reprocess_behavior: ReprocessBehavior,
    session: Session,
) -> Sequence[datetime]:
    from airflow.models import DagModel
    from airflow.models.serialized_dag import SerializedDagModel

    serdag = session.scalar(SerializedDagModel.latest_item_select_object(dag_id))
    if not serdag:
        raise DagNotFound(f"Could not find dag {dag_id}")
    dag = serdag.dag
    _validate_backfill_params(dag, reverse, from_date, to_date, reprocess_behavior)

    no_schedule = session.scalar(
        select(func.count()).where(DagModel.timetable_summary == "None", DagModel.dag_id == dag_id)
    )
    if no_schedule:
        raise DagNoScheduleException(f"{dag_id} has no schedule")

    dagrun_info_list = _get_info_list(
        dag=dag,
        from_date=from_date,
        to_date=to_date,
        reverse=reverse,
    )
    logical_dates: list[datetime] = []
    for info in dagrun_info_list:
        dr = session.scalar(
            statement=_get_latest_dag_run_row_query(dag_id=dag_id, info=info, session=session),
        )
        if dr:
            non_create_reason = _get_dag_run_no_create_reason(dr, reprocess_behavior)
            if not non_create_reason:
                logical_dates.append(info.logical_date)
        else:
            logical_dates.append(info.logical_date)
    return logical_dates


def _create_backfill_dag_run(
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
        dr = session.scalar(_get_latest_dag_run_row_query(dag_id=dag.dag_id, info=info, session=session))
        if dr:
            non_create_reason = _get_dag_run_no_create_reason(dr, reprocess_behavior)
            if non_create_reason:
                session.add(
                    BackfillDagRun(
                        backfill_id=backfill_id,
                        dag_run_id=None,
                        logical_date=info.logical_date,
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
                )
            else:
                session.add(
                    BackfillDagRun(
                        backfill_id=backfill_id,
                        dag_run_id=None,
                        logical_date=info.logical_date,
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
                )
            )
        except IntegrityError:
            log.info(
                "Skipped creating backfill dag run for dag_id=%s backfill_id=%s, logical_date=%s (already exists)",
                dag.dag_id,
                backfill_id,
                info.logical_date,
            )
            nested.rollback()

            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    exception_reason=BackfillDagRunExceptionReason.IN_FLIGHT,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )


def _get_info_list(
    *,
    from_date: datetime,
    to_date: datetime,
    reverse: bool,
    dag: SerializedDAG,
) -> list[DagRunInfo]:
    infos = dag.iter_dagrun_infos_between(from_date, to_date)
    now = timezone.utcnow()
    dagrun_info_list = [x for x in infos if x.data_interval.end < now]
    if reverse:
        dagrun_info_list = list(reversed(dagrun_info_list))
    return dagrun_info_list


def _handle_clear_run(
    session: Session,
    dag: SerializedDAG,
    dr: DagRun,
    info: DagRunInfo,
    backfill_id: int,
    sort_ordinal: int,
    run_on_latest: bool = False,
) -> None:
    """Clear the existing DAG run and update backfill metadata."""
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

    # Update backfill_id and run_type in DagRun table
    session.execute(
        update(DagRun)
        .where(DagRun.logical_date == info.logical_date, DagRun.dag_id == dag.dag_id)
        .values(
            backfill_id=backfill_id,
            run_type=DagRunType.BACKFILL_JOB,
            triggered_by=DagRunTriggeredByType.BACKFILL,
        )
    )
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            logical_date=info.logical_date,
            sort_ordinal=sort_ordinal,
        )
    )


def _create_backfill(
    *,
    dag_id: str,
    from_date: datetime,
    to_date: datetime,
    max_active_runs: int,
    reverse: bool,
    dag_run_conf: dict | None,
    triggering_user_name: str | None,
    reprocess_behavior: ReprocessBehavior | None = None,
    run_on_latest_version: bool = False,
) -> Backfill:
    from airflow.models import DagModel
    from airflow.models.serialized_dag import SerializedDagModel

    with create_session() as session:
        serdag = session.scalar(SerializedDagModel.latest_item_select_object(dag_id))
        if not serdag:
            raise DagNotFound(f"Could not find dag {dag_id}")
        no_schedule = session.scalar(
            select(func.count()).where(DagModel.timetable_summary == "None", DagModel.dag_id == dag_id)
        )
        if no_schedule:
            raise DagNoScheduleException(f"{dag_id} has no schedule")

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
                f"Another backfill is running for dag {dag_id}. "
                f"There can be only one running backfill per dag."
            )

        dag = serdag.dag
        _validate_backfill_params(dag, reverse, from_date, to_date, reprocess_behavior)

        br = Backfill(
            dag_id=dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=max_active_runs,
            dag_run_conf=dag_run_conf,
            reprocess_behavior=reprocess_behavior,
            dag_model=dag,
            triggering_user_name=triggering_user_name,
        )
        session.add(br)
        session.commit()

        dagrun_info_list = _get_info_list(
            from_date=from_date,
            to_date=to_date,
            reverse=reverse,
            dag=dag,
        )

        dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == dag_id))

        if not dag_model:
            raise RuntimeError(f"Dag {dag_id} not found")

        for backfill_sort_ordinal, info in enumerate(dagrun_info_list, start=1):
            _create_backfill_dag_run(
                dag=dag,
                info=info,
                backfill_id=br.id,
                dag_run_conf=br.dag_run_conf,
                reprocess_behavior=ReprocessBehavior(br.reprocess_behavior),
                backfill_sort_ordinal=backfill_sort_ordinal,
                triggering_user_name=br.triggering_user_name,
                run_on_latest_version=run_on_latest_version,
                session=session,
            )
            log.info(
                "created backfill dag run dag_id=%s backfill_id=%s, info=%s",
                dag.dag_id,
                br.id,
                info,
            )
    return br
