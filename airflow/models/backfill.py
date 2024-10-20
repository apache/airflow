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
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKeyConstraint, Integer, UniqueConstraint, func, select, update
from sqlalchemy.orm import relationship, validates
from sqlalchemy_jsonfield import JSONField

from airflow.api_connexion.exceptions import Conflict, NotFound
from airflow.exceptions import AirflowException
from airflow.models import clear_task_instances
from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import UtcDateTime, prohibit_commit, with_row_locks
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from datetime import datetime


log = logging.getLogger(__name__)


class AlreadyRunningBackfill(AirflowException):
    """
    Raised when attempting to create backfill and one already active.

    :meta private:
    """


class ReprocessBehavior(str, Enum):
    """
    Internal enum for setting reprocess behavior in a backfill.

    :meta private:
    """

    REPROCESS_FAILED = "reprocess-failed"
    REPROCESS_COMPLETED = "reprocess-completed"
    REPROCESS_NONE = "reprocess-none"


class Backfill(Base):
    """Model representing a backfill job."""

    __tablename__ = "backfill"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(StringID(), nullable=False)
    from_date = Column(UtcDateTime, nullable=False)
    to_date = Column(UtcDateTime, nullable=False)
    dag_run_conf = Column(JSONField(json=json), nullable=True)
    is_paused = Column(Boolean, default=False)
    """
    Controls whether new dag runs will be created for this backfill.

    Does not pause existing dag runs.
    """
    reprocess_behavior = Column(StringID(), nullable=False, default=ReprocessBehavior.REPROCESS_NONE)
    max_active_runs = Column(Integer, default=10, nullable=False)
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    completed_at = Column(UtcDateTime, nullable=True)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    backfill_dag_run_associations = relationship("BackfillDagRun", back_populates="backfill")

    def __repr__(self):
        return f"Backfill({self.dag_id=}, {self.from_date=}, {self.to_date=})"


class BackfillDagRunExceptionReason(str, Enum):
    """
    Enum for storing reasons why dag run not created.

    :meta private:
    """

    ALREADY_EXISTS = "already exists"
    UNKNOWN = "unknown"


class BackfillDagRun(Base):
    """Mapping table between backfill run and dag run."""

    __tablename__ = "backfill_dag_run"
    id = Column(Integer, primary_key=True, autoincrement=True)
    backfill_id = Column(Integer, nullable=False)
    dag_run_id = Column(Integer, nullable=True)
    exception_reason = Column(StringID())
    logical_date = Column(UtcDateTime, nullable=False)
    sort_ordinal = Column(Integer, nullable=False)

    backfill = relationship("Backfill", back_populates="backfill_dag_run_associations")
    dag_run = relationship("DagRun")

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
    def validate_sort_ordinal(self, key, val):
        if val < 1:
            raise ValueError("sort_ordinal must be >= 1")
        return val


def _create_backfill_dag_run(
    *,
    dag,
    info,
    reprocess_behavior: ReprocessBehavior,
    backfill_id,
    dag_run_conf,
    backfill_sort_ordinal,
    session,
):
    from airflow.models import DagRun

    states_to_query = None
    dr_exists = session.scalar(select(DagRun.id).where(DagRun.execution_date == info.logical_date).limit(1))
    if dr_exists:
        if reprocess_behavior is ReprocessBehavior.REPROCESS_NONE:
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    exception_reason=BackfillDagRunExceptionReason.ALREADY_EXISTS,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )
            return
        elif reprocess_behavior is ReprocessBehavior.REPROCESS_FAILED:
            states_to_query = [DagRunState.FAILED]
        elif reprocess_behavior is ReprocessBehavior.REPROCESS_COMPLETED:
            states_to_query = [DagRunState.SUCCESS, DagRunState.FAILED]
        else:
            raise RuntimeError("unexpected")
    if states_to_query:
        # we only get here if user requested some clearing behavior
        # and there are some dag runs for the logical date
        # find the ones in the requested states and clear them
        runs_to_clear_query = select(DagRun).where(
            DagRun.execution_date == info.logical_date,
            DagRun.state.in_(states_to_query),
        )
        runs_to_clear = session.scalars(with_row_locks(runs_to_clear_query, session=session)).all()
        for dr in runs_to_clear:
            clear_task_instances(
                tis=dr.get_task_instances(session=session),
                session=session,
            )
            dr.backfill_id = backfill_id
            dr.run_type = DagRunType.BACKFILL_JOB
            dr.triggered_by = DagRunTriggeredByType.BACKFILL
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=dr.id,
                    logical_date=info.logical_date,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )
        if not runs_to_clear:
            session.add(
                BackfillDagRun(
                    backfill_id=backfill_id,
                    dag_run_id=None,
                    logical_date=info.logical_date,
                    exception_reason=BackfillDagRunExceptionReason.ALREADY_EXISTS,
                    sort_ordinal=backfill_sort_ordinal,
                )
            )
        return

    if dr_exists:
        return

    # we only get here if there's no dag run for the logical date
    # todo: we're subject to a race condition here where something else
    #   could create a dag run for the same logical date at the same time
    #   but there's no easy solution for this right now
    dr = dag.create_dagrun(
        triggered_by=DagRunTriggeredByType.BACKFILL,
        execution_date=info.logical_date,
        data_interval=info.data_interval,
        start_date=timezone.utcnow(),
        state=DagRunState.QUEUED,
        external_trigger=False,
        conf=dag_run_conf,
        run_type=DagRunType.BACKFILL_JOB,
        creating_job_id=None,
        session=session,
        backfill_id=backfill_id,
    )
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            sort_ordinal=backfill_sort_ordinal,
            logical_date=info.logical_date,
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
    reprocess_behavior: ReprocessBehavior | None = None,
) -> Backfill | None:
    from airflow.models.serialized_dag import SerializedDagModel

    with create_session() as session:
        serdag = session.get(SerializedDagModel, dag_id)
        if not serdag:
            raise NotFound(f"Could not find dag {dag_id}")

        num_active = session.scalar(
            select(func.count()).where(Backfill.dag_id == dag_id, Backfill.completed_at.is_(None))
        )
        if num_active > 0:
            raise AlreadyRunningBackfill(
                f"Another backfill is running for dag {dag_id}. "
                f"There can be only one running backfill per dag."
            )

        br = Backfill(
            dag_id=dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=max_active_runs,
            dag_run_conf=dag_run_conf,
            reprocess_behavior=reprocess_behavior,
        )
        session.add(br)
        session.commit()

        dag = serdag.dag
        depends_on_past = any(x.depends_on_past for x in dag.tasks)
        if depends_on_past:
            if reverse is True:
                raise ValueError(
                    "Backfill cannot be run in reverse when the dag has tasks where depends_on_past=True"
                )

        backfill_sort_ordinal = 0
        dagrun_info_list = dag.iter_dagrun_infos_between(from_date, to_date)
        if reverse:
            dagrun_info_list = reversed([x for x in dag.iter_dagrun_infos_between(from_date, to_date)])
        for info in dagrun_info_list:
            backfill_sort_ordinal += 1
            session.commit()
            from tenacity import RetryError, Retrying, stop_after_attempt

            try:
                for attempt in Retrying(stop=stop_after_attempt(3)):
                    # we do retries here because it's possible that we check to see if dr exists
                    # before we attempt to create the dag run. if something else creates the dag
                    # run in between, we'll have to retry the transaction
                    with attempt, session.begin(), prohibit_commit(session):
                        _create_backfill_dag_run(
                            dag=dag,
                            info=info,
                            backfill_id=br.id,
                            dag_run_conf=br.dag_run_conf,
                            reprocess_behavior=br.reprocess_behavior,
                            backfill_sort_ordinal=backfill_sort_ordinal,
                            session=session,
                        )
                        log.info(
                            "created backfill dag run dag_id=%s backfill_id=%s, info=%s",
                            dag.dag_id,
                            br.id,
                            info,
                        )
            except RetryError:
                dag.log.exception(
                    "Error while attempting to create a dag run dag_id='%s' logical_date='%s'",
                    dag.dag_id,
                    info.logical_date,
                )
                session.add(
                    BackfillDagRun(
                        backfill_id=br.id,
                        dag_run_id=None,
                        exception_reason=BackfillDagRunExceptionReason.UNKNOWN,
                        logical_date=info.logical_date,
                        sort_ordinal=backfill_sort_ordinal,
                    )
                )
                session.commit()
    return br


def _cancel_backfill(backfill_id) -> Backfill:
    with create_session() as session:
        b: Backfill = session.get(Backfill, backfill_id)
        if b.completed_at is not None:
            raise Conflict("Backfill is already completed.")

        b.completed_at = timezone.utcnow()

        # first, pause
        if not b.is_paused:
            b.is_paused = True

        session.commit()

        from airflow.models import DagRun

        # now, let's mark all queued dag runs as failed
        query = (
            update(DagRun)
            .where(
                DagRun.id.in_(select(BackfillDagRun.dag_run_id).where(BackfillDagRun.backfill_id == b.id)),
                DagRun.state == DagRunState.QUEUED,
            )
            .values(state=DagRunState.FAILED)
            .execution_options(synchronize_session=False)
        )
        session.execute(query)
    return b
