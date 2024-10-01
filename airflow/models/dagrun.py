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
from __future__ import annotations

import itertools
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, NamedTuple, Sequence, TypeVar, overload

import re2
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PickleType,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    and_,
    func,
    or_,
    text,
    update,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import declared_attr, joinedload, relationship, synonym, validates
from sqlalchemy.sql.expression import case, false, select, true

from airflow import settings
from airflow.api_internal.internal_api_call import internal_api_call
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.listeners.listener import get_listener_manager
from airflow.models import Log
from airflow.models.abstractoperator import NotMapped
from airflow.models.base import Base, StringID
from airflow.models.expandinput import NotFullyPopulated
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.tasklog import LogTemplate
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.traces.tracer import Trace
from airflow.utils import timezone
from airflow.utils.dates import datetime_to_nano
from airflow.utils.helpers import chunks, is_container, prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, nulls_first, tuple_in_condition, with_row_locks
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import NOTSET, DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Query, Session

    from airflow.models.dag import DAG
    from airflow.models.operator import Operator
    from airflow.serialization.pydantic.dag_run import DagRunPydantic
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
    from airflow.serialization.pydantic.tasklog import LogTemplatePydantic
    from airflow.typing_compat import Literal
    from airflow.utils.types import ArgNotSet

    CreatedTasks = TypeVar("CreatedTasks", Iterator["dict[str, Any]"], Iterator[TI])

RUN_ID_REGEX = r"^(?:manual|scheduled|dataset_triggered)__(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00)$"


class TISchedulingDecision(NamedTuple):
    """Type of return for DagRun.task_instance_scheduling_decisions."""

    tis: list[TI]
    schedulable_tis: list[TI]
    changed_tis: bool
    unfinished_tis: list[TI]
    finished_tis: list[TI]


def _creator_note(val):
    """Creator the ``note`` association proxy."""
    if isinstance(val, str):
        return DagRunNote(content=val)
    elif isinstance(val, dict):
        return DagRunNote(**val)
    else:
        return DagRunNote(*val)


class DagRun(Base, LoggingMixin):
    """
    Invocation instance of a DAG.

    A DAG run can be created by the scheduler (i.e. scheduled runs), or by an
    external trigger (i.e. manual runs).
    """

    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(StringID(), nullable=False)
    queued_at = Column(UtcDateTime)
    execution_date = Column("logical_date", UtcDateTime, default=timezone.utcnow, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    _state = Column("state", String(50), default=DagRunState.QUEUED)
    run_id = Column(StringID(), nullable=False)
    creating_job_id = Column(Integer)
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String(50), nullable=False)
    triggered_by = Column(
        Enum(DagRunTriggeredByType, native_enum=False, length=50)
    )  # Airflow component that triggered the run.
    conf = Column(PickleType)
    # These two must be either both NULL or both datetime.
    data_interval_start = Column(UtcDateTime)
    data_interval_end = Column(UtcDateTime)
    # When a scheduler last attempted to schedule TIs for this DagRun
    last_scheduling_decision = Column(UtcDateTime)
    dag_hash = Column(String(32))
    # Foreign key to LogTemplate. DagRun rows created prior to this column's
    # existence have this set to NULL. Later rows automatically populate this on
    # insert to point to the latest LogTemplate entry.
    log_template_id = Column(
        Integer,
        ForeignKey("log_template.id", name="task_instance_log_template_id_fkey", ondelete="NO ACTION"),
        default=select(func.max(LogTemplate.__table__.c.id)),
    )
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow)
    # Keeps track of the number of times the dagrun had been cleared.
    # This number is incremented only when the DagRun is re-Queued,
    # when the DagRun is cleared.
    clear_number = Column(Integer, default=0, nullable=False, server_default="0")

    # Remove this `if` after upgrading Sphinx-AutoAPI
    if not TYPE_CHECKING and "BUILDING_AIRFLOW_DOCS" in os.environ:
        dag: DAG | None
    else:
        dag: DAG | None = None

    __table_args__ = (
        Index("dag_id_state", dag_id, _state),
        UniqueConstraint("dag_id", "run_id", name="dag_run_dag_id_run_id_key"),
        Index("idx_dag_run_dag_id", dag_id),
        Index(
            "idx_dag_run_running_dags",
            "state",
            "dag_id",
            postgresql_where=text("state='running'"),
            sqlite_where=text("state='running'"),
        ),
        # since mysql lacks filtered/partial indices, this creates a
        # duplicate index on mysql. Not the end of the world
        Index(
            "idx_dag_run_queued_dags",
            "state",
            "dag_id",
            postgresql_where=text("state='queued'"),
            sqlite_where=text("state='queued'"),
        ),
    )

    task_instances = relationship(
        TI, back_populates="dag_run", cascade="save-update, merge, delete, delete-orphan"
    )
    dag_model = relationship(
        "DagModel",
        primaryjoin="foreign(DagRun.dag_id) == DagModel.dag_id",
        uselist=False,
        viewonly=True,
    )
    dag_run_note = relationship(
        "DagRunNote",
        back_populates="dag_run",
        uselist=False,
        cascade="all, delete, delete-orphan",
    )
    note = association_proxy("dag_run_note", "content", creator=_creator_note)

    DEFAULT_DAGRUNS_TO_EXAMINE = airflow_conf.getint(
        "scheduler",
        "max_dagruns_per_loop_to_schedule",
        fallback=20,
    )

    def __init__(
        self,
        dag_id: str | None = None,
        run_id: str | None = None,
        queued_at: datetime | None | ArgNotSet = NOTSET,
        execution_date: datetime | None = None,
        start_date: datetime | None = None,
        external_trigger: bool | None = None,
        conf: Any | None = None,
        state: DagRunState | None = None,
        run_type: str | None = None,
        dag_hash: str | None = None,
        creating_job_id: int | None = None,
        data_interval: tuple[datetime, datetime] | None = None,
        triggered_by: DagRunTriggeredByType | None = None,
    ):
        if data_interval is None:
            # Legacy: Only happen for runs created prior to Airflow 2.2.
            self.data_interval_start = self.data_interval_end = None
        else:
            self.data_interval_start, self.data_interval_end = data_interval

        self.dag_id = dag_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.external_trigger = external_trigger
        self.conf = conf or {}
        if state is not None:
            self.state = state
        if queued_at is NOTSET:
            self.queued_at = timezone.utcnow() if state == DagRunState.QUEUED else None
        else:
            self.queued_at = queued_at
        self.run_type = run_type
        self.dag_hash = dag_hash
        self.creating_job_id = creating_job_id
        self.clear_number = 0
        self.triggered_by = triggered_by
        super().__init__()

    def __repr__(self):
        return (
            f"<DagRun {self.dag_id} @ {self.execution_date}: {self.run_id}, state:{self.state}, "
            f"queued_at: {self.queued_at}. externally triggered: {self.external_trigger}>"
        )

    @validates("run_id")
    def validate_run_id(self, key: str, run_id: str) -> str | None:
        if not run_id:
            return None
        regex = airflow_conf.get("scheduler", "allowed_run_id_pattern")
        if not re2.match(regex, run_id) and not re2.match(RUN_ID_REGEX, run_id):
            raise ValueError(
                f"The run_id provided '{run_id}' does not match the pattern '{regex}' or '{RUN_ID_REGEX}'"
            )
        return run_id

    @property
    def stats_tags(self) -> dict[str, str]:
        return prune_dict({"dag_id": self.dag_id, "run_type": self.run_type})

    @property
    def logical_date(self) -> datetime:
        return self.execution_date

    def get_state(self):
        return self._state

    def set_state(self, state: DagRunState) -> None:
        """
        Change the state of the DagRan.

        Changes to attributes are implemented in accordance with the following table
        (rows represent old states, columns represent new states):

        .. list-table:: State transition matrix
           :header-rows: 1
           :stub-columns: 1

           * -
             - QUEUED
             - RUNNING
             - SUCCESS
             - FAILED
           * - None
             - queued_at = timezone.utcnow()
             - if empty: start_date = timezone.utcnow()
               end_date = None
             - end_date = timezone.utcnow()
             - end_date = timezone.utcnow()
           * - QUEUED
             - queued_at = timezone.utcnow()
             - if empty: start_date = timezone.utcnow()
               end_date = None
             - end_date = timezone.utcnow()
             - end_date = timezone.utcnow()
           * - RUNNING
             - queued_at = timezone.utcnow()
               start_date = None
               end_date = None
             -
             - end_date = timezone.utcnow()
             - end_date = timezone.utcnow()
           * - SUCCESS
             - queued_at = timezone.utcnow()
               start_date = None
               end_date = None
             - start_date = timezone.utcnow()
               end_date = None
             -
             -
           * - FAILED
             - queued_at = timezone.utcnow()
               start_date = None
               end_date = None
             - start_date = timezone.utcnow()
               end_date = None
             -
             -

        """
        if state not in State.dag_states:
            raise ValueError(f"invalid DagRun state: {state}")
        if self._state != state:
            if state == DagRunState.QUEUED:
                self.queued_at = timezone.utcnow()
                self.start_date = None
                self.end_date = None
            if state == DagRunState.RUNNING:
                if self._state in State.finished_dr_states:
                    self.start_date = timezone.utcnow()
                else:
                    self.start_date = self.start_date or timezone.utcnow()
                self.end_date = None
            if self._state in State.unfinished_dr_states or self._state is None:
                if state in State.finished_dr_states:
                    self.end_date = timezone.utcnow()
            self._state = state
        else:
            if state == DagRunState.QUEUED:
                self.queued_at = timezone.utcnow()

    @declared_attr
    def state(self):
        return synonym("_state", descriptor=property(self.get_state, self.set_state))

    @provide_session
    def refresh_from_db(self, session: Session = NEW_SESSION) -> None:
        """
        Reload the current dagrun from the database.

        :param session: database session
        """
        dr = session.scalars(
            select(DagRun).where(DagRun.dag_id == self.dag_id, DagRun.run_id == self.run_id)
        ).one()
        self.id = dr.id
        self.state = dr.state

    @classmethod
    @provide_session
    def active_runs_of_dags(
        cls,
        dag_ids: Iterable[str] | None = None,
        only_running: bool = False,
        session: Session = NEW_SESSION,
    ) -> dict[str, int]:
        """Get the number of active dag runs for each dag."""
        query = select(cls.dag_id, func.count("*"))
        if dag_ids is not None:
            # 'set' called to avoid duplicate dag_ids, but converted back to 'list'
            # because SQLAlchemy doesn't accept a set here.
            query = query.where(cls.dag_id.in_(set(dag_ids)))
        if only_running:
            query = query.where(cls.state == DagRunState.RUNNING)
        else:
            query = query.where(cls.state.in_((DagRunState.RUNNING, DagRunState.QUEUED)))
        query = query.group_by(cls.dag_id)
        return dict(iter(session.execute(query)))

    @classmethod
    @retry_db_transaction
    def get_running_dag_runs_to_examine(cls, session: Session) -> Query:
        """
        Return the next DagRuns that the scheduler should attempt to schedule.

        This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
        query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
        the transaction is committed it will be unlocked.

        :meta private:
        """
        from airflow.models.dag import DagModel

        query = (
            select(cls)
            .with_hint(cls, "USE INDEX (idx_dag_run_running_dags)", dialect_name="mysql")
            .where(cls.state == DagRunState.RUNNING, cls.run_type != DagRunType.BACKFILL_JOB)
            .join(DagModel, DagModel.dag_id == cls.dag_id)
            .where(DagModel.is_paused == false(), DagModel.is_active == true())
            .order_by(
                nulls_first(cls.last_scheduling_decision, session=session),
                cls.execution_date,
            )
        )

        if not settings.ALLOW_FUTURE_EXEC_DATES:
            query = query.where(DagRun.execution_date <= func.now())

        return session.scalars(
            with_row_locks(
                query.limit(cls.DEFAULT_DAGRUNS_TO_EXAMINE),
                of=cls,
                session=session,
                skip_locked=True,
            )
        )

    @classmethod
    @retry_db_transaction
    def get_queued_dag_runs_to_set_running(cls, session: Session) -> Query:
        """
        Return the next queued DagRuns that the scheduler should attempt to schedule.

        This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
        query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
        the transaction is committed it will be unlocked.

        :meta private:
        """
        from airflow.models.dag import DagModel

        # For dag runs in the queued state, we check if they have reached the max_active_runs limit
        # and if so we drop them
        running_drs = (
            select(DagRun.dag_id, func.count(DagRun.state).label("num_running"))
            .where(DagRun.state == DagRunState.RUNNING)
            .group_by(DagRun.dag_id)
            .subquery()
        )
        query = (
            select(cls)
            .where(cls.state == DagRunState.QUEUED, cls.run_type != DagRunType.BACKFILL_JOB)
            .join(DagModel, DagModel.dag_id == cls.dag_id)
            .where(DagModel.is_paused == false(), DagModel.is_active == true())
            .outerjoin(running_drs, running_drs.c.dag_id == DagRun.dag_id)
            .where(func.coalesce(running_drs.c.num_running, 0) < DagModel.max_active_runs)
            .order_by(
                nulls_first(cls.last_scheduling_decision, session=session),
                cls.execution_date,
            )
        )

        if not settings.ALLOW_FUTURE_EXEC_DATES:
            query = query.where(DagRun.execution_date <= func.now())

        return session.scalars(
            with_row_locks(
                query.limit(cls.DEFAULT_DAGRUNS_TO_EXAMINE),
                of=cls,
                session=session,
                skip_locked=True,
            )
        )

    @classmethod
    @provide_session
    def find(
        cls,
        dag_id: str | list[str] | None = None,
        run_id: Iterable[str] | None = None,
        execution_date: datetime | Iterable[datetime] | None = None,
        state: DagRunState | None = None,
        external_trigger: bool | None = None,
        no_backfills: bool = False,
        run_type: DagRunType | None = None,
        session: Session = NEW_SESSION,
        execution_start_date: datetime | None = None,
        execution_end_date: datetime | None = None,
    ) -> list[DagRun]:
        """
        Return a set of dag runs for the given search criteria.

        :param dag_id: the dag_id or list of dag_id to find dag runs for
        :param run_id: defines the run id for this dag run
        :param run_type: type of DagRun
        :param execution_date: the execution date
        :param state: the state of the dag run
        :param external_trigger: whether this dag run is externally triggered
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :param session: database session
        :param execution_start_date: dag run that was executed from this date
        :param execution_end_date: dag run that was executed until this date
        """
        qry = select(cls)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.where(cls.dag_id.in_(dag_ids))

        if is_container(run_id):
            qry = qry.where(cls.run_id.in_(run_id))
        elif run_id is not None:
            qry = qry.where(cls.run_id == run_id)
        if is_container(execution_date):
            qry = qry.where(cls.execution_date.in_(execution_date))
        elif execution_date is not None:
            qry = qry.where(cls.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.where(cls.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.where(cls.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.where(cls.execution_date <= execution_end_date)
        if state:
            qry = qry.where(cls.state == state)
        if external_trigger is not None:
            qry = qry.where(cls.external_trigger == external_trigger)
        if run_type:
            qry = qry.where(cls.run_type == run_type)
        if no_backfills:
            qry = qry.where(cls.run_type != DagRunType.BACKFILL_JOB)

        return session.scalars(qry.order_by(cls.execution_date)).all()

    @classmethod
    @provide_session
    def find_duplicate(
        cls,
        dag_id: str,
        run_id: str,
        execution_date: datetime,
        session: Session = NEW_SESSION,
    ) -> DagRun | None:
        """
        Return an existing run for the DAG with a specific run_id or execution_date.

        *None* is returned if no such DAG run is found.

        :param dag_id: the dag_id to find duplicates for
        :param run_id: defines the run id for this dag run
        :param execution_date: the execution date
        :param session: database session
        """
        return session.scalars(
            select(cls).where(
                cls.dag_id == dag_id,
                or_(cls.run_id == run_id, cls.execution_date == execution_date),
            )
        ).one_or_none()

    @staticmethod
    def generate_run_id(run_type: DagRunType, execution_date: datetime) -> str:
        """Generate Run ID based on Run Type and Execution Date."""
        # _Ensure_ run_type is a DagRunType, not just a string from user code
        return DagRunType(run_type).generate_run_id(execution_date)

    @staticmethod
    @internal_api_call
    @provide_session
    def fetch_task_instances(
        dag_id: str | None = None,
        run_id: str | None = None,
        task_ids: list[str] | None = None,
        state: Iterable[TaskInstanceState | None] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TI]:
        """Return the task instances for this dag run."""
        tis = (
            select(TI)
            .options(joinedload(TI.dag_run))
            .where(
                TI.dag_id == dag_id,
                TI.run_id == run_id,
            )
        )

        if state:
            if isinstance(state, str):
                tis = tis.where(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.where(TI.state.is_(None))
                    else:
                        not_none_state = (s for s in state if s)
                        tis = tis.where(or_(TI.state.in_(not_none_state), TI.state.is_(None)))
                else:
                    tis = tis.where(TI.state.in_(state))

        if task_ids is not None:
            tis = tis.where(TI.task_id.in_(task_ids))
        return session.scalars(tis).all()

    @internal_api_call
    def _check_last_n_dagruns_failed(self, dag_id, max_consecutive_failed_dag_runs, session):
        """Check if last N dags failed."""
        dag_runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id)
            .order_by(DagRun.execution_date.desc())
            .limit(max_consecutive_failed_dag_runs)
            .all()
        )
        """ Marking dag as paused, if needed"""
        to_be_paused = len(dag_runs) >= max_consecutive_failed_dag_runs and all(
            dag_run.state == DagRunState.FAILED for dag_run in dag_runs
        )

        if to_be_paused:
            from airflow.models.dag import DagModel

            self.log.info(
                "Pausing DAG %s because last %s DAG runs failed.",
                self.dag_id,
                max_consecutive_failed_dag_runs,
            )
            filter_query = [
                DagModel.dag_id == self.dag_id,
            ]
            session.execute(
                update(DagModel)
                .where(or_(*filter_query))
                .values(is_paused=True)
                .execution_options(synchronize_session="fetch")
            )
            session.add(
                Log(
                    event="paused",
                    dag_id=self.dag_id,
                    owner="scheduler",
                    owner_display_name="Scheduler",
                    extra=f"[('dag_id', '{self.dag_id}'), ('is_paused', True)]",
                )
            )
        else:
            self.log.debug(
                "Limit of consecutive DAG failed dag runs is not reached, DAG %s will not be paused.",
                self.dag_id,
            )

    @provide_session
    def get_task_instances(
        self,
        state: Iterable[TaskInstanceState | None] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TI]:
        """
        Return the task instances for this dag run.

        Redirect to DagRun.fetch_task_instances method.
        Keep this method because it is widely used across the code.
        """
        task_ids = DagRun._get_partial_task_ids(self.dag)
        return DagRun.fetch_task_instances(
            dag_id=self.dag_id, run_id=self.run_id, task_ids=task_ids, state=state, session=session
        )

    @provide_session
    def get_task_instance(
        self,
        task_id: str,
        session: Session = NEW_SESSION,
        *,
        map_index: int = -1,
    ) -> TI | TaskInstancePydantic | None:
        """
        Return the task instance specified by task_id for this dag run.

        :param task_id: the task id
        :param session: Sqlalchemy ORM Session
        """
        return DagRun.fetch_task_instance(
            dag_id=self.dag_id,
            dag_run_id=self.run_id,
            task_id=task_id,
            session=session,
            map_index=map_index,
        )

    @staticmethod
    @internal_api_call
    @provide_session
    def fetch_task_instance(
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        session: Session = NEW_SESSION,
        map_index: int = -1,
    ) -> TI | TaskInstancePydantic | None:
        """
        Return the task instance specified by task_id for this dag run.

        :param dag_id: the DAG id
        :param dag_run_id: the DAG run id
        :param task_id: the task id
        :param session: Sqlalchemy ORM Session
        """
        return session.scalars(
            select(TI).filter_by(dag_id=dag_id, run_id=dag_run_id, task_id=task_id, map_index=map_index)
        ).one_or_none()

    def get_dag(self) -> DAG:
        """
        Return the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException(f"The DAG (.dag) for {self} needs to be set")

        return self.dag

    @staticmethod
    @internal_api_call
    @provide_session
    def get_previous_dagrun(
        dag_run: DagRun | DagRunPydantic, state: DagRunState | None = None, session: Session = NEW_SESSION
    ) -> DagRun | None:
        """
        Return the previous DagRun, if there is one.

        :param dag_run: the dag run
        :param session: SQLAlchemy ORM Session
        :param state: the dag run state
        """
        filters = [
            DagRun.dag_id == dag_run.dag_id,
            DagRun.execution_date < dag_run.execution_date,
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        return session.scalar(select(DagRun).where(*filters).order_by(DagRun.execution_date.desc()).limit(1))

    @staticmethod
    @internal_api_call
    @provide_session
    def get_previous_scheduled_dagrun(
        dag_run_id: int,
        session: Session = NEW_SESSION,
    ) -> DagRun | None:
        """
        Return the previous SCHEDULED DagRun, if there is one.

        :param dag_run_id: the DAG run ID
        :param session: SQLAlchemy ORM Session
        """
        dag_run = session.get(DagRun, dag_run_id)
        return session.scalar(
            select(DagRun)
            .where(
                DagRun.dag_id == dag_run.dag_id,
                DagRun.execution_date < dag_run.execution_date,
                DagRun.run_type != DagRunType.MANUAL,
            )
            .order_by(DagRun.execution_date.desc())
            .limit(1)
        )

    def _tis_for_dagrun_state(self, *, dag, tis):
        """
        Return the collection of tasks that should be considered for evaluation of terminal dag run state.

        Teardown tasks by default are not considered for the purpose of dag run state.  But
        users may enable such consideration with on_failure_fail_dagrun.
        """

        def is_effective_leaf(task):
            for down_task_id in task.downstream_task_ids:
                down_task = dag.get_task(down_task_id)
                if not down_task.is_teardown or down_task.on_failure_fail_dagrun:
                    # we found a down task that is not ignorable; not a leaf
                    return False
            # we found no ignorable downstreams
            # evaluate whether task is itself ignorable
            return not task.is_teardown or task.on_failure_fail_dagrun

        leaf_task_ids = {x.task_id for x in dag.tasks if is_effective_leaf(x)}
        if not leaf_task_ids:
            # can happen if dag is exclusively teardown tasks
            leaf_task_ids = {x.task_id for x in dag.tasks if not x.downstream_list}
        leaf_tis = {ti for ti in tis if ti.task_id in leaf_task_ids if ti.state != TaskInstanceState.REMOVED}
        return leaf_tis

    @provide_session
    def update_state(
        self, session: Session = NEW_SESSION, execute_callbacks: bool = True
    ) -> tuple[list[TI], DagCallbackRequest | None]:
        """
        Determine the overall state of the DagRun based on the state of its TaskInstances.

        :param session: Sqlalchemy ORM Session
        :param execute_callbacks: Should dag callbacks (success/failure, SLA etc.) be invoked
            directly (default: true) or recorded as a pending request in the ``returned_callback`` property
        :return: Tuple containing tis that can be scheduled in the current loop & `returned_callback` that
            needs to be executed
        """
        # Callback to execute in case of Task Failures
        callback: DagCallbackRequest | None = None

        class _UnfinishedStates(NamedTuple):
            tis: Sequence[TI]

            @classmethod
            def calculate(cls, unfinished_tis: Sequence[TI]) -> _UnfinishedStates:
                return cls(tis=unfinished_tis)

            @property
            def should_schedule(self) -> bool:
                return (
                    bool(self.tis)
                    and all(not t.task.depends_on_past for t in self.tis)  # type: ignore[union-attr]
                    and all(t.task.max_active_tis_per_dag is None for t in self.tis)  # type: ignore[union-attr]
                    and all(t.task.max_active_tis_per_dagrun is None for t in self.tis)  # type: ignore[union-attr]
                    and all(t.state != TaskInstanceState.DEFERRED for t in self.tis)
                )

            def recalculate(self) -> _UnfinishedStates:
                return self._replace(tis=[t for t in self.tis if t.state in State.unfinished])

        start_dttm = timezone.utcnow()
        self.last_scheduling_decision = start_dttm
        with Stats.timer(f"dagrun.dependency-check.{self.dag_id}"), Stats.timer(
            "dagrun.dependency-check", tags=self.stats_tags
        ):
            dag = self.get_dag()
            info = self.task_instance_scheduling_decisions(session)

            tis = info.tis
            schedulable_tis = info.schedulable_tis
            changed_tis = info.changed_tis
            finished_tis = info.finished_tis
            unfinished = _UnfinishedStates.calculate(info.unfinished_tis)

            if unfinished.should_schedule:
                are_runnable_tasks = schedulable_tis or changed_tis
                # small speed up
                if not are_runnable_tasks:
                    are_runnable_tasks, changed_by_upstream = self._are_premature_tis(
                        unfinished.tis, finished_tis, session
                    )
                    if changed_by_upstream:  # Something changed, we need to recalculate!
                        unfinished = unfinished.recalculate()

        tis_for_dagrun_state = self._tis_for_dagrun_state(dag=dag, tis=tis)

        # if all tasks finished and at least one failed, the run failed
        if not unfinished.tis and any(x.state in State.failed_states for x in tis_for_dagrun_state):
            self.log.info("Marking run %s failed", self)
            self.set_state(DagRunState.FAILED)
            self.notify_dagrun_state_changed(msg="task_failure")

            if execute_callbacks:
                dag.handle_callback(self, success=False, reason="task_failure", session=session)
            elif dag.has_on_failure_callback:
                from airflow.models.dag import DagModel

                dag_model = DagModel.get_dagmodel(dag.dag_id, session)
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=True,
                    processor_subdir=None if dag_model is None else dag_model.processor_subdir,
                    msg="task_failure",
                )

            # Check if the max_consecutive_failed_dag_runs has been provided and not 0
            # and last consecutive failures are more
            if dag.max_consecutive_failed_dag_runs > 0:
                self.log.debug(
                    "Checking consecutive failed DAG runs for DAG %s, limit is %s",
                    self.dag_id,
                    dag.max_consecutive_failed_dag_runs,
                )
                self._check_last_n_dagruns_failed(dag.dag_id, dag.max_consecutive_failed_dag_runs, session)

        # if all leaves succeeded and no unfinished tasks, the run succeeded
        elif not unfinished.tis and all(x.state in State.success_states for x in tis_for_dagrun_state):
            self.log.info("Marking run %s successful", self)
            self.set_state(DagRunState.SUCCESS)
            self.notify_dagrun_state_changed(msg="success")

            if execute_callbacks:
                dag.handle_callback(self, success=True, reason="success", session=session)
            elif dag.has_on_success_callback:
                from airflow.models.dag import DagModel

                dag_model = DagModel.get_dagmodel(dag.dag_id, session)
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=False,
                    processor_subdir=None if dag_model is None else dag_model.processor_subdir,
                    msg="success",
                )

        # if *all tasks* are deadlocked, the run failed
        elif unfinished.should_schedule and not are_runnable_tasks:
            self.log.error("Task deadlock (no runnable tasks); marking run %s failed", self)
            self.set_state(DagRunState.FAILED)
            self.notify_dagrun_state_changed(msg="all_tasks_deadlocked")

            if execute_callbacks:
                dag.handle_callback(self, success=False, reason="all_tasks_deadlocked", session=session)
            elif dag.has_on_failure_callback:
                from airflow.models.dag import DagModel

                dag_model = DagModel.get_dagmodel(dag.dag_id, session)
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=True,
                    processor_subdir=None if dag_model is None else dag_model.processor_subdir,
                    msg="all_tasks_deadlocked",
                )

        # finally, if the leaves aren't done, the dag is still running
        else:
            self.set_state(DagRunState.RUNNING)

        if self._state == DagRunState.FAILED or self._state == DagRunState.SUCCESS:
            msg = (
                "DagRun Finished: dag_id=%s, execution_date=%s, run_id=%s, "
                "run_start_date=%s, run_end_date=%s, run_duration=%s, "
                "state=%s, external_trigger=%s, run_type=%s, "
                "data_interval_start=%s, data_interval_end=%s, dag_hash=%s"
            )
            self.log.info(
                msg,
                self.dag_id,
                self.execution_date,
                self.run_id,
                self.start_date,
                self.end_date,
                (
                    (self.end_date - self.start_date).total_seconds()
                    if self.start_date and self.end_date
                    else None
                ),
                self._state,
                self.external_trigger,
                self.run_type,
                self.data_interval_start,
                self.data_interval_end,
                self.dag_hash,
            )

            with Trace.start_span_from_dagrun(dagrun=self) as span:
                if self._state is DagRunState.FAILED:
                    span.set_attribute("error", True)
                attributes = {
                    "category": "DAG runs",
                    "dag_id": str(self.dag_id),
                    "execution_date": str(self.execution_date),
                    "run_id": str(self.run_id),
                    "queued_at": str(self.queued_at),
                    "run_start_date": str(self.start_date),
                    "run_end_date": str(self.end_date),
                    "run_duration": str(
                        (self.end_date - self.start_date).total_seconds()
                        if self.start_date and self.end_date
                        else 0
                    ),
                    "state": str(self._state),
                    "external_trigger": str(self.external_trigger),
                    "run_type": str(self.run_type),
                    "data_interval_start": str(self.data_interval_start),
                    "data_interval_end": str(self.data_interval_end),
                    "dag_hash": str(self.dag_hash),
                    "conf": str(self.conf),
                }
                if span.is_recording():
                    span.add_event(name="queued", timestamp=datetime_to_nano(self.queued_at))
                    span.add_event(name="started", timestamp=datetime_to_nano(self.start_date))
                    span.add_event(name="ended", timestamp=datetime_to_nano(self.end_date))
                span.set_attributes(attributes)

            session.flush()

        self._emit_true_scheduling_delay_stats_for_finished_state(finished_tis)
        self._emit_duration_stats_for_finished_state()

        session.merge(self)
        # We do not flush here for performance reasons(It increases queries count by +20)

        return schedulable_tis, callback

    @provide_session
    def task_instance_scheduling_decisions(self, session: Session = NEW_SESSION) -> TISchedulingDecision:
        tis = self.get_task_instances(session=session, state=State.task_states)
        self.log.debug("number of tis tasks for %s: %s task(s)", self, len(tis))

        def _filter_tis_and_exclude_removed(dag: DAG, tis: list[TI]) -> Iterable[TI]:
            """Populate ``ti.task`` while excluding those missing one, marking them as REMOVED."""
            for ti in tis:
                try:
                    ti.task = dag.get_task(ti.task_id)
                except TaskNotFound:
                    if ti.state != TaskInstanceState.REMOVED:
                        self.log.error("Failed to get task for ti %s. Marking it as removed.", ti)
                        ti.state = TaskInstanceState.REMOVED
                        session.flush()
                else:
                    yield ti

        tis = list(_filter_tis_and_exclude_removed(self.get_dag(), tis))

        unfinished_tis = [t for t in tis if t.state in State.unfinished]
        finished_tis = [t for t in tis if t.state in State.finished]
        if unfinished_tis:
            schedulable_tis = [ut for ut in unfinished_tis if ut.state in SCHEDULEABLE_STATES]
            self.log.debug("number of scheduleable tasks for %s: %s task(s)", self, len(schedulable_tis))
            schedulable_tis, changed_tis, expansion_happened = self._get_ready_tis(
                schedulable_tis,
                finished_tis,
                session=session,
            )

            # During expansion, we may change some tis into non-schedulable
            # states, so we need to re-compute.
            if expansion_happened:
                changed_tis = True
                new_unfinished_tis = [t for t in unfinished_tis if t.state in State.unfinished]
                finished_tis.extend(t for t in unfinished_tis if t.state in State.finished)
                unfinished_tis = new_unfinished_tis
        else:
            schedulable_tis = []
            changed_tis = False

        return TISchedulingDecision(
            tis=tis,
            schedulable_tis=schedulable_tis,
            changed_tis=changed_tis,
            unfinished_tis=unfinished_tis,
            finished_tis=finished_tis,
        )

    def notify_dagrun_state_changed(self, msg: str = ""):
        if self.state == DagRunState.RUNNING:
            get_listener_manager().hook.on_dag_run_running(dag_run=self, msg=msg)
        elif self.state == DagRunState.SUCCESS:
            get_listener_manager().hook.on_dag_run_success(dag_run=self, msg=msg)
        elif self.state == DagRunState.FAILED:
            get_listener_manager().hook.on_dag_run_failed(dag_run=self, msg=msg)
        # deliberately not notifying on QUEUED
        # we can't get all the state changes on SchedulerJob, BackfillJob
        # or LocalTaskJob, so we don't want to "falsely advertise" we notify about that

    def _get_ready_tis(
        self,
        schedulable_tis: list[TI],
        finished_tis: list[TI],
        session: Session,
    ) -> tuple[list[TI], bool, bool]:
        old_states = {}
        ready_tis: list[TI] = []
        changed_tis = False

        if not schedulable_tis:
            return ready_tis, changed_tis, False

        # If we expand TIs, we need a new list so that we iterate over them too. (We can't alter
        # `schedulable_tis` in place and have the `for` loop pick them up
        additional_tis: list[TI] = []
        dep_context = DepContext(
            flag_upstream_failed=True,
            ignore_unmapped_tasks=True,  # Ignore this Dep, as we will expand it if we can.
            finished_tis=finished_tis,
        )

        def _expand_mapped_task_if_needed(ti: TI) -> Iterable[TI] | None:
            """
            Try to expand the ti, if needed.

            If the ti needs expansion, newly created task instances are
            returned as well as the original ti.
            The original ti is also modified in-place and assigned the
            ``map_index`` of 0.

            If the ti does not need expansion, either because the task is not
            mapped, or has already been expanded, *None* is returned.
            """
            if TYPE_CHECKING:
                assert ti.task

            if ti.map_index >= 0:  # Already expanded, we're good.
                return None

            from airflow.models.mappedoperator import MappedOperator

            if isinstance(ti.task, MappedOperator):
                # If we get here, it could be that we are moving from non-mapped to mapped
                # after task instance clearing or this ti is not yet expanded. Safe to clear
                # the db references.
                ti.clear_db_references(session=session)
            try:
                expanded_tis, _ = ti.task.expand_mapped_task(self.run_id, session=session)
            except NotMapped:  # Not a mapped task, nothing needed.
                return None
            if expanded_tis:
                return expanded_tis
            return ()

        # Check dependencies.
        expansion_happened = False
        # Set of task ids for which was already done _revise_map_indexes_if_mapped
        revised_map_index_task_ids = set()
        for schedulable in itertools.chain(schedulable_tis, additional_tis):
            if TYPE_CHECKING:
                assert schedulable.task
            old_state = schedulable.state
            if not schedulable.are_dependencies_met(session=session, dep_context=dep_context):
                old_states[schedulable.key] = old_state
                continue
            # If schedulable is not yet expanded, try doing it now. This is
            # called in two places: First and ideally in the mini scheduler at
            # the end of LocalTaskJob, and then as an "expansion of last resort"
            # in the scheduler to ensure that the mapped task is correctly
            # expanded before executed. Also see _revise_map_indexes_if_mapped
            # docstring for additional information.
            new_tis = None
            if schedulable.map_index < 0:
                new_tis = _expand_mapped_task_if_needed(schedulable)
                if new_tis is not None:
                    additional_tis.extend(new_tis)
                    expansion_happened = True
            if new_tis is None and schedulable.state in SCHEDULEABLE_STATES:
                # It's enough to revise map index once per task id,
                # checking the map index for each mapped task significantly slows down scheduling
                if schedulable.task.task_id not in revised_map_index_task_ids:
                    ready_tis.extend(self._revise_map_indexes_if_mapped(schedulable.task, session=session))
                    revised_map_index_task_ids.add(schedulable.task.task_id)
                ready_tis.append(schedulable)

        # Check if any ti changed state
        tis_filter = TI.filter_for_tis(old_states)
        if tis_filter is not None:
            fresh_tis = session.scalars(select(TI).where(tis_filter)).all()
            changed_tis = any(ti.state != old_states[ti.key] for ti in fresh_tis)

        return ready_tis, changed_tis, expansion_happened

    def _are_premature_tis(
        self,
        unfinished_tis: Sequence[TI],
        finished_tis: list[TI],
        session: Session,
    ) -> tuple[bool, bool]:
        dep_context = DepContext(
            flag_upstream_failed=True,
            ignore_in_retry_period=True,
            ignore_in_reschedule_period=True,
            finished_tis=finished_tis,
        )
        # there might be runnable tasks that are up for retry and for some reason(retry delay, etc.) are
        # not ready yet, so we set the flags to count them in
        return (
            any(ut.are_dependencies_met(dep_context=dep_context, session=session) for ut in unfinished_tis),
            dep_context.have_changed_ti_states,
        )

    def _emit_true_scheduling_delay_stats_for_finished_state(self, finished_tis: list[TI]) -> None:
        """
        Emit the true scheduling delay stats.

        The true scheduling delay stats is defined as the time when the first
        task in DAG starts minus the expected DAG run datetime.

        This helper method is used in ``update_state`` when the state of the
        DAG run is updated to a completed status (either success or failure).
        It finds the first started task within the DAG, calculates the run's
        expected start time based on the logical date and timetable, and gets
        the delay from the difference of these two values.

        The emitted data may contain outliers (e.g. when the first task was
        cleared, so the second task's start date will be used), but we can get
        rid of the outliers on the stats side through dashboards tooling.

        Note that the stat will only be emitted for scheduler-triggered DAG runs
        (i.e. when ``external_trigger`` is *False* and ``clear_number`` is equal to 0).
        """
        if self.state == TaskInstanceState.RUNNING:
            return
        if self.external_trigger:
            return
        if self.clear_number > 0:
            return
        if not finished_tis:
            return

        try:
            dag = self.get_dag()

            if not dag.timetable.periodic:
                # We can't emit this metric if there is no following schedule to calculate from!
                return

            try:
                first_start_date = min(ti.start_date for ti in finished_tis if ti.start_date)
            except ValueError:  # No start dates at all.
                pass
            else:
                # TODO: Logically, this should be DagRunInfo.run_after, but the
                # information is not stored on a DagRun, only before the actual
                # execution on DagModel.next_dagrun_create_after. We should add
                # a field on DagRun for this instead of relying on the run
                # always happening immediately after the data interval.
                data_interval_end = dag.get_run_data_interval(self).end
                true_delay = first_start_date - data_interval_end
                if true_delay.total_seconds() > 0:
                    Stats.timing(
                        f"dagrun.{dag.dag_id}.first_task_scheduling_delay", true_delay, tags=self.stats_tags
                    )
                    Stats.timing("dagrun.first_task_scheduling_delay", true_delay, tags=self.stats_tags)
        except Exception:
            self.log.warning("Failed to record first_task_scheduling_delay metric:", exc_info=True)

    def _emit_duration_stats_for_finished_state(self):
        if self.state == DagRunState.RUNNING:
            return
        if self.start_date is None:
            self.log.warning("Failed to record duration of %s: start_date is not set.", self)
            return
        if self.end_date is None:
            self.log.warning("Failed to record duration of %s: end_date is not set.", self)
            return

        duration = self.end_date - self.start_date
        timer_params = {"dt": duration, "tags": self.stats_tags}
        Stats.timing(f"dagrun.duration.{self.state}.{self.dag_id}", **timer_params)
        Stats.timing(f"dagrun.duration.{self.state}", **timer_params)

    @provide_session
    def verify_integrity(self, *, session: Session = NEW_SESSION) -> None:
        """
        Verify the DagRun by checking for removed tasks or tasks that are not in the database yet.

        It will set state to removed or add the task if required.

        :missing_indexes: A dictionary of task vs indexes that are missing.
        :param session: Sqlalchemy ORM Session
        """
        from airflow.settings import task_instance_mutation_hook

        # Set for the empty default in airflow.settings -- if it's not set this means it has been changed
        # Note: Literal[True, False] instead of bool because otherwise it doesn't correctly find the overload.
        hook_is_noop: Literal[True, False] = getattr(task_instance_mutation_hook, "is_noop", False)

        dag = self.get_dag()
        task_ids = self._check_for_removed_or_restored_tasks(
            dag, task_instance_mutation_hook, session=session
        )

        def task_filter(task: Operator) -> bool:
            return task.task_id not in task_ids and (
                self.run_type == DagRunType.BACKFILL_JOB
                or (task.start_date is None or task.start_date <= self.execution_date)
                and (task.end_date is None or self.execution_date <= task.end_date)
            )

        created_counts: dict[str, int] = defaultdict(int)
        task_creator = self._get_task_creator(created_counts, task_instance_mutation_hook, hook_is_noop)

        # Create the missing tasks, including mapped tasks
        tasks_to_create = (task for task in dag.task_dict.values() if task_filter(task))
        tis_to_create = self._create_tasks(tasks_to_create, task_creator, session=session)
        self._create_task_instances(self.dag_id, tis_to_create, created_counts, hook_is_noop, session=session)

    def _check_for_removed_or_restored_tasks(
        self, dag: DAG, ti_mutation_hook, *, session: Session
    ) -> set[str]:
        """
        Check for removed tasks/restored/missing tasks.

        :param dag: DAG object corresponding to the dagrun
        :param ti_mutation_hook: task_instance_mutation_hook function
        :param session: Sqlalchemy ORM Session

        :return: Task IDs in the DAG run

        """
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = set()
        for ti in tis:
            ti_mutation_hook(ti)
            task_ids.add(ti.task_id)
            try:
                task = dag.get_task(ti.task_id)

                should_restore_task = (task is not None) and ti.state == TaskInstanceState.REMOVED
                if should_restore_task:
                    self.log.info("Restoring task '%s' which was previously removed from DAG '%s'", ti, dag)
                    Stats.incr(f"task_restored_to_dag.{dag.dag_id}", tags=self.stats_tags)
                    # Same metric with tagging
                    Stats.incr("task_restored_to_dag", tags={**self.stats_tags, "dag_id": dag.dag_id})
                    ti.state = None
            except AirflowException:
                if ti.state == TaskInstanceState.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state != DagRunState.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '%s' for dag '%s'. Marking it as removed.", ti, dag)
                    Stats.incr(f"task_removed_from_dag.{dag.dag_id}", tags=self.stats_tags)
                    # Same metric with tagging
                    Stats.incr("task_removed_from_dag", tags={**self.stats_tags, "dag_id": dag.dag_id})
                    ti.state = TaskInstanceState.REMOVED
                continue

            try:
                num_mapped_tis = task.get_parse_time_mapped_ti_count()
            except NotMapped:
                continue
            except NotFullyPopulated:
                # What if it is _now_ dynamically mapped, but wasn't before?
                try:
                    total_length = task.get_mapped_ti_count(self.run_id, session=session)
                except NotFullyPopulated:
                    # Not all upstreams finished, so we can't tell what should be here. Remove everything.
                    if ti.map_index >= 0:
                        self.log.debug(
                            "Removing the unmapped TI '%s' as the mapping can't be resolved yet", ti
                        )
                        ti.state = TaskInstanceState.REMOVED
                    continue
                # Upstreams finished, check there aren't any extras
                if ti.map_index >= total_length:
                    self.log.debug(
                        "Removing task '%s' as the map_index is longer than the resolved mapping list (%d)",
                        ti,
                        total_length,
                    )
                    ti.state = TaskInstanceState.REMOVED
            else:
                # Check if the number of mapped literals has changed, and we need to mark this TI as removed.
                if ti.map_index >= num_mapped_tis:
                    self.log.debug(
                        "Removing task '%s' as the map_index is longer than the literal mapping list (%s)",
                        ti,
                        num_mapped_tis,
                    )
                    ti.state = TaskInstanceState.REMOVED
                elif ti.map_index < 0:
                    self.log.debug("Removing the unmapped TI '%s' as the mapping can now be performed", ti)
                    ti.state = TaskInstanceState.REMOVED

        return task_ids

    @overload
    def _get_task_creator(
        self,
        created_counts: dict[str, int],
        ti_mutation_hook: Callable,
        hook_is_noop: Literal[True],
    ) -> Callable[[Operator, Iterable[int]], Iterator[dict[str, Any]]]: ...

    @overload
    def _get_task_creator(
        self,
        created_counts: dict[str, int],
        ti_mutation_hook: Callable,
        hook_is_noop: Literal[False],
    ) -> Callable[[Operator, Iterable[int]], Iterator[TI]]: ...

    def _get_task_creator(
        self,
        created_counts: dict[str, int],
        ti_mutation_hook: Callable,
        hook_is_noop: Literal[True, False],
    ) -> Callable[[Operator, Iterable[int]], Iterator[dict[str, Any]] | Iterator[TI]]:
        """
        Get the task creator function.

        This function also updates the created_counts dictionary with the number of tasks created.

        :param created_counts: Dictionary of task_type -> count of created TIs
        :param ti_mutation_hook: task_instance_mutation_hook function
        :param hook_is_noop: Whether the task_instance_mutation_hook is a noop

        """
        if hook_is_noop:

            def create_ti_mapping(task: Operator, indexes: Iterable[int]) -> Iterator[dict[str, Any]]:
                created_counts[task.task_type] += 1
                for map_index in indexes:
                    yield TI.insert_mapping(self.run_id, task, map_index=map_index)

            creator = create_ti_mapping

        else:

            def create_ti(task: Operator, indexes: Iterable[int]) -> Iterator[TI]:
                for map_index in indexes:
                    ti = TI(task, run_id=self.run_id, map_index=map_index)
                    ti_mutation_hook(ti)
                    created_counts[ti.operator] += 1
                    yield ti

            creator = create_ti
        return creator

    def _create_tasks(
        self,
        tasks: Iterable[Operator],
        task_creator: Callable[[Operator, Iterable[int]], CreatedTasks],
        *,
        session: Session,
    ) -> CreatedTasks:
        """
        Create missing tasks -- and expand any MappedOperator that _only_ have literals as input.

        :param tasks: Tasks to create jobs for in the DAG run
        :param task_creator: Function to create task instances
        """
        map_indexes: Iterable[int]
        for task in tasks:
            try:
                count = task.get_mapped_ti_count(self.run_id, session=session)
            except (NotMapped, NotFullyPopulated):
                map_indexes = (-1,)
            else:
                if count:
                    map_indexes = range(count)
                else:
                    # Make sure to always create at least one ti; this will be
                    # marked as REMOVED later at runtime.
                    map_indexes = (-1,)
            yield from task_creator(task, map_indexes)

    def _create_task_instances(
        self,
        dag_id: str,
        tasks: Iterator[dict[str, Any]] | Iterator[TI],
        created_counts: dict[str, int],
        hook_is_noop: bool,
        *,
        session: Session,
    ) -> None:
        """
        Create the necessary task instances from the given tasks.

        :param dag_id: DAG ID associated with the dagrun
        :param tasks: the tasks to create the task instances from
        :param created_counts: a dictionary of number of tasks -> total ti created by the task creator
        :param hook_is_noop: whether the task_instance_mutation_hook is noop
        :param session: the session to use

        """
        # Fetch the information we need before handling the exception to avoid
        # PendingRollbackError due to the session being invalidated on exception
        # see https://github.com/apache/superset/pull/530
        run_id = self.run_id
        try:
            if hook_is_noop:
                session.bulk_insert_mappings(TI, tasks)
            else:
                session.bulk_save_objects(tasks)

            for task_type, count in created_counts.items():
                Stats.incr(f"task_instance_created_{task_type}", count, tags=self.stats_tags)
                # Same metric with tagging
                Stats.incr("task_instance_created", count, tags={**self.stats_tags, "task_type": task_type})
            session.flush()
        except IntegrityError:
            self.log.info(
                "Hit IntegrityError while creating the TIs for %s- %s",
                dag_id,
                run_id,
                exc_info=True,
            )
            self.log.info("Doing session rollback.")
            # TODO[HA]: We probably need to savepoint this so we can keep the transaction alive.
            session.rollback()

    def _revise_map_indexes_if_mapped(self, task: Operator, *, session: Session) -> Iterator[TI]:
        """
        Check if task increased or reduced in length and handle appropriately.

        Task instances that do not already exist are created and returned if
        possible. Expansion only happens if all upstreams are ready; otherwise
        we delay expansion to the "last resort". See comments at the call site
        for more details.
        """
        from airflow.settings import task_instance_mutation_hook

        try:
            total_length = task.get_mapped_ti_count(self.run_id, session=session)
        except NotMapped:
            return  # Not a mapped task, don't need to do anything.
        except NotFullyPopulated:
            return  # Upstreams not ready, don't need to revise this yet.

        query = session.scalars(
            select(TI.map_index).where(
                TI.dag_id == self.dag_id,
                TI.task_id == task.task_id,
                TI.run_id == self.run_id,
            )
        )
        existing_indexes = set(query)

        removed_indexes = existing_indexes.difference(range(total_length))
        if removed_indexes:
            session.execute(
                update(TI)
                .where(
                    TI.dag_id == self.dag_id,
                    TI.task_id == task.task_id,
                    TI.run_id == self.run_id,
                    TI.map_index.in_(removed_indexes),
                )
                .values(state=TaskInstanceState.REMOVED)
            )
            session.flush()

        for index in range(total_length):
            if index in existing_indexes:
                continue
            ti = TI(task, run_id=self.run_id, map_index=index, state=None)
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.refresh_from_task(task)
            session.flush()
            yield ti

    @classmethod
    @provide_session
    def get_latest_runs(cls, session: Session = NEW_SESSION) -> list[DagRun]:
        """Return the latest DagRun for each DAG."""
        subquery = (
            select(cls.dag_id, func.max(cls.execution_date).label("execution_date"))
            .group_by(cls.dag_id)
            .subquery()
        )
        return session.scalars(
            select(cls).join(
                subquery,
                and_(cls.dag_id == subquery.c.dag_id, cls.execution_date == subquery.c.execution_date),
            )
        ).all()

    @provide_session
    def schedule_tis(
        self,
        schedulable_tis: Iterable[TI],
        session: Session = NEW_SESSION,
        max_tis_per_query: int | None = None,
    ) -> int:
        """
        Set the given task instances in to the scheduled state.

        Each element of ``schedulable_tis`` should have its ``task`` attribute already set.

        Any EmptyOperator without callbacks or outlets is instead set straight to the success state.

        All the TIs should belong to this DagRun, but this code is in the hot-path, this is not checked -- it
        is the caller's responsibility to call this function only with TIs from a single dag run.
        """
        # Get list of TI IDs that do not need to executed, these are
        # tasks using EmptyOperator and without on_execute_callback / on_success_callback
        dummy_ti_ids = []
        schedulable_ti_ids = []
        for ti in schedulable_tis:
            if TYPE_CHECKING:
                assert ti.task
            if (
                ti.task.inherits_from_empty_operator
                and not ti.task.on_execute_callback
                and not ti.task.on_success_callback
                and not ti.task.outlets
            ):
                dummy_ti_ids.append((ti.task_id, ti.map_index))
            # check "start_trigger_args" to see whether the operator supports start execution from triggerer
            # if so, we'll then check "start_from_trigger" to see whether this feature is turned on and defer
            # this task.
            # if not, we'll add this "ti" into "schedulable_ti_ids" and later execute it to run in the worker
            elif ti.task.start_trigger_args is not None:
                context = ti.get_template_context()
                start_from_trigger = ti.task.expand_start_from_trigger(context=context, session=session)

                if start_from_trigger:
                    ti.start_date = timezone.utcnow()
                    if ti.state != TaskInstanceState.UP_FOR_RESCHEDULE:
                        ti.try_number += 1
                    ti.defer_task(exception=None, session=session)
                else:
                    schedulable_ti_ids.append((ti.task_id, ti.map_index))
            else:
                schedulable_ti_ids.append((ti.task_id, ti.map_index))

        count = 0

        if schedulable_ti_ids:
            schedulable_ti_ids_chunks = chunks(
                schedulable_ti_ids, max_tis_per_query or len(schedulable_ti_ids)
            )
            for schedulable_ti_ids_chunk in schedulable_ti_ids_chunks:
                count += session.execute(
                    update(TI)
                    .where(
                        TI.dag_id == self.dag_id,
                        TI.run_id == self.run_id,
                        tuple_in_condition((TI.task_id, TI.map_index), schedulable_ti_ids_chunk),
                    )
                    .values(
                        state=TaskInstanceState.SCHEDULED,
                        try_number=case(
                            (
                                or_(TI.state.is_(None), TI.state != TaskInstanceState.UP_FOR_RESCHEDULE),
                                TI.try_number + 1,
                            ),
                            else_=TI.try_number,
                        ),
                    )
                    .execution_options(synchronize_session=False)
                ).rowcount

        # Tasks using EmptyOperator should not be executed, mark them as success
        if dummy_ti_ids:
            dummy_ti_ids_chunks = chunks(dummy_ti_ids, max_tis_per_query or len(dummy_ti_ids))
            for dummy_ti_ids_chunk in dummy_ti_ids_chunks:
                count += session.execute(
                    update(TI)
                    .where(
                        TI.dag_id == self.dag_id,
                        TI.run_id == self.run_id,
                        tuple_in_condition((TI.task_id, TI.map_index), dummy_ti_ids_chunk),
                    )
                    .values(
                        state=TaskInstanceState.SUCCESS,
                        start_date=timezone.utcnow(),
                        end_date=timezone.utcnow(),
                        duration=0,
                        try_number=TI.try_number + 1,
                    )
                    .execution_options(
                        synchronize_session=False,
                    )
                ).rowcount

        return count

    @provide_session
    def get_log_template(self, *, session: Session = NEW_SESSION) -> LogTemplate | LogTemplatePydantic:
        return DagRun._get_log_template(log_template_id=self.log_template_id, session=session)

    @staticmethod
    @internal_api_call
    @provide_session
    def _get_log_template(
        log_template_id: int | None, session: Session = NEW_SESSION
    ) -> LogTemplate | LogTemplatePydantic:
        template: LogTemplate | None
        if log_template_id is None:  # DagRun created before LogTemplate introduction.
            template = session.scalar(select(LogTemplate).order_by(LogTemplate.id).limit(1))
        else:
            template = session.get(LogTemplate, log_template_id)
        if template is None:
            raise AirflowException(
                f"No log_template entry found for ID {log_template_id!r}. "
                f"Please make sure you set up the metadatabase correctly."
            )
        return template

    @staticmethod
    def _get_partial_task_ids(dag: DAG | None) -> list[str] | None:
        return dag.task_ids if dag and dag.partial else None


class DagRunNote(Base):
    """For storage of arbitrary notes concerning the dagrun instance."""

    __tablename__ = "dag_run_note"

    user_id = Column(String(128), nullable=True)
    dag_run_id = Column(Integer, primary_key=True, nullable=False)
    content = Column(String(1000).with_variant(Text(1000), "mysql"))
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    dag_run = relationship("DagRun", back_populates="dag_run_note")

    __table_args__ = (
        PrimaryKeyConstraint("dag_run_id", name="dag_run_note_pkey"),
        ForeignKeyConstraint(
            (dag_run_id,),
            ["dag_run.id"],
            name="dag_run_note_dr_fkey",
            ondelete="CASCADE",
        ),
    )

    def __init__(self, content, user_id=None):
        self.content = content
        self.user_id = user_id

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.dagrun_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + ">"
