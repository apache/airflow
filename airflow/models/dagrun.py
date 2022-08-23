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
import itertools
import os
import warnings
from collections import defaultdict
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    PickleType,
    String,
    UniqueConstraint,
    and_,
    func,
    or_,
    text,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import joinedload, relationship, synonym
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import false, select, true

from airflow import settings
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning, TaskNotFound
from airflow.models.base import Base, StringID
from airflow.models.mappedoperator import MappedOperator
from airflow.models.taskinstance import TaskInstance as TI
from airflow.models.tasklog import LogTemplate
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.utils import timezone
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, nulls_first, skip_locked, tuple_in_condition, with_row_locks
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import NOTSET, ArgNotSet, DagRunType

if TYPE_CHECKING:
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator


class TISchedulingDecision(NamedTuple):
    """Type of return for DagRun.task_instance_scheduling_decisions"""

    tis: List[TI]
    schedulable_tis: List[TI]
    changed_tis: bool
    unfinished_tis: List[TI]
    finished_tis: List[TI]


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """

    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(StringID(), nullable=False)
    queued_at = Column(UtcDateTime)
    execution_date = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.QUEUED)
    run_id = Column(StringID(), nullable=False)
    creating_job_id = Column(Integer)
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String(50), nullable=False)
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
        default=select([func.max(LogTemplate.__table__.c.id)]),
    )

    # Remove this `if` after upgrading Sphinx-AutoAPI
    if not TYPE_CHECKING and "BUILDING_AIRFLOW_DOCS" in os.environ:
        dag: "Optional[DAG]"
    else:
        dag: "Optional[DAG]" = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date', name='dag_run_dag_id_execution_date_key'),
        UniqueConstraint('dag_id', 'run_id', name='dag_run_dag_id_run_id_key'),
        Index('idx_last_scheduling_decision', last_scheduling_decision),
        Index('idx_dag_run_dag_id', dag_id),
        Index(
            'idx_dag_run_running_dags',
            'state',
            'dag_id',
            postgresql_where=text("state='running'"),
            mssql_where=text("state='running'"),
            sqlite_where=text("state='running'"),
        ),
        # since mysql lacks filtered/partial indices, this creates a
        # duplicate index on mysql. Not the end of the world
        Index(
            'idx_dag_run_queued_dags',
            'state',
            'dag_id',
            postgresql_where=text("state='queued'"),
            mssql_where=text("state='queued'"),
            sqlite_where=text("state='queued'"),
        ),
    )

    task_instances = relationship(
        TI, back_populates="dag_run", cascade='save-update, merge, delete, delete-orphan'
    )

    DEFAULT_DAGRUNS_TO_EXAMINE = airflow_conf.getint(
        'scheduler',
        'max_dagruns_per_loop_to_schedule',
        fallback=20,
    )

    def __init__(
        self,
        dag_id: Optional[str] = None,
        run_id: Optional[str] = None,
        queued_at: Union[datetime, None, ArgNotSet] = NOTSET,
        execution_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        external_trigger: Optional[bool] = None,
        conf: Optional[Any] = None,
        state: Optional[DagRunState] = None,
        run_type: Optional[str] = None,
        dag_hash: Optional[str] = None,
        creating_job_id: Optional[int] = None,
        data_interval: Optional[Tuple[datetime, datetime]] = None,
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
            self.queued_at = timezone.utcnow() if state == State.QUEUED else None
        else:
            self.queued_at = queued_at
        self.run_type = run_type
        self.dag_hash = dag_hash
        self.creating_job_id = creating_job_id
        super().__init__()

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, state:{state}, '
            'queued_at: {queued_at}. externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            state=self.state,
            queued_at=self.queued_at,
            external_trigger=self.external_trigger,
        )

    @property
    def logical_date(self) -> datetime:
        return self.execution_date

    def get_state(self):
        return self._state

    def set_state(self, state: DagRunState):
        if state not in State.dag_states:
            raise ValueError(f"invalid DagRun state: {state}")
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished else None
            if state == State.QUEUED:
                self.queued_at = timezone.utcnow()

    @declared_attr
    def state(self):
        return synonym('_state', descriptor=property(self.get_state, self.set_state))

    @provide_session
    def refresh_from_db(self, session: Session = NEW_SESSION) -> None:
        """
        Reloads the current dagrun from the database

        :param session: database session
        """
        dr = session.query(DagRun).filter(DagRun.dag_id == self.dag_id, DagRun.run_id == self.run_id).one()
        self.id = dr.id
        self.state = dr.state

    @classmethod
    @provide_session
    def active_runs_of_dags(cls, dag_ids=None, only_running=False, session=None) -> Dict[str, int]:
        """Get the number of active dag runs for each dag."""
        query = session.query(cls.dag_id, func.count('*'))
        if dag_ids is not None:
            # 'set' called to avoid duplicate dag_ids, but converted back to 'list'
            # because SQLAlchemy doesn't accept a set here.
            query = query.filter(cls.dag_id.in_(list(set(dag_ids))))
        if only_running:
            query = query.filter(cls.state == State.RUNNING)
        else:
            query = query.filter(cls.state.in_([State.RUNNING, State.QUEUED]))
        query = query.group_by(cls.dag_id)
        return {dag_id: count for dag_id, count in query.all()}

    @classmethod
    def next_dagruns_to_examine(
        cls,
        state: DagRunState,
        session: Session,
        max_number: Optional[int] = None,
    ):
        """
        Return the next DagRuns that the scheduler should attempt to schedule.

        This will return zero or more DagRun rows that are row-level-locked with a "SELECT ... FOR UPDATE"
        query, you should ensure that any scheduling decisions are made in a single transaction -- as soon as
        the transaction is committed it will be unlocked.

        :rtype: list[airflow.models.DagRun]
        """
        from airflow.models.dag import DagModel

        if max_number is None:
            max_number = cls.DEFAULT_DAGRUNS_TO_EXAMINE

        # TODO: Bake this query, it is run _A lot_
        query = (
            session.query(cls)
            .filter(cls.state == state, cls.run_type != DagRunType.BACKFILL_JOB)
            .join(DagModel, DagModel.dag_id == cls.dag_id)
            .filter(DagModel.is_paused == false(), DagModel.is_active == true())
        )
        if state == State.QUEUED:
            # For dag runs in the queued state, we check if they have reached the max_active_runs limit
            # and if so we drop them
            running_drs = (
                session.query(DagRun.dag_id, func.count(DagRun.state).label('num_running'))
                .filter(DagRun.state == DagRunState.RUNNING)
                .group_by(DagRun.dag_id)
                .subquery()
            )
            query = query.outerjoin(running_drs, running_drs.c.dag_id == DagRun.dag_id).filter(
                func.coalesce(running_drs.c.num_running, 0) < DagModel.max_active_runs
            )
        query = query.order_by(
            nulls_first(cls.last_scheduling_decision, session=session),
            cls.execution_date,
        )

        if not settings.ALLOW_FUTURE_EXEC_DATES:
            query = query.filter(DagRun.execution_date <= func.now())

        return with_row_locks(
            query.limit(max_number), of=cls, session=session, **skip_locked(session=session)
        )

    @classmethod
    @provide_session
    def find(
        cls,
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[Iterable[str]] = None,
        execution_date: Optional[Union[datetime, Iterable[datetime]]] = None,
        state: Optional[DagRunState] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: bool = False,
        run_type: Optional[DagRunType] = None,
        session: Session = NEW_SESSION,
        execution_start_date: Optional[datetime] = None,
        execution_end_date: Optional[datetime] = None,
    ) -> List["DagRun"]:
        """
        Returns a set of dag runs for the given search criteria.

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
        qry = session.query(cls)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(cls.dag_id.in_(dag_ids))

        if is_container(run_id):
            qry = qry.filter(cls.run_id.in_(run_id))
        elif run_id is not None:
            qry = qry.filter(cls.run_id == run_id)
        if is_container(execution_date):
            qry = qry.filter(cls.execution_date.in_(execution_date))
        elif execution_date is not None:
            qry = qry.filter(cls.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(cls.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(cls.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(cls.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(cls.state == state)
        if external_trigger is not None:
            qry = qry.filter(cls.external_trigger == external_trigger)
        if run_type:
            qry = qry.filter(cls.run_type == run_type)
        if no_backfills:
            qry = qry.filter(cls.run_type != DagRunType.BACKFILL_JOB)

        return qry.order_by(cls.execution_date).all()

    @classmethod
    @provide_session
    def find_duplicate(
        cls,
        dag_id: str,
        run_id: str,
        execution_date: datetime,
        session: Session = NEW_SESSION,
    ) -> Optional['DagRun']:
        """
        Return an existing run for the DAG with a specific run_id or execution_date.

        *None* is returned if no such DAG run is found.

        :param dag_id: the dag_id to find duplicates for
        :param run_id: defines the run id for this dag run
        :param execution_date: the execution date
        :param session: database session
        """
        return (
            session.query(cls)
            .filter(
                cls.dag_id == dag_id,
                or_(cls.run_id == run_id, cls.execution_date == execution_date),
            )
            .one_or_none()
        )

    @staticmethod
    def generate_run_id(run_type: DagRunType, execution_date: datetime) -> str:
        """Generate Run ID based on Run Type and Execution Date"""
        # _Ensure_ run_type is a DagRunType, not just a string from user code
        return DagRunType(run_type).generate_run_id(execution_date)

    @provide_session
    def get_task_instances(
        self,
        state: Optional[Iterable[Optional[TaskInstanceState]]] = None,
        session: Session = NEW_SESSION,
    ) -> List[TI]:
        """Returns the task instances for this dag run"""
        tis = (
            session.query(TI)
            .options(joinedload(TI.dag_run))
            .filter(
                TI.dag_id == self.dag_id,
                TI.run_id == self.run_id,
            )
        )

        if state:
            if isinstance(state, str):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if State.NONE in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TI.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(or_(TI.state.in_(not_none_state), TI.state.is_(None)))
                else:
                    tis = tis.filter(TI.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))
        return tis.all()

    @provide_session
    def get_task_instance(
        self,
        task_id: str,
        session: Session = NEW_SESSION,
        *,
        map_index: int = -1,
    ) -> Optional[TI]:
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        :param session: Sqlalchemy ORM Session
        """
        return (
            session.query(TI)
            .filter_by(dag_id=self.dag_id, run_id=self.run_id, task_id=task_id, map_index=map_index)
            .one_or_none()
        )

    def get_dag(self) -> "DAG":
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException(f"The DAG (.dag) for {self} needs to be set")

        return self.dag

    @provide_session
    def get_previous_dagrun(
        self, state: Optional[DagRunState] = None, session: Session = NEW_SESSION
    ) -> Optional['DagRun']:
        """The previous DagRun, if there is one"""
        filters = [
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date,
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        return session.query(DagRun).filter(*filters).order_by(DagRun.execution_date.desc()).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session: Session = NEW_SESSION) -> Optional['DagRun']:
        """The previous, SCHEDULED DagRun, if there is one"""
        return (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date < self.execution_date,
                DagRun.run_type != DagRunType.MANUAL,
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )

    @provide_session
    def update_state(
        self, session: Session = NEW_SESSION, execute_callbacks: bool = True
    ) -> Tuple[List[TI], Optional[DagCallbackRequest]]:
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :param session: Sqlalchemy ORM Session
        :param execute_callbacks: Should dag callbacks (success/failure, SLA etc) be invoked
            directly (default: true) or recorded as a pending request in the ``callback`` property
        :return: Tuple containing tis that can be scheduled in the current loop & `callback` that
            needs to be executed
        """
        # Callback to execute in case of Task Failures
        callback: Optional[DagCallbackRequest] = None

        start_dttm = timezone.utcnow()
        self.last_scheduling_decision = start_dttm
        with Stats.timer(f"dagrun.dependency-check.{self.dag_id}"):
            dag = self.get_dag()
            info = self.task_instance_scheduling_decisions(session)

            tis = info.tis
            schedulable_tis = info.schedulable_tis
            changed_tis = info.changed_tis
            finished_tis = info.finished_tis
            unfinished_tis = info.unfinished_tis

            none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tis)
            none_task_concurrency = all(t.task.max_active_tis_per_dag is None for t in unfinished_tis)
            none_deferred = all(t.state != State.DEFERRED for t in unfinished_tis)

            if unfinished_tis and none_depends_on_past and none_task_concurrency and none_deferred:
                # small speed up
                are_runnable_tasks = (
                    schedulable_tis
                    or self._are_premature_tis(unfinished_tis, finished_tis, session)
                    or changed_tis
                )

        leaf_task_ids = {t.task_id for t in dag.leaves}
        leaf_tis = [ti for ti in tis if ti.task_id in leaf_task_ids if ti.state != TaskInstanceState.REMOVED]

        # if all roots finished and at least one failed, the run failed
        if not unfinished_tis and any(leaf_ti.state in State.failed_states for leaf_ti in leaf_tis):
            self.log.error('Marking run %s failed', self)
            self.set_state(DagRunState.FAILED)
            if execute_callbacks:
                dag.handle_callback(self, success=False, reason='task_failure', session=session)
            elif dag.has_on_failure_callback:
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=True,
                    msg='task_failure',
                )

        # if all leaves succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tis and all(leaf_ti.state in State.success_states for leaf_ti in leaf_tis):
            self.log.info('Marking run %s successful', self)
            self.set_state(DagRunState.SUCCESS)
            if execute_callbacks:
                dag.handle_callback(self, success=True, reason='success', session=session)
            elif dag.has_on_success_callback:
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=False,
                    msg='success',
                )

        # if *all tasks* are deadlocked, the run failed
        elif (
            unfinished_tis
            and none_depends_on_past
            and none_task_concurrency
            and none_deferred
            and not are_runnable_tasks
        ):
            self.log.error('Deadlock; marking run %s failed', self)
            self.set_state(DagRunState.FAILED)
            if execute_callbacks:
                dag.handle_callback(self, success=False, reason='all_tasks_deadlocked', session=session)
            elif dag.has_on_failure_callback:
                callback = DagCallbackRequest(
                    full_filepath=dag.fileloc,
                    dag_id=self.dag_id,
                    run_id=self.run_id,
                    is_failure_callback=True,
                    msg='all_tasks_deadlocked',
                )

        # finally, if the roots aren't done, the dag is still running
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
                (self.end_date - self.start_date).total_seconds()
                if self.start_date and self.end_date
                else None,
                self._state,
                self.external_trigger,
                self.run_type,
                self.data_interval_start,
                self.data_interval_end,
                self.dag_hash,
            )
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

        def _filter_tis_and_exclude_removed(dag: "DAG", tis: List[TI]) -> Iterable[TI]:
            """Populate ``ti.task`` while excluding those missing one, marking them as REMOVED."""
            for ti in tis:
                try:
                    ti.task = dag.get_task(ti.task_id)
                except TaskNotFound:
                    if ti.state != State.REMOVED:
                        self.log.error("Failed to get task for ti %s. Marking it as removed.", ti)
                        ti.state = State.REMOVED
                        session.flush()
                else:
                    yield ti

        tis = list(_filter_tis_and_exclude_removed(self.get_dag(), tis))
        missing_indexes = self._revise_mapped_task_indexes(tis, session=session)
        if missing_indexes:
            self.verify_integrity(missing_indexes=missing_indexes, session=session)

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

            # During expansion we may change some tis into non-schedulable
            # states, so we need to re-compute.
            if expansion_happened:
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

    def _get_ready_tis(
        self,
        schedulable_tis: List[TI],
        finished_tis: List[TI],
        session: Session,
    ) -> Tuple[List[TI], bool, bool]:
        old_states = {}
        ready_tis: List[TI] = []
        changed_tis = False

        if not schedulable_tis:
            return ready_tis, changed_tis, False

        # If we expand TIs, we need a new list so that we iterate over them too. (We can't alter
        # `schedulable_tis` in place and have the `for` loop pick them up
        additional_tis: List[TI] = []
        dep_context = DepContext(
            flag_upstream_failed=True,
            ignore_unmapped_tasks=True,  # Ignore this Dep, as we will expand it if we can.
            finished_tis=finished_tis,
        )

        # Check dependencies.
        expansion_happened = False
        for schedulable in itertools.chain(schedulable_tis, additional_tis):
            old_state = schedulable.state
            if not schedulable.are_dependencies_met(session=session, dep_context=dep_context):
                old_states[schedulable.key] = old_state
                continue
            # If schedulable is from a mapped task, but not yet expanded, do it
            # now. This is called in two places: First and ideally in the mini
            # scheduler at the end of LocalTaskJob, and then as an "expansion of
            # last resort" in the scheduler to ensure that the mapped task is
            # correctly expanded before executed.
            if schedulable.map_index < 0 and isinstance(schedulable.task, MappedOperator):
                expanded_tis, _ = schedulable.task.expand_mapped_task(self.run_id, session=session)
                if expanded_tis:
                    assert expanded_tis[0] is schedulable
                    additional_tis.extend(expanded_tis[1:])
                expansion_happened = True
            if schedulable.state in SCHEDULEABLE_STATES:
                ready_tis.append(schedulable)

        # Check if any ti changed state
        tis_filter = TI.filter_for_tis(old_states)
        if tis_filter is not None:
            fresh_tis = session.query(TI).filter(tis_filter).all()
            changed_tis = any(ti.state != old_states[ti.key] for ti in fresh_tis)

        return ready_tis, changed_tis, expansion_happened

    def _are_premature_tis(
        self,
        unfinished_tis: List[TI],
        finished_tis: List[TI],
        session: Session,
    ) -> bool:
        # there might be runnable tasks that are up for retry and for some reason(retry delay, etc) are
        # not ready yet so we set the flags to count them in
        for ut in unfinished_tis:
            if ut.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    ignore_in_retry_period=True,
                    ignore_in_reschedule_period=True,
                    finished_tis=finished_tis,
                ),
                session=session,
            ):
                return True
        return False

    def _emit_true_scheduling_delay_stats_for_finished_state(self, finished_tis: List[TI]) -> None:
        """
        This is a helper method to emit the true scheduling delay stats, which is defined as
        the time when the first task in DAG starts minus the expected DAG run datetime.
        This method will be used in the update_state method when the state of the DagRun
        is updated to a completed status (either success or failure). The method will find the first
        started task within the DAG and calculate the expected DagRun start time (based on
        dag.execution_date & dag.timetable), and minus these two values to get the delay.
        The emitted data may contains outlier (e.g. when the first task was cleared, so
        the second task's start_date will be used), but we can get rid of the outliers
        on the stats side through the dashboards tooling built.
        Note, the stat will only be emitted if the DagRun is a scheduler triggered one
        (i.e. external_trigger is False).
        """
        if self.state == State.RUNNING:
            return
        if self.external_trigger:
            return
        if not finished_tis:
            return

        try:
            dag = self.get_dag()

            if not dag.timetable.periodic:
                # We can't emit this metric if there is no following schedule to calculate from!
                return

            ordered_tis_by_start_date = [ti for ti in finished_tis if ti.start_date]
            ordered_tis_by_start_date.sort(key=lambda ti: ti.start_date, reverse=False)
            first_start_date = ordered_tis_by_start_date[0].start_date
            if first_start_date:
                # TODO: Logically, this should be DagRunInfo.run_after, but the
                # information is not stored on a DagRun, only before the actual
                # execution on DagModel.next_dagrun_create_after. We should add
                # a field on DagRun for this instead of relying on the run
                # always happening immediately after the data interval.
                data_interval_end = dag.get_run_data_interval(self).end
                true_delay = first_start_date - data_interval_end
                if true_delay.total_seconds() > 0:
                    Stats.timing(f'dagrun.{dag.dag_id}.first_task_scheduling_delay', true_delay)
        except Exception:
            self.log.warning('Failed to record first_task_scheduling_delay metric:', exc_info=True)

    def _emit_duration_stats_for_finished_state(self):
        if self.state == State.RUNNING:
            return
        if self.start_date is None:
            self.log.warning('Failed to record duration of %s: start_date is not set.', self)
            return
        if self.end_date is None:
            self.log.warning('Failed to record duration of %s: end_date is not set.', self)
            return

        duration = self.end_date - self.start_date
        if self.state == State.SUCCESS:
            Stats.timing(f'dagrun.duration.success.{self.dag_id}', duration)
        elif self.state == State.FAILED:
            Stats.timing(f'dagrun.duration.failed.{self.dag_id}', duration)

    @provide_session
    def verify_integrity(
        self,
        *,
        missing_indexes: Optional[Dict["MappedOperator", Sequence[int]]] = None,
        session: Session = NEW_SESSION,
    ):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.

        :missing_indexes: A dictionary of task vs indexes that are missing.
        :param session: Sqlalchemy ORM Session
        """
        from airflow.settings import task_instance_mutation_hook

        # Set for the empty default in airflow.settings -- if it's not set this means it has been changed
        hook_is_noop = getattr(task_instance_mutation_hook, 'is_noop', False)

        dag = self.get_dag()
        task_ids: Set[str] = set()
        if missing_indexes:
            tis = self.get_task_instances(session=session)
            for ti in tis:
                task_instance_mutation_hook(ti)
                task_ids.add(ti.task_id)
        else:
            task_ids, missing_indexes = self._check_for_removed_or_restored_tasks(
                dag, task_instance_mutation_hook, session=session
            )

        def task_filter(task: "Operator") -> bool:
            return task.task_id not in task_ids and (
                self.is_backfill
                or task.start_date <= self.execution_date
                and (task.end_date is None or self.execution_date <= task.end_date)
            )

        created_counts: Dict[str, int] = defaultdict(int)

        # Get task creator function
        task_creator = self._get_task_creator(created_counts, task_instance_mutation_hook, hook_is_noop)

        # Create the missing tasks, including mapped tasks
        tasks = self._create_missing_tasks(dag, task_creator, task_filter, missing_indexes, session=session)

        self._create_task_instances(dag.dag_id, tasks, created_counts, hook_is_noop, session=session)

    def _check_for_removed_or_restored_tasks(
        self, dag: "DAG", ti_mutation_hook, *, session: Session
    ) -> Tuple[Set[str], Dict["MappedOperator", Sequence[int]]]:
        """
        Check for removed tasks/restored/missing tasks.

        :param dag: DAG object corresponding to the dagrun
        :param ti_mutation_hook: task_instance_mutation_hook function
        :param session: Sqlalchemy ORM Session

        :return: List of task_ids in the dagrun and missing task indexes

        """
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = set()
        existing_indexes: Dict["MappedOperator", List[int]] = defaultdict(list)
        expected_indexes: Dict["MappedOperator", Sequence[int]] = defaultdict(list)
        for ti in tis:
            ti_mutation_hook(ti)
            task_ids.add(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)

                should_restore_task = (task is not None) and ti.state == State.REMOVED
                if should_restore_task:
                    self.log.info("Restoring task '%s' which was previously removed from DAG '%s'", ti, dag)
                    Stats.incr(f"task_restored_to_dag.{dag.dag_id}", 1, 1)
                    ti.state = State.NONE
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state != State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '%s' for dag '%s'. Marking it as removed.", ti, dag)
                    Stats.incr(f"task_removed_from_dag.{dag.dag_id}", 1, 1)
                    ti.state = State.REMOVED
                continue

            if not task.is_mapped:
                continue
            task = cast("MappedOperator", task)
            num_mapped_tis = task.parse_time_mapped_ti_count
            # Check if the number of mapped literals has changed and we need to mark this TI as removed
            if num_mapped_tis is not None:
                if ti.map_index >= num_mapped_tis:
                    self.log.debug(
                        "Removing task '%s' as the map_index is longer than the literal mapping list (%s)",
                        ti,
                        num_mapped_tis,
                    )
                    ti.state = State.REMOVED
                elif ti.map_index < 0:
                    self.log.debug("Removing the unmapped TI '%s' as the mapping can now be performed", ti)
                    ti.state = State.REMOVED
                else:
                    self.log.info("Restoring mapped task '%s'", ti)
                    Stats.incr(f"task_restored_to_dag.{dag.dag_id}", 1, 1)
                    existing_indexes[task].append(ti.map_index)
                    expected_indexes[task] = range(num_mapped_tis)
            else:
                #  What if it is _now_ dynamically mapped, but wasn't before?
                total_length = task.run_time_mapped_ti_count(self.run_id, session=session)

                if total_length is None:
                    # Not all upstreams finished, so we can't tell what should be here. Remove everything.
                    if ti.map_index >= 0:
                        self.log.debug(
                            "Removing the unmapped TI '%s' as the mapping can't be resolved yet", ti
                        )
                        ti.state = State.REMOVED
                    continue
                # Upstreams finished, check there aren't any extras
                if ti.map_index >= total_length:
                    self.log.debug(
                        "Removing task '%s' as the map_index is longer than the resolved mapping list (%d)",
                        ti,
                        total_length,
                    )
                    ti.state = State.REMOVED
                else:
                    self.log.info("Restoring mapped task '%s'", ti)
                    Stats.incr(f"task_restored_to_dag.{dag.dag_id}", 1, 1)
                    existing_indexes[task].append(ti.map_index)
                    expected_indexes[task] = range(total_length)
        # Check if we have some missing indexes to create ti for
        missing_indexes: Dict["MappedOperator", Sequence[int]] = defaultdict(list)
        for k, v in existing_indexes.items():
            missing_indexes.update({k: list(set(expected_indexes[k]).difference(v))})
        return task_ids, missing_indexes

    def _get_task_creator(
        self, created_counts: Dict[str, int], ti_mutation_hook: Callable, hook_is_noop: bool
    ) -> Callable:
        """
        Get the task creator function.

        This function also updates the created_counts dictionary with the number of tasks created.

        :param created_counts: Dictionary of task_type -> count of created TIs
        :param ti_mutation_hook: task_instance_mutation_hook function
        :param hook_is_noop: Whether the task_instance_mutation_hook is a noop

        """
        if hook_is_noop:

            def create_ti_mapping(task: "Operator", indexes: Tuple[int, ...]) -> Generator:
                created_counts[task.task_type] += 1
                for map_index in indexes:
                    yield TI.insert_mapping(self.run_id, task, map_index=map_index)

            creator = create_ti_mapping

        else:

            def create_ti(task: "Operator", indexes: Tuple[int, ...]) -> Generator:
                for map_index in indexes:
                    ti = TI(task, run_id=self.run_id, map_index=map_index)
                    ti_mutation_hook(ti)
                    created_counts[ti.operator] += 1
                    yield ti

            creator = create_ti
        return creator

    def _create_missing_tasks(
        self,
        dag: "DAG",
        task_creator: Callable,
        task_filter: Callable,
        missing_indexes: Optional[Dict["MappedOperator", Sequence[int]]],
        *,
        session: Session,
    ) -> Iterable["Operator"]:
        """
        Create missing tasks -- and expand any MappedOperator that _only_ have literals as input

        :param dag: DAG object corresponding to the dagrun
        :param task_creator: a function that creates tasks
        :param task_filter: a function that filters tasks to create
        :param session: the session to use
        """

        def expand_mapped_literals(
            task: "Operator", sequence: Union[Sequence[int], None] = None
        ) -> Tuple["Operator", Sequence[int]]:
            if not task.is_mapped:
                return (task, (-1,))
            task = cast("MappedOperator", task)
            count = task.parse_time_mapped_ti_count or task.run_time_mapped_ti_count(
                self.run_id, session=session
            )
            if not count:
                return (task, (-1,))
            if sequence:
                return (task, sequence)
            return (task, range(count))

        tasks_and_map_idxs = map(expand_mapped_literals, filter(task_filter, dag.task_dict.values()))

        tasks = itertools.chain.from_iterable(itertools.starmap(task_creator, tasks_and_map_idxs))
        if missing_indexes:
            # If there are missing indexes, override the tasks to create
            new_tasks_and_map_idxs = itertools.starmap(
                expand_mapped_literals, [(k, v) for k, v in missing_indexes.items() if len(v) > 0]
            )
            tasks = itertools.chain.from_iterable(itertools.starmap(task_creator, new_tasks_and_map_idxs))
        return tasks

    def _create_task_instances(
        self,
        dag_id: str,
        tasks: Iterable["Operator"],
        created_counts: Dict[str, int],
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
                Stats.incr(f"task_instance_created-{task_type}", count)
            session.flush()
        except IntegrityError:
            self.log.info(
                'Hit IntegrityError while creating the TIs for %s- %s',
                dag_id,
                run_id,
                exc_info=True,
            )
            self.log.info('Doing session rollback.')
            # TODO[HA]: We probably need to savepoint this so we can keep the transaction alive.
            session.rollback()

    def _revise_mapped_task_indexes(
        self,
        tis: Iterable[TI],
        *,
        session: Session,
    ) -> Dict["MappedOperator", Sequence[int]]:
        """Check if the length of the mapped task instances changed at runtime and find the missing indexes.

        :param tis: Task instances to check
        :param session: The session to use
        """
        from airflow.models.mappedoperator import MappedOperator

        existing_indexes: Dict[MappedOperator, List[int]] = defaultdict(list)
        new_indexes: Dict[MappedOperator, Sequence[int]] = defaultdict(list)
        for ti in tis:
            task = ti.task
            if not isinstance(task, MappedOperator):
                continue
            # skip unexpanded tasks and also tasks that expands with literal arguments
            if ti.map_index < 0 or task.parse_time_mapped_ti_count:
                continue
            existing_indexes[task].append(ti.map_index)
            task.run_time_mapped_ti_count.cache_clear()  # type: ignore[attr-defined]
            new_length = task.run_time_mapped_ti_count(self.run_id, session=session) or 0

            if ti.map_index >= new_length:
                self.log.debug(
                    "Removing task '%s' as the map_index is longer than the resolved mapping list (%d)",
                    ti,
                    new_length,
                )
                ti.state = State.REMOVED
            new_indexes[task] = range(new_length)
        missing_indexes: Dict[MappedOperator, Sequence[int]] = defaultdict(list)
        for k, v in existing_indexes.items():
            missing_indexes.update({k: list(set(new_indexes[k]).difference(v))})
        return missing_indexes

    @staticmethod
    def get_run(session: Session, dag_id: str, execution_date: datetime) -> Optional['DagRun']:
        """
        Get a single DAG Run

        :meta private:
        :param session: Sqlalchemy ORM Session
        :param dag_id: DAG ID
        :param execution_date: execution date
        :return: DagRun corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.DagRun
        """
        warnings.warn(
            "This method is deprecated. Please use SQLAlchemy directly",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.external_trigger == False,  # noqa
                DagRun.execution_date == execution_date,
            )
            .first()
        )

    @property
    def is_backfill(self) -> bool:
        return self.run_type == DagRunType.BACKFILL_JOB

    @classmethod
    @provide_session
    def get_latest_runs(cls, session=None) -> List['DagRun']:
        """Returns the latest DagRun for each DAG"""
        subquery = (
            session.query(cls.dag_id, func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        return (
            session.query(cls)
            .join(
                subquery,
                and_(cls.dag_id == subquery.c.dag_id, cls.execution_date == subquery.c.execution_date),
            )
            .all()
        )

    @provide_session
    def schedule_tis(self, schedulable_tis: Iterable[TI], session: Session = NEW_SESSION) -> int:
        """
        Set the given task instances in to the scheduled state.

        Each element of ``schedulable_tis`` should have it's ``task`` attribute already set.

        Any EmptyOperator without callbacks is instead set straight to the success state.

        All the TIs should belong to this DagRun, but this code is in the hot-path, this is not checked -- it
        is the caller's responsibility to call this function only with TIs from a single dag run.
        """
        # Get list of TI IDs that do not need to executed, these are
        # tasks using EmptyOperator and without on_execute_callback / on_success_callback
        dummy_ti_ids = []
        schedulable_ti_ids = []
        for ti in schedulable_tis:
            if (
                ti.task.inherits_from_empty_operator
                and not ti.task.on_execute_callback
                and not ti.task.on_success_callback
            ):
                dummy_ti_ids.append(ti.task_id)
            else:
                schedulable_ti_ids.append((ti.task_id, ti.map_index))

        count = 0

        if schedulable_ti_ids:
            count += (
                session.query(TI)
                .filter(
                    TI.dag_id == self.dag_id,
                    TI.run_id == self.run_id,
                    tuple_in_condition((TI.task_id, TI.map_index), schedulable_ti_ids),
                )
                .update({TI.state: State.SCHEDULED}, synchronize_session=False)
            )

        # Tasks using EmptyOperator should not be executed, mark them as success
        if dummy_ti_ids:
            count += (
                session.query(TI)
                .filter(
                    TI.dag_id == self.dag_id,
                    TI.run_id == self.run_id,
                    TI.task_id.in_(dummy_ti_ids),
                )
                .update(
                    {
                        TI.state: State.SUCCESS,
                        TI.start_date: timezone.utcnow(),
                        TI.end_date: timezone.utcnow(),
                        TI.duration: 0,
                    },
                    synchronize_session=False,
                )
            )

        return count

    @provide_session
    def get_log_template(self, *, session: Session = NEW_SESSION) -> LogTemplate:
        if self.log_template_id is None:  # DagRun created before LogTemplate introduction.
            template = session.query(LogTemplate).order_by(LogTemplate.id).first()
        else:
            template = session.query(LogTemplate).get(self.log_template_id)
        if template is None:
            raise AirflowException(
                f"No log_template entry found for ID {self.log_template_id!r}. "
                f"Please make sure you set up the metadatabase correctly."
            )
        return template

    @provide_session
    def get_log_filename_template(self, *, session: Session = NEW_SESSION) -> str:
        warnings.warn(
            "This method is deprecated. Please use get_log_template instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.get_log_template(session=session).filename
