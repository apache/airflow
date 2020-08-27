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
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union

from sqlalchemy import (
    Boolean, Column, DateTime, Index, Integer, PickleType, String, UniqueConstraint, and_, func, or_,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym
from sqlalchemy.orm.session import Session

from airflow.exceptions import AirflowException
from airflow.models.base import ID_LEN, Base
from airflow.models.taskinstance import TaskInstance as TI
from airflow.settings import task_instance_mutation_hook
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.types import DagRunType


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    run_type = Column(String(50), nullable=False)
    conf = Column(PickleType)

    dag = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

    def __init__(
        self,
        dag_id: Optional[str] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        external_trigger: Optional[bool] = None,
        conf: Optional[Any] = None,
        state: Optional[str] = None,
        run_type: Optional[str] = None
    ):
        self.dag_id = dag_id
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.external_trigger = external_trigger
        self.conf = conf or {}
        self.state = state
        self.run_type = run_type
        super().__init__()

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, '
            'externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger)

    def get_state(self):
        return self._state

    def set_state(self, state):
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished() else None

    @declared_attr
    def state(self):
        return synonym('_state', descriptor=property(self.get_state, self.set_state))

    @provide_session
    def refresh_from_db(self, session: Session = None):
        """
        Reloads the current dagrun from the database

        :param session: database session
        :type session: Session
        """
        DR = DagRun

        exec_date = func.cast(self.execution_date, DateTime)

        dr = session.query(DR).filter(
            DR.dag_id == self.dag_id,
            func.cast(DR.execution_date, DateTime) == exec_date,
            DR.run_id == self.run_id
        ).one()

        self.id = dr.id
        self.state = dr.state

    @staticmethod
    @provide_session
    def find(
        dag_id: Optional[Union[str, List[str]]] = None,
        run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        state: Optional[str] = None,
        external_trigger: Optional[bool] = None,
        no_backfills: Optional[bool] = False,
        run_type: Optional[DagRunType] = None,
        session: Session = None,
        execution_start_date: Optional[datetime] = None,
        execution_end_date: Optional[datetime] = None
    ) -> List["DagRun"]:
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id or list of dag_id to find dag runs for
        :type dag_id: str or list[str]
        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param run_type: type of DagRun
        :type run_type: airflow.utils.types.DagRunType
        :param execution_date: the execution date
        :type execution_date: datetime.datetime or list[datetime.datetime]
        :param state: the state of the dag run
        :type state: str
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param execution_start_date: dag run that was executed from this date
        :type execution_start_date: datetime.datetime
        :param execution_end_date: dag run that was executed until this date
        :type execution_end_date: datetime.datetime
        """
        DR = DagRun

        qry = session.query(DR)
        dag_ids = [dag_id] if isinstance(dag_id, str) else dag_id
        if dag_ids:
            qry = qry.filter(DR.dag_id.in_(dag_ids))
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if execution_start_date and execution_end_date:
            qry = qry.filter(DR.execution_date.between(execution_start_date, execution_end_date))
        elif execution_start_date:
            qry = qry.filter(DR.execution_date >= execution_start_date)
        elif execution_end_date:
            qry = qry.filter(DR.execution_date <= execution_end_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if run_type:
            qry = qry.filter(DR.run_type == run_type.value)
        if no_backfills:
            qry = qry.filter(DR.run_type != DagRunType.BACKFILL_JOB.value)

        dr = qry.order_by(DR.execution_date).all()

        return dr

    @staticmethod
    def generate_run_id(run_type: DagRunType, execution_date: datetime) -> str:
        """Generate Run ID based on Run Type and Execution Date"""
        return f"{run_type.value}__{execution_date.isoformat()}"

    @provide_session
    def get_task_instances(self, state=None, session=None):
        """
        Returns the task instances for this dag run
        """
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
        )

        if state:
            if isinstance(state, str):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TI.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(
                            or_(TI.state.in_(not_none_state),
                                TI.state.is_(None))
                        )
                else:
                    tis = tis.filter(TI.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))
        return tis.all()

    @provide_session
    def get_task_instance(self, task_id: str, session: Session = None):
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        :type task_id: str
        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
            TI.task_id == task_id
        ).first()

        return ti

    def get_dag(self):
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException("The DAG (.dag) for {} needs to be set".format(self))

        return self.dag

    @provide_session
    def get_previous_dagrun(self, state: Optional[str] = None, session: Session = None) -> Optional['DagRun']:
        """The previous DagRun, if there is one"""

        filters = [
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date,
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        return session.query(DagRun).filter(
            *filters
        ).order_by(
            DagRun.execution_date.desc()
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session: Session = None):
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, session: Session = None) -> List[TI]:
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :param session: Sqlalchemy ORM Session
        :type session: Session
        :return: ready_tis: the tis that can be scheduled in the current loop
        :rtype ready_tis: list[airflow.models.TaskInstance]
        """

        dag = self.get_dag()
        ready_tis: List[TI] = []
        tis = list(self.get_task_instances(session=session, state=State.task_states + (State.SHUTDOWN,)))
        self.log.debug("number of tis tasks for %s: %s task(s)", self, len(tis))
        for ti in tis:
            ti.task = dag.get_task(ti.task_id)

        start_dttm = timezone.utcnow()
        unfinished_tasks = [t for t in tis if t.state in State.unfinished()]
        finished_tasks = [t for t in tis if t.state in State.finished() + [State.UPSTREAM_FAILED]]
        none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)
        none_task_concurrency = all(t.task.task_concurrency is None
                                    for t in unfinished_tasks)
        if unfinished_tasks:
            scheduleable_tasks = [ut for ut in unfinished_tasks if ut.state in SCHEDULEABLE_STATES]
            self.log.debug(
                "number of scheduleable tasks for %s: %s task(s)",
                self, len(scheduleable_tasks))
            ready_tis, changed_tis = self._get_ready_tis(scheduleable_tasks, finished_tasks, session)
            self.log.debug("ready tis length for %s: %s task(s)", self, len(ready_tis))
            if none_depends_on_past and none_task_concurrency:
                # small speed up
                are_runnable_tasks = ready_tis or self._are_premature_tis(
                    unfinished_tasks, finished_tasks, session) or changed_tis

        duration = (timezone.utcnow() - start_dttm)
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        leaf_task_ids = {t.task_id for t in dag.leaves}
        leaf_tis = [ti for ti in tis if ti.task_id in leaf_task_ids]

        # if all roots finished and at least one failed, the run failed
        if not unfinished_tasks and any(
            leaf_ti.state in {State.FAILED, State.UPSTREAM_FAILED} for leaf_ti in leaf_tis
        ):
            self.log.error('Marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='task_failure', session=session)

        # if all leafs succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tasks and all(
            leaf_ti.state in {State.SUCCESS, State.SKIPPED} for leaf_ti in leaf_tis
        ):
            self.log.info('Marking run %s successful', self)
            self.set_state(State.SUCCESS)
            dag.handle_callback(self, success=True, reason='success', session=session)

        # if *all tasks* are deadlocked, the run failed
        elif (unfinished_tasks and none_depends_on_past and
              none_task_concurrency and not are_runnable_tasks):
            self.log.error('Deadlock; marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='all_tasks_deadlocked',
                                session=session)

        # finally, if the roots aren't done, the dag is still running
        else:
            self.set_state(State.RUNNING)

        self._emit_duration_stats_for_finished_state()

        # todo: determine we want to use with_for_update to make sure to lock the run
        session.merge(self)
        session.commit()

        return ready_tis

    def _get_ready_tis(
        self,
        scheduleable_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> Tuple[List[TI], bool]:
        old_states = {}
        ready_tis: List[TI] = []
        changed_tis = False

        if not scheduleable_tasks:
            return ready_tis, changed_tis

        # Check dependencies
        for st in scheduleable_tasks:
            old_state = st.state
            if st.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    finished_tasks=finished_tasks),
                    session=session):
                ready_tis.append(st)
            else:
                old_states[st.key] = old_state

        # Check if any ti changed state
        tis_filter = TI.filter_for_tis(old_states.keys())
        if tis_filter is not None:
            fresh_tis = session.query(TI).filter(tis_filter).all()
            changed_tis = any(ti.state != old_states[ti.key] for ti in fresh_tis)

        return ready_tis, changed_tis

    def _are_premature_tis(
        self,
        unfinished_tasks: List[TI],
        finished_tasks: List[TI],
        session: Session,
    ) -> bool:
        # there might be runnable tasks that are up for retry and for some reason(retry delay, etc) are
        # not ready yet so we set the flags to count them in
        for ut in unfinished_tasks:
            if ut.are_dependencies_met(
                dep_context=DepContext(
                    flag_upstream_failed=True,
                    ignore_in_retry_period=True,
                    ignore_in_reschedule_period=True,
                    finished_tasks=finished_tasks),
                    session=session):
                return True
        return False

    def _emit_duration_stats_for_finished_state(self):
        if self.state == State.RUNNING:
            return

        duration = (self.end_date - self.start_date)
        if self.state is State.SUCCESS:
            Stats.timing('dagrun.duration.success.{}'.format(self.dag_id), duration)
        elif self.state == State.FAILED:
            Stats.timing('dagrun.duration.failed.{}'.format(self.dag_id), duration)

    @provide_session
    def verify_integrity(self, session: Session = None):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.

        :param session: Sqlalchemy ORM Session
        :type session: Session
        """
        dag = self.get_dag()
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = set()
        for ti in tis:
            task_instance_mutation_hook(ti)
            task_ids.add(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state is not State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '%s' for dag '%s'. "
                                     "Marking it as removed.", ti, dag)
                    Stats.incr(
                        "task_removed_from_dag.{}".format(dag.dag_id), 1, 1)
                    ti.state = State.REMOVED

            should_restore_task = (task is not None) and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '%s' which was previously "
                              "removed from DAG '%s'", ti, dag)
                Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                ti.state = State.NONE
            session.merge(ti)

        # check for missing tasks
        for task in dag.task_dict.values():
            if task.start_date > self.execution_date and not self.is_backfill:
                continue

            if task.task_id not in task_ids:
                Stats.incr(
                    "task_instance_created-{}".format(task.__class__.__name__),
                    1, 1)
                ti = TI(task, self.execution_date)
                task_instance_mutation_hook(ti)
                session.add(ti)

        try:
            session.commit()
        except IntegrityError as err:
            self.log.info(str(err))
            self.log.info('Hit IntegrityError while creating the TIs for '
                          f'{dag.dag_id} - {self.execution_date}.')
            self.log.info('Doing session rollback.')
            session.rollback()

    @staticmethod
    def get_run(session: Session, dag_id: str, execution_date: datetime):
        """
        Get a single DAG Run

        :param session: Sqlalchemy ORM Session
        :type session: Session
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: DagRun corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: airflow.models.DagRun
        """
        qry = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.external_trigger == False,  # noqa pylint: disable=singleton-comparison
            DagRun.execution_date == execution_date,
        )
        return qry.first()

    @property
    def is_backfill(self):
        return self.run_type == DagRunType.BACKFILL_JOB.value

    @classmethod
    @provide_session
    def get_latest_runs(cls, session=None):
        """Returns the latest DagRun for each DAG"""
        subquery = (
            session
            .query(
                cls.dag_id,
                func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        dagruns = (
            session
            .query(cls)
            .join(subquery,
                  and_(cls.dag_id == subquery.c.dag_id,
                       cls.execution_date == subquery.c.execution_date))
            .all()
        )
        return dagruns
