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
"""DagRun module"""
from datetime import datetime
from typing import Any, Iterable, List, Optional, Union, cast

from sqlalchemy import (
    Boolean, Column, DateTime, Index, Integer, PickleType, String, UniqueConstraint, and_, func, or_,
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym
from sqlalchemy.orm.session import Session

from airflow.exceptions import AirflowException
from airflow.models.base import ID_LEN, Base
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    ID_PREFIX = 'scheduled__'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    id: int = Column(Integer, primary_key=True)
    dag_id: str = Column(String(ID_LEN))
    run_id: str = Column(String(ID_LEN))
    execution_date: Optional[datetime] = Column(UtcDateTime, default=timezone.utcnow)
    start_date: Optional[datetime] = Column(UtcDateTime, default=timezone.utcnow)
    end_date: Optional[datetime] = Column(UtcDateTime)
    _state: Optional[str] = Column('state', String(50), default=State.RUNNING)
    external_trigger: bool = Column(Boolean, default=True)
    conf: Optional[Any] = Column(PickleType)

    dag: Optional[DAG] = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

    def __init__(self,
                 dag_id: Optional[str] = None,
                 run_id: Optional[str] = None,
                 execution_date: Optional[datetime] = None,
                 start_date: Optional[datetime] = None,
                 external_trigger: Optional[bool] = None,
                 conf: Optional[Any] = None,
                 state: Optional[str] = None):
        if not dag_id:
            raise ValueError("Dag_id must be set")
        self.dag_id = dag_id
        if not run_id:
            raise ValueError("Run_id must be set")
        self.run_id = run_id
        self.execution_date = execution_date
        self.start_date = start_date
        self.external_trigger = external_trigger if external_trigger else True
        self.conf = conf
        self.state = state
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

    def get_state(self) -> Optional[str]:
        """Method to retrieve state"""
        return self._state

    def set_state(self, state: str) -> None:
        """Method to set state"""
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished() else None

    @declared_attr
    def state(self):
        """State property"""
        return synonym('_state',
                       descriptor=property(self.get_state, self.set_state))

    @classmethod
    def id_for_date(cls, date, prefix=ID_FORMAT_PREFIX) -> str:
        """Unique ID generated for date"""
        return prefix.format(date.isoformat()[:19])

    @provide_session
    def refresh_from_db(self, session: Session = None) -> None:
        """
        Reloads the current dagrun from the database

        :param session: database session
        """
        exec_date = func.cast(self.execution_date, DateTime)

        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            func.cast(DagRun.execution_date, DateTime) == exec_date,
            DagRun.run_id == self.run_id
        ).one()

        self.id = dr.id  # pylint: disable=invalid-name
        self.state = dr.state

    @staticmethod
    @provide_session
    def find(dag_id: Optional[str] = None,
             run_id: Optional[str] = None,
             execution_date: Optional[datetime] = None,
             state: Optional[str] = None,
             external_trigger: Optional[bool] = None,
             no_backfills: bool = False,
             session: Session = None) -> Iterable['DagRun']:
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id to find dag runs for
        :type dag_id: str
        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: str
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """

        qry = session.query(DagRun)
        if dag_id:
            qry = qry.filter(DagRun.dag_id == dag_id)
        if run_id:
            qry = qry.filter(DagRun.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                # noinspection PyUnresolvedReferences
                qry = qry.filter(DagRun.execution_date.in_(execution_date))  # pylint: disable=no-member
            else:
                qry = qry.filter(DagRun.execution_date == execution_date)
        if state:
            qry = qry.filter(DagRun.state == state)
        if external_trigger is not None:
            qry = qry.filter(DagRun.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            # noinspection PyUnresolvedReferences
            qry = qry.filter(DagRun.run_id.notlike(BackfillJob.ID_PREFIX + '%'))  # type: ignore

        dr = qry.order_by(DagRun.execution_date).all()

        return dr

    @provide_session
    def get_task_instances(self, state: Optional[Union[str, List[str]]] = None, session: Session = None) \
            -> List[TaskInstance]:
        """
        Returns the task instances for this dag run
        """
        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.execution_date == self.execution_date,
        )
        if state:
            if isinstance(state, str):
                tis = tis.filter(TaskInstance.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    # noinspection PyUnresolvedReferences
                    tis = tis.filter(
                        or_(TaskInstance.state.in_(state),  # pylint: disable=no-member
                            TaskInstance.state.is_(None))  # pylint: disable=no-member
                    )
                else:
                    # noinspection PyUnresolvedReferences
                    tis = tis.filter(TaskInstance.state.in_(state))  # pylint: disable=no-member

        if self.dag and self.dag.partial:
            # noinspection PyUnresolvedReferences
            tis = tis.filter(TaskInstance.task_id.in_(self.dag.task_ids))  # pylint: disable=no-member

        return tis.all()

    @provide_session
    def get_task_instance(self, task_id: str, session: Session = None) -> Optional[TaskInstance]:
        """
        Returns the task instance specified by task_id for this dag run

        :param session: DB Session
        :type session: sqlalchemy.orm.session.Session
        :param task_id: the task id
        :type task_id: str
        """

        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.task_id == task_id
        ).first()

        return ti

    def get_dag(self):
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException("The DAG (.dag) for {} needs to be set"
                                   .format(self))

        return self.dag

    @provide_session
    def get_previous_dagrun(self, state: Optional[str] = None, session: Session = None) -> Optional['DagRun']:
        """The previous DagRun, if there is one"""

        session = cast(Session, session)

        if not self.execution_date:
            return None

        filters = [
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date,  # type: ignore
        ]
        if state is not None:
            filters.append(DagRun.state == state)
        # noinspection PyUnresolvedReferences
        return session.query(DagRun).filter(
            *filters
        ).order_by(
            DagRun.execution_date.desc()  # type: ignore  # pylint: disable=no-member
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session: Session = None) -> Optional['DagRun']:
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, session: Session = None) -> str:
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :return: State
        """

        dag = self.get_dag()

        tis = self.get_task_instances(session=session)
        self.log.debug("Updating state for %s considering %s task(s)", self, len(tis))

        for ti in list(tis):
            # skip in db?
            if ti.state == State.REMOVED:
                tis.remove(ti)
            else:
                ti.task = dag.get_task(ti.task_id)

        # pre-calculate
        # db is faster
        start_dttm = timezone.utcnow()
        unfinished_tasks = self.get_task_instances(
            state=State.unfinished(),
            session=session
        )
        none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)
        none_task_concurrency = all(t.task.task_concurrency is None
                                    for t in unfinished_tasks)
        no_dependencies_met = True
        # small speed up
        if unfinished_tasks and none_depends_on_past and none_task_concurrency:
            # todo: this can actually get pretty slow: one task costs between 0.01-015s
            no_dependencies_met = True
            for unfinished_task in unfinished_tasks:
                # We need to flag upstream and check for changes because upstream
                # failures/re-schedules can result in deadlock false positives
                old_state = unfinished_task.state
                deps_met = unfinished_task.are_dependencies_met(
                    dep_context=DepContext(
                        flag_upstream_failed=True,
                        ignore_in_retry_period=True,
                        ignore_in_reschedule_period=True),
                    session=session)
                if deps_met or old_state != unfinished_task.current_state(session=session):
                    no_dependencies_met = False
                    break

        duration = (timezone.utcnow() - start_dttm)
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        leaf_tis = [ti for ti in tis if ti.task_id in {t.task_id for t in dag.leaves}]

        # if all roots finished and at least one failed, the run failed
        if not unfinished_tasks and any(
            leaf_ti.state in {State.FAILED, State.UPSTREAM_FAILED} for leaf_ti in leaf_tis
        ):
            self.log.info('Marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='task_failure',
                                session=session)

        # if all leafs succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tasks and all(
            leaf_ti.state in {State.SUCCESS, State.SKIPPED} for leaf_ti in leaf_tis
        ):
            self.log.info('Marking run %s successful', self)
            self.set_state(State.SUCCESS)
            dag.handle_callback(self, success=True, reason='success', session=session)

        # if *all tasks* are deadlocked, the run failed
        elif (unfinished_tasks and none_depends_on_past and
              none_task_concurrency and no_dependencies_met):
            self.log.info('Deadlock; marking run %s failed', self)
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

        return self.state

    def _emit_duration_stats_for_finished_state(self) -> None:
        if self.state == State.RUNNING:
            return

        if not self.end_date or not self.start_date:
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
        """
        dag = self.get_dag()
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = []
        for ti in tis:
            task_ids.append(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state is not State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '{}' for dag '{}'. "
                                     "Marking it as removed.".format(ti, dag))
                    Stats.incr(
                        "task_removed_from_dag.{}".format(dag.dag_id), 1, 1)
                    ti.state = State.REMOVED

            should_restore_task = (task is not None) and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '{}' which was previously "
                              "removed from DAG '{}'".format(ti, dag))
                Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                ti.state = State.NONE

        # check for missing tasks
        for task in dag.task_dict.values():
            if task.start_date > self.execution_date and not self.is_backfill:
                continue

            if task.task_id not in task_ids:
                Stats.incr(
                    "task_instance_created-{}".format(task.__class__.__name__),
                    1, 1)
                ti = TaskInstance(task, self.execution_date)
                session.add(ti)

        session.commit()

    @staticmethod
    def get_run(session: Session, dag_id: str, execution_date: datetime) -> 'DagRun':
        """
        Retrieves Dag run.

        :param session: DB Session
        :type session: sqlalchemy.orm.session.Session
        :param dag_id: DAG ID
        :type dag_id: str
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
    def is_backfill(self) -> bool:
        """Whether the run is a backfill run."""
        from airflow.jobs import BackfillJob
        return (
            self.run_id is not None and
            self.run_id.startswith(BackfillJob.ID_PREFIX)
        )

    @classmethod
    @provide_session
    def get_latest_runs(cls, session: Session = None) -> Iterable['DagRun']:
        """Returns the latest DagRun for each DAG. """
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
