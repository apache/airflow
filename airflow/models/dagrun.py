# -*- coding: utf-8 -*-
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

import six
from sqlalchemy import (
    Column, Integer, String, Boolean, PickleType, Index, UniqueConstraint, func, DateTime, or_,
    and_
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym

from airflow.exceptions import AirflowException
from airflow.models.base import Base, ID_LEN
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
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

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    conf = Column(PickleType)

    dag = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

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
        return synonym('_state',
                       descriptor=property(self.get_state, self.set_state))

    @classmethod
    def id_for_date(cls, date, prefix=ID_FORMAT_PREFIX):
        return prefix.format(date.isoformat()[:19])

    @provide_session
    def refresh_from_db(self, session=None):
        """
        Reloads the current dagrun from the database
        :param session: database session
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
    def find(dag_id=None, run_id=None, execution_date=None,
             state=None, external_trigger=None, no_backfills=False,
             session=None):
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id to find dag runs for
        :type dag_id: int, list
        :param run_id: defines the the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        DR = DagRun

        qry = session.query(DR)
        if dag_id:
            qry = qry.filter(DR.dag_id == dag_id)
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))

        dr = qry.order_by(DR.execution_date).all()

        return dr

    @provide_session
    def get_task_instances(self, state=None, session=None):
        """
        Returns the task instances for this dag run
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import
        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.execution_date == self.execution_date,
        )
        if state:
            if isinstance(state, six.string_types):
                tis = tis.filter(TaskInstance.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    tis = tis.filter(
                        or_(TaskInstance.state.in_(state),
                            TaskInstance.state.is_(None))
                    )
                else:
                    tis = tis.filter(TaskInstance.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TaskInstance.task_id.in_(self.dag.task_ids))

        return tis.all()

    @provide_session
    def get_task_instance(self, task_id, session=None):
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        """

        from airflow.models.taskinstance import TaskInstance  # Avoid circular import
        TI = TaskInstance
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
            raise AirflowException("The DAG (.dag) for {} needs to be set"
                                   .format(self))

        return self.dag

    @provide_session
    def get_previous_dagrun(self, session=None):
        """The previous DagRun, if there is one"""

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date
        ).order_by(
            DagRun.execution_date.desc()
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session=None):
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, session=None):
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
        # small speed up
        if unfinished_tasks and none_depends_on_past and none_task_concurrency:
            # todo: this can actually get pretty slow: one task costs between 0.01-015s
            no_dependencies_met = True
            for ut in unfinished_tasks:
                # We need to flag upstream and check for changes because upstream
                # failures/re-schedules can result in deadlock false positives
                old_state = ut.state
                deps_met = ut.are_dependencies_met(
                    dep_context=DepContext(
                        flag_upstream_failed=True,
                        ignore_in_retry_period=True,
                        ignore_in_reschedule_period=True),
                    session=session)
                if deps_met or old_state != ut.current_state(session=session):
                    no_dependencies_met = False
                    break

        duration = (timezone.utcnow() - start_dttm).total_seconds() * 1000
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        root_ids = [t.task_id for t in dag.roots]
        roots = [t for t in tis if t.task_id in root_ids]

        # if all roots finished and at least one failed, the run failed
        if (not unfinished_tasks and
                any(r.state in (State.FAILED, State.UPSTREAM_FAILED) for r in roots)):
            self.log.info('Marking run %s failed', self)
            self.set_state(State.FAILED)
            dag.handle_callback(self, success=False, reason='task_failure',
                                session=session)

        # if all roots succeeded and no unfinished tasks, the run succeeded
        elif not unfinished_tasks and all(r.state in (State.SUCCESS, State.SKIPPED)
                                          for r in roots):
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

    def _emit_duration_stats_for_finished_state(self):
        if self.state == State.RUNNING:
            return

        duration = (self.end_date - self.start_date)
        if self.state is State.SUCCESS:
            Stats.timing('dagrun.duration.success.{}'.format(self.dag_id), duration)
        elif self.state == State.FAILED:
            Stats.timing('dagrun.duration.failed.{}'.format(self.dag_id), duration)

    @provide_session
    def verify_integrity(self, session=None):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.
        """
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

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

            is_task_in_dag = task is not None
            should_restore_task = is_task_in_dag and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '{}' which was previously "
                              "removed from DAG '{}'".format(ti, dag))
                Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                ti.state = State.NONE

        # check for missing tasks
        for task in six.itervalues(dag.task_dict):
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
    def get_run(session, dag_id, execution_date):
        """
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
            DagRun.external_trigger == False, # noqa
            DagRun.execution_date == execution_date,
        )
        return qry.first()

    @property
    def is_backfill(self):
        from airflow.jobs import BackfillJob
        return (
            self.run_id is not None and
            self.run_id.startswith(BackfillJob.ID_PREFIX)
        )

    @classmethod
    @provide_session
    def get_latest_runs(cls, session):
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
