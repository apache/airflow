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
"""Task instance."""
import copy
import getpass
import hashlib
import logging
import math
import os
import signal
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Iterable, Optional, Tuple, Union
from urllib.parse import quote

import dill
import lazy_object_proxy
import pendulum
from sqlalchemy import Column, Float, Index, Integer, PickleType, String, func
from sqlalchemy.orm import Session, reconstructor

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException, AirflowRescheduleException, AirflowSkipException, AirflowTaskTimeout,
)
from airflow.models.base import ID_LEN, Base
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XCom
from airflow.sentry import Sentry
from airflow.stats import Stats
from airflow.ti_deps.dep_constants import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.ti_deps.dep_context import DepContext, TIDepStatus
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.xcom import XCOM_RETURN_KEY

# Key used to identify task instance
# Tuple of: task_id, , execution_date, ?
TaskInstanceKey = Tuple[str, str, datetime, int]


class TaskInstance(  # pylint: disable=too-many-instance-attributes,too-many-public-methods
        Base, LoggingMixin):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id: str = Column(String(ID_LEN), primary_key=True)
    dag_id: str = Column(String(ID_LEN), primary_key=True)
    execution_date: datetime = Column(UtcDateTime, primary_key=True)
    start_date: Optional[datetime] = Column(UtcDateTime)
    end_date: Optional[datetime] = Column(UtcDateTime)
    duration: Optional[float] = Column(Float)
    state: Optional[str] = Column(String(20))
    _try_number: int = Column('try_number', Integer, default=0)
    max_tries: int = Column(Integer)
    hostname: Optional[str] = Column(String(1000))
    unixname: Optional[str] = Column(String(1000))
    job_id: Optional[int] = Column(Integer)
    pool: str = Column(String(50), nullable=False)
    queue: Optional[str] = Column(String(256))
    priority_weight: int = Column(Integer)
    operator: Optional[str] = Column(String(1000))
    queued_dttm: Optional[datetime] = Column(UtcDateTime)
    pid: int = Column(Integer)
    executor_config: Any = Column(PickleType(pickler=dill))

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_date', dag_id, execution_date),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    def __init__(self, task: Any,  # BaseOperator # to avoid cyclic import
                 execution_date: datetime,
                 state: Optional[str] = None):
        super().__init__()
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        from airflow.models.baseoperator import BaseOperator
        self.task: BaseOperator = task
        self._log: logging.Logger = logging.getLogger("airflow.task")

        # make sure we have a localized execution_date stored in UTC
        if execution_date and not timezone.is_localized(execution_date):
            self.log.warning("execution date %s has no timezone information. Using "
                             "default from dag or system", execution_date)
            if self.task.has_dag():
                execution_date = timezone.make_aware(execution_date,
                                                     self.task.dag.timezone)
            else:
                execution_date = timezone.make_aware(execution_date)

            execution_date = timezone.convert_to_utc(execution_date)

        self.execution_date = execution_date

        self.queue = task.queue
        self.pool = task.pool
        self.priority_weight = task.priority_weight_total
        self.try_number = 0
        self.max_tries = self.task.retries
        self.unixname = getpass.getuser()
        self.run_as_user = task.run_as_user
        if state:
            self.state = state
        self.hostname = ''
        self.executor_config = task.executor_config
        self.test_mode = False
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow tasks run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self) -> int:
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TaskInstance is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value: int):
        self._try_number = value

    @property
    def prev_attempted_tries(self) -> int:
        """
        Based on this instance's try_number, this will calculate
        the number of previously attempted tries, defaulting to 0.
        """
        # Expose this for the Task Tries and GANTT graph views.
        # Using `try_number` throws off the counts for non-running tasks.
        # Also useful in error logging contexts to get
        # the try number for the last try that was attempted.
        # https://issues.apache.org/jira/browse/AIRFLOW-2143

        return self._try_number

    @property
    def next_try_number(self) -> int:
        """Returns next try number that will be used."""
        return self._try_number + 1

    def command_as_list(  # pylint: disable=too-many-arguments
            self,
            mark_success: bool = False,
            ignore_all_deps: bool = False,
            ignore_task_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_ti_state: bool = False,
            local: bool = False,
            pickle_id: Optional[str] = None,
            raw: bool = False,
            job_id: Optional[str] = None,
            pool: Optional[str] = None,
            cfg_path: Optional[str] = None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag
        path: Optional[str] = None
        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id: str,  # pylint: disable=too-many-arguments
                         task_id: str,
                         execution_date: datetime,
                         mark_success: bool = False,
                         ignore_all_deps: bool = False,
                         ignore_depends_on_past: bool = False,
                         ignore_task_deps: bool = False,
                         ignore_ti_state: bool = False,
                         local: bool = False,
                         pickle_id: Optional[str] = None,
                         file_path: Optional[str] = None,
                         raw: bool = False,
                         job_id: Optional[str] = None,
                         pool: Optional[str] = None,
                         cfg_path: Optional[str] = None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :param task_id: Task ID
        :param execution_date: Execution date for the task
        :param mark_success: Whether to mark the task as successful
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for backfills)
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :param local: Whether to run the task locally
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :param cfg_path: the Path to the configuration file
        :return: shell command that can be used to run the task instance
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "tasks", "run", str(dag_id), str(task_id), str(iso)]
        if mark_success:
            cmd.extend(["--mark_success"])
        if pickle_id:
            cmd.extend(["--pickle", str(pickle_id)])
        if job_id:
            cmd.extend(["--job_id", str(job_id)])
        if ignore_all_deps:
            cmd.extend(["-A"])
        if ignore_task_deps:
            cmd.extend(["-i"])
        if ignore_depends_on_past:
            cmd.extend(["-I"])
        if ignore_ti_state:
            cmd.extend(["--force"])
        if local:
            cmd.extend(["--local"])
        if pool:
            cmd.extend(["--pool", pool])
        if raw:
            cmd.extend(["--raw"])
        if file_path:
            cmd.extend(["-sd", file_path])
        if cfg_path:
            cmd.extend(["--cfg_path", cfg_path])
        return cmd

    @property
    def log_filepath(self) -> str:
        """Returns path of the log file to use for that task instance."""
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        return ("{log}/{dag_id}/{task_id}/{iso}.log".format(
            log=log, dag_id=self.dag_id, task_id=self.task_id, iso=iso))

    @property
    def log_url(self) -> str:
        """Returns URL of the log to use for that task instance."""
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/log?"
            "execution_date={iso}"
            "&task_id={task_id}"
            "&dag_id={dag_id}"
        ).format(iso=iso, task_id=self.task_id, dag_id=self.dag_id)

    @property
    def mark_success_url(self) -> None:
        """Returns URL to use to mark success for the task instance."""
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/success"
            "?task_id={task_id}"
            "&dag_id={dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(task_id=self.task_id, dag_id=self.dag_id, iso=iso)

    @provide_session
    def current_state(self, session: Session = None) -> str:
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session: Session = None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self,
                        session: Session = None,
                        lock_for_update: bool = False,
                        refresh_executor_config: bool = False) -> None:
        """
        Refreshes the task instance from the database based on the primary key

        :param session: db session
        :param refresh_executor_config: if True, revert executor config to
            result from DB. Often, however, we will want to keep the newest
            version
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.execution_date == self.execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremented by one already.
            # noinspection PyProtectedMember
            self.try_number = ti._try_number  # pylint: disable=protected-access
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.pid = ti.pid
            if refresh_executor_config:
                self.executor_config = ti.executor_config
        else:
            self.state = None

    @provide_session
    def clear_xcom_data(self, session: Session = None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self) -> TaskInstanceKey:
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date, self.try_number

    @provide_session
    def set_state(self, state: str, session: Session = None, commit: bool = True):
        """Sets state for the task and commits it to the database."""
        self.state = state
        self.start_date = timezone.utcnow()
        self.end_date = timezone.utcnow()
        session.merge(self)
        if commit:
            session.commit()

    @property
    def is_premature(self) -> bool:
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session: Session = None) -> bool:
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id in task.downstream_task_ids,
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def _get_previous_ti(
        self,
        state: Optional[str] = None,
        session: Session = None
    ) -> Optional['TaskInstance']:
        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TaskInstance is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task, execution_date=previous_scheduled_date)

            dr.dag = dag

            # We always ignore schedule in dagrun lookup when `state` is given or `schedule_interval is None`.
            # For legacy reasons, when `catchup=True`, we use `get_previous_scheduled_dagrun` unless
            # `ignore_schedule` is `True`.
            ignore_schedule = state is not None or dag.schedule_interval is None
            if dag.catchup is True and not ignore_schedule:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session, state=state)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @property
    def previous_ti(self) -> Optional['TaskInstance']:
        """The task instance for the task that ran before this task instance."""
        return self._get_previous_ti()

    @property
    def previous_ti_success(self) -> Optional['TaskInstance']:
        """The ti from prior successful dag run for this task, by execution date."""
        return self._get_previous_ti(state=State.SUCCESS)

    @property
    def previous_execution_date_success(self) -> Optional[pendulum.datetime]:
        """The execution date from property previous_ti_success."""
        self.log.debug("previous_execution_date_success was called")
        prev_ti = self._get_previous_ti(state=State.SUCCESS)
        return prev_ti and prev_ti.execution_date

    @property
    def previous_start_date_success(self) -> Optional[pendulum.datetime]:
        """The start date from property previous_ti_success."""
        self.log.debug("previous_start_date_success was called")
        prev_ti = self._get_previous_ti(state=State.SUCCESS)
        return prev_ti and prev_ti.start_date

    @provide_session
    def are_dependencies_met(
            self,
            dep_context: Optional[DepContext] = None,
            session: Session = None,
            verbose: bool = False) -> bool:
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :param session: database session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self, dep_status.dep_name, dep_status.reason
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context: Optional[DepContext] = None,
            session: Session = None) -> Generator[TIDepStatus, None, None]:
        """Generator that returns status of all failed dependencies."""
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self) -> datetime:
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay: timedelta = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occur in the modded_hash calculation.
            min_backoff = int(math.ceil(delay.total_seconds() * (2 ** (self.try_number - 2))))
            # deterministic per task instance
            task_instance_hash = int(hashlib.sha1("{}#{}#{}#{}".format(
                self.dag_id,
                self.task_id,
                self.execution_date,
                self.try_number).encode('utf-8')).hexdigest(), 16)
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + task_instance_hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        if not self.end_date:
            self.log.warning("End date is not set for next_retry_datetime. Setting end date to current time.")
            self.end_date = datetime.utcnow()
        return self.end_date + delay

    def ready_for_retry(self) -> bool:
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < timezone.utcnow())

    @provide_session
    def pool_full(self, session: Session) -> bool:
        """
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False
        from airflow.models.pool import Pool

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            return False
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    @provide_session
    def _check_and_change_state_before_execution(  # pylint: disable=too-many-arguments
            self,
            verbose: bool = True,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            test_mode: bool = False,
            job_id: Optional[int] = None,
            pool: Optional[str] = None,
            session: Session = None):
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
        :param ignore_ti_state: Disregards previous task instance state
        :param mark_success: Don't run the task, mark its state as success
        :param test_mode: Doesn't record success or failure in the DB
        :param job_id: ID of the job to check
        :param pool: specifies the pool to use to run the task instance
        :param session: session to use
        :return: whether the state was changed to running or not
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.__class__.__name__

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        header = "\n" + ("-" * 80)  # Line break

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps)
            if not self.are_dependencies_met(
                    dep_context=non_requeueable_dep_context,
                    session=session,
                    verbose=True):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            self.start_date = timezone.utcnow()
            task_reschedules = TaskReschedule.find_for_task_instance(self, session)
            if task_reschedules:
                self.start_date = task_reschedules[0].start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state)
            if not self.are_dependencies_met(
                    dep_context=dep_context,
                    session=session,
                    verbose=True):
                self.state = State.NONE
                self.log.warning(header)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.", self.try_number, self.max_tries + 1
                )
                self.log.warning(header)
                self.queued_dttm = timezone.utcnow()
                session.merge(self)
                session.commit()
                return False

        # print status message
        self.log.info(header)
        self.log.info("Starting attempt %s of %s", self.try_number, self.max_tries + 1)
        self.log.info(header)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        if settings.engine:
            settings.engine.dispose()
        if verbose:
            if mark_success:
                self.log.info("Marking success for %s on %s", self.task, self.execution_date)
            else:
                self.log.info("Executing %s on %s", self.task, self.execution_date)
        return True

    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(  # pylint: disable=too-many-statements
            self,
            job_id: Optional[int] = None,
            mark_success: bool = False,
            test_mode: bool = False,
            pool: Optional[str] = None,
            session: Session = None):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.__class__.__name__

        context: Dict[str, Any] = {}
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                context = self.get_template_context()

                task_copy = copy.copy(task)
                self.task = task_copy

                def signal_handler(signum, frame):  # pylint: disable=unused-variable
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                start_time = time.time()

                self.render_templates(context=context)
                task_copy.pre_execute(context=context)

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                # If the task returns a result, push an XCom containing it
                if task_copy.do_xcom_push and result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                task_copy.post_execute(context=context, result=result)

                end_time = time.time()
                duration = end_time - start_time
                Stats.timing(
                    'dag.{dag_id}.{task_id}.duration'.format(
                        dag_id=task_copy.dag_id,
                        task_id=task_copy.task_id),
                    duration)

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException as e:
            # log only if exception has any arguments to prevent log flooding
            if e.args:
                self.log.info(e)
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode)
            return
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(str(e), test_mode, context, session=session)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, context)
            raise

        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as exception:  # pylint: disable=broad-except
            self.log.error("Failed when executing success callback")
            self.log.exception(exception)

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()

    @provide_session
    def run(  # pylint: disable=too-many-arguments
            self,
            verbose: bool = True,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            mark_success: bool = False,
            test_mode: bool = False,
            job_id: Optional[int] = None,
            pool: Optional[str] = None,
            session: Session = None):
        """Executes the task instance."""
        res = self._check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)

    def dry_run(self):
        """Dry run the task instance."""
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def _handle_reschedule(self,
                           actual_start_date: datetime,
                           reschedule_exception: AirflowRescheduleException,
                           test_mode: bool = False,
                           session: Session = None):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Log reschedule request
        session.add(TaskReschedule(self.task, self.execution_date, self._try_number,
                    actual_start_date, self.end_date,
                    reschedule_exception.reschedule_date))

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @provide_session
    def handle_failure(self,
                       error: Exception,
                       test_mode: bool = False,
                       context: Optional[Dict[str, Any]] = None,
                       session: Session = None):
        """Handles failure during task processing."""
        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        if context is not None:
            context['exception'] = error

        # Let's go deeper
        try:
            # Since this function is called only when the TaskInstance state is running,
            # try_number contains the current try_number (not the next). We
            # only mark task instance as FAILED if the next task instance
            # try_number exceeds the max_tries.
            if self.is_eligible_to_retry():
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')
                if task.email_on_retry and task.email:
                    self.email_alert(error)
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info('All retries failed; marking task as FAILED')
                else:
                    self.log.info('Marking task as FAILED.')
                if task.email_on_failure and task.email:
                    self.email_alert(error)
        except Exception as exception:  # pylint: disable=broad-except
            self.log.error('Failed to send email to: %s', task.email)
            self.log.exception(exception)

        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as exception:  # pylint: disable=broad-except
            self.log.error("Failed at executing callback")
            self.log.exception(exception)

        if not test_mode:
            session.merge(self)
        session.commit()

    def is_eligible_to_retry(self) -> bool:
        """Is task instance is eligible for retry"""
        return self.task.retries > 0 and self.try_number <= self.max_tries

    @provide_session
    def get_template_context(self,   # pylint: disable=too-many-locals
                             session: Session = None) -> Dict[str, Any]:
        """Get context of the template."""
        task = self.task
        from airflow import macros

        params: Dict[str, Any] = {}
        run_id: Optional[str] = ''
        from airflow.models import DagRun
        dag_run: Optional[DagRun] = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            dag_run = DagRun.get_dagrun(dag_id=self.dag_id,
                                        execution_date=self.execution_date,
                                        session=session)
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')

        # For manually triggered dagruns that aren't run on a schedule, next/previous
        # schedule dates don't make sense, and should be set to execution date for
        # consistency with how execution_date is set for manually triggered tasks, i.e.
        # triggered_date == execution_date.
        if dag_run and dag_run.external_trigger:
            prev_execution_date = self.execution_date
            next_execution_date = self.execution_date
        else:
            prev_execution_date = task.dag.previous_schedule(self.execution_date)
            next_execution_date = task.dag.following_schedule(self.execution_date)

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')
            next_execution_date = pendulum.instance(next_execution_date)

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')
            prev_execution_date = pendulum.instance(prev_execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = self.execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = "{dag_id}__{task_id}__{ds_nodash}".format(
            dag_id=task.dag_id, task_id=task.task_id, ds_nodash=ds_nodash)

        if task.params:
            params.update(task.params)

        if dag_run and conf.getboolean('core', 'dag_run_conf_overrides_params'):
            dag_run.overwrite_params_with_dag_run_conf(params=params)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in templates by using
            {var.value.your_variable_name}.
            """
            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

        class VariableJsonAccessor:
            """
            Wrapper around deserialized Variables. This way you can get variables
            in templates by using {var.json.your_variable_name}.
            """
            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

        return {
            'conf': conf,
            'dag': task.dag,
            'dag_run': dag_run,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'execution_date': pendulum.instance(self.execution_date),
            'inlets': task.inlets,
            'macros': macros,
            'next_ds': next_ds,
            'next_ds_nodash': next_ds_nodash,
            'next_execution_date': next_execution_date,
            'outlets': task.outlets,
            'params': params,
            'prev_ds': prev_ds,
            'prev_ds_nodash': prev_ds_nodash,
            'prev_execution_date': prev_execution_date,
            'prev_execution_date_success': lazy_object_proxy.Proxy(
                lambda: self.previous_execution_date_success),
            'prev_start_date_success': lazy_object_proxy.Proxy(lambda: self.previous_start_date_success),
            'run_id': run_id,
            'task': task,
            'task_instance': self,
            'task_instance_key_str': ti_key_str,
            'test_mode': self.test_mode,
            'ti': self,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'var': {
                'json': VariableJsonAccessor(),
                'value': VariableAccessor(),
            },
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
        }

    def render_templates(self, context: Optional[Dict[str, Any]] = None) -> None:
        """Render templates in the operator fields."""
        good_context: Dict[str, Any] = context if context else self.get_template_context()

        self.task.render_template_fields(good_context)

    def email_alert(self, exception: Exception):
        """Send email when exception occured."""
        exception_html = str(exception).replace('\n', '<br>')
        jinja_context = self.get_template_context()
        # This function is called after changing the state
        # from State.RUNNING so use prev_attempted_tries.
        jinja_context.update(dict(
            exception=exception,
            exception_html=exception_html,
            try_number=self.prev_attempted_tries,
            max_tries=self.max_tries))

        jinja_env = self.task.get_template_env()

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        def render(key: str, content: Any):
            if conf.has_option('email', key):
                path = conf.get('email', key)
                with open(path) as file:
                    content = file.read()

            return jinja_env.from_string(content).render(**jinja_context)

        subject = render('subject_template', default_subject)
        html_content = render('html_content_template', default_html_content)
        send_email(self.task.email, subject, html_content)

    def set_duration(self):
        """Set duration of the task."""
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key: str,
            value: Any,
            execution_date: Optional[datetime] = None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids: Optional[Union[str, Iterable[str]]] = None,
            dag_id: Optional[str] = None,
            key: str = XCOM_RETURN_KEY,
            include_prior_dates: bool = False) -> Optional[Any]:
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        query = XCom.get_many(
            execution_date=self.execution_date,
            key=key,
            dag_ids=dag_id,
            task_ids=task_ids,
            include_prior_dates=include_prior_dates
        ).with_entities(XCom.value)

        # Since we're only fetching the values field, and not the
        # whole class, the @recreate annotation does not kick in.
        # Therefore we need to deserialize the fields by ourselves.

        if is_container(task_ids):
            return [XCom.deserialize_value(xcom) for xcom in query]
        else:
            xcom = query.first()
            if xcom:
                return XCom.deserialize_value(xcom)
            return None

    @provide_session
    def get_num_running_task_instances(self, session: Session):
        """Efficient way of getting number of running task instances."""
        # .count() is inefficient
        return session.query(func.count()).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.state == State.RUNNING
        ).scalar()

    def init_run_context(self, raw=False):
        """
        Sets the log context.
        """
        self.raw = raw
        self._set_context(self)


# State of the task instance.
# Stores string version of the task state.
TaskInstanceState = Tuple[TaskInstanceKey, str]


class SimpleTaskInstance:
    """
    Simplified Task Instance.

    Used to send data between processes via Queues.

    """
    def __init__(self, ti: TaskInstance):
        self._dag_id: str = ti.dag_id
        self._task_id: str = ti.task_id
        self._execution_date: datetime = ti.execution_date
        self._start_date: Optional[datetime] = ti.start_date
        self._end_date: Optional[datetime] = ti.end_date
        self._try_number: int = ti.try_number
        self._state: Optional[str] = ti.state
        self._executor_config: Any = ti.executor_config
        self._run_as_user: Optional[str] = None
        if hasattr(ti, 'run_as_user'):
            self._run_as_user = ti.run_as_user
        self._pool: Optional[str] = None
        if hasattr(ti, 'pool'):
            self._pool = ti.pool
        self._priority_weight: Optional[int] = None
        if hasattr(ti, 'priority_weight'):
            self._priority_weight = ti.priority_weight
        self._queue: Optional[str] = ti.queue
        self._key = ti.key

    # pylint: disable=missing-docstring
    @property
    def dag_id(self) -> str:
        return self._dag_id

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def execution_date(self) -> datetime:
        return self._execution_date

    @property
    def start_date(self) -> Optional[datetime]:
        return self._start_date

    @property
    def end_date(self) -> Optional[datetime]:
        return self._end_date

    @property
    def try_number(self) -> int:
        return self._try_number

    @property
    def state(self) -> Optional[str]:
        return self._state

    @property
    def pool(self) -> Optional[str]:
        return self._pool

    @property
    def priority_weight(self) -> Optional[int]:
        return self._priority_weight

    @property
    def queue(self) -> Optional[str]:
        return self._queue

    @property
    def key(self) -> TaskInstanceKey:
        return self._key

    @property
    def executor_config(self):
        return self._executor_config

    @provide_session
    def construct_task_instance(self, session=None, lock_for_update=False) -> TaskInstance:
        """
        Construct a TaskInstance from the database based on the primary key

        :param session: DB session.
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        :return: the task instance constructed
        """

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self._dag_id,
            TaskInstance.task_id == self._task_id,
            TaskInstance.execution_date == self._execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        return ti
