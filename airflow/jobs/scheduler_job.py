# pylint: disable=no-name-in-module
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
#

import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from contextlib import redirect_stderr, redirect_stdout, suppress
from datetime import timedelta
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple

from setproctitle import setproctitle
from sqlalchemy import and_, func, not_, or_
from sqlalchemy.orm.session import make_transient

from airflow import models, settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException, TaskNotFound
from airflow.executors.executor_loader import UNPICKLEABLE_EXECUTORS
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, SlaMiss, errors
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType
from airflow.operators.dummy_operator import DummyOperator
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import SCHEDULED_DEPS
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.utils import asciiart, helpers, timezone
from airflow.utils.dag_processing import (
    AbstractDagFileProcessorProcess, DagFileProcessorAgent, FailureCallbackRequest, SimpleDag, SimpleDagBag,
)
from airflow.utils.email import get_email_address_list, send_email
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

TI = models.TaskInstance
DR = models.DagRun
DM = models.DagModel


class DagFileProcessorProcess(AbstractDagFileProcessorProcess, LoggingMixin, MultiprocessingStartMethodMixin):
    """Runs DAG processing in a separate process using DagFileProcessor

    :param file_path: a Python file containing Airflow DAG definitions
    :type file_path: str
    :param pickle_dags: whether to serialize the DAG objects to the DB
    :type pickle_dags: bool
    :param dag_ids: If specified, only look at these DAG ID's
    :type dag_ids: List[str]
    :param failure_callback_requests: failure callback to execute
    :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
    """

    # Counter that increments every time an instance of this class is created
    class_creation_counter = 0

    def __init__(
        self,
        file_path: str,
        pickle_dags: bool,
        dag_ids: Optional[List[str]],
        failure_callback_requests: List[FailureCallbackRequest]
    ):
        super().__init__()
        self._file_path = file_path
        self._pickle_dags = pickle_dags
        self._dag_ids = dag_ids
        self._failure_callback_requests = failure_callback_requests

        # The process that was launched to process the given .
        self._process = None
        # The result of Scheduler.process_file(file_path).
        self._result = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessorProcess.class_creation_counter

        self._parent_channel = None
        self._result_queue = None
        DagFileProcessorProcess.class_creation_counter += 1

    @property
    def file_path(self):
        return self._file_path

    @staticmethod
    def _run_file_processor(result_channel,
                            file_path,
                            pickle_dags,
                            dag_ids,
                            thread_name,
                            failure_callback_requests):
        """
        Process the given file.

        :param result_channel: the connection to use for passing back the result
        :type result_channel: multiprocessing.Connection
        :param file_path: the file to process
        :type file_path: str
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :type pickle_dags: bool
        :param dag_ids: if specified, only examine DAG ID's that are
            in this list
        :type dag_ids: list[str]
        :param thread_name: the name to use for the process that is launched
        :type thread_name: str
        :param failure_callback_requests: failure callback to execute
        :type failure_callback_requests: list[airflow.utils.dag_processing.FailureCallbackRequest]
        :return: the process that was launched
        :rtype: multiprocessing.Process
        """
        # This helper runs in the newly created process
        log = logging.getLogger("airflow.processor")

        set_context(log, file_path)
        setproctitle("airflow scheduler - DagFileProcessor {}".format(file_path))

        try:
            # redirect stdout/stderr to log
            with redirect_stdout(StreamLogWriter(log, logging.INFO)),\
                    redirect_stderr(StreamLogWriter(log, logging.WARN)):

                # Re-configure the ORM engine as there are issues with multiple processes
                settings.configure_orm()

                # Change the thread name to differentiate log lines. This is
                # really a separate process, but changing the name of the
                # process doesn't work, so changing the thread name instead.
                threading.current_thread().name = thread_name
                start_time = time.time()

                log.info("Started process (PID=%s) to work on %s", os.getpid(), file_path)
                dag_file_processor = DagFileProcessor(dag_ids=dag_ids, log=log)
                result = dag_file_processor.process_file(
                    file_path=file_path,
                    pickle_dags=pickle_dags,
                    failure_callback_requests=failure_callback_requests,
                )
                result_channel.send(result)
                end_time = time.time()
                log.info(
                    "Processing %s took %.3f seconds", file_path, end_time - start_time
                )
        except Exception:  # pylint: disable=broad-except
            # Log exceptions through the logging framework.
            log.exception("Got an exception! Propagating...")
            raise
        finally:
            result_channel.close()
            # We re-initialized the ORM within this Process above so we need to
            # tear it down manually here
            settings.dispose_orm()

    def start(self):
        """
        Launch the process and start processing the DAG.
        """
        start_method = self._get_multiprocessing_start_method()
        context = multiprocessing.get_context(start_method)

        self._parent_channel, _child_channel = context.Pipe()
        self._process = context.Process(
            target=type(self)._run_file_processor,
            args=(
                _child_channel,
                self.file_path,
                self._pickle_dags,
                self._dag_ids,
                "DagFileProcessor{}".format(self._instance_id),
                self._failure_callback_requests
            ),
            name="DagFileProcessor{}-Process".format(self._instance_id)
        )
        self._start_time = timezone.utcnow()
        self._process.start()

    def kill(self):
        """
        Kill the process launched to process the file, and ensure consistent state.
        """
        if self._process is None:
            raise AirflowException("Tried to kill before starting!")
        # The queue will likely get corrupted, so remove the reference
        self._result_queue = None
        self._kill_process()

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call terminate before starting!")

        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        with suppress(TimeoutError):
            self._process._popen.wait(5)  # pylint: disable=protected-access
        if sigkill:
            self._kill_process()
        self._parent_channel.close()

    def _kill_process(self):
        if self._process.is_alive():
            self.log.warning("Killing PID %s", self._process.pid)
            os.kill(self._process.pid, signal.SIGKILL)

    @property
    def pid(self):
        """
        :return: the PID of the process launched to process the given file
        :rtype: int
        """
        if self._process is None:
            raise AirflowException("Tried to get PID before starting!")
        return self._process.pid

    @property
    def exit_code(self):
        """
        After the process is finished, this can be called to get the return code

        :return: the exit code of the process
        :rtype: int
        """
        if not self._done:
            raise AirflowException("Tried to call retcode before process was finished!")
        return self._process.exitcode

    @property
    def done(self):
        """
        Check if the process launched to process this file is done.

        :return: whether the process is finished running
        :rtype: bool
        """
        if self._process is None:
            raise AirflowException("Tried to see if it's done before starting!")

        if self._done:
            return True

        if self._parent_channel.poll():
            try:
                self._result = self._parent_channel.recv()
                self._done = True
                self.log.debug("Waiting for %s", self._process)
                self._process.join()
                self._parent_channel.close()
                return True
            except EOFError:
                pass

        if not self._process.is_alive():
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            self._parent_channel.close()
            return True

        return False

    @property
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: airflow.utils.dag_processing.SimpleDag
        """
        if not self.done:
            raise AirflowException("Tried to get the result before it's done!")
        return self._result

    @property
    def start_time(self):
        """
        :return: when this started to process the file
        :rtype: datetime
        """
        if self._start_time is None:
            raise AirflowException("Tried to get start time before it started!")
        return self._start_time

    @property
    def waitable_handle(self):
        return self._process.sentinel


class DagFileProcessor(LoggingMixin):
    """
    Process a Python file containing Airflow DAGs.

    This includes:

    1. Execute the file and look for DAG objects in the namespace.
    2. Pickle the DAG and save it to the DB (if necessary).
    3. For each DAG, see what tasks should run and create appropriate task
    instances in the DB.
    4. Record any errors importing the file into ORM
    5. Kill (in ORM) any task instances belonging to the DAGs that haven't
    issued a heartbeat in a while.

    Returns a list of SimpleDag objects that represent the DAGs found in
    the file

    :param dag_ids: If specified, only look at these DAG ID's
    :type dag_ids: List[str]
    :param log: Logger to save the processing process
    :type log: logging.Logger
    """

    UNIT_TEST_MODE = conf.getboolean('core', 'UNIT_TEST_MODE')

    def __init__(self, dag_ids, log):
        super().__init__()
        self.dag_ids = dag_ids
        self._log = log

    @provide_session
    def manage_slas(self, dag: DAG, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        We are assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        if not any([isinstance(ti.sla, timedelta) for ti in dag.tasks]):
            self.log.info("Skipping SLA check for %s because no tasks in DAG have SLAs", dag)
            return

        qry = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti')
            )
            .with_hint(TI, 'USE INDEX (PRIMARY)', dialect_name='mysql')
            .filter(TI.dag_id == dag.dag_id)
            .filter(
                or_(
                    TI.state == State.SUCCESS,
                    TI.state == State.SKIPPED
                )
            )
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == qry.c.task_id,
            TI.execution_date == qry.c.max_ti,
        ).all()

        ts = timezone.utcnow()
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            if not isinstance(task.sla, timedelta):
                continue

            dttm = dag.following_schedule(ti.execution_date)
            while dttm < timezone.utcnow():
                following_schedule = dag.following_schedule(dttm)
                if following_schedule + task.sla < timezone.utcnow():
                    session.merge(SlaMiss(
                        task_id=ti.task_id,
                        dag_id=ti.dag_id,
                        execution_date=dttm,
                        timestamp=ts))
                dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.notification_sent == False, SlaMiss.dag_id == dag.dag_id)  # noqa pylint: disable=singleton-comparison
            .all()
        )

        if slas:  # pylint: disable=too-many-nested-blocks
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(
                    TI.state != State.SUCCESS,
                    TI.execution_date.in_(sla_dates),
                    TI.dag_id == dag.dag_id
                ).all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.log.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                try:
                    dag.sla_miss_callback(dag, task_list, blocking_task_list, slas,
                                          blocking_tis)
                    notification_sent = True
                except Exception:  # pylint: disable=broad-except
                    self.log.exception("Could not call sla_miss_callback for DAG %s",
                                       dag.dag_id)
            email_content = """\
            Here's a list of tasks that missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(task_list=task_list, blocking_task_list=blocking_task_list,
                       bug=asciiart.bug)

            tasks_missed_sla = []
            for sla in slas:
                try:
                    task = dag.get_task(sla.task_id)
                except TaskNotFound:
                    # task already deleted from DAG, skip it
                    self.log.warning(
                        "Task %s doesn't exist in DAG anymore, skipping SLA miss notification.",
                        sla.task_id)
                    continue
                tasks_missed_sla.append(task)

            emails = set()
            for task in tasks_missed_sla:
                if task.email:
                    if isinstance(task.email, str):
                        emails |= set(get_email_address_list(task.email))
                    elif isinstance(task.email, (list, tuple)):
                        emails |= set(task.email)
            if emails:
                try:
                    send_email(
                        emails,
                        "[airflow] SLA miss on DAG=" + dag.dag_id,
                        email_content)
                    email_sent = True
                    notification_sent = True
                except Exception:  # pylint: disable=broad-except
                    Stats.incr('sla_email_notification_failure')
                    self.log.exception("Could not send SLA Miss email notification for"
                                       " DAG %s", dag.dag_id)
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()

    @staticmethod
    def update_import_errors(session, dagbag):
        """
        For the DAGs in the given DagBag, record any associated import errors and clears
        errors for files that no longer have them. These are usually displayed through the
        Airflow UI so that users know that there are issues parsing DAGs.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        :param dagbag: DagBag containing DAGs with import errors
        :type dagbag: airflow.models.DagBag
        """
        # Clear the errors of the processed files
        for dagbag_file in dagbag.file_last_changed:
            session.query(errors.ImportError).filter(
                errors.ImportError.filename == dagbag_file
            ).delete()

        # Add the errors of the processed files
        for filename, stacktrace in dagbag.import_errors.items():
            session.add(errors.ImportError(
                filename=filename,
                timestamp=timezone.utcnow(),
                stacktrace=stacktrace))
        session.commit()

    # pylint: disable=too-many-return-statements,too-many-branches
    @provide_session
    def create_dag_run(
        self,
        dag: DAG,
        dag_runs: Optional[List[DagRun]] = None,
        session=None,
    ) -> None:
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval.
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        # pylint: disable=too-many-nested-blocks
        if not dag.schedule_interval:
            return None

        if dag_runs is None:
            active_runs = DagRun.find(
                dag_id=dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )
        else:
            active_runs = [
                dag_run
                for dag_run in dag_runs
                if not dag_run.external_trigger
            ]
        # return if already reached maximum active runs and no timeout setting
        if len(active_runs) >= dag.max_active_runs and not dag.dagrun_timeout:
            return None
        timeouted_runs = 0
        for dr in active_runs:
            if (
                dr.start_date and dag.dagrun_timeout and
                dr.start_date < timezone.utcnow() - dag.dagrun_timeout
            ):
                dr.state = State.FAILED
                dr.end_date = timezone.utcnow()
                dag.handle_callback(dr, success=False, reason='dagrun_timeout',
                                    session=session)
                timeouted_runs += 1
        session.commit()
        if len(active_runs) - timeouted_runs >= dag.max_active_runs:
            return None

        # this query should be replaced by find dagrun
        qry = (
            session.query(func.max(DagRun.execution_date))
            .filter_by(dag_id=dag.dag_id)
            .filter(or_(
                DagRun.external_trigger == False,  # noqa: E712 pylint: disable=singleton-comparison
                DagRun.run_type == DagRunType.SCHEDULED.value
            ))
        )
        last_scheduled_run = qry.scalar()

        # don't schedule @once again
        if dag.schedule_interval == '@once' and last_scheduled_run:
            return None

        # don't do scheduler catchup for dag's that don't have dag.catchup = True
        if not (dag.catchup or dag.schedule_interval == '@once'):
            # The logic is that we move start_date up until
            # one period before, so that timezone.utcnow() is AFTER
            # the period end, and the job can be created...
            now = timezone.utcnow()
            next_start = dag.following_schedule(now)
            last_start = dag.previous_schedule(now)
            if next_start <= now or isinstance(dag.schedule_interval, timedelta):
                new_start = last_start
            else:
                new_start = dag.previous_schedule(last_start)

            if dag.start_date:
                if new_start >= dag.start_date:
                    dag.start_date = new_start
            else:
                dag.start_date = new_start

        next_run_date = None
        if not last_scheduled_run:
            # First run
            task_start_dates = [t.start_date for t in dag.tasks]
            if task_start_dates:
                next_run_date = dag.normalize_schedule(min(task_start_dates))
                self.log.debug(
                    "Next run date based on tasks %s",
                    next_run_date
                )
        else:
            next_run_date = dag.following_schedule(last_scheduled_run)

        # make sure backfills are also considered
        last_run = dag.get_last_dagrun(session=session)
        if last_run and next_run_date:
            while next_run_date <= last_run.execution_date:
                next_run_date = dag.following_schedule(next_run_date)

        # don't ever schedule prior to the dag's start_date
        if dag.start_date:
            next_run_date = (dag.start_date if not next_run_date
                             else max(next_run_date, dag.start_date))
            if next_run_date == dag.start_date:
                next_run_date = dag.normalize_schedule(dag.start_date)

            self.log.debug(
                "Dag start date: %s. Next run date: %s",
                dag.start_date, next_run_date
            )

        # don't ever schedule in the future or if next_run_date is None
        if not next_run_date or next_run_date > timezone.utcnow():
            return None

        # this structure is necessary to avoid a TypeError from concatenating
        # NoneType
        period_end = None
        if dag.schedule_interval == '@once':
            period_end = next_run_date
        elif next_run_date:
            period_end = dag.following_schedule(next_run_date)

        # Don't schedule a dag beyond its end_date (as specified by the dag param)
        if next_run_date and dag.end_date and next_run_date > dag.end_date:
            return None

        # Don't schedule a dag beyond its end_date (as specified by the task params)
        # Get the min task end date, which may come from the dag.default_args
        min_task_end_date = min([t.end_date for t in dag.tasks if t.end_date], default=None)
        if next_run_date and min_task_end_date and next_run_date > min_task_end_date:
            return None

        if next_run_date and period_end and period_end <= timezone.utcnow():
            next_run = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                execution_date=next_run_date,
                start_date=timezone.utcnow(),
                state=State.RUNNING,
                external_trigger=False
            )
            return next_run

        return None

    @provide_session
    def _process_task_instances(
        self, dag: DAG, dag_runs: List[DagRun], session=None
    ) -> List[TaskInstanceKeyType]:
        """
        This method schedules the tasks for a single DAG by looking at the
        active DAG runs and adding task instances that should run to the
        queue.
        """

        # update the state of the previously active dag runs
        active_dag_runs = 0
        task_instances_list = []
        for run in dag_runs:
            self.log.info("Examining DAG run %s", run)
            # don't consider runs that are executed in the future unless
            # specified by config and schedule_interval is None
            if run.execution_date > timezone.utcnow() and not dag.allow_future_exec_dates:
                self.log.error(
                    "Execution date is in future: %s",
                    run.execution_date
                )
                continue

            if active_dag_runs >= dag.max_active_runs:
                self.log.info("Number of active dag runs reached max_active_run.")
                break

            # skip backfill dagruns for now as long as they are not really scheduled
            if run.is_backfill:
                continue

            # todo: run.dag is transient but needs to be set
            run.dag = dag  # type: ignore
            # todo: preferably the integrity check happens at dag collection time
            run.verify_integrity(session=session)
            ready_tis = run.update_state(session=session)
            if run.state == State.RUNNING:
                active_dag_runs += 1
                self.log.debug("Examining active DAG run: %s", run)
                for ti in ready_tis:
                    self.log.debug('Queuing task: %s', ti)
                    task_instances_list.append(ti.key)
        return task_instances_list

    @provide_session
    def _process_dags(self, dags: List[DAG], session=None):
        """
        Iterates over the dags and processes them. Processing includes:

        1. Create appropriate DagRun(s) in the DB.
        2. Create appropriate TaskInstance(s) in the DB.
        3. Send emails for tasks that have missed SLAs (if CHECK_SLAS config enabled).

        :param dags: the DAGs from the DagBag to process
        :type dags: List[airflow.models.DAG]
        :rtype: list[TaskInstance]
        :return: A list of generated TaskInstance objects
        """
        check_slas = conf.getboolean('core', 'CHECK_SLAS', fallback=True)
        use_job_schedule = conf.getboolean('scheduler', 'USE_JOB_SCHEDULE')

        # pylint: disable=too-many-nested-blocks
        tis_out: List[TaskInstanceKeyType] = []
        dag_ids = [dag.dag_id for dag in dags]
        dag_runs = DagRun.find(dag_id=dag_ids, state=State.RUNNING, session=session)
        # As per the docs of groupby (https://docs.python.org/3/library/itertools.html#itertools.groupby)
        # we need to use `list()` otherwise the result will be wrong/incomplete
        dag_runs_by_dag_id = {k: list(v) for k, v in groupby(dag_runs, lambda d: d.dag_id)}

        for dag in dags:
            dag_id = dag.dag_id
            self.log.info("Processing %s", dag_id)
            dag_runs_for_dag = dag_runs_by_dag_id.get(dag_id) or []

            # Only creates DagRun for DAGs that are not subdag since
            # DagRun of subdags are created when SubDagOperator executes.
            if not dag.is_subdag and use_job_schedule:
                dag_run = self.create_dag_run(dag, dag_runs=dag_runs_for_dag)
                if dag_run:
                    dag_runs_for_dag.append(dag_run)
                    expected_start_date = dag.following_schedule(dag_run.execution_date)
                    if expected_start_date:
                        schedule_delay = dag_run.start_date - expected_start_date
                        Stats.timing(
                            'dagrun.schedule_delay.{dag_id}'.format(dag_id=dag.dag_id),
                            schedule_delay)
                    self.log.info("Created %s", dag_run)

            if dag_runs_for_dag:
                tis_out.extend(self._process_task_instances(dag, dag_runs_for_dag))
                if check_slas:
                    self.manage_slas(dag)

        return tis_out

    def _find_dags_to_process(self, dags: List[DAG]) -> List[DAG]:
        """
        Find the DAGs that are not paused to process.

        :param dags: specified DAGs
        :return: DAGs to process
        """
        if self.dag_ids:
            dags = [dag for dag in dags
                    if dag.dag_id in self.dag_ids]
        return dags

    @provide_session
    def execute_on_failure_callbacks(self, dagbag, failure_callback_requests, session=None):
        """
        Execute on failure callbacks. These objects can come from SchedulerJob or from
        DagFileProcessorManager.

        :param failure_callback_requests: failure callbacks to execute
        :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
        :param session: DB session.
        """
        for request in failure_callback_requests:
            simple_ti = request.simple_task_instance
            if simple_ti.dag_id in dagbag.dags:
                dag = dagbag.dags[simple_ti.dag_id]
                if simple_ti.task_id in dag.task_ids:
                    task = dag.get_task(simple_ti.task_id)
                    ti = TI(task, simple_ti.execution_date)
                    # Get properties needed for failure handling from SimpleTaskInstance.
                    ti.start_date = simple_ti.start_date
                    ti.end_date = simple_ti.end_date
                    ti.try_number = simple_ti.try_number
                    ti.state = simple_ti.state
                    ti.test_mode = self.UNIT_TEST_MODE
                    ti.handle_failure(request.msg, ti.test_mode, ti.get_template_context())
                    self.log.info('Executed failure callback for %s in state %s', ti, ti.state)
        session.commit()

    @provide_session
    def process_file(
        self, file_path, failure_callback_requests, pickle_dags=False, session=None
    ) -> Tuple[List[SimpleDag], int]:
        """
        Process a Python file containing Airflow DAGs.

        This includes:

        1. Execute the file and look for DAG objects in the namespace.
        2. Pickle the DAG and save it to the DB (if necessary).
        3. For each DAG, see what tasks should run and create appropriate task
        instances in the DB.
        4. Record any errors importing the file into ORM
        5. Kill (in ORM) any task instances belonging to the DAGs that haven't
        issued a heartbeat in a while.

        Returns a list of SimpleDag objects that represent the DAGs found in
        the file

        :param file_path: the path to the Python file that should be executed
        :type file_path: str
        :param failure_callback_requests: failure callback to execute
        :type failure_callback_requests: List[airflow.utils.dag_processing.FailureCallbackRequest]
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: a tuple with list of SimpleDags made from the Dags found in the file and
            count of import errors.
        :rtype: Tuple[List[SimpleDag], int]
        """
        self.log.info("Processing file %s for tasks to queue", file_path)

        try:
            dagbag = models.DagBag(file_path, include_examples=False)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Failed at reloading the DAG file %s", file_path)
            Stats.incr('dag_file_refresh_error', 1, 1)
            return [], 0

        if len(dagbag.dags) > 0:
            self.log.info("DAG(s) %s retrieved from %s", dagbag.dags.keys(), file_path)
        else:
            self.log.warning("No viable dags retrieved from %s", file_path)
            self.update_import_errors(session, dagbag)
            return [], len(dagbag.import_errors)

        try:
            self.execute_on_failure_callbacks(dagbag, failure_callback_requests)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Error executing failure callback!")

        # Save individual DAGs in the ORM and update DagModel.last_scheduled_time
        dagbag.sync_to_db()

        paused_dag_ids = DagModel.get_paused_dag_ids(dag_ids=dagbag.dag_ids)

        unpaused_dags = [dag for dag_id, dag in dagbag.dags.items() if dag_id not in paused_dag_ids]

        simple_dags = self._prepare_simple_dags(unpaused_dags, pickle_dags, session)

        dags = self._find_dags_to_process(unpaused_dags)

        ti_keys_to_schedule = self._process_dags(dags, session)

        self._schedule_task_instances(dagbag, ti_keys_to_schedule, session)

        # Record import errors into the ORM
        try:
            self.update_import_errors(session, dagbag)
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Error logging import errors!")

        return simple_dags, len(dagbag.import_errors)

    @provide_session
    def _schedule_task_instances(
        self,
        dagbag: models.DagBag,
        ti_keys_to_schedule: List[TaskInstanceKeyType],
        session=None
    ) -> None:
        """
        Checks whether the tasks specified by `ti_keys_to_schedule` parameter can be scheduled and
        updates the information in the database,

        :param dagbag: DagBag
        :type dagbag: models.DagBag
        :param ti_keys_to_schedule: List of task instnace keys which can be scheduled.
        :type ti_keys_to_schedule: list
        """
        # Refresh all task instances that will be scheduled
        filter_for_tis = TI.filter_for_tis(ti_keys_to_schedule)

        refreshed_tis: List[TI] = []

        if filter_for_tis is not None:
            refreshed_tis = session.query(TI).filter(filter_for_tis).with_for_update().all()

        for ti in refreshed_tis:
            # Add task to task instance
            dag = dagbag.dags[ti.key[0]]
            ti.task = dag.get_task(ti.key[1])

            # We check only deps needed to set TI to SCHEDULED state here.
            # Deps needed to set TI to QUEUED state will be batch checked later
            # by the scheduler for better performance.
            dep_context = DepContext(deps=SCHEDULED_DEPS, ignore_task_deps=True)

            # Only schedule tasks that have their dependencies met, e.g. to avoid
            # a task that recently got its state changed to RUNNING from somewhere
            # other than the scheduler from getting its state overwritten.
            if ti.are_dependencies_met(
                dep_context=dep_context,
                session=session,
                verbose=True
            ):
                # Task starts out in the scheduled state. All tasks in the
                # scheduled state will be sent to the executor
                ti.state = State.SCHEDULED
                # If the task is dummy, then mark it as done automatically
                if isinstance(ti.task, DummyOperator) \
                        and not ti.task.on_execute_callback \
                        and not ti.task.on_success_callback:
                    ti.state = State.SUCCESS
                    ti.start_date = ti.end_date = timezone.utcnow()
                    ti.duration = 0

            # Also save this task instance to the DB.
            self.log.info("Creating / updating %s in ORM", ti)
            session.merge(ti)
        # commit batch
        session.commit()

    @provide_session
    def _prepare_simple_dags(self, dags: List[DAG], pickle_dags: bool, session=None) -> List[SimpleDag]:
        """
        Convert DAGS to  SimpleDags. If necessary, it also Pickle the DAGs

        :param dags: List of DAGs
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: List of SimpleDag
        :rtype: List[airflow.utils.dag_processing.SimpleDag]
        """

        simple_dags = []
        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag in dags:
            pickle_id = dag.pickle(session).id if pickle_dags else None
            simple_dags.append(SimpleDag(dag, pickle_id=pickle_id))
        return simple_dags


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs for a specific time interval and schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and sees if the dependencies for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.

    :param dag_id: if specified, only schedule tasks with this DAG ID
    :type dag_id: str
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :type dag_ids: list[str]
    :param subdir: directory containing Python files with Airflow DAG
        definitions, or a specific path to a file
    :type subdir: str
    :param num_runs: The number of times to try to schedule each DAG file.
        -1 for unlimited times.
    :type num_runs: int
    :param processor_poll_interval: The number of seconds to wait between
        polls of running processors
    :type processor_poll_interval: int
    :param do_pickle: once a DAG object is obtained by executing the Python
        file, whether to serialize the DAG object to the DB
    :type do_pickle: bool
    """

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }
    heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')

    def __init__(
            self,
            dag_id: Optional[str] = None,
            dag_ids: Optional[List[str]] = None,
            subdir: str = settings.DAGS_FOLDER,
            num_runs: int = conf.getint('scheduler', 'num_runs'),
            processor_poll_interval: float = conf.getfloat('scheduler', 'processor_poll_interval'),
            do_pickle: bool = False,
            log: Any = None,
            *args, **kwargs):
        # for BaseJob compatibility
        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)

        self.subdir = subdir

        self.num_runs = num_runs
        self._processor_poll_interval = processor_poll_interval

        self.do_pickle = do_pickle
        super().__init__(*args, **kwargs)

        if log:
            self._log = log

        # Check what SQL backend we use
        sql_conn = conf.get('core', 'sql_alchemy_conn').lower()
        self.using_sqlite = sql_conn.startswith('sqlite')
        self.using_mysql = sql_conn.startswith('mysql')

        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        self.processor_agent = None

    def register_exit_signals(self):
        """
        Register signals that stop child processes
        """
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):  # pylint: disable=unused-argument
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    def is_alive(self, grace_multiplier=None):
        """
        Is this SchedulerJob alive?

        We define alive as in a state of running and a heartbeat within the
        threshold defined in the ``scheduler_health_check_threshold`` config
        setting.

        ``grace_multiplier`` is accepted for compatibility with the parent class.

        :rtype: boolean
        """
        if grace_multiplier is not None:
            # Accept the same behaviour as superclass
            return super().is_alive(grace_multiplier=grace_multiplier)
        scheduler_health_check_threshold = conf.getint('scheduler', 'scheduler_health_check_threshold')
        return (
            self.state == State.RUNNING and
            (timezone.utcnow() - self.latest_heartbeat).total_seconds() < scheduler_health_check_threshold
        )

    @provide_session
    def _change_state_for_tis_without_dagrun(
        self,
        simple_dag_bag: SimpleDagBag,
        old_states: List[State],
        new_state: State,
        session=None
    ) -> None:
        """
        For all DAG IDs in the SimpleDagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        does not exist or exists but is not in the running state. This
        normally should not happen, but it can if the state of DagRuns are
        changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_states: list[airflow.utils.state.State]
        :param new_state: set TaskInstances to this state
        :type new_state: airflow.utils.state.State
        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag and with states in the old_states will be examined
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        tis_changed = 0
        query = session \
            .query(models.TaskInstance) \
            .outerjoin(models.DagRun, and_(
                models.TaskInstance.dag_id == models.DagRun.dag_id,
                models.TaskInstance.execution_date == models.DagRun.execution_date)) \
            .filter(models.TaskInstance.dag_id.in_(simple_dag_bag.dag_ids)) \
            .filter(models.TaskInstance.state.in_(old_states)) \
            .filter(or_(
                # pylint: disable=comparison-with-callable
                models.DagRun.state != State.RUNNING,
                models.DagRun.state.is_(None)))  # pylint: disable=no-member
        # We need to do this for mysql as well because it can cause deadlocks
        # as discussed in https://issues.apache.org/jira/browse/AIRFLOW-2516
        if self.using_sqlite or self.using_mysql:
            tis_to_change = query.with_for_update().all()
            for ti in tis_to_change:
                ti.set_state(new_state, session=session)
                tis_changed += 1
        else:
            subq = query.subquery()
            tis_changed = session \
                .query(models.TaskInstance) \
                .filter(and_(
                    models.TaskInstance.dag_id == subq.c.dag_id,
                    models.TaskInstance.task_id == subq.c.task_id,
                    models.TaskInstance.execution_date ==
                    subq.c.execution_date)) \
                .update({models.TaskInstance.state: new_state}, synchronize_session=False)
            session.commit()

        if tis_changed > 0:
            self.log.warning(
                "Set %s task instances to state=%s as their associated DagRun was not in RUNNING state",
                tis_changed, new_state
            )
            Stats.gauge('scheduler.tasks.without_dagrun', tis_changed)

    @provide_session
    def __get_concurrency_maps(self, states: List[State], session=None):
        """
        Get the concurrency maps.

        :param states: List of states to query for
        :type states: list[airflow.utils.state.State]
        :return: A map from (dag_id, task_id) to # of task instances and
         a map from (dag_id, task_id) to # of task instances in the given state list
        :rtype: dict[tuple[str, str], int]
        """
        ti_concurrency_query = (
            session
            .query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        dag_map: Dict[str, int] = defaultdict(int)
        task_map: Dict[Tuple[str, str], int] = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            dag_map[dag_id] += count
            task_map[(dag_id, task_id)] = count
        return dag_map, task_map

    # pylint: disable=too-many-locals,too-many-statements
    @provide_session
    def _find_executable_task_instances(self, simple_dag_bag: SimpleDagBag, session=None):
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag concurrency, executor state, and priority.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :return: list[airflow.models.TaskInstance]
        """
        executable_tis: List[TI] = []

        # Get all task instances associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        task_instances_to_examine = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(simple_dag_bag.dag_ids))
            .outerjoin(
                DR, and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
            )
            .filter(or_(DR.run_id.is_(None), DR.run_type != DagRunType.BACKFILL_JOB.value))
            .outerjoin(DM, DM.dag_id == TI.dag_id)
            .filter(or_(DM.dag_id.is_(None), not_(DM.is_paused)))
            .filter(TI.state == State.SCHEDULED)
            .all()
        )
        Stats.gauge('scheduler.tasks.pending', len(task_instances_to_examine))

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(
            [repr(x) for x in task_instances_to_examine])
        self.log.info(
            "%s tasks up for execution:\n\t%s", len(task_instances_to_examine),
            task_instance_str
        )

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # dag_id to # of running tasks and (dag_id, task_id) to # of running tasks.
        dag_concurrency_map, task_concurrency_map = self.__get_concurrency_maps(
            states=list(EXECUTION_STATES), session=session)

        num_tasks_in_executor = 0
        # Number of tasks that cannot be scheduled because of no open slot in pool
        num_starving_tasks_total = 0

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        # pylint: disable=too-many-nested-blocks
        for pool, task_instances in pool_to_task_instances.items():
            pool_name = pool
            if pool not in pools:
                self.log.warning(
                    "Tasks using non-existent pool '%s' will not be scheduled",
                    pool
                )
                continue

            open_slots = pools[pool].open_slots(session=session)

            num_ready = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name=%s) with %s open slots "
                "and %s task instances ready to be queued",
                pool, open_slots, num_ready
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date))

            num_starving_tasks = 0
            for current_index, task_instance in enumerate(priority_sorted_task_instances):
                if open_slots <= 0:
                    self.log.info(
                        "Not scheduling since there are %s open slots in pool %s",
                        open_slots, pool
                    )
                    # Can't schedule any more since there are no more open slots.
                    num_unhandled = len(priority_sorted_task_instances) - current_index
                    num_starving_tasks += num_unhandled
                    num_starving_tasks_total += num_unhandled
                    break

                # Check to make sure that the task concurrency of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id
                simple_dag = simple_dag_bag.get_dag(dag_id)

                current_dag_concurrency = dag_concurrency_map[dag_id]
                dag_concurrency_limit = simple_dag_bag.get_dag(dag_id).concurrency
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id, current_dag_concurrency, dag_concurrency_limit
                )
                if current_dag_concurrency >= dag_concurrency_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's task concurrency limit of %s",
                        task_instance, dag_id, dag_concurrency_limit
                    )
                    continue

                task_concurrency_limit = simple_dag.get_task_special_arg(
                    task_instance.task_id,
                    'task_concurrency')
                if task_concurrency_limit is not None:
                    current_task_concurrency = task_concurrency_map[
                        (task_instance.dag_id, task_instance.task_id)
                    ]

                    if current_task_concurrency >= task_concurrency_limit:
                        self.log.info("Not executing %s since the task concurrency for"
                                      " this task has been reached.", task_instance)
                        continue

                if self.executor.has_task(task_instance):
                    self.log.debug(
                        "Not handling task %s as the executor reports it is running",
                        task_instance.key
                    )
                    num_tasks_in_executor += 1
                    continue

                if task_instance.pool_slots > open_slots:
                    self.log.info("Not executing %s since it requires %s slots "
                                  "but there are %s open slots in the pool %s.",
                                  task_instance, task_instance.pool_slots, open_slots, pool)
                    num_starving_tasks += 1
                    num_starving_tasks_total += 1
                    # Though we can execute tasks with lower priority if there's enough room
                    continue

                executable_tis.append(task_instance)
                open_slots -= task_instance.pool_slots
                dag_concurrency_map[dag_id] += 1
                task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

            Stats.gauge(f'pool.starving_tasks.{pool_name}', num_starving_tasks)

        Stats.gauge('scheduler.tasks.starving', num_starving_tasks_total)
        Stats.gauge('scheduler.tasks.running', num_tasks_in_executor)
        Stats.gauge('scheduler.tasks.executable', len(executable_tis))

        task_instance_str = "\n\t".join(
            [repr(x) for x in executable_tis])
        self.log.info(
            "Setting the following tasks to queued state:\n\t%s", task_instance_str)
        # so these dont expire on commit
        for ti in executable_tis:
            copy_dag_id = ti.dag_id
            copy_execution_date = ti.execution_date
            copy_task_id = ti.task_id
            make_transient(ti)
            ti.dag_id = copy_dag_id
            ti.execution_date = copy_execution_date
            ti.task_id = copy_task_id
        return executable_tis

    @provide_session
    def _change_state_for_executable_task_instances(self, task_instances: List[TI], session=None):
        """
        Changes the state of task instances in the list with one of the given states
        to QUEUED atomically, and returns the TIs changed in SimpleTaskInstance format.

        :param task_instances: TaskInstances to change the state of
        :type task_instances: list[airflow.models.TaskInstance]
        :rtype: list[airflow.models.taskinstance.SimpleTaskInstance]
        """
        if len(task_instances) == 0:
            session.commit()
            return []

        tis_to_set_to_queued = (
            session
            .query(TI)
            .filter(TI.filter_for_tis(task_instances))
            .filter(TI.state == State.SCHEDULED)
            .with_for_update()
            .all()
        )

        if len(tis_to_set_to_queued) == 0:
            self.log.info("No tasks were able to have their state changed to queued.")
            session.commit()
            return []

        # set TIs to queued state
        filter_for_tis = TI.filter_for_tis(tis_to_set_to_queued)
        session.query(TI).filter(filter_for_tis).update(
            {TI.state: State.QUEUED, TI.queued_dttm: timezone.utcnow()}, synchronize_session=False
        )
        session.commit()

        # Generate a list of SimpleTaskInstance for the use of queuing
        # them in the executor.
        simple_task_instances = [SimpleTaskInstance(ti) for ti in tis_to_set_to_queued]

        task_instance_str = "\n\t".join([repr(x) for x in tis_to_set_to_queued])
        self.log.info("Setting the following %s tasks to queued state:\n\t%s",
                      len(tis_to_set_to_queued), task_instance_str)
        return simple_task_instances

    def _enqueue_task_instances_with_queued_state(self, simple_dag_bag,
                                                  simple_task_instances):
        """
        Takes task_instances, which should have been set to queued, and enqueues them
        with the executor.

        :param simple_task_instances: TaskInstances to enqueue
        :type simple_task_instances: list[SimpleTaskInstance]
        :param simple_dag_bag: Should contains all of the task_instances' dags
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        """
        # actually enqueue them
        for simple_task_instance in simple_task_instances:
            simple_dag = simple_dag_bag.get_dag(simple_task_instance.dag_id)
            command = TI.generate_command(
                simple_task_instance.dag_id,
                simple_task_instance.task_id,
                simple_task_instance.execution_date,
                local=True,
                mark_success=False,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=simple_task_instance.pool,
                file_path=simple_dag.full_filepath,
                pickle_id=simple_dag.pickle_id,
            )

            priority = simple_task_instance.priority_weight
            queue = simple_task_instance.queue
            self.log.info(
                "Sending %s to executor with priority %s and queue %s",
                simple_task_instance.key, priority, queue
            )

            self.executor.queue_command(
                simple_task_instance,
                command,
                priority=priority,
                queue=queue,
            )

    @provide_session
    def _execute_task_instances(
        self,
        simple_dag_bag: SimpleDagBag,
        session=None
    ) -> int:
        """
        Attempts to execute TaskInstances that should be executed by the scheduler.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: airflow.utils.dag_processing.SimpleDagBag
        :return: Number of task instance with state changed.
        """
        executable_tis = self._find_executable_task_instances(simple_dag_bag, session=session)

        def query(result, items):
            simple_tis_with_state_changed = \
                self._change_state_for_executable_task_instances(items, session=session)
            self._enqueue_task_instances_with_queued_state(
                simple_dag_bag,
                simple_tis_with_state_changed)
            session.commit()
            return result + len(simple_tis_with_state_changed)

        return helpers.reduce_in_chunks(query, executable_tis, 0, self.max_tis_per_query)

    @provide_session
    def _change_state_for_tasks_failed_to_execute(self, session=None):
        """
        If there are tasks left over in the executor,
        we set them back to SCHEDULED to avoid creating hanging tasks.

        :param session: session for ORM operations
        """
        if not self.executor.queued_tasks:
            return

        filter_for_ti_state_change = (
            [and_(
                TI.dag_id == dag_id,
                TI.task_id == task_id,
                TI.execution_date == execution_date,
                # The TI.try_number will return raw try_number+1 since the
                # ti is not running. And we need to -1 to match the DB record.
                TI._try_number == try_number - 1,  # pylint: disable=protected-access
                TI.state == State.QUEUED)
                for dag_id, task_id, execution_date, try_number
                in self.executor.queued_tasks.keys()])
        ti_query = session.query(TI).filter(or_(*filter_for_ti_state_change))
        tis_to_set_to_scheduled = ti_query.with_for_update().all()
        if not tis_to_set_to_scheduled:
            return

        # set TIs to queued state
        filter_for_tis = TI.filter_for_tis(tis_to_set_to_scheduled)
        session.query(TI).filter(filter_for_tis).update(
            {TI.state: State.SCHEDULED, TI.queued_dttm: None}, synchronize_session=False
        )

        for task_instance in tis_to_set_to_scheduled:
            self.executor.queued_tasks.pop(task_instance.key)

        task_instance_str = "\n\t".join(repr(x) for x in tis_to_set_to_scheduled)
        self.log.info("Set the following tasks to scheduled state:\n\t%s", task_instance_str)

    @provide_session
    def _process_executor_events(self, simple_dag_bag, session=None):
        """
        Respond to executor events.
        """
        # TODO: this shares quite a lot of code with _manage_executor_state
        for key, value in self.executor.get_event_buffer(simple_dag_bag.dag_ids).items():
            state, info = value
            dag_id, task_id, execution_date, try_number = key
            self.log.info(
                "Executor reports execution of %s.%s execution_date=%s "
                "exited with status %s for try_number %s",
                dag_id, task_id, execution_date, state, try_number
            )
            if state not in (State.FAILED, State.SUCCESS):
                continue

            # Process finished tasks
            qry = session.query(TI).filter(
                TI.dag_id == dag_id,
                TI.task_id == task_id,
                TI.execution_date == execution_date
            )
            ti = qry.first()
            if not ti:
                self.log.warning("TaskInstance %s went missing from the database", ti)
                continue

            # TODO: should we fail RUNNING as well, as we do in Backfills?
            if ti.try_number == try_number and ti.state == State.QUEUED:
                Stats.incr('scheduler.tasks.killed_externally')
                self.log.error(
                    "Executor reports task instance %s finished (%s) although the task says its %s. "
                    "(Info: %s) Was the task killed externally?",
                    ti, state, ti.state, info
                )
                simple_dag = simple_dag_bag.get_dag(dag_id)
                self.processor_agent.send_callback_to_execute(
                    full_filepath=simple_dag.full_filepath,
                    task_instance=ti,
                    msg=f"Executor reports task instance finished ({state}) although the "
                        f"task says its {ti.state}. (Info: {info}) Was the task killed externally?"
                )

    def _execute(self):
        self.log.info("Starting the scheduler")

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = self.do_pickle and self.executor_class not in UNPICKLEABLE_EXECUTORS

        self.log.info("Processing each file at most %s times", self.num_runs)

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        processor_timeout_seconds = conf.getint('core', 'dag_file_processor_timeout')
        processor_timeout = timedelta(seconds=processor_timeout_seconds)
        self.processor_agent = DagFileProcessorAgent(
            dag_directory=self.subdir,
            max_runs=self.num_runs,
            processor_factory=type(self)._create_dag_file_processor,
            processor_timeout=processor_timeout,
            dag_ids=self.dag_ids,
            pickle_dags=pickle_dags,
            async_mode=async_mode,
        )

        try:
            self.executor.start()

            self.log.info("Resetting orphaned tasks for active dag runs")
            self.reset_state_for_orphaned_tasks()

            self.register_exit_signals()

            # Start after resetting orphaned tasks to avoid stressing out DB.
            self.processor_agent.start()

            execute_start_time = timezone.utcnow()

            self._run_scheduler_loop()

            # Stop any processors
            self.processor_agent.terminate()

            # Verify that all files were processed, and if so, deactivate DAGs that
            # haven't been touched by the scheduler as they likely have been
            # deleted.
            if self.processor_agent.all_files_processed:
                self.log.info(
                    "Deactivating DAGs that haven't been touched since %s",
                    execute_start_time.isoformat()
                )
                models.DAG.deactivate_stale_dags(execute_start_time)

            self.executor.end()

            settings.Session.remove()
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Exception when executing execute_helper")
        finally:
            self.processor_agent.end()
            self.log.info("Exited execute loop")

    @staticmethod
    def _create_dag_file_processor(file_path, failure_callback_requests, dag_ids, pickle_dags):
        """
        Creates DagFileProcessorProcess instance.
        """
        return DagFileProcessorProcess(
            file_path=file_path,
            pickle_dags=pickle_dags,
            dag_ids=dag_ids,
            failure_callback_requests=failure_callback_requests
        )

    def _run_scheduler_loop(self):
        """
        The actual scheduler loop. The main steps in the loop are:
            #. Harvest DAG parsing results through DagFileProcessorAgent
            #. Find and queue executable tasks
                #. Change task instance state in DB
                #. Queue tasks in executor
            #. Heartbeat executor
                #. Execute queued tasks in executor asynchronously
                #. Sync on the states of running tasks

        Following is a graphic representation of these steps.

        .. image:: ../docs/img/scheduler_loop.jpg

        :rtype: None
        """
        is_unit_test = conf.getboolean('core', 'unit_test_mode')

        # For the execute duration, parse and schedule DAGs
        while True:
            loop_start_time = time.time()

            if self.using_sqlite:
                self.processor_agent.run_single_parsing_loop()
                # For the sqlite case w/ 1 thread, wait until the processor
                # is finished to avoid concurrent access to the DB.
                self.log.debug("Waiting for processors to finish since we're using sqlite")
                self.processor_agent.wait_until_finished()

            simple_dags = self.processor_agent.harvest_simple_dags()

            self.log.debug("Harvested %d SimpleDAGs", len(simple_dags))

            # Send tasks for execution if available
            simple_dag_bag = SimpleDagBag(simple_dags)

            if not self._validate_and_run_task_instances(simple_dag_bag=simple_dag_bag):
                continue

            # Heartbeat the scheduler periodically
            self.heartbeat(only_if_necessary=True)

            self._emit_pool_metrics()

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            self.log.debug("Ran scheduling loop in %.2f seconds", loop_duration)

            if not is_unit_test:
                time.sleep(self._processor_poll_interval)

            if self.processor_agent.done:
                self.log.info(
                    "Exiting scheduler loop as all files have been processed %d times", self.num_runs
                )
                break

    def _validate_and_run_task_instances(self, simple_dag_bag: SimpleDagBag) -> bool:
        if simple_dag_bag.simple_dags:
            try:
                self._process_and_execute_tasks(simple_dag_bag)
            except Exception as e:  # pylint: disable=broad-except
                self.log.error("Error queuing tasks")
                self.log.exception(e)
                return False

        # Call heartbeats
        self.log.debug("Heartbeating the executor")
        self.executor.heartbeat()

        self._change_state_for_tasks_failed_to_execute()

        # Process events from the executor
        self._process_executor_events(simple_dag_bag)
        return True

    def _process_and_execute_tasks(self, simple_dag_bag):
        # Handle cases where a DAG run state is set (perhaps manually) to
        # a non-running state. Handle task instances that belong to
        # DAG runs in those states
        # If a task instance is up for retry but the corresponding DAG run
        # isn't running, mark the task instance as FAILED so we don't try
        # to re-run it.
        self._change_state_for_tis_without_dagrun(
            simple_dag_bag=simple_dag_bag,
            old_states=[State.UP_FOR_RETRY],
            new_state=State.FAILED
        )
        # If a task instance is scheduled or queued or up for reschedule,
        # but the corresponding DAG run isn't running, set the state to
        # NONE so we don't try to re-run it.
        self._change_state_for_tis_without_dagrun(
            simple_dag_bag=simple_dag_bag,
            old_states=[State.QUEUED, State.SCHEDULED, State.UP_FOR_RESCHEDULE],
            new_state=State.NONE
        )
        self._execute_task_instances(simple_dag_bag)

    @provide_session
    def _emit_pool_metrics(self, session=None) -> None:
        pools = models.Pool.slots_stats(session)
        for pool_name, slot_stats in pools.items():
            Stats.gauge(f'pool.open_slots.{pool_name}', slot_stats["open"])
            Stats.gauge(f'pool.queued_slots.{pool_name}', slot_stats[State.QUEUED])
            Stats.gauge(f'pool.running_slots.{pool_name}', slot_stats[State.RUNNING])

    @provide_session
    def heartbeat_callback(self, session=None):
        Stats.incr('scheduler_heartbeat', 1, 1)
