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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import getpass
import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from time import sleep

import six
from past.builtins import basestring
from sqlalchemy import (Column, Index, Integer, String, and_, func, not_, or_)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import make_transient

from airflow import configuration as conf
from airflow import executors, models, settings
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.settings import Stats
from airflow.task.task_runner import get_task_runner
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils import asciiart, helpers, timezone
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.dag_processing import (AbstractDagFileProcessor,
                                          DagFileProcessorAgent,
                                          SimpleDag,
                                          SimpleDagBag,
                                          list_py_file_paths)
from airflow.utils.db import create_session, provide_session
from airflow.utils.email import get_email_address_list, send_email
from airflow.utils.log.logging_mixin import LoggingMixin, StreamLogWriter, set_context
from airflow.utils.net import get_hostname
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State

Base = models.Base
ID_LEN = models.ID_LEN


class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have its own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    latest_heartbeat = Column(UtcDateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
        Index('idx_job_state_heartbeat', state, latest_heartbeat),
    )

    def __init__(
            self,
            executor=executors.GetDefaultExecutor(),
            heartrate=conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC'),
            *args, **kwargs):
        self.hostname = get_hostname()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = timezone.utcnow()
        self.latest_heartbeat = timezone.utcnow()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        super(BaseJob, self).__init__(*args, **kwargs)

    def is_alive(self):
        return (
            (timezone.utcnow() - self.latest_heartbeat).seconds <
            (conf.getint('scheduler', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    @provide_session
    def kill(self, session=None):
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = timezone.utcnow()
        try:
            self.on_kill()
        except Exception as e:
            self.log.error('on_kill() method failed: {}'.format(e))
        session.merge(job)
        session.commit()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        """
        Will be called when an external kill command is received
        """
        pass

    def heartbeat_callback(self, session=None):
        pass

    def heartbeat(self):
        """
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        """
        try:
            with create_session() as session:
                job = session.query(BaseJob).filter_by(id=self.id).one()
                make_transient(job)
                session.commit()

            if job.state == State.SHUTDOWN:
                self.kill()

            # Figure out how long to sleep for
            sleep_for = 0
            if job.latest_heartbeat:
                sleep_for = max(
                    0,
                    self.heartrate - (timezone.utcnow() -
                                      job.latest_heartbeat).total_seconds())

            sleep(sleep_for)

            # Update last heartbeat time
            with create_session() as session:
                job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
                job.latest_heartbeat = timezone.utcnow()
                session.merge(job)
                session.commit()

                self.heartbeat_callback(session=session)
                self.log.debug('[heartbeat]')
        except OperationalError as e:
            self.log.error("Scheduler heartbeat got an exception: %s", str(e))

    def run(self):
        Stats.incr(self.__class__.__name__.lower() + '_start', 1, 1)
        # Adding an entry in the DB
        with create_session() as session:
            self.state = State.RUNNING
            session.add(self)
            session.commit()
            id_ = self.id
            make_transient(self)
            self.id = id_

            try:
                self._execute()
                # In case of max runs or max duration
                self.state = State.SUCCESS
            except SystemExit:
                # In case of ^C or SIGTERM
                self.state = State.SUCCESS
            except Exception:
                self.state = State.FAILED
                raise
            finally:
                self.end_date = timezone.utcnow()
                session.merge(self)
                session.commit()

        Stats.incr(self.__class__.__name__.lower() + '_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")

    @provide_session
    def reset_state_for_orphaned_tasks(self, filter_by_dag_run=None, session=None):
        """
        This function checks if there are any tasks in the dagrun (or all)
        that have a scheduled state but are not known by the
        executor. If it finds those it will reset the state to None
        so they will get picked up again.
        The batch option is for performance reasons as the queries are made in
        sequence.

        :param filter_by_dag_run: the dag_run we want to process, None if all
        :type filter_by_dag_run: models.DagRun
        :return: the TIs reset (in expired SQLAlchemy state)
        :rtype: List(TaskInstance)
        """
        queued_tis = self.executor.queued_tasks
        # also consider running as the state might not have changed in the db yet
        running_tis = self.executor.running

        resettable_states = [State.SCHEDULED, State.QUEUED]
        TI = models.TaskInstance
        DR = models.DagRun
        if filter_by_dag_run is None:
            resettable_tis = (
                session
                .query(TI)
                .join(
                    DR,
                    and_(
                        TI.dag_id == DR.dag_id,
                        TI.execution_date == DR.execution_date))
                .filter(
                    DR.state == State.RUNNING,
                    DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'),
                    TI.state.in_(resettable_states))).all()
        else:
            resettable_tis = filter_by_dag_run.get_task_instances(state=resettable_states,
                                                                  session=session)
        tis_to_reset = []
        # Can't use an update here since it doesn't support joins
        for ti in resettable_tis:
            if ti.key not in queued_tis and ti.key not in running_tis:
                tis_to_reset.append(ti)

        if len(tis_to_reset) == 0:
            return []

        def query(result, items):
            filter_for_tis = ([and_(TI.dag_id == ti.dag_id,
                                    TI.task_id == ti.task_id,
                                    TI.execution_date == ti.execution_date)
                               for ti in items])
            reset_tis = (
                session
                .query(TI)
                .filter(or_(*filter_for_tis), TI.state.in_(resettable_states))
                .with_for_update()
                .all())
            for ti in reset_tis:
                ti.state = State.NONE
                session.merge(ti)
            return result + reset_tis

        reset_tis = helpers.reduce_in_chunks(query,
                                             tis_to_reset,
                                             [],
                                             self.max_tis_per_query)

        task_instance_str = '\n\t'.join(
            ["{}".format(x) for x in reset_tis])
        session.commit()

        self.log.info(
            "Reset the following %s TaskInstances:\n\t%s",
            len(reset_tis), task_instance_str
        )
        return reset_tis


class DagFileProcessor(AbstractDagFileProcessor, LoggingMixin):
    """Helps call SchedulerJob.process_file() in a separate process."""

    # Counter that increments everytime an instance of this class is created
    class_creation_counter = 0

    def __init__(self, file_path, pickle_dags, dag_id_white_list, zombies):
        """
        :param file_path: a Python file containing Airflow DAG definitions
        :type file_path: unicode
        :param pickle_dags: whether to serialize the DAG objects to the DB
        :type pickle_dags: bool
        :param dag_id_whitelist: If specified, only look at these DAG ID's
        :type dag_id_whitelist: list[unicode]
        :param zombies: zombie task instances to kill
        :type zombies: list[SimpleTaskInstance]
        """
        self._file_path = file_path
        # Queue that's used to pass results from the child process.
        self._result_queue = multiprocessing.Queue()
        # The process that was launched to process the given .
        self._process = None
        self._dag_id_white_list = dag_id_white_list
        self._pickle_dags = pickle_dags
        self._zombies = zombies
        # The result of Scheduler.process_file(file_path).
        self._result = None
        # Whether the process is done running.
        self._done = False
        # When the process started.
        self._start_time = None
        # This ID is use to uniquely name the process / thread that's launched
        # by this processor instance
        self._instance_id = DagFileProcessor.class_creation_counter
        DagFileProcessor.class_creation_counter += 1

    @property
    def file_path(self):
        return self._file_path

    @staticmethod
    def _launch_process(result_queue,
                        file_path,
                        pickle_dags,
                        dag_id_white_list,
                        thread_name,
                        zombies):
        """
        Launch a process to process the given file.

        :param result_queue: the queue to use for passing back the result
        :type result_queue: multiprocessing.Queue
        :param file_path: the file to process
        :type file_path: unicode
        :param pickle_dags: whether to pickle the DAGs found in the file and
            save them to the DB
        :type pickle_dags: bool
        :param dag_id_white_list: if specified, only examine DAG ID's that are
            in this list
        :type dag_id_white_list: list[unicode]
        :param thread_name: the name to use for the process that is launched
        :type thread_name: unicode
        :return: the process that was launched
        :rtype: multiprocessing.Process
        :param zombies: zombie task instances to kill
        :type zombies: list[SimpleTaskInstance]
        """
        def helper():
            # This helper runs in the newly created process
            log = logging.getLogger("airflow.processor")

            stdout = StreamLogWriter(log, logging.INFO)
            stderr = StreamLogWriter(log, logging.WARN)

            set_context(log, file_path)

            try:
                # redirect stdout/stderr to log
                sys.stdout = stdout
                sys.stderr = stderr

                # Re-configure the ORM engine as there are issues with multiple processes
                settings.configure_orm()

                # Change the thread name to differentiate log lines. This is
                # really a separate process, but changing the name of the
                # process doesn't work, so changing the thread name instead.
                threading.current_thread().name = thread_name
                start_time = time.time()

                log.info("Started process (PID=%s) to work on %s",
                         os.getpid(), file_path)
                scheduler_job = SchedulerJob(dag_ids=dag_id_white_list, log=log)
                result = scheduler_job.process_file(file_path,
                                                    zombies,
                                                    pickle_dags)
                result_queue.put(result)
                end_time = time.time()
                log.info(
                    "Processing %s took %.3f seconds", file_path, end_time - start_time
                )
            except Exception:
                # Log exceptions through the logging framework.
                log.exception("Got an exception! Propagating...")
                raise
            finally:
                sys.stdout = sys.__stdout__
                sys.stderr = sys.__stderr__
                # We re-initialized the ORM within this Process above so we need to
                # tear it down manually here
                settings.dispose_orm()

        p = multiprocessing.Process(target=helper,
                                    args=(),
                                    name="{}-Process".format(thread_name))
        p.start()
        return p

    def start(self):
        """
        Launch the process and start processing the DAG.
        """
        self._process = DagFileProcessor._launch_process(
            self._result_queue,
            self.file_path,
            self._pickle_dags,
            self._dag_id_white_list,
            "DagFileProcessor{}".format(self._instance_id),
            self._zombies)
        self._start_time = timezone.utcnow()

    def terminate(self, sigkill=False):
        """
        Terminate (and then kill) the process launched to process the file.

        :param sigkill: whether to issue a SIGKILL if SIGTERM doesn't work.
        :type sigkill: bool
        """
        if self._process is None:
            raise AirflowException("Tried to call stop before starting!")
        # The queue will likely get corrupted, so remove the reference
        self._result_queue = None
        self._process.terminate()
        # Arbitrarily wait 5s for the process to die
        self._process.join(5)
        if sigkill and self._process.is_alive():
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

        # In case result queue is corrupted.
        if self._result_queue and not self._result_queue.empty():
            self._result = self._result_queue.get_nowait()
            self._done = True
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        # Potential error case when process dies
        if self._result_queue and not self._process.is_alive():
            self._done = True
            # Get the object from the queue or else join() can hang.
            if not self._result_queue.empty():
                self._result = self._result_queue.get_nowait()
            self.log.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        return False

    @property
    def result(self):
        """
        :return: result of running SchedulerJob.process_file()
        :rtype: SimpleDag
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


class SchedulerJob(BaseJob):
    """
    This SchedulerJob runs for a specific time interval and schedules the jobs
    that are ready to run. It figures out the latest runs for each
    task and sees if the dependencies for the next schedules are met.
    If so, it creates appropriate TaskInstances and sends run commands to the
    executor. It does this for each task in each DAG and repeats.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            subdir=settings.DAGS_FOLDER,
            num_runs=-1,
            processor_poll_interval=1.0,
            run_duration=None,
            do_pickle=False,
            log=None,
            *args, **kwargs):
        """
        :param dag_id: if specified, only schedule tasks with this DAG ID
        :type dag_id: unicode
        :param dag_ids: if specified, only schedule tasks with these DAG IDs
        :type dag_ids: list[unicode]
        :param subdir: directory containing Python files with Airflow DAG
            definitions, or a specific path to a file
        :type subdir: unicode
        :param num_runs: The number of times to try to schedule each DAG file.
            -1 for unlimited within the run_duration.
        :type num_runs: int
        :param processor_poll_interval: The number of seconds to wait between
            polls of running processors
        :type processor_poll_interval: int
        :param run_duration: how long to run (in seconds) before exiting
        :type run_duration: int
        :param do_pickle: once a DAG object is obtained by executing the Python
            file, whether to serialize the DAG object to the DB
        :type do_pickle: bool
        """
        # for BaseJob compatibility
        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)

        self.subdir = subdir

        self.num_runs = num_runs
        self.run_duration = run_duration
        self._processor_poll_interval = processor_poll_interval

        self.do_pickle = do_pickle
        super(SchedulerJob, self).__init__(*args, **kwargs)

        self.heartrate = conf.getint('scheduler', 'SCHEDULER_HEARTBEAT_SEC')
        self.max_threads = conf.getint('scheduler', 'max_threads')

        if log:
            self._log = log

        self.using_sqlite = False
        if 'sqlite' in conf.get('core', 'sql_alchemy_conn'):
            self.using_sqlite = True

        self.max_tis_per_query = conf.getint('scheduler', 'max_tis_per_query')
        if run_duration is None:
            self.run_duration = conf.getint('scheduler',
                                            'run_duration')

        self.processor_agent = None

        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):
        """
        Helper method to clean up processor_agent to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal {}".format(signum))
        if self.processor_agent:
            self.processor_agent.end()
        sys.exit(os.EX_OK)

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        Where assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        if not any([ti.sla for ti in dag.tasks]):
            self.log.info(
                "Skipping SLA check for %s because no tasks in DAG have SLAs",
                dag
            )
            return

        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .with_hint(TI, 'USE INDEX (PRIMARY)', dialect_name='mysql')
            .filter(TI.dag_id == dag.dag_id)
            .filter(or_(
                TI.state == State.SUCCESS,
                TI.state == State.SKIPPED))
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = timezone.utcnow()
        SlaMiss = models.SlaMiss
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if task.sla:
                dttm = dag.following_schedule(dttm)
                while dttm < timezone.utcnow():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < timezone.utcnow():
                        session.merge(models.SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.notification_sent == False)  # noqa: E712
            .filter(SlaMiss.dag_id == dag.dag_id)
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(TI.state != State.SUCCESS)
                .filter(TI.execution_date.in_(sla_dates))
                .filter(TI.dag_id == dag.dag_id)
                .all()
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
                except Exception:
                    self.log.exception("Could not call sla_miss_callback for DAG %s",
                                       dag.dag_id)
            email_content = """\
            Here's a list of tasks that missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(bug=asciiart.bug, **locals())
            emails = set()
            for task in dag.tasks:
                if task.email:
                    if isinstance(task.email, basestring):
                        emails |= set(get_email_address_list(task.email))
                    elif isinstance(task.email, (list, tuple)):
                        emails |= set(task.email)
            if emails and len(slas):
                try:
                    send_email(
                        emails,
                        "[airflow] SLA miss on DAG=" + dag.dag_id,
                        email_content)
                    email_sent = True
                    notification_sent = True
                except Exception:
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
        :type dagbag: models.Dagbag
        """
        # Clear the errors of the processed files
        for dagbag_file in dagbag.file_last_changed:
            session.query(models.ImportError).filter(
                models.ImportError.filename == dagbag_file
            ).delete()

        # Add the errors of the processed files
        for filename, stacktrace in six.iteritems(dagbag.import_errors):
            session.add(models.ImportError(
                filename=filename,
                stacktrace=stacktrace))
        session.commit()

    @provide_session
    def create_dag_run(self, dag, session=None):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval.
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        if dag.schedule_interval and conf.getboolean('scheduler', 'USE_JOB_SCHEDULE'):
            active_runs = DagRun.find(
                dag_id=dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )
            # return if already reached maximum active runs and no timeout setting
            if len(active_runs) >= dag.max_active_runs and not dag.dagrun_timeout:
                return
            timedout_runs = 0
            for dr in active_runs:
                if (
                        dr.start_date and dag.dagrun_timeout and
                        dr.start_date < timezone.utcnow() - dag.dagrun_timeout):
                    dr.state = State.FAILED
                    dr.end_date = timezone.utcnow()
                    dag.handle_callback(dr, success=False, reason='dagrun_timeout',
                                        session=session)
                    timedout_runs += 1
            session.commit()
            if len(active_runs) - timedout_runs >= dag.max_active_runs:
                return

            # this query should be replaced by find dagrun
            qry = (
                session.query(func.max(DagRun.execution_date))
                .filter_by(dag_id=dag.dag_id)
                .filter(or_(
                    DagRun.external_trigger == False,  # noqa: E712
                    # add % as a wildcard for the like query
                    DagRun.run_id.like(DagRun.ID_PREFIX + '%')
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
                if next_start <= now:
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

            # don't ever schedule in the future
            if next_run_date > timezone.utcnow():
                return

            # this structure is necessary to avoid a TypeError from concatenating
            # NoneType
            if dag.schedule_interval == '@once':
                period_end = next_run_date
            elif next_run_date:
                period_end = dag.following_schedule(next_run_date)

            # Don't schedule a dag beyond its end_date (as specified by the dag param)
            if next_run_date and dag.end_date and next_run_date > dag.end_date:
                return

            # Don't schedule a dag beyond its end_date (as specified by the task params)
            # Get the min task end date, which may come from the dag.default_args
            min_task_end_date = []
            task_end_dates = [t.end_date for t in dag.tasks if t.end_date]
            if task_end_dates:
                min_task_end_date = min(task_end_dates)
            if next_run_date and min_task_end_date and next_run_date > min_task_end_date:
                return

            if next_run_date and period_end and period_end <= timezone.utcnow():
                next_run = dag.create_dagrun(
                    run_id=DagRun.ID_PREFIX + next_run_date.isoformat(),
                    execution_date=next_run_date,
                    start_date=timezone.utcnow(),
                    state=State.RUNNING,
                    external_trigger=False
                )
                return next_run

    @provide_session
    def _process_task_instances(self, dag, queue, session=None):
        """
        This method schedules the tasks for a single DAG by looking at the
        active DAG runs and adding task instances that should run to the
        queue.
        """

        # update the state of the previously active dag runs
        dag_runs = DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)
        active_dag_runs = []
        for run in dag_runs:
            self.log.info("Examining DAG run %s", run)
            # don't consider runs that are executed in the future
            if run.execution_date > timezone.utcnow():
                self.log.error(
                    "Execution date is in future: %s",
                    run.execution_date
                )
                continue

            if len(active_dag_runs) >= dag.max_active_runs:
                self.log.info("Active dag runs > max_active_run.")
                continue

            # skip backfill dagruns for now as long as they are not really scheduled
            if run.is_backfill:
                continue

            # todo: run.dag is transient but needs to be set
            run.dag = dag
            # todo: preferably the integrity check happens at dag collection time
            run.verify_integrity(session=session)
            run.update_state(session=session)
            if run.state == State.RUNNING:
                make_transient(run)
                active_dag_runs.append(run)

        for run in active_dag_runs:
            self.log.debug("Examining active DAG run: %s", run)
            # this needs a fresh session sometimes tis get detached
            tis = run.get_task_instances(state=(State.NONE,
                                                State.UP_FOR_RETRY))

            # this loop is quite slow as it uses are_dependencies_met for
            # every task (in ti.is_runnable). This is also called in
            # update_state above which has already checked these tasks
            for ti in tis:
                task = dag.get_task(ti.task_id)

                # fixme: ti.task is transient but needs to be set
                ti.task = task

                # future: remove adhoc
                if task.adhoc:
                    continue

                if ti.are_dependencies_met(
                        dep_context=DepContext(flag_upstream_failed=True),
                        session=session):
                    self.log.debug('Queuing task: %s', ti)
                    queue.append(ti.key)

    @provide_session
    def _change_state_for_tis_without_dagrun(self,
                                             simple_dag_bag,
                                             old_states,
                                             new_state,
                                             session=None):
        """
        For all DAG IDs in the SimpleDagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        does not exist or exists but is not in the running state. This
        normally should not happen, but it can if the state of DagRuns are
        changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_state: list[State]
        :param new_state: set TaskInstances to this state
        :type new_state: State
        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag and with states in the old_state will be examined
        :type simple_dag_bag: SimpleDagBag
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
                models.DagRun.state != State.RUNNING,
                models.DagRun.state.is_(None)))
        if self.using_sqlite:
            tis_to_change = query \
                .with_for_update() \
                .all()
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
                .update({models.TaskInstance.state: new_state},
                        synchronize_session=False)
            session.commit()

        if tis_changed > 0:
            self.log.warning(
                "Set %s task instances to state=%s as their associated DagRun was not in RUNNING state",
                tis_changed, new_state
            )

    @provide_session
    def __get_task_concurrency_map(self, states, session=None):
        """
        Returns a map from tasks to number in the states list given.

        :param states: List of states to query for
        :type states: List[State]
        :return: A map from (dag_id, task_id) to count of tasks in states
        :rtype: Dict[[String, String], Int]

        """
        TI = models.TaskInstance
        ti_concurrency_query = (
            session
            .query(TI.task_id, TI.dag_id, func.count('*'))
            .filter(TI.state.in_(states))
            .group_by(TI.task_id, TI.dag_id)
        ).all()
        task_map = defaultdict(int)
        for result in ti_concurrency_query:
            task_id, dag_id, count = result
            task_map[(dag_id, task_id)] = count
        return task_map

    @provide_session
    def _find_executable_task_instances(self, simple_dag_bag, states, session=None):
        """
        Finds TIs that are ready for execution with respect to pool limits,
        dag concurrency, executor state, and priority.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: SimpleDagBag
        :param executor: the executor that runs task instances
        :type executor: BaseExecutor
        :param states: Execute TaskInstances in these states
        :type states: Tuple[State]
        :return: List[TaskInstance]
        """
        executable_tis = []

        # Get all the queued task instances from associated with scheduled
        # DagRuns which are not backfilled, in the given states,
        # and the dag is not paused
        TI = models.TaskInstance
        DR = models.DagRun
        DM = models.DagModel
        ti_query = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(simple_dag_bag.dag_ids))
            .outerjoin(
                DR,
                and_(DR.dag_id == TI.dag_id, DR.execution_date == TI.execution_date)
            )
            .filter(or_(DR.run_id == None,  # noqa: E711
                    not_(DR.run_id.like(BackfillJob.ID_PREFIX + '%'))))
            .outerjoin(DM, DM.dag_id == TI.dag_id)
            .filter(or_(DM.dag_id == None,  # noqa: E711
                    not_(DM.is_paused)))
        )
        if None in states:
            ti_query = ti_query.filter(
                or_(TI.state == None, TI.state.in_(states))  # noqa: E711
            )
        else:
            ti_query = ti_query.filter(TI.state.in_(states))

        task_instances_to_examine = ti_query.all()

        if len(task_instances_to_examine) == 0:
            self.log.debug("No tasks to consider for execution.")
            return executable_tis

        # Put one task instance on each line
        task_instance_str = "\n\t".join(
            ["{}".format(x) for x in task_instances_to_examine])
        self.log.info("{} tasks up for execution:\n\t{}"
                      .format(len(task_instances_to_examine),
                              task_instance_str))

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        states_to_count_as_running = [State.RUNNING, State.QUEUED]
        task_concurrency_map = self.__get_task_concurrency_map(
            states=states_to_count_as_running, session=session)

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        for pool, task_instances in pool_to_task_instances.items():
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                if pool not in pools:
                    self.log.warning(
                        "Tasks using non-existent pool '%s' will not be scheduled",
                        pool
                    )
                    open_slots = 0
                else:
                    open_slots = pools[pool].open_slots(session=session)

            num_queued = len(task_instances)
            self.log.info(
                "Figuring out tasks to run in Pool(name={pool}) with {open_slots} "
                "open slots and {num_queued} task instances in queue".format(
                    **locals()
                )
            )

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date))

            # DAG IDs with running tasks that equal the concurrency limit of the dag
            dag_id_to_possibly_running_task_count = {}

            for task_instance in priority_sorted_task_instances:
                if open_slots <= 0:
                    self.log.info(
                        "Not scheduling since there are %s open slots in pool %s",
                        open_slots, pool
                    )
                    # Can't schedule any more since there are no more open slots.
                    break

                # Check to make sure that the task concurrency of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id
                simple_dag = simple_dag_bag.get_dag(dag_id)

                if dag_id not in dag_id_to_possibly_running_task_count:
                    dag_id_to_possibly_running_task_count[dag_id] = \
                        DAG.get_num_task_instances(
                            dag_id,
                            simple_dag_bag.get_dag(dag_id).task_ids,
                            states=states_to_count_as_running,
                            session=session)

                current_task_concurrency = dag_id_to_possibly_running_task_count[dag_id]
                task_concurrency_limit = simple_dag_bag.get_dag(dag_id).concurrency
                self.log.info(
                    "DAG %s has %s/%s running and queued tasks",
                    dag_id, current_task_concurrency, task_concurrency_limit
                )
                if current_task_concurrency >= task_concurrency_limit:
                    self.log.info(
                        "Not executing %s since the number of tasks running or queued "
                        "from DAG %s is >= to the DAG's task concurrency limit of %s",
                        task_instance, dag_id, task_concurrency_limit
                    )
                    continue

                task_concurrency = simple_dag.get_task_special_arg(
                    task_instance.task_id,
                    'task_concurrency')
                if task_concurrency is not None:
                    num_running = task_concurrency_map[
                        (task_instance.dag_id, task_instance.task_id)
                    ]

                    if num_running >= task_concurrency:
                        self.log.info("Not executing %s since the task concurrency for"
                                      " this task has been reached.", task_instance)
                        continue
                    else:
                        task_concurrency_map[(task_instance.dag_id, task_instance.task_id)] += 1

                if self.executor.has_task(task_instance):
                    self.log.debug(
                        "Not handling task %s as the executor reports it is running",
                        task_instance.key
                    )
                    continue
                executable_tis.append(task_instance)
                open_slots -= 1
                dag_id_to_possibly_running_task_count[dag_id] += 1

        task_instance_str = "\n\t".join(
            ["{}".format(x) for x in executable_tis])
        self.log.info(
            "Setting the follow tasks to queued state:\n\t%s", task_instance_str)
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
    def _change_state_for_executable_task_instances(self, task_instances,
                                                    acceptable_states, session=None):
        """
        Changes the state of task instances in the list with one of the given states
        to QUEUED atomically, and returns the TIs changed.

        :param task_instances: TaskInstances to change the state of
        :type task_instances: List[TaskInstance]
        :param acceptable_states: Filters the TaskInstances updated to be in these states
        :type acceptable_states: Iterable[State]
        :return: List[TaskInstance]
        """
        if len(task_instances) == 0:
            session.commit()
            return []

        TI = models.TaskInstance
        filter_for_ti_state_change = (
            [and_(
                TI.dag_id == ti.dag_id,
                TI.task_id == ti.task_id,
                TI.execution_date == ti.execution_date)
                for ti in task_instances])
        ti_query = (
            session
            .query(TI)
            .filter(or_(*filter_for_ti_state_change)))

        if None in acceptable_states:
            ti_query = ti_query.filter(
                or_(TI.state == None, TI.state.in_(acceptable_states))  # noqa: E711
            )
        else:
            ti_query = ti_query.filter(TI.state.in_(acceptable_states))

        tis_to_set_to_queued = (
            ti_query
            .with_for_update()
            .all())
        if len(tis_to_set_to_queued) == 0:
            self.log.info("No tasks were able to have their state changed to queued.")
            session.commit()
            return []

        # set TIs to queued state
        for task_instance in tis_to_set_to_queued:
            task_instance.state = State.QUEUED
            task_instance.queued_dttm = (timezone.utcnow()
                                         if not task_instance.queued_dttm
                                         else task_instance.queued_dttm)
            session.merge(task_instance)

        # save which TIs we set before session expires them
        filter_for_ti_enqueue = ([and_(TI.dag_id == ti.dag_id,
                                  TI.task_id == ti.task_id,
                                  TI.execution_date == ti.execution_date)
                                  for ti in tis_to_set_to_queued])
        session.commit()

        # requery in batches since above was expired by commit

        def query(result, items):
            tis_to_be_queued = (
                session
                .query(TI)
                .filter(or_(*items))
                .all())
            task_instance_str = "\n\t".join(
                ["{}".format(x) for x in tis_to_be_queued])
            self.log.info("Setting the following {} tasks to queued state:\n\t{}"
                          .format(len(tis_to_be_queued),
                                  task_instance_str))
            return result + tis_to_be_queued

        tis_to_be_queued = helpers.reduce_in_chunks(query,
                                                    filter_for_ti_enqueue,
                                                    [],
                                                    self.max_tis_per_query)

        return tis_to_be_queued

    def _enqueue_task_instances_with_queued_state(self, simple_dag_bag, task_instances):
        """
        Takes task_instances, which should have been set to queued, and enqueues them
        with the executor.

        :param task_instances: TaskInstances to enqueue
        :type task_instances: List[TaskInstance]
        :param simple_dag_bag: Should contains all of the task_instances' dags
        :type simple_dag_bag: SimpleDagBag
        """
        TI = models.TaskInstance
        # actually enqueue them
        for task_instance in task_instances:
            simple_dag = simple_dag_bag.get_dag(task_instance.dag_id)
            command = TI.generate_command(
                task_instance.dag_id,
                task_instance.task_id,
                task_instance.execution_date,
                local=True,
                mark_success=False,
                ignore_all_deps=False,
                ignore_depends_on_past=False,
                ignore_task_deps=False,
                ignore_ti_state=False,
                pool=task_instance.pool,
                file_path=simple_dag.full_filepath,
                pickle_id=simple_dag.pickle_id)

            priority = task_instance.priority_weight
            queue = task_instance.queue
            self.log.info(
                "Sending %s to executor with priority %s and queue %s",
                task_instance.key, priority, queue
            )

            # save attributes so sqlalchemy doesnt expire them
            copy_dag_id = task_instance.dag_id
            copy_task_id = task_instance.task_id
            copy_execution_date = task_instance.execution_date
            make_transient(task_instance)
            task_instance.dag_id = copy_dag_id
            task_instance.task_id = copy_task_id
            task_instance.execution_date = copy_execution_date

            self.executor.queue_command(
                task_instance,
                command,
                priority=priority,
                queue=queue)

    @provide_session
    def _execute_task_instances(self,
                                simple_dag_bag,
                                states,
                                session=None):
        """
        Attempts to execute TaskInstances that should be executed by the scheduler.

        There are three steps:
        1. Pick TIs by priority with the constraint that they are in the expected states
        and that we do exceed max_active_runs or pool limits.
        2. Change the state for the TIs above atomically.
        3. Enqueue the TIs in the executor.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
            simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: SimpleDagBag
        :param states: Execute TaskInstances in these states
        :type states: Tuple[State]
        :return: None
        """
        executable_tis = self._find_executable_task_instances(simple_dag_bag, states,
                                                              session=session)

        def query(result, items):
            tis_with_state_changed = self._change_state_for_executable_task_instances(
                items,
                states,
                session=session)
            self._enqueue_task_instances_with_queued_state(
                simple_dag_bag,
                tis_with_state_changed)
            session.commit()
            return result + len(tis_with_state_changed)

        return helpers.reduce_in_chunks(query, executable_tis, 0, self.max_tis_per_query)

    def _process_dags(self, dagbag, dags, tis_out):
        """
        Iterates over the dags and processes them. Processing includes:

        1. Create appropriate DagRun(s) in the DB.
        2. Create appropriate TaskInstance(s) in the DB.
        3. Send emails for tasks that have missed SLAs.

        :param dagbag: a collection of DAGs to process
        :type dagbag: models.DagBag
        :param dags: the DAGs from the DagBag to process
        :type dags: DAG
        :param tis_out: A queue to add generated TaskInstance objects
        :type tis_out: multiprocessing.Queue[TaskInstance]
        :return: None
        """
        for dag in dags:
            dag = dagbag.get_dag(dag.dag_id)
            if dag.is_paused:
                self.log.info("Not processing DAG %s since it's paused", dag.dag_id)
                continue

            if not dag:
                self.log.error("DAG ID %s was not found in the DagBag", dag.dag_id)
                continue

            self.log.info("Processing %s", dag.dag_id)

            dag_run = self.create_dag_run(dag)
            if dag_run:
                self.log.info("Created %s", dag_run)
            self._process_task_instances(dag, tis_out)
            self.manage_slas(dag)

        models.DagStat.update([d.dag_id for d in dags])

    @provide_session
    def _process_executor_events(self, simple_dag_bag, session=None):
        """
        Respond to executor events.
        """
        # TODO: this shares quite a lot of code with _manage_executor_state

        TI = models.TaskInstance
        for key, state in list(self.executor.get_event_buffer(simple_dag_bag.dag_ids)
                                   .items()):
            dag_id, task_id, execution_date, try_number = key
            self.log.info(
                "Executor reports %s.%s execution_date=%s as %s for try_number %s",
                dag_id, task_id, execution_date, state, try_number
            )
            if state == State.FAILED or state == State.SUCCESS:
                qry = session.query(TI).filter(TI.dag_id == dag_id,
                                               TI.task_id == task_id,
                                               TI.execution_date == execution_date)
                ti = qry.first()
                if not ti:
                    self.log.warning("TaskInstance %s went missing from the database", ti)
                    continue

                # TODO: should we fail RUNNING as well, as we do in Backfills?
                if ti.try_number == try_number and ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    self.log.error(msg)
                    try:
                        simple_dag = simple_dag_bag.get_dag(dag_id)
                        dagbag = models.DagBag(simple_dag.full_filepath)
                        dag = dagbag.get_dag(dag_id)
                        ti.task = dag.get_task(task_id)
                        ti.handle_failure(msg)
                    except Exception:
                        self.log.error("Cannot load the dag bag to handle failure for %s"
                                       ". Setting task to FAILED without callbacks or "
                                       "retries. Do you have enough resources?", ti)
                        ti.state = State.FAILED
                        session.merge(ti)
                        session.commit()

    def _execute(self):
        self.log.info("Starting the scheduler")

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in \
                (executors.LocalExecutor, executors.SequentialExecutor):
            pickle_dags = True

        self.log.info("Running execute loop for %s seconds", self.run_duration)
        self.log.info("Processing each file at most %s times", self.num_runs)

        # Build up a list of Python files that could contain DAGs
        self.log.info("Searching for files in %s", self.subdir)
        known_file_paths = list_py_file_paths(self.subdir)
        self.log.info("There are %s files in %s", len(known_file_paths), self.subdir)

        def processor_factory(file_path, zombies):
            return DagFileProcessor(file_path,
                                    pickle_dags,
                                    self.dag_ids,
                                    zombies)

        # When using sqlite, we do not use async_mode
        # so the scheduler job and DAG parser don't access the DB at the same time.
        async_mode = not self.using_sqlite

        self.processor_agent = DagFileProcessorAgent(self.subdir,
                                                     known_file_paths,
                                                     self.num_runs,
                                                     processor_factory,
                                                     async_mode)

        try:
            self._execute_helper()
        finally:
            self.processor_agent.end()
            self.log.info("Exited execute loop")

    def _execute_helper(self):
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

        :return: None
        """
        self.executor.start()

        self.log.info("Resetting orphaned tasks for active dag runs")
        self.reset_state_for_orphaned_tasks()

        # Start after resetting orphaned tasks to avoid stressing out DB.
        self.processor_agent.start()

        execute_start_time = timezone.utcnow()

        # Last time that self.heartbeat() was called.
        last_self_heartbeat_time = timezone.utcnow()

        # For the execute duration, parse and schedule DAGs
        while (timezone.utcnow() - execute_start_time).total_seconds() < \
                self.run_duration or self.run_duration < 0:
            self.log.debug("Starting Loop...")
            loop_start_time = time.time()

            if self.using_sqlite:
                self.processor_agent.heartbeat()
                # For the sqlite case w/ 1 thread, wait until the processor
                # is finished to avoid concurrent access to the DB.
                self.log.debug(
                    "Waiting for processors to finish since we're using sqlite")
                self.processor_agent.wait_until_finished()

            self.log.info("Harvesting DAG parsing results")
            simple_dags = self.processor_agent.harvest_simple_dags()

            # Send tasks for execution if available
            simple_dag_bag = SimpleDagBag(simple_dags)
            if len(simple_dags) > 0:
                try:
                    simple_dag_bag = SimpleDagBag(simple_dags)

                    # Handle cases where a DAG run state is set (perhaps manually) to
                    # a non-running state. Handle task instances that belong to
                    # DAG runs in those states

                    # If a task instance is up for retry but the corresponding DAG run
                    # isn't running, mark the task instance as FAILED so we don't try
                    # to re-run it.
                    self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                              [State.UP_FOR_RETRY],
                                                              State.FAILED)
                    # If a task instance is scheduled or queued, but the corresponding
                    # DAG run isn't running, set the state to NONE so we don't try to
                    # re-run it.
                    self._change_state_for_tis_without_dagrun(simple_dag_bag,
                                                              [State.QUEUED,
                                                               State.SCHEDULED],
                                                              State.NONE)

                    self._execute_task_instances(simple_dag_bag,
                                                 (State.SCHEDULED,))
                except Exception as e:
                    self.log.error("Error queuing tasks")
                    self.log.exception(e)
                    continue

            # Call heartbeats
            self.log.debug("Heartbeating the executor")
            self.executor.heartbeat()

            # Process events from the executor
            self._process_executor_events(simple_dag_bag)

            # Heartbeat the scheduler periodically
            time_since_last_heartbeat = (timezone.utcnow() -
                                         last_self_heartbeat_time).total_seconds()
            if time_since_last_heartbeat > self.heartrate:
                self.log.debug("Heartbeating the scheduler")
                self.heartbeat()
                last_self_heartbeat_time = timezone.utcnow()

            loop_end_time = time.time()
            loop_duration = loop_end_time - loop_start_time
            self.log.debug(
                "Ran scheduling loop in %.2f seconds",
                loop_duration)
            self.log.debug("Sleeping for %.2f seconds", self._processor_poll_interval)
            time.sleep(self._processor_poll_interval)

            # Exit early for a test mode
            if self.processor_agent.done:
                self.log.info("Exiting scheduler loop as all files"
                              " have been processed {} times".format(self.num_runs))
                break

            if loop_duration < 1:
                sleep_length = 1 - loop_duration
                self.log.debug(
                    "Sleeping for {0:.2f} seconds to prevent excessive logging"
                    .format(sleep_length))
                sleep(sleep_length)

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

    @provide_session
    def process_file(self, file_path, zombies, pickle_dags=False, session=None):
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
        :type file_path: unicode
        :param zombies: zombie task instances to kill.
        :type zombies: list[SimpleTaskInstance]
        :param pickle_dags: whether serialize the DAGs found in the file and
            save them to the db
        :type pickle_dags: bool
        :return: a list of SimpleDags made from the Dags found in the file
        :rtype: list[SimpleDag]
        """
        self.log.info("Processing file %s for tasks to queue", file_path)
        # As DAGs are parsed from this file, they will be converted into SimpleDags
        simple_dags = []

        try:
            dagbag = models.DagBag(file_path, include_examples=False)
        except Exception:
            self.log.exception("Failed at reloading the DAG file %s", file_path)
            Stats.incr('dag_file_refresh_error', 1, 1)
            return []

        if len(dagbag.dags) > 0:
            self.log.info("DAG(s) %s retrieved from %s", dagbag.dags.keys(), file_path)
        else:
            self.log.warning("No viable dags retrieved from %s", file_path)
            self.update_import_errors(session, dagbag)
            return []

        # Save individual DAGs in the ORM and update DagModel.last_scheduled_time
        for dag in dagbag.dags.values():
            dag.sync_to_db()

        paused_dag_ids = [dag.dag_id for dag in dagbag.dags.values()
                          if dag.is_paused]

        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag_id in dagbag.dags:
            dag = dagbag.get_dag(dag_id)
            pickle_id = None
            if pickle_dags:
                pickle_id = dag.pickle(session).id

            # Only return DAGs that are not paused
            if dag_id not in paused_dag_ids:
                simple_dags.append(SimpleDag(dag, pickle_id=pickle_id))

        if len(self.dag_ids) > 0:
            dags = [dag for dag in dagbag.dags.values()
                    if dag.dag_id in self.dag_ids and
                    dag.dag_id not in paused_dag_ids]
        else:
            dags = [dag for dag in dagbag.dags.values()
                    if not dag.parent_dag and
                    dag.dag_id not in paused_dag_ids]

        # Not using multiprocessing.Queue() since it's no longer a separate
        # process and due to some unusual behavior. (empty() incorrectly
        # returns true?)
        ti_keys_to_schedule = []

        self._process_dags(dagbag, dags, ti_keys_to_schedule)

        for ti_key in ti_keys_to_schedule:
            dag = dagbag.dags[ti_key[0]]
            task = dag.get_task(ti_key[1])
            ti = models.TaskInstance(task, ti_key[2])

            ti.refresh_from_db(session=session, lock_for_update=True)
            # We can defer checking the task dependency checks to the worker themselves
            # since they can be expensive to run in the scheduler.
            dep_context = DepContext(deps=QUEUE_DEPS, ignore_task_deps=True)

            # Only schedule tasks that have their dependencies met, e.g. to avoid
            # a task that recently got its state changed to RUNNING from somewhere
            # other than the scheduler from getting its state overwritten.
            # TODO(aoen): It's not great that we have to check all the task instance
            # dependencies twice; once to get the task scheduled, and again to actually
            # run the task. We should try to come up with a way to only check them once.
            if ti.are_dependencies_met(
                    dep_context=dep_context,
                    session=session,
                    verbose=True):
                # Task starts out in the scheduled state. All tasks in the
                # scheduled state will be sent to the executor
                ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            self.log.info("Creating / updating %s in ORM", ti)
            session.merge(ti)
        # commit batch
        session.commit()

        # Record import errors into the ORM
        try:
            self.update_import_errors(session, dagbag)
        except Exception:
            self.log.exception("Error logging import errors!")
        try:
            dagbag.kill_zombies(zombies)
        except Exception:
            self.log.exception("Error killing zombies!")

        return simple_dags

    @provide_session
    def heartbeat_callback(self, session=None):
        Stats.incr('scheduler_heartbeat', 1, 1)


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """
    ID_PREFIX = 'backfill_'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    class _DagRunTaskStatus(object):
        """
        Internal status of the backfill job. This class is intended to be instantiated
        only within a BackfillJob instance and will track the execution of tasks,
        e.g. running, skipped, succeeded, failed, etc. Information about the dag runs
        related to the backfill job are also being tracked in this structure,
        .e.g finished runs, etc. Any other status related information related to the
        execution of dag runs / tasks can be included in this structure since it makes
        it easier to pass it around.
        """
        # TODO(edgarRd): AIRFLOW-1444: Add consistency check on counts
        def __init__(self,
                     to_run=None,
                     running=None,
                     skipped=None,
                     succeeded=None,
                     failed=None,
                     not_ready=None,
                     deadlocked=None,
                     active_runs=None,
                     executed_dag_run_dates=None,
                     finished_runs=0,
                     total_runs=0,
                     ):
            """
            :param to_run: Tasks to run in the backfill
            :type to_run: dict[Tuple[String, String, DateTime], TaskInstance]
            :param running: Maps running task instance key to task instance object
            :type running: dict[Tuple[String, String, DateTime], TaskInstance]
            :param skipped: Tasks that have been skipped
            :type skipped: set[Tuple[String, String, DateTime]]
            :param succeeded: Tasks that have succeeded so far
            :type succeeded: set[Tuple[String, String, DateTime]]
            :param failed: Tasks that have failed
            :type failed: set[Tuple[String, String, DateTime]]
            :param not_ready: Tasks not ready for execution
            :type not_ready: set[Tuple[String, String, DateTime]]
            :param deadlocked: Deadlocked tasks
            :type deadlocked: set[Tuple[String, String, DateTime]]
            :param active_runs: Active dag runs at a certain point in time
            :type active_runs: list[DagRun]
            :param executed_dag_run_dates: Datetime objects for the executed dag runs
            :type executed_dag_run_dates: set[Datetime]
            :param finished_runs: Number of finished runs so far
            :type finished_runs: int
            :param total_runs: Number of total dag runs able to run
            :type total_runs: int
            """
            self.to_run = to_run or dict()
            self.running = running or dict()
            self.skipped = skipped or set()
            self.succeeded = succeeded or set()
            self.failed = failed or set()
            self.not_ready = not_ready or set()
            self.deadlocked = deadlocked or set()
            self.active_runs = active_runs or list()
            self.executed_dag_run_dates = executed_dag_run_dates or set()
            self.finished_runs = finished_runs
            self.total_runs = total_runs

    def __init__(
            self,
            dag,
            start_date=None,
            end_date=None,
            mark_success=False,
            donot_pickle=False,
            ignore_first_depends_on_past=False,
            ignore_task_deps=False,
            pool=None,
            delay_on_limit_secs=1.0,
            verbose=False,
            conf=None,
            rerun_failed_tasks=False,
            *args, **kwargs):
        """
        :param dag: DAG object.
        :type dag: `class DAG`.
        :param start_date: start date for the backfill date range.
        :type start_date: datetime.
        :param end_date: end date for the backfill date range.
        :type end_date: datetime
        :param mark_success: flag whether to mark the task auto success.
        :type mark_success: bool
        :param donot_pickle: whether pickle
        :type donot_pickle: bool
        :param ignore_first_depends_on_past: whether to ignore depend on past
        :type ignore_first_depends_on_past: bool
        :param ignore_task_deps: whether to ignore the task dependency
        :type ignore_task_deps: bool
        :param pool:
        :type pool: list
        :param delay_on_limit_secs:
        :param verbose:
        :type verbose: flag to whether display verbose message to backfill console
        :param conf: a dictionary which user could pass k-v pairs for backfill
        :type conf: dictionary
        :param rerun_failed_tasks: flag to whether to
                                   auto rerun the failed task in backfill
        :type rerun_failed_tasks: bool
        :param args:
        :param kwargs:
        """
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.pool = pool
        self.delay_on_limit_secs = delay_on_limit_secs
        self.verbose = verbose
        self.conf = conf
        self.rerun_failed_tasks = rerun_failed_tasks
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _update_counters(self, ti_status):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.

        :param ti_status: the internal status of the backfill job tasks
        :type ti_status: BackfillJob._DagRunTaskStatus
        """
        for key, ti in list(ti_status.running.items()):
            ti.refresh_from_db()
            if ti.state == State.SUCCESS:
                ti_status.succeeded.add(key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.SKIPPED:
                ti_status.skipped.add(key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(key)
                ti_status.running.pop(key)
                continue
            # special case: if the task needs to run again put it back
            elif ti.state == State.UP_FOR_RETRY:
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti
                )
                ti.set_state(State.SCHEDULED)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti

    def _manage_executor_state(self, running):
        """
        Checks if the executor agrees with the state of task instances
        that are running

        :param running: dict of key, task to verify
        """
        executor = self.executor

        for key, state in list(executor.get_event_buffer().items()):
            if key not in running:
                self.log.warning(
                    "%s state %s not in running=%s",
                    key, state, running.values()
                )
                continue

            ti = running[key]
            ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if state == State.FAILED or state == State.SUCCESS:
                if ti.state == State.RUNNING or ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    self.log.error(msg)
                    ti.handle_failure(msg)

    @provide_session
    def _get_dag_run(self, run_date, session=None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.

        :param run_date: the execution date for the dag run
        :type run_date: datetime
        :param session: the database session object
        :type session: Session
        :return: a DagRun in state RUNNING or None
        """
        run_id = BackfillJob.ID_FORMAT_PREFIX.format(run_date.isoformat())

        # consider max_active_runs but ignore when running subdags
        respect_dag_max_active_limit = (True
                                        if (self.dag.schedule_interval and
                                            not self.dag.is_subdag)
                                        else False)

        current_active_dag_count = self.dag.get_num_active_runs(external_trigger=False)

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        run = DagRun.find(dag_id=self.dag.dag_id,
                          execution_date=run_date,
                          session=session)

        if run is not None and len(run) > 0:
            run = run[0]
            if run.state == State.RUNNING:
                respect_dag_max_active_limit = False
        else:
            run = None

        # enforce max_active_runs limit for dag, special cases already
        # handled by respect_dag_max_active_limit
        if (respect_dag_max_active_limit and
                current_active_dag_count >= self.dag.max_active_runs):
            return None

        run = run or self.dag.create_dagrun(
            run_id=run_id,
            execution_date=run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session,
            conf=self.conf,
        )

        # set required transient field
        run.dag = self.dag

        # explicitly mark as backfill and running
        run.state = State.RUNNING
        run.run_id = run_id
        run.verify_integrity(session=session)
        return run

    @provide_session
    def _task_instances_for_dag_run(self, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.

        :param dag_run: the dag run to get the tasks from
        :type dag_run: models.DagRun
        :param session: the database session object
        :type session: Session
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        # TODO(edgarRd): AIRFLOW-1464 change to batch query to improve perf
        for ti in dag_run.get_task_instances():
            # all tasks part of the backfill are scheduled to run
            if ti.state == State.NONE:
                ti.set_state(State.SCHEDULED, session=session)
            if ti.state != State.REMOVED:
                tasks_to_run[ti.key] = ti

        return tasks_to_run

    def _log_progress(self, ti_status):
        msg = ' | '.join([
            "[backfill progress]",
            "finished run {0} of {1}",
            "tasks waiting: {2}",
            "succeeded: {3}",
            "running: {4}",
            "failed: {5}",
            "skipped: {6}",
            "deadlocked: {7}",
            "not ready: {8}"
        ]).format(
            ti_status.finished_runs,
            ti_status.total_runs,
            len(ti_status.to_run),
            len(ti_status.succeeded),
            len(ti_status.running),
            len(ti_status.failed),
            len(ti_status.skipped),
            len(ti_status.deadlocked),
            len(ti_status.not_ready))
        self.log.info(msg)

        self.log.debug(
            "Finished dag run loop iteration. Remaining tasks %s",
            ti_status.to_run.values()
        )

    @provide_session
    def _process_backfill_task_instances(self,
                                         ti_status,
                                         executor,
                                         pickle_id,
                                         start_date=None, session=None):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.

        :param ti_status: the internal status of the job
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to run the task instances
        :type executor: BaseExecutor
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :type pickle_id: int
        :param start_date: the start date of the backfill job
        :type start_date: datetime
        :param session: the current session object
        :type session: Session
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """

        executed_run_dates = []

        while ((len(ti_status.to_run) > 0 or len(ti_status.running) > 0) and
                len(ti_status.deadlocked) == 0):
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            for task in self.dag.topological_sort():
                for key, ti in list(ti_status.to_run.items()):
                    if task.task_id != ti.task_id:
                        continue

                    ti.refresh_from_db()

                    task = self.dag.get_task(ti.task_id)
                    ti.task = task

                    ignore_depends_on_past = (
                        self.ignore_first_depends_on_past and
                        ti.execution_date == (start_date or ti.start_date))
                    self.log.debug(
                        "Task instance to run %s state %s", ti, ti.state)

                    # The task was already marked successful or skipped by a
                    # different Job. Don't rerun it.
                    if ti.state == State.SUCCESS:
                        ti_status.succeeded.add(key)
                        self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue
                    elif ti.state == State.SKIPPED:
                        ti_status.skipped.add(key)
                        self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue

                    # guard against externally modified tasks instances or
                    # in case max concurrency has been reached at task runtime
                    elif ti.state == State.NONE:
                        self.log.warning(
                            "FIXME: task instance {} state was set to None "
                            "externally. This should not happen"
                        )
                        ti.set_state(State.SCHEDULED, session=session)
                    if self.rerun_failed_tasks:
                        # Rerun failed tasks or upstreamed failed tasks
                        if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                            self.log.error("Task instance {ti} "
                                           "with state {state}".format(ti=ti,
                                                                       state=ti.state))
                            if key in ti_status.running:
                                ti_status.running.pop(key)
                            # Reset the failed task in backfill to scheduled state
                            ti.set_state(State.SCHEDULED, session=session)
                    else:
                        # Default behaviour which works for subdag.
                        if ti.state in (State.FAILED, State.UPSTREAM_FAILED):
                            self.log.error("Task instance {ti} "
                                           "with {state} state".format(ti=ti,
                                                                       state=ti.state))
                            ti_status.failed.add(key)
                            ti_status.to_run.pop(key)
                            if key in ti_status.running:
                                ti_status.running.pop(key)
                            continue

                    backfill_context = DepContext(
                        deps=RUN_DEPS,
                        ignore_depends_on_past=ignore_depends_on_past,
                        ignore_task_deps=self.ignore_task_deps,
                        flag_upstream_failed=True)

                    # Is the task runnable? -- then run it
                    # the dependency checker can change states of tis
                    if ti.are_dependencies_met(
                            dep_context=backfill_context,
                            session=session,
                            verbose=self.verbose):
                        ti.refresh_from_db(lock_for_update=True, session=session)
                        if ti.state == State.SCHEDULED or ti.state == State.UP_FOR_RETRY:
                            if executor.has_task(ti):
                                self.log.debug(
                                    "Task Instance %s already in executor "
                                    "waiting for queue to clear",
                                    ti
                                )
                            else:
                                self.log.debug('Sending %s to executor', ti)
                                # Skip scheduled state, we are executing immediately
                                ti.state = State.QUEUED
                                session.merge(ti)

                                cfg_path = None
                                if executor.__class__ in (executors.LocalExecutor,
                                                          executors.SequentialExecutor):
                                    cfg_path = tmp_configuration_copy()

                                executor.queue_task_instance(
                                    ti,
                                    mark_success=self.mark_success,
                                    pickle_id=pickle_id,
                                    ignore_task_deps=self.ignore_task_deps,
                                    ignore_depends_on_past=ignore_depends_on_past,
                                    pool=self.pool,
                                    cfg_path=cfg_path)
                                ti_status.running[key] = ti
                                ti_status.to_run.pop(key)
                        session.commit()
                        continue

                    if ti.state == State.UPSTREAM_FAILED:
                        self.log.error("Task instance %s upstream failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue

                    # special case
                    if ti.state == State.UP_FOR_RETRY:
                        self.log.debug(
                            "Task instance %s retry period not "
                            "expired yet", ti)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        ti_status.to_run[key] = ti
                        continue

                    # all remaining tasks
                    self.log.debug('Adding %s to not_ready', ti)
                    ti_status.not_ready.add(key)

            # execute the tasks in the queue
            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (ti_status.not_ready and
                    ti_status.not_ready == set(ti_status.to_run) and
                    len(ti_status.running) == 0):
                self.log.warning(
                    "Deadlock discovered for ti_status.to_run=%s",
                    ti_status.to_run.values()
                )
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            # check executor state
            self._manage_executor_state(ti_status.running)

            # update the task counters
            self._update_counters(ti_status=ti_status)

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)
                if run.state in State.finished():
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

                if run.dag.is_paused:
                    models.DagStat.update([run.dag_id], session=session)

            self._log_progress(ti_status)

        # return updated status
        return executed_run_dates

    @provide_session
    def _collect_errors(self, ti_status, session=None):
        err = ''
        if ti_status.failed:
            err += (
                "---------------------------------------------------\n"
                "Some task instances failed:\n{}\n".format(ti_status.failed))
        if ti_status.deadlocked:
            err += (
                '---------------------------------------------------\n'
                'BackfillJob is deadlocked.')
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=self.verbose) !=
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=self.verbose)
                for t in ti_status.deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += ' These tasks have succeeded:\n{}\n'.format(ti_status.succeeded)
            err += ' These tasks are running:\n{}\n'.format(ti_status.running)
            err += ' These tasks have failed:\n{}\n'.format(ti_status.failed)
            err += ' These tasks are skipped:\n{}\n'.format(ti_status.skipped)
            err += ' These tasks are deadlocked:\n{}\n'.format(ti_status.deadlocked)

        return err

    @provide_session
    def _execute_for_run_dates(self, run_dates, ti_status, executor, pickle_id,
                               start_date, session=None):
        """
        Computes the dag runs and their respective task instances for
        the given run dates and executes the task instances.
        Returns a list of execution dates of the dag runs that were executed.

        :param run_dates: Execution dates for dag runs
        :type run_dates: list
        :param ti_status: internal BackfillJob status structure to tis track progress
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to use, it must be previously started
        :type executor: BaseExecutor
        :param pickle_id: numeric id of the pickled dag, None if not pickled
        :type pickle_id: int
        :param start_date: backfill start date
        :type start_date: datetime
        :param session: the current session object
        :type session: Session
        """
        for next_run_date in run_dates:
            dag_run = self._get_dag_run(next_run_date, session=session)
            tis_map = self._task_instances_for_dag_run(dag_run,
                                                       session=session)
            if dag_run is None:
                continue

            ti_status.active_runs.append(dag_run)
            ti_status.to_run.update(tis_map or {})

        processed_dag_run_dates = self._process_backfill_task_instances(
            ti_status=ti_status,
            executor=executor,
            pickle_id=pickle_id,
            start_date=start_date,
            session=session)

        ti_status.executed_dag_run_dates.update(processed_dag_run_dates)

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        ti_status = BackfillJob._DagRunTaskStatus()

        start_date = self.bf_start_date

        # Get intervals between the start/end dates, which will turn into dag runs
        run_dates = self.dag.get_run_dates(start_date=start_date,
                                           end_date=self.bf_end_date)
        if len(run_dates) == 0:
            self.log.info("No run dates were found for the given dates and dag interval.")
            return

        # picklin'
        pickle_id = None
        if not self.donot_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = models.DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()

        ti_status.total_runs = len(run_dates)  # total dag runs in backfill

        try:
            remaining_dates = ti_status.total_runs
            while remaining_dates > 0:
                dates_to_process = [run_date for run_date in run_dates
                                    if run_date not in ti_status.executed_dag_run_dates]

                self._execute_for_run_dates(run_dates=dates_to_process,
                                            ti_status=ti_status,
                                            executor=executor,
                                            pickle_id=pickle_id,
                                            start_date=start_date,
                                            session=session)

                remaining_dates = (
                    ti_status.total_runs - len(ti_status.executed_dag_run_dates)
                )
                err = self._collect_errors(ti_status=ti_status, session=session)
                if err:
                    raise AirflowException(err)

                if remaining_dates > 0:
                    self.log.info(
                        "max_active_runs limit for dag %s has been reached "
                        " - waiting for other dag runs to finish",
                        self.dag_id
                    )
                    time.sleep(self.delay_on_limit_secs)
        finally:
            executor.end()
            session.commit()

        self.log.info("Backfill done. Exiting.")


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            pickle_id=None,
            pool=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.on_kill()
            raise AirflowException("LocalTaskJob received SIGTERM signal")
        signal.signal(signal.SIGTERM, signal_handler)

        if not self.task_instance._check_and_change_state_before_execution(
                mark_success=self.mark_success,
                ignore_all_deps=self.ignore_all_deps,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps,
                ignore_ti_state=self.ignore_ti_state,
                job_id=self.id,
                pool=self.pool):
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            last_heartbeat_time = time.time()
            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.task_runner.return_code()
                if return_code is not None:
                    self.log.info("Task exited with return code %s", return_code)
                    return

                # Periodically heartbeat so that the scheduler doesn't think this
                # is a zombie
                try:
                    self.heartbeat()
                    last_heartbeat_time = time.time()
                except OperationalError:
                    Stats.incr('local_task_job_heartbeat_failure', 1, 1)
                    self.log.exception(
                        "Exception while trying to heartbeat! Sleeping for %s seconds",
                        self.heartrate
                    )
                    time.sleep(self.heartrate)

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                time_since_last_heartbeat = time.time() - last_heartbeat_time
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limited exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            self.on_kill()

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            self.task_runner.terminate()
            return

        self.task_instance.refresh_from_db()
        ti = self.task_instance

        fqdn = get_hostname()
        same_hostname = fqdn == ti.hostname
        same_process = ti.pid == os.getpid()

        if ti.state == State.RUNNING:
            if not same_hostname:
                self.log.warning("The recorded hostname {ti.hostname} "
                                 "does not match this instance's hostname "
                                 "{fqdn}".format(**locals()))
                raise AirflowException("Hostname of job runner does not match")
            elif not same_process:
                current_pid = os.getpid()
                self.log.warning("Recorded pid {ti.pid} does not match "
                                 "the current pid "
                                 "{current_pid}".format(**locals()))
                raise AirflowException("PID of job runner does not match")
        elif (
                self.task_runner.return_code() is None and
                hasattr(self.task_runner, 'process')
        ):
            self.log.warning(
                "State of this instance has been externally set to %s. "
                "Taking the poison pill.",
                ti.state
            )
            self.task_runner.terminate()
            self.terminating = True
