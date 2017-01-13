# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from past.builtins import basestring
from collections import defaultdict, Counter

from datetime import datetime

import getpass
import logging
import socket
import subprocess
import multiprocessing
import os
import signal
import sys
import threading
import time
from time import sleep

import psutil
from sqlalchemy import Column, Integer, String, DateTime, func, Index, or_
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.session import make_transient
from tabulate import tabulate

from airflow import executors, models, settings
from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.settings import Stats
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils.state import State
from airflow.utils.db import provide_session, pessimistic_connection_handling
from airflow.utils.dag_processing import (AbstractDagFileProcessor,
                                          DagFileProcessorManager,
                                          SimpleDag,
                                          SimpleDagBag,
                                          list_py_file_paths)
from airflow.utils.email import send_email
from airflow.utils.helpers import kill_descendant_processes
from airflow.utils.logging import LoggingMixin
from airflow.utils import asciiart


Base = models.Base
DagRun = models.DagRun
ID_LEN = models.ID_LEN
Stats = settings.Stats


class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have it's own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
    )

    def __init__(
            self,
            executor=executors.DEFAULT_EXECUTOR,
            heartrate=conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC'),
            *args, **kwargs):
        self.hostname = socket.getfqdn()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)

    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (conf.getint('scheduler', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    def kill(self):
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = datetime.now()
        try:
            self.on_kill()
        except:
            self.logger.error('on_kill() method failed')
        session.merge(job)
        session.commit()
        session.close()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        '''
        Will be called when an external kill command is received
        '''
        pass

    def heartbeat_callback(self, session=None):
        pass

    def heartbeat(self):
        '''
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
        '''
        session = settings.Session()
        job = session.query(BaseJob).filter_by(id=self.id).one()
        make_transient(job)
        session.commit()
        session.close()

        if job.state == State.SHUTDOWN:
            self.kill()

        # Figure out how long to sleep for
        sleep_for = 0
        if job.latest_heartbeat:
            sleep_for = max(
                0,
                self.heartrate - (datetime.now() - job.latest_heartbeat).total_seconds())

        # Don't keep session open while sleeping as it leaves a connection open
        session.close()
        sleep(sleep_for)

        # Update last heartbeat time
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.latest_heartbeat = datetime.now()
        session.merge(job)
        session.commit()

        self.heartbeat_callback(session=session)
        session.close()
        self.logger.debug('[heart] Boom.')

    def run(self):
        Stats.incr(self.__class__.__name__.lower() + '_start', 1, 1)
        # Adding an entry in the DB
        session = settings.Session()
        self.state = State.RUNNING
        session.add(self)
        session.commit()
        id_ = self.id
        make_transient(self)
        self.id = id_

        # Run
        self._execute()

        # Marking the success in the DB
        self.end_date = datetime.now()
        self.state = State.SUCCESS
        session.merge(self)
        session.commit()
        session.close()

        Stats.incr(self.__class__.__name__.lower() + '_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")


class DagFileProcessor(AbstractDagFileProcessor):
    """Helps call SchedulerJob.process_file() in a separate process."""

    # Counter that increments everytime an instance of this class is created
    class_creation_counter = 0

    def __init__(self, file_path, pickle_dags, dag_id_white_list, log_file):
        """
        :param file_path: a Python file containing Airflow DAG definitions
        :type file_path: unicode
        :param pickle_dags: whether to serialize the DAG objects to the DB
        :type pickle_dags: bool
        :param dag_id_whitelist: If specified, only look at these DAG ID's
        :type dag_id_whitelist: list[unicode]
        :param log_file: the path to the file where log lines should be output
        :type log_file: unicode
        """
        self._file_path = file_path
        self._log_file = log_file
        # Queue that's used to pass results from the child process.
        self._result_queue = multiprocessing.Queue()
        # The process that was launched to process the given .
        self._process = None
        self._dag_id_white_list = dag_id_white_list
        self._pickle_dags = pickle_dags
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

    @property
    def log_file(self):
        return self._log_file

    @staticmethod
    def _launch_process(result_queue,
                        file_path,
                        pickle_dags,
                        dag_id_white_list,
                        thread_name,
                        log_file):
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
        :param log_file: the logging output for the process should be directed
        to this file
        :type log_file: unicode
        :return: the process that was launched
        :rtype: multiprocessing.Process
        """
        def helper():
            # This helper runs in the newly created process

            # Re-direct stdout and stderr to a separate log file. Otherwise,
            # the main log becomes too hard to read. No buffering to enable
            # responsive file tailing
            parent_dir, _ = os.path.split(log_file)

            # Create the parent directory for the log file if necessary.
            if not os.path.isdir(parent_dir):
                os.makedirs(parent_dir)

            f = open(log_file, "a")
            original_stdout = sys.stdout
            original_stderr = sys.stderr

            sys.stdout = f
            sys.stderr = f

            try:
                # Re-configure logging to use the new output streams
                log_format = settings.LOG_FORMAT_WITH_THREAD_NAME
                settings.configure_logging(log_format=log_format)
                # Re-configure the ORM engine as there are issues with multiple processes
                settings.configure_orm()

                # Change the thread name to differentiate log lines. This is
                # really a separate process, but changing the name of the
                # process doesn't work, so changing the thread name instead.
                threading.current_thread().name = thread_name
                start_time = time.time()

                logging.info("Started process (PID=%s) to work on %s",
                             os.getpid(),
                             file_path)
                scheduler_job = SchedulerJob(dag_ids=dag_id_white_list)
                result = scheduler_job.process_file(file_path,
                                                    pickle_dags)
                result_queue.put(result)
                end_time = time.time()
                logging.info("Processing %s took %.3f seconds",
                             file_path,
                             end_time - start_time)
            except:
                # Log exceptions through the logging framework.
                logging.exception("Got an exception! Propagating...")
                raise
            finally:
                sys.stdout = original_stdout
                sys.stderr = original_stderr
                f.close()

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
            self.log_file)
        self._start_time = datetime.now()

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
            logging.warn("Killing PID %s", self._process.pid)
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

        if not self._result_queue.empty():
            self._result = self._result_queue.get_nowait()
            self._done = True
            logging.debug("Waiting for %s", self._process)
            self._process.join()
            return True

        # Potential error case when process dies
        if not self._process.is_alive():
            self._done = True
            # Get the object from the queue or else join() can hang.
            if not self._result_queue.empty():
                self._result = self._result_queue.get_nowait()
            logging.debug("Waiting for %s", self._process)
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
            subdir=models.DAGS_FOLDER,
            num_runs=-1,
            file_process_interval=conf.getint('scheduler',
                                              'min_file_process_interval'),
            processor_poll_interval=1.0,
            run_duration=None,
            do_pickle=False,
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
        :param processor_poll_interval: The number of seconds to wait between
        polls of running processors
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
        self.max_threads = min(conf.getint('scheduler', 'max_threads'), multiprocessing.cpu_count())
        self.using_sqlite = False
        if 'sqlite' in conf.get('core', 'sql_alchemy_conn'):
            if self.max_threads > 1:
                self.logger.error("Cannot use more than 1 thread when using sqlite. Setting max_threads to 1")
            self.max_threads = 1
            self.using_sqlite = True

        # How often to scan the DAGs directory for new files. Default to 5 minutes.
        self.dag_dir_list_interval = conf.getint('scheduler',
                                                 'dag_dir_list_interval')
        # How often to print out DAG file processing stats to the log. Default to
        # 30 seconds.
        self.print_stats_interval = conf.getint('scheduler',
                                                'print_stats_interval')
        # Parse and schedule each file no faster than this interval. Default
        # to 3 minutes.
        self.file_process_interval = file_process_interval
        # Directory where log files for the processes that scheduled the DAGs reside
        self.child_process_log_directory = conf.get('scheduler',
                                                    'child_process_log_directory')
        if run_duration is None:
            self.run_duration = conf.getint('scheduler',
                                            'run_duration')

    @provide_session
    def manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        Where assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .filter(TI.dag_id == dag.dag_id)
            .filter(TI.state == State.SUCCESS)
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = datetime.now()
        SlaMiss = models.SlaMiss
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if task.sla:
                dttm = dag.following_schedule(dttm)
                while dttm < datetime.now():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < datetime.now():
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
            .filter(or_(SlaMiss.email_sent == False,
                        SlaMiss.notification_sent == False))
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
                self.logger.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                dag.sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
                notification_sent = True
            email_content = """\
            Here's a list of tasks thas missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(bug=asciiart.bug, **locals())
            emails = []
            for t in dag.tasks:
                if t.email:
                    if isinstance(t.email, basestring):
                        l = [t.email]
                    elif isinstance(t.email, (list, tuple)):
                        l = t.email
                    for email in l:
                        if email not in emails:
                            emails.append(email)
            if emails and len(slas):
                send_email(
                    emails,
                    "[airflow] SLA miss on DAG=" + dag.dag_id,
                    email_content)
                email_sent = True
                notification_sent = True
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()
            session.close()

    @staticmethod
    def record_import_errors(session, dagbag):
        """
        For the DAGs in the given DagBag, record any associated import errors.
        These are usually displayed through the Airflow UI so that users know
        that there are issues parsing DAGs.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        :param dagbag: DagBag containing DAGs with import errors
        :type dagbag: models.Dagbag
        """
        for filename, stacktrace in list(dagbag.import_errors.items()):
            session.query(models.ImportError).filter(
                models.ImportError.filename == filename
            ).delete()
            session.add(models.ImportError(
                filename=filename, stacktrace=stacktrace))
        session.commit()

    @staticmethod
    def clear_import_errors(session):
        """
        Remove all the known import errors from the DB.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        """
        session.query(models.ImportError).delete()
        session.commit()

    @provide_session
    def create_dag_run(self, dag, session=None):
        """
        This method checks whether a new DagRun needs to be created
        for a DAG based on scheduling interval
        Returns DagRun if one is scheduled. Otherwise returns None.
        """
        if dag.schedule_interval:
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
                        dr.start_date < datetime.now() - dag.dagrun_timeout):
                    dr.state = State.FAILED
                    dr.end_date = datetime.now()
                    timedout_runs += 1
            session.commit()
            if len(active_runs) - timedout_runs >= dag.max_active_runs:
                return

            # this query should be replaced by find dagrun
            qry = (
                session.query(func.max(DagRun.execution_date))
                .filter_by(dag_id=dag.dag_id)
                .filter(or_(
                    DagRun.external_trigger == False,
                    # add % as a wildcard for the like query
                    DagRun.run_id.like(DagRun.ID_PREFIX + '%')
                ))
            )
            last_scheduled_run = qry.scalar()

            # don't schedule @once again
            if dag.schedule_interval == '@once' and last_scheduled_run:
                return None

            # don't do scheduler catchup for dag's that don't have dag.catchup = True
            if not dag.catchup:
                # The logic is that we move start_date up until
                # one period before, so that datetime.now() is AFTER
                # the period end, and the job can be created...
                now = datetime.now()
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
                    self.logger.debug("Next run date based on tasks {}"
                                      .format(next_run_date))
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

                self.logger.debug("Dag start date: {}. Next run date: {}"
                                  .format(dag.start_date, next_run_date))

            # don't ever schedule in the future
            if next_run_date > datetime.now():
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

            if next_run_date and period_end and period_end <= datetime.now():
                next_run = dag.create_dagrun(
                    run_id='scheduled__' + next_run_date.isoformat(),
                    execution_date=next_run_date,
                    start_date=datetime.now(),
                    state=State.RUNNING,
                    external_trigger=False
                )
                return next_run

    def _process_task_instances(self, dag, queue):
        """
        This method schedules the tasks for a single DAG by looking at the
        active DAG runs and adding task instances that should run to the
        queue.
        """
        session = settings.Session()

        # update the state of the previously active dag runs
        dag_runs = DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)
        active_dag_runs = []
        for run in dag_runs:
            self.logger.info("Examining DAG run {}".format(run))
            # don't consider runs that are executed in the future
            if run.execution_date > datetime.now():
                self.logger.error("Execution date is in future: {}"
                                  .format(run.execution_date))
                continue

            if len(active_dag_runs) >= dag.max_active_runs:
                self.logger.info("Active dag runs > max_active_run.")
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
            self.logger.debug("Examining active DAG run {}".format(run))
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
                    self.logger.debug('Queuing task: {}'.format(ti))
                    queue.append(ti.key)

        session.close()

    @provide_session
    def _change_state_for_tis_without_dagrun(self,
                                             simple_dag_bag,
                                             old_states,
                                             new_state,
                                             session=None):
        """
        For all DAG IDs in the SimpleDagBag, look for task instances in the
        old_states and set them to new_state if the corresponding DagRun
        exists but is not in the running state. This normally should not
        happen, but it can if the state of DagRuns are changed manually.

        :param old_states: examine TaskInstances in this state
        :type old_state: list[State]
        :param new_state: set TaskInstances to this state
        :type new_state: State
        :param simple_dag_bag: TaskInstances associated with DAGs in the
        simple_dag_bag and with states in the old_state will be examined
        :type simple_dag_bag: SimpleDagBag
        """

        task_instances_to_change = (
            session
            .query(models.TaskInstance)
            .filter(models.TaskInstance.dag_id.in_(simple_dag_bag.dag_ids))
            .filter(models.TaskInstance.state.in_(old_states))
            .with_for_update()
            .all()
        )
        """:type: list[TaskInstance]"""

        for task_instance in task_instances_to_change:
            dag_runs = DagRun.find(dag_id=task_instance.dag_id,
                                   execution_date=task_instance.execution_date,
                                   )

            if len(dag_runs) == 0:
                self.logger.warn("DagRun for %s %s does not exist",
                                 task_instance.dag_id,
                                 task_instance.execution_date)
                continue

            # There should only be one DAG run. Add some logging info if this
            # is not the case for later debugging.
            if len(dag_runs) > 1:
                self.logger.warn("Multiple DagRuns found for {} {}: {}"
                                 .format(task_instance.dag_id,
                                         task_instance.execution_date,
                                         dag_runs))

            dag_is_running = True
            for dag_run in dag_runs:
                if dag_run.state == State.RUNNING:
                    dag_is_running = True
                    break

            if not dag_is_running:
                self.logger.warn("Setting {} to state={} as it does not have "
                                 "a DagRun in the {} state"
                                 .format(task_instance,
                                         new_state,
                                         State.RUNNING))
                task_instance.state = new_state
                session.merge(task_instance)
        session.commit()

    @provide_session
    def _execute_task_instances(self,
                                simple_dag_bag,
                                states,
                                session=None):
        """
        Fetches task instances from ORM in the specified states, figures
        out pool limits, and sends them to the executor for execution.

        :param simple_dag_bag: TaskInstances associated with DAGs in the
        simple_dag_bag will be fetched from the DB and executed
        :type simple_dag_bag: SimpleDagBag
        :param executor: the executor that runs task instances
        :type executor: BaseExecutor
        :param states: Execute TaskInstances in these states
        :type states: Tuple[State]
        :return: None
        """
        # Get all the relevant task instances
        TI = models.TaskInstance
        task_instances_to_examine = (
            session
            .query(TI)
            .filter(TI.dag_id.in_(simple_dag_bag.dag_ids))
            .filter(TI.state.in_(states))
            .all()
        )

        # Put one task instance on each line
        if len(task_instances_to_examine) == 0:
            self.logger.info("No tasks to send to the executor")
            return

        task_instance_str = "\n\t".join(
            ["{}".format(x) for x in task_instances_to_examine])
        self.logger.info("Tasks up for execution:\n\t{}".format(task_instance_str))

        # Get the pool settings
        pools = {p.pool: p for p in session.query(models.Pool).all()}

        pool_to_task_instances = defaultdict(list)
        for task_instance in task_instances_to_examine:
            pool_to_task_instances[task_instance.pool].append(task_instance)

        # Go through each pool, and queue up a task for execution if there are
        # any open slots in the pool.
        for pool, task_instances in pool_to_task_instances.items():
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            num_queued = len(task_instances)
            self.logger.info("Figuring out tasks to run in Pool(name={pool}) "
                             "with {open_slots} open slots and {num_queued} "
                             "task instances in queue".format(**locals()))

            if open_slots <= 0:
                continue

            priority_sorted_task_instances = sorted(
                task_instances, key=lambda ti: (-ti.priority_weight, ti.execution_date))

            # DAG IDs with running tasks that equal the concurrency limit of the dag
            dag_id_to_running_task_count = {}

            for task_instance in priority_sorted_task_instances:
                if open_slots <= 0:
                    self.logger.info("No more slots free")
                    # Can't schedule any more since there are no more open slots.
                    break

                if self.executor.has_task(task_instance):
                    self.logger.debug("Not handling task {} as the executor reports it is running"
                                      .format(task_instance.key))
                    continue
 
                if simple_dag_bag.get_dag(task_instance.dag_id).is_paused:
                    self.logger.info("Not executing queued {} since {} is paused"
                                     .format(task_instance, task_instance.dag_id))
                    continue

                # todo: remove this logic when backfills will be part of the scheduler
                dag_run = task_instance.get_dagrun()
                if dag_run and dag_run.is_backfill:
                    continue

                # Check to make sure that the task concurrency of the DAG hasn't been
                # reached.
                dag_id = task_instance.dag_id

                if dag_id not in dag_id_to_running_task_count:
                    dag_id_to_running_task_count[dag_id] = \
                        DagRun.get_running_tasks(
                            session,
                            dag_id,
                            simple_dag_bag.get_dag(dag_id).task_ids)

                current_task_concurrency = dag_id_to_running_task_count[dag_id]
                task_concurrency_limit = simple_dag_bag.get_dag(dag_id).concurrency
                self.logger.info("DAG {} has {}/{} running tasks"
                                 .format(dag_id,
                                         current_task_concurrency,
                                         task_concurrency_limit))
                if current_task_concurrency > task_concurrency_limit:
                    self.logger.info("Not executing {} since the number "
                                     "of tasks running from DAG {} is >= to the "
                                     "DAG's task concurrency limit of {}"
                                     .format(task_instance,
                                             dag_id,
                                             task_concurrency_limit))
                    continue

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
                    file_path=simple_dag_bag.get_dag(task_instance.dag_id).full_filepath,
                    pickle_id=simple_dag_bag.get_dag(task_instance.dag_id).pickle_id)

                priority = task_instance.priority_weight
                queue = task_instance.queue
                self.logger.info("Sending to executor {} with priority {} and queue {}"
                                 .format(task_instance.key, priority, queue))

                # Set the state to queued
                task_instance.refresh_from_db(lock_for_update=True, session=session)
                if task_instance.state not in states:
                    self.logger.info("Task {} was set to {} outside this scheduler."
                                     .format(task_instance.key, task_instance.state))
                    session.commit()
                    continue

                self.logger.info("Setting state of {} to {}".format(
                    task_instance.key, State.QUEUED))
                task_instance.state = State.QUEUED
                task_instance.queued_dttm = (datetime.now()
                                             if not task_instance.queued_dttm
                                             else task_instance.queued_dttm)
                session.merge(task_instance)
                session.commit()

                # These attributes will be lost after the object expires, so save them.
                task_id_ = task_instance.task_id
                dag_id_ = task_instance.dag_id
                execution_date_ = task_instance.execution_date
                make_transient(task_instance)
                task_instance.task_id = task_id_
                task_instance.dag_id = dag_id_
                task_instance.execution_date = execution_date_

                self.executor.queue_command(
                    task_instance,
                    command,
                    priority=priority,
                    queue=queue)

                open_slots -= 1

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
                self.logger.info("Not processing DAG {} since it's paused"
                                 .format(dag.dag_id))
                continue

            if not dag:
                self.logger.error("DAG ID {} was not found in the DagBag"
                                  .format(dag.dag_id))
                continue

            self.logger.info("Processing {}".format(dag.dag_id))

            dag_run = self.create_dag_run(dag)
            if dag_run:
                self.logger.info("Created {}".format(dag_run))
            self._process_task_instances(dag, tis_out)
            self.manage_slas(dag)

        models.DagStat.clean_dirty([d.dag_id for d in dags])

    def _process_executor_events(self):
        """
        Respond to executor events.

        :param executor: the executor that's running the task instances
        :type executor: BaseExecutor
        :return: None
        """
        for key, executor_state in list(self.executor.get_event_buffer().items()):
            dag_id, task_id, execution_date = key
            self.logger.info("Executor reports {}.{} execution_date={} as {}"
                             .format(dag_id,
                                     task_id,
                                     execution_date,
                                     executor_state))

    def _log_file_processing_stats(self,
                                   known_file_paths,
                                   processor_manager):
        """
        Print out stats about how files are getting processed.

        :param known_file_paths: a list of file paths that may contain Airflow
        DAG definitions
        :type known_file_paths: list[unicode]
        :param processor_manager: manager for the file processors
        :type stats: DagFileProcessorManager
        :return: None
        """

        # File Path: Path to the file containing the DAG definition
        # PID: PID associated with the process that's processing the file. May
        # be empty.
        # Runtime: If the process is currently running, how long it's been
        # running for in seconds.
        # Last Runtime: If the process ran before, how long did it take to
        # finish in seconds
        # Last Run: When the file finished processing in the previous run.
        headers = ["File Path",
                   "PID",
                   "Runtime",
                   "Last Runtime",
                   "Last Run"]

        rows = []
        for file_path in known_file_paths:
            last_runtime = processor_manager.get_last_runtime(file_path)
            processor_pid = processor_manager.get_pid(file_path)
            processor_start_time = processor_manager.get_start_time(file_path)
            runtime = ((datetime.now() - processor_start_time).total_seconds()
                       if processor_start_time else None)
            last_run = processor_manager.get_last_finish_time(file_path)

            rows.append((file_path,
                         processor_pid,
                         runtime,
                         last_runtime,
                         last_run))

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows = sorted(rows, key=lambda x: x[3] or 0.0)

        formatted_rows = []
        for file_path, pid, runtime, last_runtime, last_run in rows:
            formatted_rows.append((file_path,
                                   pid,
                                   "{:.2f}s".format(runtime)
                                   if runtime else None,
                                   "{:.2f}s".format(last_runtime)
                                   if last_runtime else None,
                                   last_run.strftime("%Y-%m-%dT%H:%M:%S")
                                   if last_run else None))
        log_str = ("\n" +
                   "=" * 80 +
                   "\n" +
                   "DAG File Processing Stats\n\n" +
                   tabulate(formatted_rows, headers=headers) +
                   "\n" +
                   "=" * 80)

        self.logger.info(log_str)

    @provide_session
    def _reset_state_for_orphaned_tasks(self, dag_run, session=None):
        """
        This function checks for a DagRun if there are any tasks
        that have a scheduled state but are not known by the
        executor. If it finds those it will reset the state to None
        so they will get picked up again.
        """
        queued_tis = self.executor.queued_tasks

        # also consider running as the state might not have changed in the db yet
        running = self.executor.running
        tis = list()
        tis.extend(dag_run.get_task_instances(state=State.SCHEDULED, session=session))
        tis.extend(dag_run.get_task_instances(state=State.QUEUED, session=session))

        for ti in tis:
            if ti.key not in queued_tis and ti.key not in running:
                self.logger.debug("Rescheduling orphaned task {}".format(ti))
                ti.state = State.NONE
        session.commit()

    def _execute(self):
        self.logger.info("Starting the scheduler")
        pessimistic_connection_handling()

        logging.basicConfig(level=logging.DEBUG)

        # DAGs can be pickled for easier remote execution by some executors
        pickle_dags = False
        if self.do_pickle and self.executor.__class__ not in \
                (executors.LocalExecutor, executors.SequentialExecutor):
            pickle_dags = True

        # Use multiple processes to parse and generate tasks for the
        # DAGs in parallel. By processing them in separate processes,
        # we can get parallelism and isolation from potentially harmful
        # user code.
        self.logger.info("Processing files using up to {} processes at a time "
                         .format(self.max_threads))
        self.logger.info("Running execute loop for {} seconds"
                         .format(self.run_duration))
        self.logger.info("Processing each file at most {} times"
                         .format(self.num_runs))
        self.logger.info("Process each file at most once every {} seconds"
                         .format(self.file_process_interval))
        self.logger.info("Checking for new files in {} every {} seconds"
                         .format(self.subdir, self.dag_dir_list_interval))

        # Build up a list of Python files that could contain DAGs
        self.logger.info("Searching for files in {}".format(self.subdir))
        known_file_paths = list_py_file_paths(self.subdir)
        self.logger.info("There are {} files in {}"
                         .format(len(known_file_paths), self.subdir))

        def processor_factory(file_path, log_file_path):
            return DagFileProcessor(file_path,
                                    pickle_dags,
                                    self.dag_ids,
                                    log_file_path)

        processor_manager = DagFileProcessorManager(self.subdir,
                                                    known_file_paths,
                                                    self.max_threads,
                                                    self.file_process_interval,
                                                    self.child_process_log_directory,
                                                    self.num_runs,
                                                    processor_factory)

        try:
            self._execute_helper(processor_manager)
        finally:
            self.logger.info("Exited execute loop")

            # Kill all child processes on exit since we don't want to leave
            # them as orphaned.
            pids_to_kill = processor_manager.get_all_pids()
            if len(pids_to_kill) > 0:
                # First try SIGTERM
                this_process = psutil.Process(os.getpid())
                # Only check child processes to ensure that we don't have a case
                # where we kill the wrong process because a child process died
                # but the PID got reused.
                child_processes = [x for x in this_process.children(recursive=True)
                                   if x.is_running() and x.pid in pids_to_kill]
                for child in child_processes:
                    self.logger.info("Terminating child PID: {}".format(child.pid))
                    child.terminate()
                timeout = 5
                self.logger.info("Waiting up to {}s for processes to exit..."
                                 .format(timeout))
                try:
                    psutil.wait_procs(child_processes, timeout)
                except psutil.TimeoutExpired:
                    self.logger.debug("Ran out of time while waiting for "
                                      "processes to exit")

                # Then SIGKILL
                child_processes = [x for x in this_process.children(recursive=True)
                                   if x.is_running() and x.pid in pids_to_kill]
                if len(child_processes) > 0:
                    for child in child_processes:
                        self.logger.info("Killing child PID: {}".format(child.pid))
                        child.kill()
                        child.wait()

    def _execute_helper(self, processor_manager):
        """
        :param processor_manager: manager to use
        :type processor_manager: DagFileProcessorManager
        :return: None
        """
        self.executor.start()

        session = settings.Session()
        self.logger.info("Resetting state for orphaned tasks")
        # grab orphaned tasks and make sure to reset their state
        active_runs = DagRun.find(
            state=State.RUNNING,
            external_trigger=False,
            session=session
        )
        for dr in active_runs:
            self.logger.info("Resetting {} {}".format(dr.dag_id,
                                                      dr.execution_date))
            self._reset_state_for_orphaned_tasks(dr, session=session)

        self.logger.info("Removing old import errors")
        self.clear_import_errors(session)
        session.close()

        execute_start_time = datetime.now()

        # Last time stats were printed
        last_stat_print_time = datetime(2000, 1, 1)
        # Last time that self.heartbeat() was called.
        last_self_heartbeat_time = datetime.now()
        # Last time that the DAG dir was traversed to look for files
        last_dag_dir_refresh_time = datetime.now()

        # Use this value initially
        known_file_paths = processor_manager.file_paths

        # For the execute duration, parse and schedule DAGs
        while (datetime.now() - execute_start_time).total_seconds() < \
                self.run_duration or self.run_duration < 0:
            self.logger.debug("Starting Loop...")
            loop_start_time = time.time()

            # Traverse the DAG directory for Python files containing DAGs
            # periodically
            elapsed_time_since_refresh = (datetime.now() -
                                          last_dag_dir_refresh_time).total_seconds()

            if elapsed_time_since_refresh > self.dag_dir_list_interval:
                # Build up a list of Python files that could contain DAGs
                self.logger.info("Searching for files in {}".format(self.subdir))
                known_file_paths = list_py_file_paths(self.subdir)
                last_dag_dir_refresh_time = datetime.now()
                self.logger.info("There are {} files in {}"
                                 .format(len(known_file_paths), self.subdir))
                processor_manager.set_file_paths(known_file_paths)

            # Kick of new processes and collect results from finished ones
            self.logger.info("Heartbeating the process manager")
            simple_dags = processor_manager.heartbeat()

            if self.using_sqlite:
                # For the sqlite case w/ 1 thread, wait until the processor
                # is finished to avoid concurrent access to the DB.
                self.logger.debug("Waiting for processors to finish since we're "
                                  "using sqlite")
                processor_manager.wait_until_finished()

            # Send tasks for execution if available
            if len(simple_dags) > 0:
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

            # Call hearbeats
            self.logger.info("Heartbeating the executor")
            self.executor.heartbeat()

            # Process events from the executor
            self._process_executor_events()

            # Heartbeat the scheduler periodically
            time_since_last_heartbeat = (datetime.now() -
                                         last_self_heartbeat_time).total_seconds()
            if time_since_last_heartbeat > self.heartrate:
                self.logger.info("Heartbeating the scheduler")
                self.heartbeat()
                last_self_heartbeat_time = datetime.now()

            # Occasionally print out stats about how fast the files are getting processed
            if ((datetime.now() - last_stat_print_time).total_seconds() >
                    self.print_stats_interval):
                if len(known_file_paths) > 0:
                    self._log_file_processing_stats(known_file_paths,
                                                    processor_manager)
                last_stat_print_time = datetime.now()

            loop_end_time = time.time()
            self.logger.debug("Ran scheduling loop in {:.2f}s"
                              .format(loop_end_time - loop_start_time))
            self.logger.debug("Sleeping for {:.2f}s"
                              .format(self._processor_poll_interval))
            time.sleep(self._processor_poll_interval)

            # Exit early for a test mode
            if processor_manager.max_runs_reached():
                self.logger.info("Exiting loop as all files have been processed "
                                 "{} times".format(self.num_runs))
                break

        # Stop any processors
        processor_manager.terminate()

        # Verify that all files were processed, and if so, deactivate DAGs that
        # haven't been touched by the scheduler as they likely have been
        # deleted.
        all_files_processed = True
        for file_path in known_file_paths:
            if processor_manager.get_last_finish_time(file_path) is None:
                all_files_processed = False
                break
        if all_files_processed:
            self.logger.info("Deactivating DAGs that haven't been touched since {}"
                             .format(execute_start_time.isoformat()))
            models.DAG.deactivate_stale_dags(execute_start_time)

        self.executor.end()

        settings.Session.remove()

    @provide_session
    def process_file(self, file_path, pickle_dags=False, session=None):
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
        :param pickle_dags: whether serialize the DAGs found in the file and
        save them to the db
        :type pickle_dags: bool
        :return: a list of SimpleDags made from the Dags found in the file
        :rtype: list[SimpleDag]
        """
        self.logger.info("Processing file {} for tasks to queue".format(file_path))
        # As DAGs are parsed from this file, they will be converted into SimpleDags
        simple_dags = []

        try:
            dagbag = models.DagBag(file_path)
        except Exception:
            self.logger.exception("Failed at reloading the DAG file {}".format(file_path))
            Stats.incr('dag_file_refresh_error', 1, 1)
            return []

        if len(dagbag.dags) > 0:
            self.logger.info("DAG(s) {} retrieved from {}"
                             .format(dagbag.dags.keys(),
                                     file_path))
        else:
            self.logger.warn("No viable dags retrieved from {}".format(file_path))
            return []

        # Save individual DAGs in the ORM and update DagModel.last_scheduled_time
        sync_time = datetime.now()
        for dag in dagbag.dags.values():
            models.DAG.sync_to_db(dag, dag.owner, sync_time)

        paused_dag_ids = [dag.dag_id for dag in dagbag.dags.values()
                          if dag.is_paused]

        # Pickle the DAGs (if necessary) and put them into a SimpleDag
        for dag_id in dagbag.dags:
            dag = dagbag.get_dag(dag_id)
            pickle_id = None
            if pickle_dags:
                pickle_id = dag.pickle(session).id

            task_ids = [task.task_id for task in dag.tasks]

            # Only return DAGs that are not paused
            if dag_id not in paused_dag_ids:
                simple_dags.append(SimpleDag(dag.dag_id,
                                             task_ids,
                                             dag.full_filepath,
                                             dag.concurrency,
                                             dag.is_paused,
                                             pickle_id))

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
            # a task that recently got it's state changed to RUNNING from somewhere
            # other than the scheduler from getting it's state overwritten.
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
            self.logger.info("Creating / updating {} in ORM".format(ti))
            session.merge(ti)
            session.commit()

        # Record import errors into the ORM
        try:
            self.record_import_errors(session, dagbag)
        except Exception:
            self.logger.exception("Error logging import errors!")
        try:
            dagbag.kill_zombies()
        except Exception:
            self.logger.exception("Error killing zombies!")

        return simple_dags

    @provide_session
    def heartbeat_callback(self, session=None):
        Stats.gauge('scheduler_heartbeat', 1, 1)


class BackfillJob(BaseJob):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def __init__(
            self,
            dag,
            start_date=None,
            end_date=None,
            mark_success=False,
            include_adhoc=False,
            donot_pickle=False,
            ignore_first_depends_on_past=False,
            ignore_task_deps=False,
            pool=None,
            *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.pool = pool
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _execute(self):
        """
        Runs a dag for a specified date range.
        """
        session = settings.Session()
        DagRun = models.DagRun

        # consider max_active_runs but ignore when running subdags
        # "parent.child" as a dag_id is by convention a subdag
        if self.dag.schedule_interval and not "." in self.dag.dag_id:
            active_runs = DagRun.find(
                dag_id=self.dag.dag_id,
                state=State.RUNNING,
                external_trigger=False,
                session=session
            )

            # return if already reached maximum active runs
            if len(active_runs) >= self.dag.max_active_runs:
                self.logger.info("Dag {} has reached maximum amount of {} dag runs"
                                 .format(self.dag.dag_id, self.dag.max_active_runs))
                return

        start_date = self.bf_start_date
        end_date = self.bf_end_date

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
        executor_fails = Counter()

        # Build a list of all instances to run
        tasks_to_run = {}
        failed = set()
        succeeded = set()
        started = set()
        skipped = set()
        not_ready = set()
        deadlocked = set()

        # create dag runs
        dr_start_date = start_date or min([t.start_date for t in self.dag.tasks])
        next_run_date = self.dag.normalize_schedule(dr_start_date)
        end_date = end_date or datetime.now()

        active_dag_runs = []
        while next_run_date and next_run_date <= end_date:
            run_id = 'backfill_' + next_run_date.isoformat()

            # check if we are scheduling on top of a already existing dag_run
            # we could find a "scheduled" run instead of a "backfill"
            run = DagRun.find(dag_id=self.dag.dag_id,
                              execution_date=next_run_date,
                              session=session)
            if not run:
                run = self.dag.create_dagrun(
                    run_id=run_id,
                    execution_date=next_run_date,
                    start_date=datetime.now(),
                    state=State.RUNNING,
                    external_trigger=False,
                    session=session,
                )
            else:
                run = run[0]

            # set required transient field
            run.dag = self.dag

            # explictely mark running as we can fill gaps
            run.state = State.RUNNING
            run.verify_integrity(session=session)

            # for some reason if we dont refresh the reference to run is lost
            run.refresh_from_db()
            make_transient(run)
            active_dag_runs.append(run)

            next_run_date = self.dag.following_schedule(next_run_date)

        run_count = 0
        for run in active_dag_runs:
            logging.info("Checking run {}".format(run))
            run_count = run_count + 1

            def get_task_instances_for_dag_run(dag_run):
                # this needs a fresh session sometimes tis get detached
                # can be more finegrained (excluding success or skipped)
                tasks = {}
                for ti in dag_run.get_task_instances():
                    tasks[ti.key] = ti
                return tasks

            # Triggering what is ready to get triggered
            while not deadlocked:
                tasks_to_run = get_task_instances_for_dag_run(run)
                self.logger.debug("Clearing out not_ready list")
                not_ready.clear()

                for key, ti in list(tasks_to_run.items()):
                    task = self.dag.get_task(ti.task_id)
                    ti.task = task

                    ignore_depends_on_past = (
                        self.ignore_first_depends_on_past and
                        ti.execution_date == (start_date or ti.start_date))
                    self.logger.debug("Task instance to run {} state {}"
                                      .format(ti, ti.state))
                    # The task was already marked successful or skipped by a
                    # different Job. Don't rerun it.
                    if ti.state == State.SUCCESS:
                        succeeded.add(key)
                        self.logger.debug("Task instance {} succeeded. "
                                          "Don't rerun.".format(ti))
                        tasks_to_run.pop(key)
                        continue
                    elif ti.state == State.SKIPPED:
                        skipped.add(key)
                        self.logger.debug("Task instance {} skipped. "
                                          "Don't rerun.".format(ti))
                        tasks_to_run.pop(key)
                        continue
                    elif ti.state == State.FAILED:
                        self.logger.error("Task instance {} failed".format(ti))
                        failed.add(key)
                        tasks_to_run.pop(key)
                        continue

                    backfill_context = DepContext(
                        deps=RUN_DEPS,
                        ignore_depends_on_past=ignore_depends_on_past,
                        ignore_task_deps=self.ignore_task_deps,
                        flag_upstream_failed=True)
                    # Is the task runnable? -- then run it
                    if ti.are_dependencies_met(
                            dep_context=backfill_context,
                            session=session,
                            verbose=True):
                        self.logger.debug('Sending {} to executor'.format(ti))
                        if ti.state == State.NONE:
                            ti.state = State.SCHEDULED
                            session.merge(ti)
                        session.commit()
                        executor.queue_task_instance(
                            ti,
                            mark_success=self.mark_success,
                            pickle_id=pickle_id,
                            ignore_task_deps=self.ignore_task_deps,
                            ignore_depends_on_past=ignore_depends_on_past,
                            pool=self.pool)
                        started.add(key)

                    # Mark the task as not ready to run
                    elif ti.state in (State.NONE, State.UPSTREAM_FAILED):
                        self.logger.debug('Adding {} to not_ready'.format(ti))
                        not_ready.add(key)

                    session.commit()

                self.heartbeat()
                executor.heartbeat()

                # If the set of tasks that aren't ready ever equals the set of
                # tasks to run, then the backfill is deadlocked
                if not_ready and not_ready == set(tasks_to_run):
                    self.logger.warn("Deadlock discovered for tasks_to_run={}"
                                     .format(tasks_to_run.values()))
                    deadlocked.update(tasks_to_run.values())
                    tasks_to_run.clear()

                # Reacting to events
                for key, state in list(executor.get_event_buffer().items()):
                    if key not in tasks_to_run:
                        self.logger.warn("{} state {} not in tasks_to_run={}"
                                         .format(key, state,
                                                 tasks_to_run.values()))
                        continue
                    ti = tasks_to_run[key]
                    ti.refresh_from_db()
                    logging.info("Executor state: {} task {}".format(state, ti))
                    # executor reports failure
                    if state == State.FAILED:

                        # task reports running
                        if ti.state == State.RUNNING:
                            msg = (
                                'Executor reports that task instance {} failed '
                                'although the task says it is running.'.format(ti))
                            self.logger.error(msg)
                            ti.handle_failure(msg)
                            tasks_to_run.pop(key)

                        # task reports skipped
                        elif ti.state == State.SKIPPED:
                            self.logger.error("Skipping {} ".format(ti))
                            skipped.add(key)
                            tasks_to_run.pop(key)

                        # anything else is a failure
                        else:
                            self.logger.error("Task instance {} failed".format(ti))
                            failed.add(key)
                            tasks_to_run.pop(key)

                    # executor reports success
                    elif state == State.SUCCESS:

                        # task reports success
                        if ti.state == State.SUCCESS:
                            self.logger.info(
                                'Task instance {} succeeded'.format(ti))
                            succeeded.add(key)
                            tasks_to_run.pop(key)

                        # task reports failure
                        elif ti.state == State.FAILED:
                            self.logger.error("Task instance {} failed".format(ti))
                            failed.add(key)
                            tasks_to_run.pop(key)

                        # task reports skipped
                        elif ti.state == State.SKIPPED:
                            self.logger.info("Task instance {} skipped".format(ti))
                            skipped.add(key)
                            tasks_to_run.pop(key)

                        # this probably won't ever be triggered
                        elif ti in not_ready:
                            self.logger.info(
                                "{} wasn't expected to run, but it did".format(ti))

                        # executor reports success but task does not - this is weird
                        elif ti.state not in (
                                State.SCHEDULED,
                                State.QUEUED,
                                State.UP_FOR_RETRY):
                            self.logger.error(
                                "The airflow run command failed "
                                "at reporting an error. This should not occur "
                                "in normal circumstances. Task state is '{}',"
                                "reported state is '{}'. TI is {}"
                                "".format(ti.state, state, ti))

                            # if the executor fails 3 or more times, stop trying to
                            # run the task
                            executor_fails[key] += 1
                            if executor_fails[key] >= 3:
                                msg = (
                                    'The airflow run command failed to report an '
                                    'error for task {} three or more times. The '
                                    'task is being marked as failed. This is very '
                                    'unusual and probably means that an error is '
                                    'taking place before the task even '
                                    'starts.'.format(key))
                                self.logger.error(msg)
                                ti.handle_failure(msg)
                                tasks_to_run.pop(key)

                msg = ' | '.join([
                    "[backfill progress]",
                    "dag run {6} of {7}",
                    "tasks waiting: {0}",
                    "succeeded: {1}",
                    "kicked_off: {2}",
                    "failed: {3}",
                    "skipped: {4}",
                    "deadlocked: {5}"
                ]).format(
                    len(tasks_to_run),
                    len(succeeded),
                    len(started),
                    len(failed),
                    len(skipped),
                    len(deadlocked),
                    run_count,
                    len(active_dag_runs))
                self.logger.info(msg)

                self.logger.debug("Finished dag run loop iteration. "
                                  "Remaining tasks {}"
                                  .format(tasks_to_run.values()))
                if len(tasks_to_run) == 0:
                    break

            # update dag run state
            run.update_state(session=session)
            if run.dag.is_paused:
                models.DagStat.clean_dirty([run.dag_id], session=session)

        executor.end()

        session.commit()
        session.close()

        err = ''
        if failed:
            err += (
                "---------------------------------------------------\n"
                "Some task instances failed:\n{}\n".format(failed))
        if deadlocked:
            err += (
                '---------------------------------------------------\n'
                'BackfillJob is deadlocked.')
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=True) !=
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=True)
                for t in deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += ' These tasks have succeeded:\n{}\n'.format(succeeded)
            err += ' These tasks have started:\n{}\n'.format(started)
            err += ' These tasks have failed:\n{}\n'.format(failed)
            err += ' These tasks are skipped:\n{}\n'.format(skipped)
            err += ' These tasks are deadlocked:\n{}\n'.format(deadlocked)
        if err:
            raise AirflowException(err)

        self.logger.info("Backfill done. Exiting.")


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

        # Keeps track of the fact that the task instance has been observed
        # as running at least once
        self.was_running = False

        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        try:
            command = self.task_instance.command(
                raw=True,
                ignore_all_deps = self.ignore_all_deps,
                ignore_depends_on_past = self.ignore_depends_on_past,
                ignore_task_deps = self.ignore_task_deps,
                ignore_ti_state = self.ignore_ti_state,
                pickle_id = self.pickle_id,
                mark_success = self.mark_success,
                job_id = self.id,
                pool = self.pool
            )
            self.process = subprocess.Popen(['bash', '-c', command])
            self.logger.info("Subprocess PID is {}".format(self.process.pid))
            ti = self.task_instance
            session = settings.Session()
            ti.pid = self.process.pid
            ti.hostname = socket.getfqdn()
            session.merge(ti)
            session.commit()
            session.close()

            last_heartbeat_time = time.time()
            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.process.poll()
                if return_code is not None:
                    return

                # Periodically heartbeat so that the scheduler doesn't think this
                # is a zombie
                try:
                    self.heartbeat()
                    last_heartbeat_time = time.time()
                except OperationalError:
                    Stats.incr('local_task_job_heartbeat_failure', 1, 1)
                    self.logger.exception("Exception while trying to heartbeat! "
                                          "Sleeping for {}s".format(self.heartrate))
                    time.sleep(self.heartrate)

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                time_since_last_heartbeat = time.time() - last_heartbeat_time
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.logger.error("Heartbeat time limited exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            # Kill processes that were left running
            kill_descendant_processes(self.logger)

    def on_kill(self):
        self.process.terminate()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # task is already terminating, let it breathe
            return

        # Suicide pill
        TI = models.TaskInstance
        ti = self.task_instance
        new_ti = session.query(TI).filter(
            TI.dag_id==ti.dag_id, TI.task_id==ti.task_id,
            TI.execution_date==ti.execution_date).scalar()
        if new_ti.state == State.RUNNING:
            self.was_running = True
            fqdn = socket.getfqdn()
            if not (fqdn == new_ti.hostname and self.process.pid == new_ti.pid):
                logging.warning("Recorded hostname and pid of {new_ti.hostname} "
                                "and {new_ti.pid} do not match this instance's "
                                "which are {fqdn} and "
                                "{self.process.pid}. Taking the poison pill. So "
                                "long."
                                .format(**locals()))
                raise AirflowException("Another worker/process is running this job")
        elif self.was_running and hasattr(self, 'process'):
            logging.warning(
                "State of this instance has been externally set to "
                "{self.task_instance.state}. "
                "Taking the poison pill. So long.".format(**locals()))
            self.process.terminate()
            self.terminating = True
