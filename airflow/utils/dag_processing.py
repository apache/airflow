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
"""Processes DAGs."""
import enum
import importlib
import logging
import multiprocessing
import os
import signal
import sys
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from importlib import import_module
from typing import Any, Callable, Dict, KeysView, List, NamedTuple, Optional, Tuple

from setproctitle import setproctitle  # pylint: disable=no-name-in-module
from sqlalchemy import or_
from tabulate import tabulate

import airflow.models
from airflow.configuration import conf
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.exceptions import AirflowException
from airflow.jobs.local_task_job import LocalTaskJob as LJ
from airflow.models import Connection, errors
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.process_utils import kill_child_processes_by_pids, reap_process_group
from airflow.utils.session import provide_session
from airflow.utils.state import State


class SimpleDag(BaseDag):
    """
    A simplified representation of a DAG that contains all attributes
    required for instantiating and scheduling its associated tasks.

    :param dag: the DAG
    :type dag: airflow.models.DAG
    :param pickle_id: ID associated with the pickled version of this DAG.
    :type pickle_id: unicode
    """

    def __init__(self, dag, pickle_id: Optional[str] = None):
        self._dag_id: str = dag.dag_id
        self._task_ids: List[str] = [task.task_id for task in dag.tasks]
        self._full_filepath: str = dag.full_filepath
        self._concurrency: int = dag.concurrency
        self._pickle_id: Optional[str] = pickle_id
        self._task_special_args: Dict[str, Any] = {}
        for task in dag.tasks:
            special_args = {}
            if task.task_concurrency is not None:
                special_args['task_concurrency'] = task.task_concurrency
            if special_args:
                self._task_special_args[task.task_id] = special_args

    @property
    def dag_id(self) -> str:
        """
        :return: the DAG ID
        :rtype: unicode
        """
        return self._dag_id

    @property
    def task_ids(self) -> List[str]:
        """
        :return: A list of task IDs that are in this DAG
        :rtype: list[unicode]
        """
        return self._task_ids

    @property
    def full_filepath(self) -> str:
        """
        :return: The absolute path to the file that contains this DAG's definition
        :rtype: unicode
        """
        return self._full_filepath

    @property
    def concurrency(self) -> int:
        """
        :return: maximum number of tasks that can run simultaneously from this DAG
        :rtype: int
        """
        return self._concurrency

    @property
    def pickle_id(self) -> Optional[str]:    # pylint: disable=invalid-overridden-method
        """
        :return: The pickle ID for this DAG, if it has one. Otherwise None.
        :rtype: unicode
        """
        return self._pickle_id

    @property
    def task_special_args(self) -> Dict[str, Any]:
        """Special arguments of the task."""
        return self._task_special_args

    def get_task_special_arg(self, task_id: str, special_arg_name: str):
        """Retrieve special arguments of the task."""
        if task_id in self._task_special_args and special_arg_name in self._task_special_args[task_id]:
            return self._task_special_args[task_id][special_arg_name]
        else:
            return None


class SimpleDagBag(BaseDagBag):
    """
    A collection of SimpleDag objects with some convenience methods.
    """

    def __init__(self, simple_dags: List[SimpleDag]):
        """
        Constructor.

        :param simple_dags: SimpleDag objects that should be in this
        :type list(airflow.utils.dag_processing.SimpleDag)
        """
        self.simple_dags = simple_dags
        self.dag_id_to_simple_dag: Dict[str, SimpleDag] = {}

        for simple_dag in simple_dags:
            self.dag_id_to_simple_dag[simple_dag.dag_id] = simple_dag

    @property
    def dag_ids(self) -> KeysView[str]:
        """
        :return: IDs of all the DAGs in this
        :rtype: list[unicode]
        """
        return self.dag_id_to_simple_dag.keys()

    def get_dag(self, dag_id: str) -> SimpleDag:
        """
        :param dag_id: DAG ID
        :type dag_id: unicode
        :return: if the given DAG ID exists in the bag, return the BaseDag
        corresponding to that ID. Otherwise, throw an Exception
        :rtype: airflow.utils.dag_processing.SimpleDag
        """
        if dag_id not in self.dag_id_to_simple_dag:
            raise AirflowException("Unknown DAG ID {}".format(dag_id))
        return self.dag_id_to_simple_dag[dag_id]


class AbstractDagFileProcessorProcess(metaclass=ABCMeta):
    """
    Processes a DAG file. See SchedulerJob.process_file() for more details.
    """

    @abstractmethod
    def start(self):
        """
        Launch the process to process the file
        """
        raise NotImplementedError()

    @abstractmethod
    def terminate(self, sigkill: bool = False):
        """
        Terminate (and then kill) the process launched to process the file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def pid(self) -> int:
        """
        :return: the PID of the process launched to process the given file
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def exit_code(self) -> int:
        """
        After the process is finished, this can be called to get the return code
        :return: the exit code of the process
        :rtype: int
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def done(self) -> bool:
        """
        Check if the process launched to process this file is done.
        :return: whether the process is finished running
        :rtype: bool
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def result(self) -> Tuple[List[SimpleDag], int]:
        """
        A list of simple dags found, and the number of import errors

        :return: result of running SchedulerJob.process_file()
        :rtype: tuple[list[airflow.utils.dag_processing.SimpleDag], int]
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def start_time(self):
        """
        :return: When this started to process the file
        :rtype: datetime
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def file_path(self):
        """
        :return: the path to the file that this is processing
        :rtype: unicode
        """
        raise NotImplementedError()


class DagParsingStat(NamedTuple):
    """Information on processing progress"""
    file_paths: List[str]
    done: bool
    all_files_processed: bool


class DagFileStat(NamedTuple):
    """Information about single processing of one file"""
    num_dags: int
    import_errors: int
    last_finish_time: datetime
    last_duration: float
    run_count: int


class DagParsingSignal(enum.Enum):
    """All signals sent to parser."""
    AGENT_HEARTBEAT = 'agent_heartbeat'
    TERMINATE_MANAGER = 'terminate_manager'
    END_MANAGER = 'end_manager'


class FailureCallbackRequest(NamedTuple):
    """A message with information about the callback to be executed."""
    full_filepath: str
    simple_task_instance: SimpleTaskInstance
    msg: str


class DagFileProcessorAgent(LoggingMixin):
    """
    Agent for DAG file processing. It is responsible for all DAG parsing
    related jobs in scheduler process. Mainly it can spin up DagFileProcessorManager
    in a subprocess, collect DAG parsing results from it and communicate
    signal/DAG parsing stat with it.

    This class runs in the main `airflow scheduler` process.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :type dag_directory: str
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :type max_runs: int
    :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path, log_file_path)
    :type processor_factory: (str, str, list) -> (AbstractDagFileProcessorProcess)
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :type processor_timeout: timedelta
    :param async_mode: Whether to start agent in async mode
    :type async_mode: bool
    """

    def __init__(self,
                 dag_directory,
                 max_runs,
                 processor_factory,
                 processor_timeout,
                 async_mode):
        self._file_path_queue = []
        self._dag_directory = dag_directory
        self._max_runs = max_runs
        self._processor_factory = processor_factory
        self._processor_timeout = processor_timeout
        self._async_mode = async_mode
        # Map from file path to the processor
        self._processors = {}
        # Pipe for communicating signals
        self._process = None
        self._done = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        self._parent_signal_conn = None
        self._collected_dag_buffer = []

    def start(self):
        """
        Launch DagFileProcessorManager processor and start DAG parsing loop in manager.
        """
        self._parent_signal_conn, child_signal_conn = multiprocessing.Pipe()
        self._process = multiprocessing.Process(
            target=type(self)._run_processor_manager,
            args=(
                self._dag_directory,
                self._max_runs,
                self._processor_factory,
                self._processor_timeout,
                child_signal_conn,
                self._async_mode,
            )
        )
        self._process.start()

        self.log.info("Launched DagFileProcessorManager with pid: %s", self._process.pid)

    def heartbeat(self):
        """
        Should only be used when launched DAG file processor manager in sync mode.
        Send agent heartbeat signal to the manager, requesting that it runs one
        processing "loop".

        Call wait_until_finished to ensure that any launched processors have
        finished before continuing
        """
        if not self._process.is_alive():
            return

        try:
            self._parent_signal_conn.send(DagParsingSignal.AGENT_HEARTBEAT)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_simple_dags calls _heartbeat_manager.
            pass

    def send_callback_to_execute(self, full_filepath: str, task_instance: TaskInstance, msg: str):
        """
        Sends information about the callback to be executed by DagFileProcessor.

        :param full_filepath: DAG File path
        :type full_filepath: str
        :param task_instance: Task Instance for which the callback is to be executed.
        :type task_instance: airflow.models.taskinstance.TaskInstance
        :param msg: Message sent in callback.
        :type msg: str
        """
        try:
            request = FailureCallbackRequest(
                full_filepath=full_filepath,
                simple_task_instance=SimpleTaskInstance(task_instance),
                msg=msg
            )
            self._parent_signal_conn.send(request)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_simple_dags calls _heartbeat_manager.
            pass

    def wait_until_finished(self):
        """Waits until DAG parsing is finished."""
        while self._parent_signal_conn.poll(timeout=None):
            try:
                result = self._parent_signal_conn.recv()
            except EOFError:
                break
            self._process_message(result)
            if isinstance(result, DagParsingStat):
                # In sync mode we don't send this message from the Manager
                # until all the running processors have finished
                return

    @staticmethod
    def _run_processor_manager(dag_directory,
                               max_runs,
                               processor_factory,
                               processor_timeout,
                               signal_conn,
                               async_mode):

        # Make this process start as a new process group - that makes it easy
        # to kill all sub-process of this at the OS-level, rather than having
        # to iterate the child processes
        os.setpgid(0, 0)

        setproctitle("airflow scheduler -- DagFileProcessorManager")
        # Reload configurations and settings to avoid collision with parent process.
        # Because this process may need custom configurations that cannot be shared,
        # e.g. RotatingFileHandler. And it can cause connection corruption if we
        # do not recreate the SQLA connection pool.
        os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER'] = 'True'
        os.environ['AIRFLOW__LOGGING__COLORED_CONSOLE_LOG'] = 'False'
        # Replicating the behavior of how logging module was loaded
        # in logging_config.py
        importlib.reload(import_module(airflow.settings.LOGGING_CLASS_PATH.rsplit('.', 1)[0]))
        importlib.reload(airflow.settings)
        airflow.settings.initialize()
        del os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER']
        processor_manager = DagFileProcessorManager(dag_directory,
                                                    max_runs,
                                                    processor_factory,
                                                    processor_timeout,
                                                    signal_conn,
                                                    async_mode)

        processor_manager.start()

    def harvest_simple_dags(self):
        """
        Harvest DAG parsing results from result queue and sync metadata from stat queue.

        :return: List of parsing result in SimpleDag format.
        """
        # Receive any pending messages before checking if the process has exited.
        while self._parent_signal_conn.poll(timeout=0.01):
            try:
                result = self._parent_signal_conn.recv()
            except (EOFError, ConnectionError):
                break
            self._process_message(result)
        simple_dags = self._collected_dag_buffer
        self._collected_dag_buffer = []

        # If it died unexpectedly restart the manager process
        self._heartbeat_manager()

        return simple_dags

    def _process_message(self, message):
        self.log.debug("Received message of type %s", type(message).__name__)
        if isinstance(message, DagParsingStat):
            self._sync_metadata(message)
        else:
            self._collected_dag_buffer.append(message)

    def _heartbeat_manager(self):
        """
        Heartbeat DAG file processor and restart it if we are not done.
        """
        if self._process and not self._process.is_alive():
            self._process.join(timeout=0)
            if not self.done:
                self.log.warning(
                    "DagFileProcessorManager (PID=%d) exited with exit code %d - re-launching",
                    self._process.pid, self._process.exitcode
                )
                self.start()

    def _sync_metadata(self, stat):
        """
        Sync metadata from stat queue and only keep the latest stat.
        """
        self._done = stat.done
        self._all_files_processed = stat.all_files_processed

    @property
    def done(self):
        """
        Has DagFileProcessorManager ended?
        """
        return self._done

    @property
    def all_files_processed(self):
        """
        Have all files been processed at least once?
        """
        return self._all_files_processed

    def terminate(self):
        """
        Send termination signal to DAG parsing processor manager
        and expect it to terminate all DAG file processors.
        """
        if self._process and self._process.is_alive():
            self.log.info("Sending termination message to manager.")
            try:
                self._parent_signal_conn.send(DagParsingSignal.TERMINATE_MANAGER)
            except ConnectionError:
                pass

    def end(self):
        """
        Terminate (and then kill) the manager process launched.
        :return:
        """
        if not self._process:
            self.log.warning('Ending without manager process.')
            return
        reap_process_group(self._process.pid, logger=self.log)
        self._parent_signal_conn.close()


class DagFileProcessorManager(LoggingMixin):  # pylint: disable=too-many-instance-attributes
    """
    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them and put the results to a multiprocessing.Queue
    for DagFileProcessorAgent to harvest. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :type dag_directory: unicode
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :type max_runs: int
    :param processor_factory: function that creates processors for DAG
        definition files. Arguments are (dag_definition_path)
    :type processor_factory: (unicode, unicode, list) -> (AbstractDagFileProcessorProcess)
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :type processor_timeout: timedelta
    :param signal_conn: connection to communicate signal with processor agent.
    :type signal_conn: multiprocessing.connection.Connection
    :param async_mode: whether to start the manager in async mode
    :type async_mode: bool
    """

    def __init__(self,
                 dag_directory: str,
                 max_runs: int,
                 processor_factory: Callable[
                     [str, List[FailureCallbackRequest]],
                     AbstractDagFileProcessorProcess
                 ],
                 processor_timeout: timedelta,
                 signal_conn: Connection,
                 async_mode: bool = True):
        self._file_paths: List[str] = []
        self._file_path_queue: List[str] = []
        self._dag_directory = dag_directory
        self._max_runs = max_runs
        self._processor_factory = processor_factory
        self._signal_conn = signal_conn
        self._async_mode = async_mode
        self._parsing_start_time: Optional[datetime] = None

        self._parallelism = conf.getint('scheduler', 'max_threads')
        if 'sqlite' in conf.get('core', 'sql_alchemy_conn') and self._parallelism > 1:
            self.log.warning(
                "Because we cannot use more than 1 thread (max_threads = "
                "%d ) when using sqlite. So we set parallelism to 1.", self._parallelism
            )
            self._parallelism = 1

        # Parse and schedule each file no faster than this interval.
        self._file_process_interval = conf.getint('scheduler',
                                                  'min_file_process_interval')
        # How often to print out DAG file processing stats to the log. Default to
        # 30 seconds.
        self.print_stats_interval = conf.getint('scheduler',
                                                'print_stats_interval')
        # How many seconds do we wait for tasks to heartbeat before mark them as zombies.
        self._zombie_threshold_secs = (
            conf.getint('scheduler', 'scheduler_zombie_task_threshold'))
        # Map from file path to the processor
        self._processors: Dict[str, AbstractDagFileProcessorProcess] = {}

        self._num_run = 0

        # Map from file path to stats about the file
        self._file_stats: Dict[str, DagFileStat] = {}

        self._last_zombie_query_time = None
        # Last time that the DAG dir was traversed to look for files
        self.last_dag_dir_refresh_time = timezone.make_aware(datetime.fromtimestamp(0))
        # Last time stats were printed
        self.last_stat_print_time = timezone.datetime(2000, 1, 1)
        # TODO: Remove magic number
        self._zombie_query_interval = 10
        # How long to wait before timing out a process to parse a DAG file
        self._processor_timeout = processor_timeout

        # How often to scan the DAGs directory for new files. Default to 5 minutes.
        self.dag_dir_list_interval = conf.getint('scheduler', 'dag_dir_list_interval')

        # Mapping file name and callbacks requests
        self._callback_to_execute: Dict[str, List[FailureCallbackRequest]] = defaultdict(list)

        self._log = logging.getLogger('airflow.processor_manager')

    def register_exit_signals(self):
        """
        Register signals that stop child processes
        """
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, signum, frame):  # pylint: disable=unused-argument
        """
        Helper method to clean up DAG file processors to avoid leaving orphan processes.
        """
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.terminate()
        self.end()
        self.log.debug("Finished terminating DAG processors.")
        sys.exit(os.EX_OK)

    def start(self):
        """
        Use multiple processes to parse and generate tasks for the
        DAGs in parallel. By processing them in separate processes,
        we can get parallelism and isolation from potentially harmful
        user code.
        """

        self.register_exit_signals()

        # Start a new process group
        os.setpgid(0, 0)

        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        self.log.info(
            "Checking for new files in %s every %s seconds", self._dag_directory, self.dag_dir_list_interval
        )

        # In sync mode we want timeout=None -- wait forever until a message is received
        if self._async_mode:
            poll_time = 0.0
        else:
            poll_time = None

        # Used to track how long it takes us to get once around every file in the DAG folder.
        self._parsing_start_time = timezone.utcnow()
        while True:
            loop_start_time = time.time()

            # pylint: disable=no-else-break
            if self._signal_conn.poll(poll_time):
                agent_signal = self._signal_conn.recv()
                self.log.debug("Received %s signal from DagFileProcessorAgent", agent_signal)
                if agent_signal == DagParsingSignal.TERMINATE_MANAGER:
                    self.terminate()
                    break
                elif agent_signal == DagParsingSignal.END_MANAGER:
                    self.end()
                    sys.exit(os.EX_OK)
                elif agent_signal == DagParsingSignal.AGENT_HEARTBEAT:
                    # continue the loop to parse dags
                    pass
                elif isinstance(agent_signal, FailureCallbackRequest):
                    self._add_callback_to_queue(agent_signal)
                else:
                    raise AirflowException("Invalid message")
            elif not self._async_mode:
                # In "sync" mode we don't want to parse the DAGs until we
                # are told to (as that would open another connection to the
                # SQLite DB which isn't a good practice
                continue
            # pylint: enable=no-else-break
            self._refresh_dag_dir()
            self._find_zombies()  # pylint: disable=no-value-for-parameter

            self._kill_timed_out_processors()

            # Generate more file paths to process if we processed all the files
            # already.
            if not self._file_path_queue:
                self.emit_metrics()
                self.prepare_file_path_queue()

            self.start_new_processes()

            # Update number of loop iteration.
            self._num_run += 1

            simple_dags = self.collect_results()
            for simple_dag in simple_dags:
                self._signal_conn.send(simple_dag)

            if not self._async_mode:
                self.log.debug(
                    "Waiting for processors to finish since we're using sqlite")
                # Wait until the running DAG processors are finished before
                # sending a DagParsingStat message back. This means the Agent
                # can tell we've got to the end of this iteration when it sees
                # this type of message
                self.wait_until_finished()

                # Collect anything else that has finished, but don't kick off any more processors
                simple_dags = self.collect_results()
                for simple_dag in simple_dags:
                    self._signal_conn.send(simple_dag)

            self._print_stat()

            all_files_processed = all(self.get_last_finish_time(x) is not None for x in self.file_paths)
            max_runs_reached = self.max_runs_reached()

            dag_parsing_stat = DagParsingStat(self._file_paths,
                                              max_runs_reached,
                                              all_files_processed,
                                              )
            self._signal_conn.send(dag_parsing_stat)

            if max_runs_reached:
                self.log.info("Exiting dag parsing loop as all files "
                              "have been processed %s times", self._max_runs)
                break

            if self._async_mode:
                loop_duration = time.time() - loop_start_time
                if loop_duration < 1:
                    poll_time = 1 - loop_duration
                else:
                    poll_time = 0.0

    def _add_callback_to_queue(self, request: FailureCallbackRequest):
        self._callback_to_execute[request.full_filepath].append(request)
        # Callback has a higher priority over DAG Run scheduling
        if request.full_filepath in self._file_path_queue:
            self._file_path_queue.remove(request.full_filepath)
        self._file_path_queue.insert(0, request.full_filepath)

    def _refresh_dag_dir(self):
        """
        Refresh file paths from dag dir if we haven't done it for too long.
        """
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_dag_dir_refresh_time).total_seconds()
        if elapsed_time_since_refresh > self.dag_dir_list_interval:
            # Build up a list of Python files that could contain DAGs
            self.log.info("Searching for files in %s", self._dag_directory)
            self._file_paths = list_py_file_paths(self._dag_directory)
            self.last_dag_dir_refresh_time = now
            self.log.info("There are %s files in %s", len(self._file_paths), self._dag_directory)
            self.set_file_paths(self._file_paths)

            # noinspection PyBroadException
            try:
                self.log.debug("Removing old import errors")
                self.clear_nonexistent_import_errors()  # pylint: disable=no-value-for-parameter
            except Exception:  # pylint: disable=broad-except
                self.log.exception("Error removing old import errors")

            if STORE_SERIALIZED_DAGS:
                from airflow.models.serialized_dag import SerializedDagModel
                from airflow.models.dag import DagModel
                SerializedDagModel.remove_deleted_dags(self._file_paths)
                DagModel.deactivate_deleted_dags(self._file_paths)

    def _print_stat(self):
        """
        Occasionally print out stats about how fast the files are getting processed
        """
        if 0 < self.print_stats_interval < (
                timezone.utcnow() - self.last_stat_print_time).total_seconds():
            if self._file_paths:
                self._log_file_processing_stats(self._file_paths)
            self.last_stat_print_time = timezone.utcnow()

    @provide_session
    def clear_nonexistent_import_errors(self, session):
        """
        Clears import errors for files that no longer exist.

        :param session: session for ORM operations
        :type session: sqlalchemy.orm.session.Session
        """
        query = session.query(errors.ImportError)
        if self._file_paths:
            query = query.filter(
                ~errors.ImportError.filename.in_(self._file_paths)
            )
        query.delete(synchronize_session='fetch')
        session.commit()

    def _log_file_processing_stats(self, known_file_paths):
        """
        Print out stats about how files are getting processed.

        :param known_file_paths: a list of file paths that may contain Airflow
            DAG definitions
        :type known_file_paths: list[unicode]
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
                   "# DAGs",
                   "# Errors",
                   "Last Runtime",
                   "Last Run"]

        rows = []
        now = timezone.utcnow()
        for file_path in known_file_paths:
            last_runtime = self.get_last_runtime(file_path)
            num_dags = self.get_last_dag_count(file_path)
            num_errors = self.get_last_error_count(file_path)
            file_name = os.path.basename(file_path)
            file_name = os.path.splitext(file_name)[0].replace(os.sep, '.')

            processor_pid = self.get_pid(file_path)
            processor_start_time = self.get_start_time(file_path)
            runtime = ((now - processor_start_time) if processor_start_time else None)
            last_run = self.get_last_finish_time(file_path)
            if last_run:
                seconds_ago = (now - last_run).total_seconds()
                Stats.gauge('dag_processing.last_run.seconds_ago.{}'.format(file_name), seconds_ago)
            if runtime:
                Stats.timing('dag_processing.last_duration.{}'.format(file_name), runtime)
                # TODO: Remove before Airflow 2.0
                Stats.timing('dag_processing.last_runtime.{}'.format(file_name), runtime)

            rows.append((file_path,
                         processor_pid,
                         runtime,
                         num_dags,
                         num_errors,
                         last_runtime,
                         last_run))

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows = sorted(rows, key=lambda x: x[3] or 0.0)

        formatted_rows = []
        for file_path, pid, runtime, num_dags, num_errors, last_runtime, last_run in rows:
            formatted_rows.append((file_path,
                                   pid,
                                   "{:.2f}s".format(runtime.total_seconds()) if runtime else None,
                                   num_dags,
                                   num_errors,
                                   "{:.2f}s".format(last_runtime) if last_runtime else None,
                                   last_run.strftime("%Y-%m-%dT%H:%M:%S") if last_run else None
                                   ))
        log_str = ("\n" +
                   "=" * 80 +
                   "\n" +
                   "DAG File Processing Stats\n\n" +
                   tabulate(formatted_rows, headers=headers) +
                   "\n" +
                   "=" * 80)

        self.log.info(log_str)

    def get_pid(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the PID of the process processing the given file or None if
            the specified file is not being processed
        :rtype: int
        """
        if file_path in self._processors:
            return self._processors[file_path].pid
        return None

    def get_all_pids(self):
        """
        :return: a list of the PIDs for the processors that are running
        :rtype: List[int]
        """
        return [x.pid for x in self._processors.values()]

    def get_last_runtime(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the runtime (in seconds) of the process of the last run, or
            None if the file was never processed.
        :rtype: float
        """
        stat = self._file_stats.get(file_path)
        return stat.last_duration if stat else None

    def get_last_dag_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the number of dags loaded from that file, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.num_dags if stat else None

    def get_last_error_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the number of import errors from processing, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.import_errors if stat else None

    def get_last_finish_time(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :type file_path: unicode
        :return: the finish time of the process of the last run, or None if the
            file was never processed.
        :rtype: datetime
        """
        stat = self._file_stats.get(file_path)
        return stat.last_finish_time if stat else None

    def get_start_time(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the start time of the process that's processing the
            specified file or None if the file is not currently being processed
        :rtype: datetime
        """
        if file_path in self._processors:
            return self._processors[file_path].start_time
        return None

    def get_run_count(self, file_path):
        """
        :param file_path: the path to the file that's being processed
        :type file_path: unicode
        :return: the number of times the given file has been parsed
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.run_count if stat else 0

    def set_file_paths(self, new_file_paths):
        """
        Update this with a new set of paths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :type new_file_paths: list[unicode]
        :return: None
        """
        self._file_paths = new_file_paths
        self._file_path_queue = [x for x in self._file_path_queue
                                 if x in new_file_paths]
        # Stop processors that are working on deleted files
        filtered_processors = {}
        for file_path, processor in self._processors.items():
            if file_path in new_file_paths:
                filtered_processors[file_path] = processor
            else:
                self.log.warning("Stopping processor for %s", file_path)
                Stats.decr('dag_processing.processes')
                processor.terminate()
                self._file_stats.pop(file_path)
        self._processors = filtered_processors

    def wait_until_finished(self):
        """
        Sleeps until all the processors are done.
        """
        for processor in self._processors.values():
            while not processor.done:
                time.sleep(0.1)

    def collect_results(self):
        """
        Collect the result from any finished DAG processors

        :return: a list of SimpleDags that were produced by processors that
            have finished since the last time this was called
        :rtype: list[airflow.utils.dag_processing.SimpleDag]
        """
        finished_processors: Dict[str, AbstractDagFileProcessorProcess] = {}
        running_processors: Dict[str, AbstractDagFileProcessorProcess] = {}

        for file_path, processor in self._processors.items():
            if processor.done:
                self.log.debug("Processor for %s finished", file_path)
                Stats.decr('dag_processing.processes')
                now = timezone.utcnow()
                finished_processors[file_path] = processor

                stat = DagFileStat(
                    len(processor.result[0]) if processor.result is not None else 0,
                    processor.result[1] if processor.result is not None else -1,
                    now,
                    (now - processor.start_time).total_seconds(),
                    self.get_run_count(file_path) + 1,
                )
                self._file_stats[file_path] = stat
            else:
                running_processors[file_path] = processor
        self._processors = running_processors

        self.log.debug("%s/%s DAG parsing processes running",
                       len(self._processors), self._parallelism)

        self.log.debug("%s file paths queued for processing",
                       len(self._file_path_queue))

        # Collect all the DAGs that were found in the processed files
        simple_dags = []
        for file_path, processor in finished_processors.items():
            if processor.result is None:
                self.log.error(
                    "Processor for %s exited with return code %s.",
                    processor.file_path, processor.exit_code
                )
            else:
                for simple_dag in processor.result[0]:
                    simple_dags.append(simple_dag)

        return simple_dags

    def start_new_processes(self):
        """"
        Start more processors if we have enough slots and files to process
        """
        while self._parallelism - len(self._processors) > 0 and self._file_path_queue:
            file_path = self._file_path_queue.pop(0)
            callback_to_execute_for_file = self._callback_to_execute[file_path]
            processor = self._processor_factory(file_path, callback_to_execute_for_file)
            del self._callback_to_execute[file_path]
            Stats.incr('dag_processing.processes')

            processor.start()
            self.log.debug(
                "Started a process (PID: %s) to generate tasks for %s",
                processor.pid, file_path
            )
            self._processors[file_path] = processor

    def prepare_file_path_queue(self):
        """
        Generate more file paths to process. Result are saved in _file_path_queue.
        """
        self._parsing_start_time = timezone.utcnow()
        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        file_paths_in_progress = self._processors.keys()
        now = timezone.utcnow()
        file_paths_recently_processed = []
        for file_path in self._file_paths:
            last_finish_time = self.get_last_finish_time(file_path)
            if (last_finish_time is not None and
                (now - last_finish_time).total_seconds() <
                    self._file_process_interval):
                file_paths_recently_processed.append(file_path)

        files_paths_at_run_limit = [file_path
                                    for file_path, stat in self._file_stats.items()
                                    if stat.run_count == self._max_runs]

        files_paths_to_queue = list(set(self._file_paths) -
                                    set(file_paths_in_progress) -
                                    set(file_paths_recently_processed) -
                                    set(files_paths_at_run_limit))

        for file_path, processor in self._processors.items():
            self.log.debug(
                "File path %s is still being processed (started: %s)",
                processor.file_path, processor.start_time.isoformat()
            )

        self.log.debug(
            "Queuing the following files for processing:\n\t%s",
            "\n\t".join(files_paths_to_queue)
        )

        for file_path in files_paths_to_queue:
            if file_path not in self._file_stats:
                self._file_stats[file_path] = DagFileStat(0, 0, None, None, 0)

        self._file_path_queue.extend(files_paths_to_queue)

    @provide_session
    def _find_zombies(self, session):
        """
        Find zombie task instances, which are tasks haven't heartbeated for too long
        and update the current zombie list.
        """
        now = timezone.utcnow()
        if not self._last_zombie_query_time or \
                (now - self._last_zombie_query_time).total_seconds() > self._zombie_query_interval:
            # to avoid circular imports
            self.log.info("Finding 'running' jobs without a recent heartbeat")
            TI = airflow.models.TaskInstance
            DM = airflow.models.DagModel
            limit_dttm = timezone.utcnow() - timedelta(seconds=self._zombie_threshold_secs)
            self.log.info("Failing jobs without heartbeat after %s", limit_dttm)

            zombies = (
                session.query(TI, DM.fileloc)
                .join(LJ, TI.job_id == LJ.id)
                .join(DM, TI.dag_id == DM.dag_id)
                .filter(TI.state == State.RUNNING)
                .filter(
                    or_(
                        LJ.state != State.RUNNING,
                        LJ.latest_heartbeat < limit_dttm,
                    )
                ).all()
            )

            self._last_zombie_query_time = timezone.utcnow()
            for ti, file_loc in zombies:
                request = FailureCallbackRequest(
                    full_filepath=file_loc,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Detected as zombie",
                )
                self.log.info("Detected zombie job: %s", request)
                self._add_callback_to_queue(request)
                Stats.incr('zombies_killed')

    def _kill_timed_out_processors(self):
        """
        Kill any file processors that timeout to defend against process hangs.
        """
        now = timezone.utcnow()
        for file_path, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self._processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s started at %s has timed out, "
                    "killing it.",
                    file_path, processor.pid, processor.start_time.isoformat())
                Stats.decr('dag_processing.processes')
                Stats.incr('dag_processing.processor_timeouts')
                # TODO: Remove ater Airflow 2.0
                Stats.incr('dag_file_processor_timeouts')
                processor.kill()

    def max_runs_reached(self):
        """
        :return: whether all file paths have been processed max_runs times
        """
        if self._max_runs == -1:  # Unlimited runs.
            return False
        for stat in self._file_stats.values():
            if stat.run_count < self._max_runs:
                return False
        if self._num_run < self._max_runs:
            return False
        return True

    def terminate(self):
        """
        Stops all running processors
        :return: None
        """
        for processor in self._processors.values():
            Stats.decr('dag_processing.processes')
            processor.terminate()

    def end(self):
        """
        Kill all child processes on exit since we don't want to leave
        them as orphaned.
        """
        pids_to_kill = self.get_all_pids()
        if pids_to_kill:
            kill_child_processes_by_pids(pids_to_kill)

    def emit_metrics(self):
        """
        Emmit metrics about dag parsing summary

        This is called once every time around the parsing "loop" - i.e. after
        all files have been parsed.
        """

        parse_time = (timezone.utcnow() - self._parsing_start_time).total_seconds()
        Stats.gauge('dag_processing.total_parse_time', parse_time)
        Stats.gauge('dagbag_size', sum(stat.num_dags for stat in self._file_stats.values()))
        Stats.gauge('dag_processing.import_errors',
                    sum(stat.import_errors for stat in self._file_stats.values()))

        # TODO: Remove before Airflow 2.0
        Stats.gauge('collect_dags', parse_time)
        Stats.gauge('dagbag_import_errors', sum(stat.import_errors for stat in self._file_stats.values()))

    # pylint: disable=missing-docstring
    @property
    def file_paths(self):
        return self._file_paths
