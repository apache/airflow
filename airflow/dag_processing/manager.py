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
import inspect
import logging
import multiprocessing
import os
import random
import signal
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from importlib import import_module
from multiprocessing.connection import Connection as MultiprocessingConnection
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Union, cast

from setproctitle import setproctitle
from sqlalchemy.orm import Session
from tabulate import tabulate

import airflow.models
from airflow.callbacks.callback_requests import CallbackRequest, SlaCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.models import DagModel, DagWarning, DbCallbackRequest, errors
from airflow.models.serialized_dag import SerializedDagModel
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.file import list_py_file_paths, might_contain_dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.mixins import MultiprocessingStartMethodMixin
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import (
    kill_child_processes_by_pids,
    reap_process_group,
    set_new_process_group,
)
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import prohibit_commit, skip_locked, with_row_locks

if TYPE_CHECKING:
    import pathlib


class DagParsingStat(NamedTuple):
    """Information on processing progress"""

    done: bool
    all_files_processed: bool


class DagFileStat(NamedTuple):
    """Information about single processing of one file"""

    num_dags: int
    import_errors: int
    last_finish_time: Optional[datetime]
    last_duration: Optional[timedelta]
    run_count: int


class DagParsingSignal(enum.Enum):
    """All signals sent to parser."""

    AGENT_RUN_ONCE = 'agent_run_once'
    TERMINATE_MANAGER = 'terminate_manager'
    END_MANAGER = 'end_manager'


class DagFileProcessorAgent(LoggingMixin, MultiprocessingStartMethodMixin):
    """
    Agent for DAG file processing. It is responsible for all DAG parsing
    related jobs in scheduler process. Mainly it can spin up DagFileProcessorManager
    in a subprocess, collect DAG parsing results from it and communicate
    signal/DAG parsing stat with it.

    This class runs in the main `airflow scheduler` process.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :param pickle_dags: whether to pickle DAGs.
    :param async_mode: Whether to start agent in async mode
    """

    def __init__(
        self,
        dag_directory: str,
        max_runs: int,
        processor_timeout: timedelta,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        async_mode: bool,
    ):
        super().__init__()
        self._file_path_queue: List[str] = []
        self._dag_directory: str = dag_directory
        self._max_runs = max_runs
        self._processor_timeout = processor_timeout
        self._dag_ids = dag_ids
        self._pickle_dags = pickle_dags
        self._async_mode = async_mode
        # Map from file path to the processor
        self._processors: Dict[str, DagFileProcessorProcess] = {}
        # Pipe for communicating signals
        self._process: Optional[multiprocessing.process.BaseProcess] = None
        self._done: bool = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        self._parent_signal_conn: Optional[MultiprocessingConnection] = None

        self._last_parsing_stat_received_at: float = time.monotonic()

    def start(self) -> None:
        """Launch DagFileProcessorManager processor and start DAG parsing loop in manager."""
        mp_start_method = self._get_multiprocessing_start_method()
        context = multiprocessing.get_context(mp_start_method)
        self._last_parsing_stat_received_at = time.monotonic()

        self._parent_signal_conn, child_signal_conn = context.Pipe()
        process = context.Process(
            target=type(self)._run_processor_manager,
            args=(
                self._dag_directory,
                self._max_runs,
                self._processor_timeout,
                child_signal_conn,
                self._dag_ids,
                self._pickle_dags,
                self._async_mode,
            ),
        )
        self._process = process

        process.start()

        self.log.info("Launched DagFileProcessorManager with pid: %s", process.pid)

    def run_single_parsing_loop(self) -> None:
        """
        Should only be used when launched DAG file processor manager in sync mode.
        Send agent heartbeat signal to the manager, requesting that it runs one
        processing "loop".

        Call wait_until_finished to ensure that any launched processors have
        finished before continuing
        """
        if not self._parent_signal_conn or not self._process:
            raise ValueError("Process not started.")
        if not self._process.is_alive():
            return

        try:
            self._parent_signal_conn.send(DagParsingSignal.AGENT_RUN_ONCE)
        except ConnectionError:
            # If this died cos of an error then we will noticed and restarted
            # when harvest_serialized_dags calls _heartbeat_manager.
            pass

    def get_callbacks_pipe(self) -> MultiprocessingConnection:
        """Returns the pipe for sending Callbacks to DagProcessorManager."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        return self._parent_signal_conn

    def wait_until_finished(self) -> None:
        """Waits until DAG parsing is finished."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        if self._async_mode:
            raise RuntimeError("wait_until_finished should only be called in sync_mode")
        while self._parent_signal_conn.poll(timeout=None):
            try:
                result = self._parent_signal_conn.recv()
            except EOFError:
                return
            self._process_message(result)
            if isinstance(result, DagParsingStat):
                # In sync mode (which is the only time we call this function) we don't send this message from
                # the Manager until all the running processors have finished
                return

    @staticmethod
    def _run_processor_manager(
        dag_directory: str,
        max_runs: int,
        processor_timeout: timedelta,
        signal_conn: MultiprocessingConnection,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        async_mode: bool,
    ) -> None:

        # Make this process start as a new process group - that makes it easy
        # to kill all sub-process of this at the OS-level, rather than having
        # to iterate the child processes
        set_new_process_group()

        setproctitle("airflow scheduler -- DagFileProcessorManager")
        # Reload configurations and settings to avoid collision with parent process.
        # Because this process may need custom configurations that cannot be shared,
        # e.g. RotatingFileHandler. And it can cause connection corruption if we
        # do not recreate the SQLA connection pool.
        os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER'] = 'True'
        os.environ['AIRFLOW__LOGGING__COLORED_CONSOLE_LOG'] = 'False'
        # Replicating the behavior of how logging module was loaded
        # in logging_config.py

        # TODO: This reloading should be removed when we fix our logging behaviour
        # In case of "spawn" method of starting processes for multiprocessing, reinitializing of the
        # SQLAlchemy engine causes extremely unexpected behaviour of messing with objects already loaded
        # in a parent process (likely via resources shared in memory by the ORM libraries).
        # This caused flaky tests in our CI for many months and has been discovered while
        # iterating on https://github.com/apache/airflow/pull/19860
        # The issue that describes the problem and possible remediation is
        # at https://github.com/apache/airflow/issues/19934

        importlib.reload(import_module(airflow.settings.LOGGING_CLASS_PATH.rsplit('.', 1)[0]))  # type: ignore
        importlib.reload(airflow.settings)
        airflow.settings.initialize()
        del os.environ['CONFIG_PROCESSOR_MANAGER_LOGGER']
        processor_manager = DagFileProcessorManager(
            dag_directory=dag_directory,
            max_runs=max_runs,
            processor_timeout=processor_timeout,
            dag_ids=dag_ids,
            pickle_dags=pickle_dags,
            signal_conn=signal_conn,
            async_mode=async_mode,
        )

        processor_manager.start()

    def heartbeat(self) -> None:
        """Check if the DagFileProcessorManager process is alive, and process any pending messages"""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        # Receive any pending messages before checking if the process has exited.
        while self._parent_signal_conn.poll(timeout=0.01):
            try:
                result = self._parent_signal_conn.recv()
            except (EOFError, ConnectionError):
                break
            self._process_message(result)

        # If it died unexpectedly restart the manager process
        self._heartbeat_manager()

    def _process_message(self, message):
        self.log.debug("Received message of type %s", type(message).__name__)
        if isinstance(message, DagParsingStat):
            self._sync_metadata(message)
        else:
            raise RuntimeError(f"Unexpected message received of type {type(message).__name__}")

    def _heartbeat_manager(self):
        """Heartbeat DAG file processor and restart it if we are not done."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        if self._process and not self._process.is_alive():
            self._process.join(timeout=0)
            if not self.done:
                self.log.warning(
                    "DagFileProcessorManager (PID=%d) exited with exit code %d - re-launching",
                    self._process.pid,
                    self._process.exitcode,
                )
                self.start()

        if self.done:
            return

        parsing_stat_age = time.monotonic() - self._last_parsing_stat_received_at
        if parsing_stat_age > self._processor_timeout.total_seconds():
            Stats.incr('dag_processing.manager_stalls')
            self.log.error(
                "DagFileProcessorManager (PID=%d) last sent a heartbeat %.2f seconds ago! Restarting it",
                self._process.pid,
                parsing_stat_age,
            )
            reap_process_group(self._process.pid, logger=self.log)
            self.start()

    def _sync_metadata(self, stat):
        """Sync metadata from stat queue and only keep the latest stat."""
        self._done = stat.done
        self._all_files_processed = stat.all_files_processed
        self._last_parsing_stat_received_at = time.monotonic()

    @property
    def done(self) -> bool:
        """Has DagFileProcessorManager ended?"""
        return self._done

    @property
    def all_files_processed(self):
        """Have all files been processed at least once?"""
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
        # Give the Manager some time to cleanly shut down, but not too long, as
        # it's better to finish sooner than wait for (non-critical) work to
        # finish
        self._process.join(timeout=1.0)
        reap_process_group(self._process.pid, logger=self.log)
        self._parent_signal_conn.close()


class DagFileProcessorManager(LoggingMixin):
    """
    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them and put the results to a multiprocessing.Queue
    for DagFileProcessorAgent to harvest. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :param dag_directory: Directory where DAG definitions are kept. All
        files in file_paths should be under this directory
    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :param signal_conn: connection to communicate signal with processor agent.
    :param dag_ids: if specified, only schedule tasks with these DAG IDs
    :param pickle_dags: whether to pickle DAGs.
    :param async_mode: whether to start the manager in async mode
    """

    def __init__(
        self,
        dag_directory: Union[str, "pathlib.Path"],
        max_runs: int,
        processor_timeout: timedelta,
        dag_ids: Optional[List[str]],
        pickle_dags: bool,
        signal_conn: Optional[MultiprocessingConnection] = None,
        async_mode: bool = True,
    ):
        super().__init__()
        self._file_paths: List[str] = []
        self._file_path_queue: List[str] = []
        self._dag_directory = dag_directory
        self._max_runs = max_runs
        # signal_conn is None for dag_processor_standalone mode.
        self._direct_scheduler_conn = signal_conn
        self._pickle_dags = pickle_dags
        self._dag_ids = dag_ids
        self._async_mode = async_mode
        self._parsing_start_time: Optional[int] = None

        # Set the signal conn in to non-blocking mode, so that attempting to
        # send when the buffer is full errors, rather than hangs for-ever
        # attempting to send (this is to avoid deadlocks!)
        #
        # Don't do this in sync_mode, as we _need_ the DagParsingStat sent to
        # continue the scheduler
        if self._async_mode and self._direct_scheduler_conn is not None:
            os.set_blocking(self._direct_scheduler_conn.fileno(), False)

        self._parallelism = conf.getint('scheduler', 'parsing_processes')
        if (
            conf.get_mandatory_value('database', 'sql_alchemy_conn').startswith('sqlite')
            and self._parallelism > 1
        ):
            self.log.warning(
                "Because we cannot use more than 1 thread (parsing_processes = "
                "%d) when using sqlite. So we set parallelism to 1.",
                self._parallelism,
            )
            self._parallelism = 1

        # Parse and schedule each file no faster than this interval.
        self._file_process_interval = conf.getint('scheduler', 'min_file_process_interval')
        # How often to print out DAG file processing stats to the log. Default to
        # 30 seconds.
        self.print_stats_interval = conf.getint('scheduler', 'print_stats_interval')

        # Map from file path to the processor
        self._processors: Dict[str, DagFileProcessorProcess] = {}

        self._num_run = 0

        # Map from file path to stats about the file
        self._file_stats: Dict[str, DagFileStat] = {}

        # Last time that the DAG dir was traversed to look for files
        self.last_dag_dir_refresh_time = timezone.make_aware(datetime.fromtimestamp(0))
        # Last time stats were printed
        self.last_stat_print_time = 0
        # Last time we cleaned up DAGs which are no longer in files
        self.last_deactivate_stale_dags_time = timezone.make_aware(datetime.fromtimestamp(0))
        # How often to check for DAGs which are no longer in files
        self.deactivate_stale_dags_interval = conf.getint('scheduler', 'deactivate_stale_dags_interval')
        # How long to wait before timing out a process to parse a DAG file
        self._processor_timeout = processor_timeout
        # How often to scan the DAGs directory for new files. Default to 5 minutes.
        self.dag_dir_list_interval = conf.getint('scheduler', 'dag_dir_list_interval')

        # Mapping file name and callbacks requests
        self._callback_to_execute: Dict[str, List[CallbackRequest]] = defaultdict(list)

        self._log = logging.getLogger('airflow.processor_manager')

        self.waitables: Dict[Any, Union[MultiprocessingConnection, DagFileProcessorProcess]] = (
            {
                self._direct_scheduler_conn: self._direct_scheduler_conn,
            }
            if self._direct_scheduler_conn is not None
            else {}
        )

    def register_exit_signals(self):
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        # So that we ignore the debug dump signal, making it easier to send
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def _exit_gracefully(self, signum, frame):
        """Helper method to clean up DAG file processors to avoid leaving orphan processes."""
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.log.debug("Current Stacktrace is: %s", '\n'.join(map(str, inspect.stack())))
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

        set_new_process_group()

        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        self.log.info(
            "Checking for new files in %s every %s seconds", self._dag_directory, self.dag_dir_list_interval
        )

        return self._run_parsing_loop()

    @provide_session
    def _deactivate_stale_dags(self, session=None):
        """
        Detects DAGs which are no longer present in files

        Deactivate them and remove them in the serialized_dag table
        """
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_deactivate_stale_dags_time).total_seconds()
        if elapsed_time_since_refresh > self.deactivate_stale_dags_interval:
            last_parsed = {
                fp: self.get_last_finish_time(fp) for fp in self.file_paths if self.get_last_finish_time(fp)
            }
            to_deactivate = set()
            dags_parsed = (
                session.query(DagModel.dag_id, DagModel.fileloc, DagModel.last_parsed_time)
                .filter(DagModel.is_active)
                .all()
            )
            for dag in dags_parsed:
                # The largest valid difference between a DagFileStat's last_finished_time and a DAG's
                # last_parsed_time is _processor_timeout. Longer than that indicates that the DAG is
                # no longer present in the file.
                if (
                    dag.fileloc in last_parsed
                    and (dag.last_parsed_time + self._processor_timeout) < last_parsed[dag.fileloc]
                ):
                    self.log.info("DAG %s is missing and will be deactivated.", dag.dag_id)
                    to_deactivate.add(dag.dag_id)

            if to_deactivate:
                deactivated = (
                    session.query(DagModel)
                    .filter(DagModel.dag_id.in_(to_deactivate))
                    .update({DagModel.is_active: False}, synchronize_session="fetch")
                )
                if deactivated:
                    self.log.info("Deactivated %i DAGs which are no longer present in file.", deactivated)

                for dag_id in to_deactivate:
                    SerializedDagModel.remove_dag(dag_id)
                    self.log.info("Deleted DAG %s in serialized_dag table", dag_id)

            self.last_deactivate_stale_dags_time = timezone.utcnow()

    def _run_parsing_loop(self):

        # In sync mode we want timeout=None -- wait forever until a message is received
        if self._async_mode:
            poll_time = 0.0
        else:
            poll_time = None

        self._refresh_dag_dir()
        self.prepare_file_path_queue()
        max_callbacks_per_loop = conf.getint("scheduler", "max_callbacks_per_loop")
        standalone_dag_processor = conf.getboolean("scheduler", "standalone_dag_processor")
        if self._async_mode:
            # If we're in async mode, we can start up straight away. If we're
            # in sync mode we need to be told to start a "loop"
            self.start_new_processes()
        while True:
            loop_start_time = time.monotonic()
            ready = multiprocessing.connection.wait(self.waitables.keys(), timeout=poll_time)
            if self._direct_scheduler_conn is not None and self._direct_scheduler_conn in ready:
                agent_signal = self._direct_scheduler_conn.recv()

                self.log.debug("Received %s signal from DagFileProcessorAgent", agent_signal)
                if agent_signal == DagParsingSignal.TERMINATE_MANAGER:
                    self.terminate()
                    break
                elif agent_signal == DagParsingSignal.END_MANAGER:
                    self.end()
                    sys.exit(os.EX_OK)
                elif agent_signal == DagParsingSignal.AGENT_RUN_ONCE:
                    # continue the loop to parse dags
                    pass
                elif isinstance(agent_signal, CallbackRequest):
                    self._add_callback_to_queue(agent_signal)
                else:
                    raise ValueError(f"Invalid message {type(agent_signal)}")

            if not ready and not self._async_mode:
                # In "sync" mode we don't want to parse the DAGs until we
                # are told to (as that would open another connection to the
                # SQLite DB which isn't a good practice

                # This shouldn't happen, as in sync mode poll should block for
                # ever. Lets be defensive about that.
                self.log.warning(
                    "wait() unexpectedly returned nothing ready after infinite timeout (%r)!", poll_time
                )

                continue

            for sentinel in ready:
                if sentinel is self._direct_scheduler_conn:
                    continue

                processor = self.waitables.get(sentinel)
                if not processor:
                    continue

                self._collect_results_from_processor(processor)
                self.waitables.pop(sentinel)
                self._processors.pop(processor.file_path)

            if standalone_dag_processor:
                self._fetch_callbacks(max_callbacks_per_loop)
            self._deactivate_stale_dags()
            DagWarning.purge_inactive_dag_warnings()
            self._refresh_dag_dir()

            self._kill_timed_out_processors()

            # Generate more file paths to process if we processed all the files
            # already.
            if not self._file_path_queue:
                self.emit_metrics()
                self.prepare_file_path_queue()

            self.start_new_processes()

            # Update number of loop iteration.
            self._num_run += 1

            if not self._async_mode:
                self.log.debug("Waiting for processors to finish since we're using sqlite")
                # Wait until the running DAG processors are finished before
                # sending a DagParsingStat message back. This means the Agent
                # can tell we've got to the end of this iteration when it sees
                # this type of message
                self.wait_until_finished()

            # Collect anything else that has finished, but don't kick off any more processors
            self.collect_results()

            self._print_stat()

            all_files_processed = all(self.get_last_finish_time(x) is not None for x in self.file_paths)
            max_runs_reached = self.max_runs_reached()

            try:
                if self._direct_scheduler_conn:
                    self._direct_scheduler_conn.send(
                        DagParsingStat(
                            max_runs_reached,
                            all_files_processed,
                        )
                    )
            except BlockingIOError:
                # Try again next time around the loop!

                # It is better to fail, than it is deadlock. This should
                # "almost never happen" since the DagParsingStat object is
                # small, and in async mode this stat is not actually _required_
                # for normal operation (It only drives "max runs")
                self.log.debug("BlockingIOError received trying to send DagParsingStat, ignoring")

            if max_runs_reached:
                self.log.info(
                    "Exiting dag parsing loop as all files have been processed %s times", self._max_runs
                )
                break

            if self._async_mode:
                loop_duration = time.monotonic() - loop_start_time
                if loop_duration < 1:
                    poll_time = 1 - loop_duration
                else:
                    poll_time = 0.0

    @provide_session
    def _fetch_callbacks(self, max_callbacks: int, session: Session = NEW_SESSION):
        """Fetches callbacks from database and add them to the internal queue for execution."""
        self.log.debug("Fetching callbacks from the database.")
        with prohibit_commit(session) as guard:
            query = (
                session.query(DbCallbackRequest)
                .order_by(DbCallbackRequest.priority_weight.asc())
                .limit(max_callbacks)
            )
            callbacks = with_row_locks(
                query, of=DbCallbackRequest, session=session, **skip_locked(session=session)
            ).all()
            for callback in callbacks:
                try:
                    self._add_callback_to_queue(callback.get_callback_request())
                    session.delete(callback)
                except Exception as e:
                    self.log.warning("Error adding callback for execution: %s, %s", callback, e)
            guard.commit()

    def _add_callback_to_queue(self, request: CallbackRequest):

        # requests are sent by dag processors. SLAs exist per-dag, but can be generated once per SLA-enabled
        # task in the dag. If treated like other callbacks, SLAs can cause feedback where a SLA arrives,
        # goes to the front of the queue, gets processed, triggers more SLAs from the same DAG, which go to
        # the front of the queue, and we never get round to picking stuff off the back of the queue
        if isinstance(request, SlaCallbackRequest):
            if request in self._callback_to_execute[request.full_filepath]:
                self.log.debug("Skipping already queued SlaCallbackRequest")
                return

            # not already queued, queue the file _at the back_, and add the request to the file's callbacks
            self.log.debug("Queuing SlaCallbackRequest for %s", request.dag_id)
            self._callback_to_execute[request.full_filepath].append(request)
            if request.full_filepath not in self._file_path_queue:
                self._file_path_queue.append(request.full_filepath)

        # Other callbacks have a higher priority over DAG Run scheduling, so those callbacks gazump, even if
        # already in the queue
        else:
            self.log.debug("Queuing %s CallbackRequest: %s", type(request).__name__, request)
            self._callback_to_execute[request.full_filepath].append(request)
            if request.full_filepath in self._file_path_queue:
                # Remove file paths matching request.full_filepath from self._file_path_queue
                # Since we are already going to use that filepath to run callback,
                # there is no need to have same file path again in the queue
                self._file_path_queue = [
                    file_path for file_path in self._file_path_queue if file_path != request.full_filepath
                ]
            self._file_path_queue.insert(0, request.full_filepath)

    def _refresh_dag_dir(self):
        """Refresh file paths from dag dir if we haven't done it for too long."""
        now = timezone.utcnow()
        elapsed_time_since_refresh = (now - self.last_dag_dir_refresh_time).total_seconds()
        if elapsed_time_since_refresh > self.dag_dir_list_interval:
            # Build up a list of Python files that could contain DAGs
            self.log.info("Searching for files in %s", self._dag_directory)
            self._file_paths = list_py_file_paths(self._dag_directory)
            self.last_dag_dir_refresh_time = now
            self.log.info("There are %s files in %s", len(self._file_paths), self._dag_directory)
            self.set_file_paths(self._file_paths)

            try:
                self.log.debug("Removing old import errors")
                self.clear_nonexistent_import_errors()
            except Exception:
                self.log.exception("Error removing old import errors")

            # Check if file path is a zipfile and get the full path of the python file.
            # Without this, SerializedDagModel.remove_deleted_files would delete zipped dags.
            # Likewise DagCode.remove_deleted_code
            dag_filelocs = []
            for fileloc in self._file_paths:
                if not fileloc.endswith(".py") and zipfile.is_zipfile(fileloc):
                    with zipfile.ZipFile(fileloc) as z:
                        dag_filelocs.extend(
                            [
                                os.path.join(fileloc, info.filename)
                                for info in z.infolist()
                                if might_contain_dag(info.filename, True, z)
                            ]
                        )
                else:
                    dag_filelocs.append(fileloc)

            SerializedDagModel.remove_deleted_dags(dag_filelocs)
            DagModel.deactivate_deleted_dags(self._file_paths)

            from airflow.models.dagcode import DagCode

            DagCode.remove_deleted_code(dag_filelocs)

    def _print_stat(self):
        """Occasionally print out stats about how fast the files are getting processed"""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if self._file_paths:
                self._log_file_processing_stats(self._file_paths)
            self.last_stat_print_time = time.monotonic()

    @provide_session
    def clear_nonexistent_import_errors(self, session):
        """
        Clears import errors for files that no longer exist.

        :param session: session for ORM operations
        """
        query = session.query(errors.ImportError)
        if self._file_paths:
            query = query.filter(~errors.ImportError.filename.in_(self._file_paths))
        query.delete(synchronize_session='fetch')
        session.commit()

    def _log_file_processing_stats(self, known_file_paths):
        """
        Print out stats about how files are getting processed.

        :param known_file_paths: a list of file paths that may contain Airflow
            DAG definitions
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
        headers = ["File Path", "PID", "Runtime", "# DAGs", "# Errors", "Last Runtime", "Last Run"]

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
            runtime = (now - processor_start_time) if processor_start_time else None
            last_run = self.get_last_finish_time(file_path)
            if last_run:
                seconds_ago = (now - last_run).total_seconds()
                Stats.gauge(f'dag_processing.last_run.seconds_ago.{file_name}', seconds_ago)

            rows.append((file_path, processor_pid, runtime, num_dags, num_errors, last_runtime, last_run))

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows = sorted(rows, key=lambda x: x[3] or 0.0)

        formatted_rows = []
        for file_path, pid, runtime, num_dags, num_errors, last_runtime, last_run in rows:
            formatted_rows.append(
                (
                    file_path,
                    pid,
                    f"{runtime.total_seconds():.2f}s" if runtime else None,
                    num_dags,
                    num_errors,
                    f"{last_runtime:.2f}s" if last_runtime else None,
                    last_run.strftime("%Y-%m-%dT%H:%M:%S") if last_run else None,
                )
            )
        log_str = (
            "\n"
            + "=" * 80
            + "\n"
            + "DAG File Processing Stats\n\n"
            + tabulate(formatted_rows, headers=headers)
            + "\n"
            + "=" * 80
        )

        self.log.info(log_str)

    def get_pid(self, file_path):
        """
        :param file_path: the path to the file that's being processed
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
        :return: the runtime (in seconds) of the process of the last run, or
            None if the file was never processed.
        :rtype: float
        """
        stat = self._file_stats.get(file_path)
        return stat.last_duration.total_seconds() if stat and stat.last_duration else None

    def get_last_dag_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :return: the number of dags loaded from that file, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.num_dags if stat else None

    def get_last_error_count(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :return: the number of import errors from processing, or None if the file
            was never processed.
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.import_errors if stat else None

    def get_last_finish_time(self, file_path):
        """
        :param file_path: the path to the file that was processed
        :return: the finish time of the process of the last run, or None if the
            file was never processed.
        :rtype: datetime
        """
        stat = self._file_stats.get(file_path)
        return stat.last_finish_time if stat else None

    def get_start_time(self, file_path):
        """
        :param file_path: the path to the file that's being processed
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
        :return: the number of times the given file has been parsed
        :rtype: int
        """
        stat = self._file_stats.get(file_path)
        return stat.run_count if stat else 0

    def set_file_paths(self, new_file_paths):
        """
        Update this with a new set of paths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :return: None
        """
        self._file_paths = new_file_paths
        self._file_path_queue = [x for x in self._file_path_queue if x in new_file_paths]
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
        """Sleeps until all the processors are done."""
        for processor in self._processors.values():
            while not processor.done:
                time.sleep(0.1)

    def _collect_results_from_processor(self, processor) -> None:
        self.log.debug("Processor for %s finished", processor.file_path)
        Stats.decr('dag_processing.processes')
        last_finish_time = timezone.utcnow()

        if processor.result is not None:
            num_dags, count_import_errors = processor.result
        else:
            self.log.error(
                "Processor for %s exited with return code %s.", processor.file_path, processor.exit_code
            )
            count_import_errors = -1
            num_dags = 0

        last_duration = last_finish_time - processor.start_time
        stat = DagFileStat(
            num_dags=num_dags,
            import_errors=count_import_errors,
            last_finish_time=last_finish_time,
            last_duration=last_duration,
            run_count=self.get_run_count(processor.file_path) + 1,
        )
        self._file_stats[processor.file_path] = stat

        file_name = os.path.splitext(os.path.basename(processor.file_path))[0].replace(os.sep, '.')
        Stats.timing(f'dag_processing.last_duration.{file_name}', last_duration)

    def collect_results(self) -> None:
        """Collect the result from any finished DAG processors"""
        ready = multiprocessing.connection.wait(
            self.waitables.keys() - [self._direct_scheduler_conn], timeout=0
        )

        for sentinel in ready:
            if sentinel is self._direct_scheduler_conn:
                continue
            processor = cast(DagFileProcessorProcess, self.waitables[sentinel])
            self.waitables.pop(processor.waitable_handle)
            self._processors.pop(processor.file_path)
            self._collect_results_from_processor(processor)

        self.log.debug("%s/%s DAG parsing processes running", len(self._processors), self._parallelism)

        self.log.debug("%s file paths queued for processing", len(self._file_path_queue))

    @staticmethod
    def _create_process(file_path, pickle_dags, dag_ids, callback_requests):
        """Creates DagFileProcessorProcess instance."""
        return DagFileProcessorProcess(
            file_path=file_path, pickle_dags=pickle_dags, dag_ids=dag_ids, callback_requests=callback_requests
        )

    def start_new_processes(self):
        """Start more processors if we have enough slots and files to process"""
        while self._parallelism - len(self._processors) > 0 and self._file_path_queue:
            file_path = self._file_path_queue.pop(0)
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file_path in self._processors.keys():
                continue

            callback_to_execute_for_file = self._callback_to_execute[file_path]
            processor = self._create_process(
                file_path, self._pickle_dags, self._dag_ids, callback_to_execute_for_file
            )

            del self._callback_to_execute[file_path]
            Stats.incr('dag_processing.processes')

            processor.start()
            self.log.debug("Started a process (PID: %s) to generate tasks for %s", processor.pid, file_path)
            self._processors[file_path] = processor
            self.waitables[processor.waitable_handle] = processor

    def prepare_file_path_queue(self):
        """Generate more file paths to process. Result are saved in _file_path_queue."""
        self._parsing_start_time = time.perf_counter()
        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        file_paths_in_progress = self._processors.keys()
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        list_mode = conf.get("scheduler", "file_parsing_sort_mode")

        files_with_mtime = {}
        file_paths = []
        is_mtime_mode = list_mode == "modified_time"

        file_paths_recently_processed = []
        for file_path in self._file_paths:

            if is_mtime_mode:
                try:
                    files_with_mtime[file_path] = os.path.getmtime(file_path)
                except FileNotFoundError:
                    self.log.warning("Skipping processing of missing file: %s", file_path)
                    continue
                file_modified_time = timezone.make_aware(datetime.fromtimestamp(files_with_mtime[file_path]))
            else:
                file_paths.append(file_path)
                file_modified_time = None

            # Find file paths that were recently processed to exclude them
            # from being added to file_path_queue
            # unless they were modified recently and parsing mode is "modified_time"
            # in which case we don't honor "self._file_process_interval" (min_file_process_interval)
            last_finish_time = self.get_last_finish_time(file_path)
            if (
                last_finish_time is not None
                and (now - last_finish_time).total_seconds() < self._file_process_interval
                and not (is_mtime_mode and file_modified_time and (file_modified_time > last_finish_time))
            ):
                file_paths_recently_processed.append(file_path)

        # Sort file paths via last modified time
        if is_mtime_mode:
            file_paths = sorted(files_with_mtime, key=files_with_mtime.get, reverse=True)
        elif list_mode == "alphabetical":
            file_paths = sorted(file_paths)
        elif list_mode == "random_seeded_by_host":
            # Shuffle the list seeded by hostname so multiple schedulers can work on different
            # set of files. Since we set the seed, the sort order will remain same per host
            random.Random(get_hostname()).shuffle(file_paths)

        files_paths_at_run_limit = [
            file_path for file_path, stat in self._file_stats.items() if stat.run_count == self._max_runs
        ]

        file_paths_to_exclude = set(file_paths_in_progress).union(
            file_paths_recently_processed, files_paths_at_run_limit
        )

        # Do not convert the following list to set as set does not preserve the order
        # and we need to maintain the order of file_paths for `[scheduler] file_parsing_sort_mode`
        files_paths_to_queue = [
            file_path for file_path in file_paths if file_path not in file_paths_to_exclude
        ]

        for file_path, processor in self._processors.items():
            self.log.debug(
                "File path %s is still being processed (started: %s)",
                processor.file_path,
                processor.start_time.isoformat(),
            )

        self.log.debug("Queuing the following files for processing:\n\t%s", "\n\t".join(files_paths_to_queue))

        for file_path in files_paths_to_queue:
            if file_path not in self._file_stats:
                self._file_stats[file_path] = DagFileStat(
                    num_dags=0, import_errors=0, last_finish_time=None, last_duration=None, run_count=0
                )

        self._file_path_queue.extend(files_paths_to_queue)

    def _kill_timed_out_processors(self):
        """Kill any file processors that timeout to defend against process hangs."""
        now = timezone.utcnow()
        processors_to_remove = []
        for file_path, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self._processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s started at %s has timed out, killing it.",
                    file_path,
                    processor.pid,
                    processor.start_time.isoformat(),
                )
                Stats.decr('dag_processing.processes')
                Stats.incr('dag_processing.processor_timeouts')
                # TODO: Remove after Airflow 2.0
                Stats.incr('dag_file_processor_timeouts')
                processor.kill()

                # Clean up processor references
                self.waitables.pop(processor.waitable_handle)
                processors_to_remove.append(file_path)

        # Clean up `self._processors` after iterating over it
        for proc in processors_to_remove:
            self._processors.pop(proc)

    def max_runs_reached(self):
        """:return: whether all file paths have been processed max_runs times"""
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
        Emit metrics about dag parsing summary

        This is called once every time around the parsing "loop" - i.e. after
        all files have been parsed.
        """
        parse_time = time.perf_counter() - self._parsing_start_time
        Stats.gauge('dag_processing.total_parse_time', parse_time)
        Stats.gauge('dagbag_size', sum(stat.num_dags for stat in self._file_stats.values()))
        Stats.gauge(
            'dag_processing.import_errors', sum(stat.import_errors for stat in self._file_stats.values())
        )

    @property
    def file_paths(self):
        return self._file_paths
