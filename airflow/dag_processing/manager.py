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

from __future__ import annotations

import functools
import importlib
import inspect
import logging
import multiprocessing
import os
import random
import selectors
import signal
import sys
import time
import zipfile
from collections import defaultdict, deque
from collections.abc import Callable, Iterator
from datetime import datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import attrs
from setproctitle import setproctitle
from sqlalchemy import delete, select, update
from tabulate import tabulate
from uuid6 import uuid7

import airflow.models
from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest, TaskCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ParseImportError
from airflow.secrets.cache import SecretCache
from airflow.stats import Stats
from airflow.traces.tracer import Trace
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
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import prohibit_commit, with_row_locks

if TYPE_CHECKING:
    from multiprocessing.connection import Connection as MultiprocessingConnection

    from sqlalchemy.orm import Session

    from airflow.dag_processing.bundles.base import BaseDagBundle


class DagParsingStat(NamedTuple):
    """Information on processing progress."""

    done: bool
    all_files_processed: bool


@attrs.define
class DagFileStat:
    """Information about single processing of one file."""

    num_dags: int = 0
    import_errors: int = 0
    last_finish_time: datetime | None = None
    last_duration: float | None = None
    run_count: int = 0
    last_num_of_db_queries: int = 0


log = logging.getLogger("airflow.processor_manager")


class DagFileInfo(NamedTuple):
    """Information about a DAG file."""

    path: str  # absolute path of the file
    bundle_name: str


class DagFileProcessorAgent(LoggingMixin, MultiprocessingStartMethodMixin):
    """
    Agent for DAG file processing.

    It is responsible for all DAG parsing related jobs in scheduler process.
    Mainly it can spin up DagFileProcessorManager in a subprocess,
    collect DAG parsing results from it and communicate signal/DAG parsing stat with it.

    This class runs in the main `airflow scheduler` process when standalone_dag_processor is not enabled.

    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    """

    def __init__(
        self,
        max_runs: int,
        processor_timeout: timedelta,
    ):
        super().__init__()
        self._max_runs = max_runs
        self._processor_timeout = processor_timeout
        self._process: multiprocessing.Process | None = None
        self._done: bool = False
        # Initialized as true so we do not deactivate w/o any actual DAG parsing.
        self._all_files_processed = True

        # Pipe for communicating with the Agent -- it sends CallbackRequests to us via this conn, or `None` to
        # signal a clean shutdown.
        self._parent_signal_conn: MultiprocessingConnection | None = None

        self._last_parsing_stat_received_at: float = time.monotonic()

    def start(self) -> None:
        """Launch DagFileProcessorManager processor and start DAG parsing loop in manager."""
        context = self._get_multiprocessing_context()
        self._last_parsing_stat_received_at = time.monotonic()

        parent_signal_conn, child_signal_conn = context.Pipe()
        process = context.Process(
            target=type(self)._run_processor_manager,
            args=(
                self._max_runs,
                self._processor_timeout,
                child_signal_conn,
            ),
        )

        self._process = process

        self._parent_signal_conn = parent_signal_conn

        process.start()
        # We don't want this end anymore
        child_signal_conn.close()

        self.log.info("Launched DagFileProcessorManager with pid: %s", process.pid)

    def get_callbacks_pipe(self) -> MultiprocessingConnection:
        """Return the pipe for sending Callbacks to DagProcessorManager."""
        if not self._parent_signal_conn:
            raise ValueError("Process not started.")
        return self._parent_signal_conn

    @staticmethod
    def _run_processor_manager(
        max_runs: int,
        processor_timeout: timedelta,
        signal_conn: MultiprocessingConnection,
    ) -> None:
        # Make this process start as a new process group - that makes it easy
        # to kill all sub-process of this at the OS-level, rather than having
        # to iterate the child processes

        set_new_process_group()
        setproctitle("airflow scheduler -- DagFileProcessorManager")
        reload_configuration_for_dag_processing()
        processor_manager = DagFileProcessorManager(
            max_runs=max_runs,
            processor_timeout=processor_timeout.total_seconds(),
            signal_conn=signal_conn,
        )
        processor_manager.run()

    def heartbeat(self) -> None:
        """Check if the DagFileProcessorManager process is alive, and process any pending messages."""
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
            Stats.incr("dag_processing.manager_stalls")
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
        """Whether the DagFileProcessorManager finished."""
        return self._done

    @property
    def all_files_processed(self):
        """Whether all files been processed at least once."""
        return self._all_files_processed

    def terminate(self):
        """Send termination signal to DAG parsing processor manager to terminate all DAG file processors."""
        if self._process and self._process.is_alive():
            self.log.info("Sending termination message to manager.")
            try:
                self._parent_signal_conn.send(None)
            except ConnectionError:
                pass
            self._parent_signal_conn.close()

    def end(self):
        """Terminate (and then kill) the manager process launched."""
        if not self._process:
            self.log.warning("Ending without manager process.")
            return
        # Give the Manager some time to cleanly shut down, but not too long, as
        # it's better to finish sooner than wait for (non-critical) work to
        # finish
        self._process.join(timeout=1.0)
        reap_process_group(self._process.pid, logger=self.log)
        self._parent_signal_conn.close()


def _config_int_factory(section: str, key: str):
    return functools.partial(conf.getint, section, key)


def _config_bool_factory(section: str, key: str):
    return functools.partial(conf.getboolean, section, key)


def _resolve_path(instance: Any, attribute: attrs.Attribute, val: str | os.PathLike[str] | None):
    if val is not None:
        val = Path(val).resolve()
    return val


@attrs.define
class DagFileProcessorManager:
    """
    Manage processes responsible for parsing DAGs.

    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them and put the results to a multiprocessing.Queue
    for DagFileProcessorAgent to harvest. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :param max_runs: The number of times to parse and schedule each file. -1
        for unlimited.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    :param signal_conn: connection to communicate signal with processor agent.
    """

    max_runs: int
    processor_timeout: float = attrs.field(factory=_config_int_factory("core", "dag_file_processor_timeout"))
    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)
    _direct_scheduler_conn: MultiprocessingConnection | None = attrs.field(alias="signal_conn", default=None)

    _parallelism: int = attrs.field(factory=_config_int_factory("scheduler", "parsing_processes"))

    dag_dir_list_interval: int = attrs.field(
        factory=_config_int_factory("scheduler", "dag_dir_list_interval")
    )
    parsing_cleanup_interval: float = attrs.field(
        factory=_config_int_factory("scheduler", "parsing_cleanup_interval")
    )
    _file_process_interval: float = attrs.field(
        factory=_config_int_factory("scheduler", "min_file_process_interval")
    )
    stale_dag_threshold: float = attrs.field(factory=_config_int_factory("scheduler", "stale_dag_threshold"))

    log: logging.Logger = attrs.field(default=log, init=False)

    _last_deactivate_stale_dags_time: float = attrs.field(default=0, init=False)
    print_stats_interval: float = attrs.field(
        factory=_config_int_factory("scheduler", "print_stats_interval")
    )
    last_stat_print_time: float = attrs.field(default=0, init=False)

    heartbeat: Callable[[], None] = attrs.field(default=lambda: None)
    """An overridable heartbeat called once every time around the loop"""

    _file_paths: list[DagFileInfo] = attrs.field(factory=list, init=False)
    _file_path_queue: deque[DagFileInfo] = attrs.field(factory=deque, init=False)
    _file_stats: dict[DagFileInfo, DagFileStat] = attrs.field(
        factory=lambda: defaultdict(DagFileStat), init=False
    )

    _dag_bundles: list[BaseDagBundle] = attrs.field(factory=list, init=False)
    _bundle_versions: dict[str, str] = attrs.field(factory=dict, init=False)

    _processors: dict[DagFileInfo, DagFileProcessorProcess] = attrs.field(factory=dict, init=False)

    _parsing_start_time: float = attrs.field(init=False)
    _num_run: int = attrs.field(default=0, init=False)

    _callback_to_execute: dict[str, list[CallbackRequest]] = attrs.field(
        factory=lambda: defaultdict(list), init=False
    )

    standalone_dag_processor: bool = attrs.field(
        factory=_config_bool_factory("scheduler", "standalone_dag_processor")
    )
    max_callbacks_per_loop: int = attrs.field(
        factory=_config_int_factory("scheduler", "max_callbacks_per_loop")
    )

    def __attrs_post_init__(self):
        if self._direct_scheduler_conn is not None:
            os.set_blocking(self._direct_scheduler_conn.fileno(), False)

    def register_exit_signals(self):
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        # So that we ignore the debug dump signal, making it easier to send
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def _exit_gracefully(self, signum, frame):
        """Clean up DAG file processors to avoid leaving orphan processes."""
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.log.debug("Current Stacktrace is: %s", "\n".join(map(str, inspect.stack())))
        self.terminate()
        self.end()
        self.log.debug("Finished terminating DAG processors.")
        sys.exit(os.EX_OK)

    def run(self):
        """
        Use multiple processes to parse and generate tasks for the DAGs in parallel.

        By processing them in separate processes, we can get parallelism and isolation
        from potentially harmful user code.
        """
        self.register_exit_signals()

        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        # TODO: AIP-66 move to report by bundle self.log.info(
        #     "Checking for new files in %s every %s seconds", self._dag_directory, self.dag_dir_list_interval
        # )

        from airflow.dag_processing.bundles.manager import DagBundlesManager

        DagBundlesManager().sync_bundles_to_db()

        self.log.info("Getting all DAG bundles")
        self._dag_bundles = list(DagBundlesManager().get_all_dag_bundles())

        return self._run_parsing_loop()

    def _scan_stale_dags(self):
        """Scan and deactivate DAGs which are no longer present in files."""
        now = time.monotonic()
        elapsed_time_since_refresh = now - self._last_deactivate_stale_dags_time
        if elapsed_time_since_refresh > self.parsing_cleanup_interval:
            last_parsed = {
                fp: stat.last_finish_time for fp, stat in self._file_stats.items() if stat.last_finish_time
            }
            self.deactivate_stale_dags(
                last_parsed=last_parsed,
                stale_dag_threshold=self.stale_dag_threshold,
            )
            self._last_deactivate_stale_dags_time = time.monotonic()

    @provide_session
    def deactivate_stale_dags(
        self,
        last_parsed: dict[DagFileInfo, datetime | None],
        stale_dag_threshold: int,
        session: Session = NEW_SESSION,
    ):
        """Detect and deactivate DAGs which are no longer present in files."""
        to_deactivate = set()
        query = select(
            DagModel.dag_id, DagModel.bundle_name, DagModel.fileloc, DagModel.last_parsed_time
        ).where(DagModel.is_active)
        # TODO: AIP-66 by bundle!
        dags_parsed = session.execute(query)

        for dag in dags_parsed:
            # The largest valid difference between a DagFileStat's last_finished_time and a DAG's
            # last_parsed_time is the processor_timeout. Longer than that indicates that the DAG is
            # no longer present in the file. We have a stale_dag_threshold configured to prevent a
            # significant delay in deactivation of stale dags when a large timeout is configured
            dag_file_path = DagFileInfo(path=dag.fileloc, bundle_name=dag.bundle_name)
            if (
                dag_file_path in last_parsed
                and (dag.last_parsed_time + timedelta(seconds=stale_dag_threshold))
                < last_parsed[dag_file_path]
            ):
                self.log.info("DAG %s is missing and will be deactivated.", dag.dag_id)
                to_deactivate.add(dag.dag_id)

        if to_deactivate:
            deactivated_dagmodel = session.execute(
                update(DagModel)
                .where(DagModel.dag_id.in_(to_deactivate))
                .values(is_active=False)
                .execution_options(synchronize_session="fetch")
            )
            deactivated = deactivated_dagmodel.rowcount
            if deactivated:
                self.log.info("Deactivated %i DAGs which are no longer present in file.", deactivated)

    def _run_parsing_loop(self):
        # initialize cache to mutualize calls to Variable.get in DAGs
        # needs to be done before this process is forked to create the DAG parsing processes.
        SecretCache.init()

        if self._direct_scheduler_conn is not None:
            self.selector.register(
                self._direct_scheduler_conn, selectors.EVENT_READ, self._read_from_direct_scheduler_conn
            )

        poll_time = 0.0

        while True:
            loop_start_time = time.monotonic()

            self.heartbeat()

            self._kill_timed_out_processors()

            self._refresh_dag_bundles()

            if not self._file_path_queue:
                # Generate more file paths to process if we processed all the files already. Note for this to
                # clear down, we must have cleared all files found from scanning the dags dir _and_ have
                # cleared all files added as a result of callbacks
                self.prepare_file_path_queue()
                self.emit_metrics()
            else:
                # if new files found in dag dir, add them
                self.add_new_file_path_to_queue()

            self._refresh_requested_filelocs()

            self._start_new_processes()

            self._service_processor_sockets(timeout=poll_time)

            self._collect_results()

            if self.standalone_dag_processor:
                for callback in self._fetch_callbacks():
                    self._add_callback_to_queue(callback)
            self._scan_stale_dags()
            DagWarning.purge_inactive_dag_warnings()

            # Update number of loop iteration.
            self._num_run += 1

            self._print_stat()

            if self._direct_scheduler_conn:
                all_files_processed = all(
                    self._file_stats[x].last_finish_time is not None for x in self._file_paths
                )
                try:
                    self._direct_scheduler_conn.send(
                        DagParsingStat(
                            self.max_runs_reached(),
                            all_files_processed,
                        )
                    )
                except BlockingIOError:
                    # Try again next time around the loop!

                    # It is better to fail, than it is deadlock. This should "almost never happen" since the
                    # DagParsingStat object is small, and  is not actually _required_ for normal operation (It
                    # only drives "max runs")
                    self.log.debug("BlockingIOError received trying to send DagParsingStat, ignoring")

            if self.max_runs_reached():
                self.log.info(
                    "Exiting dag parsing loop as all files have been processed %s times", self.max_runs
                )
                break

            loop_duration = time.monotonic() - loop_start_time
            if loop_duration < 1:
                poll_time = 1 - loop_duration
            else:
                poll_time = 0.0

    def _service_processor_sockets(self, timeout: float | None = 1.0):
        """
        Service subprocess events by polling sockets for activity.

        This runs `select` (or a platform equivalent) to look for activity on the sockets connected to the
        parsing subprocesses, and calls the registered handler function for each socket.

        All the parsing processes socket handlers are registered into a single Selector
        """
        events = self.selector.select(timeout=timeout)
        for key, _ in events:
            socket_handler = key.data
            need_more = socket_handler(key.fileobj)

            if not need_more:
                self.selector.unregister(key.fileobj)
                key.fileobj.close()  # type: ignore[union-attr]

    def _read_from_direct_scheduler_conn(self, conn: MultiprocessingConnection) -> bool:
        try:
            agent_signal = conn.recv()
        except (EOFError, ConnectionError):
            self.terminate()
            sys.exit(os.EX_OK)

        self.log.debug("Received %s signal from DagFileProcessorAgent", agent_signal)
        if isinstance(agent_signal, (TaskCallbackRequest, DagCallbackRequest)):
            self._add_callback_to_queue(agent_signal)
        elif agent_signal is None:
            self.terminate()
            sys.exit(os.EX_OK)
        else:
            raise ValueError(f"Invalid message {type(agent_signal)}")
        return True

    def _refresh_requested_filelocs(self) -> None:
        """Refresh filepaths from dag dir as requested by users via APIs."""
        return
        # TODO: AIP-66 make bundle aware - fileloc will be relative (eventually), thus not unique in order to know what file to repase
        # Get values from DB table
        filelocs = self._get_priority_filelocs()
        for fileloc in filelocs:
            # Try removing the fileloc if already present
            try:
                self._file_path_queue.remove(fileloc)
            except ValueError:
                pass
            # enqueue fileloc to the start of the queue.
            self._file_path_queue.appendleft(fileloc)

    @provide_session
    @retry_db_transaction
    def _fetch_callbacks(
        self,
        session: Session = NEW_SESSION,
    ) -> list[CallbackRequest]:
        """Fetch callbacks from database and add them to the internal queue for execution."""
        self.log.debug("Fetching callbacks from the database.")

        callback_queue: list[CallbackRequest] = []
        with prohibit_commit(session) as guard:
            query = select(DbCallbackRequest)
            query = query.order_by(DbCallbackRequest.priority_weight.asc()).limit(self.max_callbacks_per_loop)
            query = with_row_locks(query, of=DbCallbackRequest, session=session, skip_locked=True)
            callbacks = session.scalars(query)
            for callback in callbacks:
                try:
                    callback_queue.append(callback.get_callback_request())
                    session.delete(callback)
                except Exception as e:
                    self.log.warning("Error adding callback for execution: %s, %s", callback, e)
            guard.commit()
        return callback_queue

    def _add_callback_to_queue(self, request: CallbackRequest):
        self.log.debug("Queuing %s CallbackRequest: %s", type(request).__name__, request)
        self.log.warning("Callbacks are not implemented yet!")
        # TODO: AIP-66 make callbacks bundle aware
        return
        self._callback_to_execute[request.full_filepath].append(request)
        if request.full_filepath in self._file_path_queue:
            # Remove file paths matching request.full_filepath from self._file_path_queue
            # Since we are already going to use that filepath to run callback,
            # there is no need to have same file path again in the queue
            self._file_path_queue = deque(
                file_path for file_path in self._file_path_queue if file_path != request.full_filepath
            )
        self._add_paths_to_queue([request.full_filepath], True)
        Stats.incr("dag_processing.other_callback_count")

    @classmethod
    @provide_session
    def _get_priority_filelocs(cls, session: Session = NEW_SESSION):
        """Get filelocs from DB table."""
        filelocs: list[str] = []
        requests = session.scalars(select(DagPriorityParsingRequest))
        for request in requests:
            filelocs.append(request.fileloc)
            session.delete(request)
        return filelocs

    def _refresh_dag_bundles(self):
        """Refresh DAG bundles, if required."""
        now = timezone.utcnow()

        self.log.info("Refreshing DAG bundles")

        for bundle in self._dag_bundles:
            # TODO: AIP-66 test to make sure we get a fresh record from the db and it's not cached
            with create_session() as session:
                bundle_model = session.get(DagBundleModel, bundle.name)
                elapsed_time_since_refresh = (
                    now - (bundle_model.last_refreshed or timezone.utc_epoch())
                ).total_seconds()
                current_version = bundle.get_current_version()
                if (
                    elapsed_time_since_refresh < bundle.refresh_interval
                    and bundle_model.latest_version == current_version
                    and bundle.name in self._bundle_versions
                ):
                    self.log.info("Not time to refresh %s", bundle.name)
                    continue

                try:
                    bundle.refresh()
                except Exception:
                    self.log.exception("Error refreshing bundle %s", bundle.name)
                    continue

                bundle_model.last_refreshed = now

                new_version = bundle.get_current_version()
                if bundle.supports_versioning:
                    # We can short-circuit the rest of the refresh if the version hasn't changed
                    # and we've already fully "refreshed" this bundle before in this dag processor.
                    if current_version == new_version and bundle.name in self._bundle_versions:
                        self.log.debug("Bundle %s version not changed after refresh", bundle.name)
                        continue

                    bundle_model.latest_version = new_version

                    self.log.info("Version changed for %s, new version: %s", bundle.name, new_version)

            bundle_file_paths = self._find_files_in_bundle(bundle)

            new_file_paths = [f for f in self._file_paths if f.bundle_name != bundle.name]
            new_file_paths.extend(
                DagFileInfo(path=path, bundle_name=bundle.name) for path in bundle_file_paths
            )
            self.set_file_paths(new_file_paths)

            self.deactivate_deleted_dags(bundle_file_paths)
            self.clear_nonexistent_import_errors()

            self._bundle_versions[bundle.name] = bundle.get_current_version()

    def _find_files_in_bundle(self, bundle: BaseDagBundle) -> list[str]:
        """Refresh file paths from bundle dir."""
        # Build up a list of Python files that could contain DAGs
        self.log.info("Searching for files in %s at %s", bundle.name, bundle.path)
        file_paths = list_py_file_paths(bundle.path)
        self.log.info("Found %s files for bundle %s", len(file_paths), bundle.name)

        return file_paths

    def deactivate_deleted_dags(self, file_paths: set[str]) -> None:
        """Deactivate DAGs that come from files that are no longer present."""

        def _iter_dag_filelocs(fileloc: str) -> Iterator[str]:
            """
            Get "full" paths to DAGs if inside ZIP files.

            This is the format used by the remove/delete functions.
            """
            if fileloc.endswith(".py") or not zipfile.is_zipfile(fileloc):
                yield fileloc
                return
            try:
                with zipfile.ZipFile(fileloc) as z:
                    for info in z.infolist():
                        if might_contain_dag(info.filename, True, z):
                            yield os.path.join(fileloc, info.filename)
            except zipfile.BadZipFile:
                self.log.exception("There was an error accessing ZIP file %s %s", fileloc)

        dag_filelocs = {full_loc for path in file_paths for full_loc in _iter_dag_filelocs(path)}

        # TODO: AIP-66: make bundle aware, as fileloc won't be unique long term.
        DagModel.deactivate_deleted_dags(dag_filelocs)

    def _print_stat(self):
        """Occasionally print out stats about how fast the files are getting processed."""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if self._file_paths:
                self._log_file_processing_stats(self._file_paths)
            self.last_stat_print_time = time.monotonic()

    @provide_session
    def clear_nonexistent_import_errors(self, session=NEW_SESSION):
        """
        Clear import errors for files that no longer exist.

        :param file_paths: list of paths to DAG definition files
        :param session: session for ORM operations
        """
        self.log.debug("Removing old import errors")
        try:
            query = delete(ParseImportError)

            if self._file_paths:
                query = query.where(
                    ParseImportError.filename.notin_([f.path for f in self._file_paths]),
                )

            session.execute(query.execution_options(synchronize_session="fetch"))
            session.commit()
        except Exception:
            self.log.exception("Error removing old import errors")

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
        # Last # of DB Queries: The number of queries performed to the
        # Airflow database during last parsing of the file.
        headers = [
            "File Path",
            "PID",
            "Current Duration",
            "# DAGs",
            "# Errors",
            "Last Duration",
            "Last Run At",
        ]

        rows = []
        utcnow = timezone.utcnow()
        now = time.monotonic()
        for file_path in known_file_paths:
            stat = self._file_stats[file_path]
            proc = self._processors.get(file_path)
            num_dags = stat.num_dags
            num_errors = stat.import_errors
            file_name = Path(file_path.path).stem
            processor_pid = proc.pid if proc else None
            processor_start_time = proc.start_time if proc else None
            runtime = (now - processor_start_time) if processor_start_time else None
            last_run = stat.last_finish_time
            if last_run:
                seconds_ago = (utcnow - last_run).total_seconds()
                Stats.gauge(f"dag_processing.last_run.seconds_ago.{file_name}", seconds_ago)

            rows.append(
                (
                    file_path,
                    processor_pid,
                    runtime,
                    num_dags,
                    num_errors,
                    stat.last_duration,
                    last_run,
                )
            )

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows.sort(key=lambda x: x[5] or 0.0, reverse=True)

        formatted_rows = []
        for (
            file_path,
            pid,
            runtime,
            num_dags,
            num_errors,
            last_runtime,
            last_run,
        ) in rows:
            formatted_rows.append(
                (
                    file_path,
                    pid,
                    f"{runtime:.2f}s" if runtime else None,
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

    def set_file_paths(self, new_file_paths: list[DagFileInfo]):
        """
        Update this with a new set of DagFilePaths to DAG definition files.

        :param new_file_paths: list of paths to DAG definition files
        :return: None
        """
        self._file_paths = new_file_paths

        # clean up the queues; remove anything queued which no longer in the list, including callbacks
        self._file_path_queue = deque(x for x in self._file_path_queue if x in new_file_paths)
        Stats.gauge("dag_processing.file_path_queue_size", len(self._file_path_queue))

        # TODO: AIP-66 make callbacks bundle aware
        # callback_paths_to_del = [x for x in self._callback_to_execute if x not in new_file_paths]
        # for path_to_del in callback_paths_to_del:
        #     del self._callback_to_execute[path_to_del]

        # Stop processors that are working on deleted files
        filtered_processors = {}
        for file_path, processor in self._processors.items():
            if file_path in new_file_paths:
                filtered_processors[file_path] = processor
            else:
                self.log.warning("Stopping processor for %s", file_path)
                Stats.decr("dag_processing.processes", tags={"file_path": file_path, "action": "stop"})
                processor.kill(signal.SIGKILL)
                self._file_stats.pop(file_path)

        to_remove = set(self._file_stats).difference(self._file_paths)
        for key in to_remove:
            # Remove the stats for any dag files that don't exist anymore
            del self._file_stats[key]

        self._processors = filtered_processors

    @provide_session
    def _collect_results(self, session: Session = NEW_SESSION):
        # TODO: Use an explicit session in this fn
        finished = []
        for dag_file, proc in self._processors.items():
            if not proc.is_ready:
                # This processor hasn't finished yet, or we haven't read all the output from it yet
                continue
            finished.append(dag_file)

            # Collect the DAGS and import errors into the DB, emit metrics etc.
            self._file_stats[dag_file] = process_parse_results(
                run_duration=time.time() - proc.start_time,
                finish_time=timezone.utcnow(),
                run_count=self._file_stats[dag_file].run_count,
                bundle_name=dag_file.bundle_name,
                bundle_version=self._bundle_versions[dag_file.bundle_name],
                parsing_result=proc.parsing_result,
                session=session,
            )

        for dag_file in finished:
            self._processors.pop(dag_file)

    def _create_process(self, dag_file: DagFileInfo) -> DagFileProcessorProcess:
        id = uuid7()

        # callback_to_execute_for_file = self._callback_to_execute.pop(file_path, [])
        callback_to_execute_for_file: list[CallbackRequest] = []

        return DagFileProcessorProcess.start(
            id=id,
            path=dag_file.path,
            callbacks=callback_to_execute_for_file,
            selector=self.selector,
        )

    def _start_new_processes(self):
        """Start more processors if we have enough slots and files to process."""
        while self._parallelism > len(self._processors) and self._file_path_queue:
            file_path = self._file_path_queue.popleft()
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file_path in self._processors:
                continue

            processor = self._create_process(file_path)
            Stats.incr("dag_processing.processes", tags={"file_path": file_path, "action": "start"})

            self._processors[file_path] = processor
            Stats.gauge("dag_processing.file_path_queue_size", len(self._file_path_queue))

    def add_new_file_path_to_queue(self):
        for file_path in self._file_paths:
            if file_path not in self._file_stats:
                # We found new file after refreshing dir. add to parsing queue at start
                self.log.info("Adding new file %s to parsing queue", file_path)
                self._file_path_queue.appendleft(file_path)

    def prepare_file_path_queue(self):
        """
        Scan dags dir to generate more file paths to process.

        Note this method is only called when the file path queue is empty
        """
        self._parsing_start_time = time.perf_counter()
        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        file_paths_in_progress = set(self._processors)
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        list_mode = conf.get("scheduler", "file_parsing_sort_mode")

        files_with_mtime: dict[str, datetime] = {}
        file_paths = []
        is_mtime_mode = list_mode == "modified_time"

        file_paths_recently_processed = []
        file_paths_to_stop_watching = set()
        for file_path in self._file_paths:
            if is_mtime_mode:
                try:
                    files_with_mtime[file_path] = os.path.getmtime(file_path.path)
                except FileNotFoundError:
                    self.log.warning("Skipping processing of missing file: %s", file_path)
                    self._file_stats.pop(file_path, None)
                    file_paths_to_stop_watching.add(file_path)
                    continue
                file_modified_time = datetime.fromtimestamp(files_with_mtime[file_path], tz=timezone.utc)
            else:
                file_paths.append(file_path)
                file_modified_time = None

            # Find file paths that were recently processed to exclude them
            # from being added to file_path_queue
            # unless they were modified recently and parsing mode is "modified_time"
            # in which case we don't honor "self._file_process_interval" (min_file_process_interval)
            if (
                (last_finish_time := self._file_stats[file_path].last_finish_time) is not None
                and (now - last_finish_time).total_seconds() < self._file_process_interval
                and not (is_mtime_mode and file_modified_time and (file_modified_time > last_finish_time))
            ):
                file_paths_recently_processed.append(file_path)

        # Sort file paths via last modified time
        if is_mtime_mode:
            file_paths = sorted(files_with_mtime, key=files_with_mtime.get, reverse=True)
        elif list_mode == "alphabetical":
            file_paths.sort()
        elif list_mode == "random_seeded_by_host":
            # Shuffle the list seeded by hostname so multiple schedulers can work on different
            # set of files. Since we set the seed, the sort order will remain same per host
            random.Random(get_hostname()).shuffle(file_paths)

        if file_paths_to_stop_watching:
            self.set_file_paths(
                [path for path in self._file_paths if path not in file_paths_to_stop_watching]
            )

        files_paths_at_run_limit = [
            file_path for file_path, stat in self._file_stats.items() if stat.run_count == self.max_runs
        ]

        file_paths_to_exclude = file_paths_in_progress.union(
            file_paths_recently_processed,
            files_paths_at_run_limit,
        )

        # Do not convert the following list to set as set does not preserve the order
        # and we need to maintain the order of file_paths for `[scheduler] file_parsing_sort_mode`
        files_paths_to_queue = [
            file_path for file_path in file_paths if file_path not in file_paths_to_exclude
        ]

        if self.log.isEnabledFor(logging.DEBUG):
            for path, processor in self._processors.items():
                self.log.debug(
                    "File path %s is still being processed (started: %s)", path, processor.start_time
                )

            self.log.debug(
                "Queuing the following files for processing:\n\t%s",
                "\n\t".join(f.path for f in files_paths_to_queue),
            )
        self._add_paths_to_queue(files_paths_to_queue, False)
        Stats.incr("dag_processing.file_path_queue_update_count")

    def _kill_timed_out_processors(self):
        """Kill any file processors that timeout to defend against process hangs."""
        now = time.time()
        processors_to_remove = []
        for file_path, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self.processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s started %d ago killing it.",
                    file_path,
                    processor.pid,
                    duration,
                )
                Stats.decr("dag_processing.processes", tags={"file_path": file_path, "action": "timeout"})
                Stats.incr("dag_processing.processor_timeouts", tags={"file_path": file_path})
                processor.kill(signal.SIGKILL)

                processors_to_remove.append(file_path)

                stat = DagFileStat(
                    num_dags=0,
                    import_errors=1,
                    last_finish_time=timezone.utcnow(),
                    last_duration=duration,
                    run_count=self._file_stats[file_path].run_count + 1,
                    last_num_of_db_queries=0,
                )
                self._file_stats[file_path] = stat

        # Clean up `self._processors` after iterating over it
        for proc in processors_to_remove:
            self._processors.pop(proc)

    def _add_paths_to_queue(self, file_paths_to_enqueue: list[DagFileInfo], add_at_front: bool):
        """Add stuff to the back or front of the file queue, unless it's already present."""
        new_file_paths = list(p for p in file_paths_to_enqueue if p not in self._file_path_queue)
        if add_at_front:
            self._file_path_queue.extendleft(new_file_paths)
        else:
            self._file_path_queue.extend(new_file_paths)
        Stats.gauge("dag_processing.file_path_queue_size", len(self._file_path_queue))

    def max_runs_reached(self):
        """:return: whether all file paths have been processed max_runs times."""
        if self.max_runs == -1:  # Unlimited runs.
            return False
        if self._num_run < self.max_runs:
            return False
        return all(stat.run_count >= self.max_runs for stat in self._file_stats.values())

    def terminate(self):
        """Stop all running processors."""
        for file_path, processor in self._processors.items():
            Stats.decr("dag_processing.processes", tags={"file_path": file_path, "action": "terminate"})
            # SIGTERM, wait 5s, SIGKILL if still alive
            processor.kill(signal.SIGTERM, escalation_delay=5.0)

    def end(self):
        """Kill all child processes on exit since we don't want to leave them as orphaned."""
        pids_to_kill = [p.pid for p in self._processors.values()]
        if pids_to_kill:
            kill_child_processes_by_pids(pids_to_kill)

    def emit_metrics(self):
        """
        Emit metrics about dag parsing summary.

        This is called once every time around the parsing "loop" - i.e. after
        all files have been parsed.
        """
        with Trace.start_span(span_name="emit_metrics", component="DagFileProcessorManager") as span:
            parse_time = time.perf_counter() - self._parsing_start_time
            Stats.gauge("dag_processing.total_parse_time", parse_time)
            Stats.gauge("dagbag_size", sum(stat.num_dags for stat in self._file_stats.values()))
            Stats.gauge(
                "dag_processing.import_errors", sum(stat.import_errors for stat in self._file_stats.values())
            )
            span.set_attributes(
                {
                    "total_parse_time": parse_time,
                    "dag_bag_size": sum(stat.num_dags for stat in self._file_stats.values()),
                    "import_errors": sum(stat.import_errors for stat in self._file_stats.values()),
                }
            )


def reload_configuration_for_dag_processing():
    # Reload configurations and settings to avoid collision with parent process.
    # Because this process may need custom configurations that cannot be shared,
    # e.g. RotatingFileHandler. And it can cause connection corruption if we
    # do not recreate the SQLA connection pool.
    os.environ["CONFIG_PROCESSOR_MANAGER_LOGGER"] = "True"
    os.environ["AIRFLOW__LOGGING__COLORED_CONSOLE_LOG"] = "False"
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
    importlib.reload(import_module(airflow.settings.LOGGING_CLASS_PATH.rsplit(".", 1)[0]))  # type: ignore
    importlib.reload(airflow.settings)
    airflow.settings.initialize()
    del os.environ["CONFIG_PROCESSOR_MANAGER_LOGGER"]


def process_parse_results(
    run_duration: float,
    finish_time: datetime,
    run_count: int,
    bundle_name: str,
    bundle_version: str | None,
    parsing_result: DagFileParsingResult | None,
    session: Session,
) -> DagFileStat:
    """Take the parsing result and stats about the parser process and convert it into a DagFileState."""
    stat = DagFileStat(
        last_finish_time=finish_time,
        last_duration=run_duration,
        run_count=run_count + 1,
    )

    # TODO: AIP-66 emit metrics
    # file_name = Path(dag_file.path).stem
    # Stats.timing(f"dag_processing.last_duration.{file_name}", stat.last_duration)
    # Stats.timing("dag_processing.last_duration", stat.last_duration, tags={"file_name": file_name})

    if parsing_result is None:
        stat.import_errors = 1
    else:
        # record DAGs and import errors to database
        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            dags=parsing_result.serialized_dags,
            import_errors=parsing_result.import_errors or {},
            warnings=set(parsing_result.warnings or []),
            session=session,
        )
        stat.num_dags = len(parsing_result.serialized_dags)
        if parsing_result.import_errors:
            stat.import_errors = len(parsing_result.import_errors)
    return stat
