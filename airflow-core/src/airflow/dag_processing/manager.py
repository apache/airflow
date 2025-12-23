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

import contextlib
import functools
import inspect
import logging
import os
import random
import selectors
import signal
import sys
import time
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import attrs
import structlog
from sqlalchemy import select, update
from sqlalchemy.orm import load_only
from tabulate import tabulate
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.exceptions import AirflowException
from airflow.models.asset import remove_references_to_deleted_dags
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ParseImportError
from airflow.observability.stats import Stats
from airflow.observability.trace import DebugTrace
from airflow.sdk import SecretCache
from airflow.sdk.log import init_log_file, logging_processors
from airflow.utils.file import list_py_file_paths, might_contain_dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import (
    kill_child_processes_by_pids,
)
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import prohibit_commit, with_row_locks

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from socket import socket

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.sdk.api.client import Client


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


@dataclass(frozen=True)
class DagFileInfo:
    """Information about a DAG file."""

    rel_path: Path
    bundle_name: str
    bundle_path: Path | None = field(compare=False, default=None)
    bundle_version: str | None = None

    @property
    def absolute_path(self) -> Path:
        if not self.bundle_path:
            raise ValueError("bundle_path not set")
        return self.bundle_path / self.rel_path


def _config_int_factory(section: str, key: str):
    return functools.partial(conf.getint, section, key)


def _config_bool_factory(section: str, key: str):
    return functools.partial(conf.getboolean, section, key)


def _config_get_factory(section: str, key: str):
    return functools.partial(conf.get, section, key)


def _resolve_path(instance: Any, attribute: attrs.Attribute, val: str | os.PathLike[str] | None):
    if val is not None:
        val = Path(val).resolve()
    return val


def utc_epoch() -> datetime:
    # pendulum utcnow() is not used as that sets a TimezoneInfo object
    # instead of a Timezone. This is not picklable and also creates issues
    # when using replace()
    result = datetime(1970, 1, 1)
    result = result.replace(tzinfo=timezone.utc)

    return result


@attrs.define(kw_only=True)
class DagFileProcessorManager(LoggingMixin):
    """
    Manage processes responsible for parsing DAGs.

    Given a list of DAG definition files, this kicks off several processors
    in parallel to process them and put the results to a multiprocessing.Queue
    for DagFileProcessorAgent to harvest. The parallelism is limited and as the
    processors finish, more are launched. The files are processed over and
    over again, but no more often than the specified interval.

    :param max_runs: The number of times to parse each file. -1 for unlimited.
    :param bundle_names_to_parse: List of bundle names to parse. If None, all bundles are parsed.
    :param processor_timeout: How long to wait before timing out a DAG file processor
    """

    max_runs: int
    bundle_names_to_parse: list[str] | None = None
    processor_timeout: float = attrs.field(
        factory=_config_int_factory("dag_processor", "dag_file_processor_timeout")
    )
    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)

    _parallelism: int = attrs.field(factory=_config_int_factory("dag_processor", "parsing_processes"))

    parsing_cleanup_interval: float = attrs.field(
        factory=_config_int_factory("scheduler", "parsing_cleanup_interval")
    )
    _file_process_interval: float = attrs.field(
        factory=_config_int_factory("dag_processor", "min_file_process_interval")
    )
    stale_dag_threshold: float = attrs.field(
        factory=_config_int_factory("dag_processor", "stale_dag_threshold")
    )

    _last_deactivate_stale_dags_time: float = attrs.field(default=0, init=False)
    print_stats_interval: float = attrs.field(
        factory=_config_int_factory("dag_processor", "print_stats_interval")
    )
    last_stat_print_time: float = attrs.field(default=0, init=False)

    heartbeat: Callable[[], None] = attrs.field(default=lambda: None)
    """An overridable heartbeat called once every time around the loop"""

    _file_queue: deque[DagFileInfo] = attrs.field(factory=deque, init=False)
    _file_stats: dict[DagFileInfo, DagFileStat] = attrs.field(
        factory=lambda: defaultdict(DagFileStat), init=False
    )

    _dag_bundles: list[BaseDagBundle] = attrs.field(factory=list, init=False)
    _bundle_versions: dict[str, str | None] = attrs.field(factory=dict, init=False)

    _processors: dict[DagFileInfo, DagFileProcessorProcess] = attrs.field(factory=dict, init=False)

    _parsing_start_time: float = attrs.field(init=False)
    _num_run: int = attrs.field(default=0, init=False)

    _callback_to_execute: dict[DagFileInfo, list[CallbackRequest]] = attrs.field(
        factory=lambda: defaultdict(list), init=False
    )

    max_callbacks_per_loop: int = attrs.field(
        factory=_config_int_factory("dag_processor", "max_callbacks_per_loop")
    )

    base_log_dir: str = attrs.field(
        factory=_config_get_factory("logging", "dag_processor_child_process_log_directory")
    )
    _latest_log_symlink_date: datetime = attrs.field(factory=datetime.today, init=False)

    bundle_refresh_check_interval: int = attrs.field(
        factory=_config_int_factory("dag_processor", "bundle_refresh_check_interval")
    )
    _bundles_last_refreshed: float = attrs.field(default=0, init=False)
    """Last time we checked if any bundles are ready to be refreshed"""
    _force_refresh_bundles: set[str] = attrs.field(factory=set, init=False)
    """List of bundles that need to be force refreshed in the next loop"""

    _api_server: InProcessExecutionAPI = attrs.field(init=False, factory=InProcessExecutionAPI)
    """API server to interact with Metadata DB"""

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
        # TODO: Temporary until AIP-92 removes DB access from DagProcessorManager.
        # The manager needs MetastoreBackend to retrieve connections from the database
        # during bundle initialization (e.g., GitDagBundle.__init__ → GitHook needs git credentials).
        # This marks the manager as "server" context so ensure_secrets_backend_loaded() provides
        # MetastoreBackend instead of falling back to EnvironmentVariablesBackend only.
        # Child parser processes explicitly override this by setting _AIRFLOW_PROCESS_CONTEXT=client
        # in _parse_file_entrypoint() to prevent inheriting server privileges.
        # Related: https://github.com/apache/airflow/pull/57459
        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "server"

        self.register_exit_signals()

        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)

        DagBundlesManager().sync_bundles_to_db()

        dag_bundles = list(DagBundlesManager().get_all_dag_bundles())
        if self.bundle_names_to_parse:
            dag_bundles = [b for b in dag_bundles if b.name in self.bundle_names_to_parse]
        self._dag_bundles = dag_bundles

        for bundle in self._dag_bundles:
            self.log.info(
                "Checking for new files in bundle %s every %s seconds", bundle.name, bundle.refresh_interval
            )

        self._symlink_latest_log_directory()

        return self._run_parsing_loop()

    def _scan_stale_dags(self):
        """Scan and deactivate DAGs which are no longer present in files."""
        now = time.monotonic()
        elapsed_time_since_refresh = now - self._last_deactivate_stale_dags_time
        if elapsed_time_since_refresh > self.parsing_cleanup_interval:
            last_parsed = {
                file_info: stat.last_finish_time
                for file_info, stat in self._file_stats.items()
                if stat.last_finish_time
            }
            self.deactivate_stale_dags(last_parsed=last_parsed)
            self._last_deactivate_stale_dags_time = time.monotonic()

    @provide_session
    def deactivate_stale_dags(
        self,
        last_parsed: dict[DagFileInfo, datetime | None],
        session: Session = NEW_SESSION,
    ):
        """Detect and deactivate DAGs which are no longer present in files."""
        to_deactivate = set()
        bundle_names = {b.name for b in self._dag_bundles}
        query = select(
            DagModel.dag_id,
            DagModel.bundle_name,
            DagModel.fileloc,
            DagModel.last_parsed_time,
            DagModel.relative_fileloc,
        ).where(~DagModel.is_stale, DagModel.bundle_name.in_(bundle_names))
        dags_parsed = session.execute(query)

        for dag in dags_parsed:
            # When the Dag's last_parsed_time is more than the stale_dag_threshold older than the
            # Dag file's last_finish_time, the Dag is considered stale as has apparently been removed from the file,
            # This is especially relevant for Dag files that generate Dags in a dynamic manner.
            file_info = DagFileInfo(rel_path=Path(dag.relative_fileloc), bundle_name=dag.bundle_name)
            if last_finish_time := last_parsed.get(file_info, None):
                if dag.last_parsed_time + timedelta(seconds=self.stale_dag_threshold) < last_finish_time:
                    self.log.info("DAG %s is missing and will be deactivated.", dag.dag_id)
                    to_deactivate.add(dag.dag_id)

        if to_deactivate:
            deactivated_dagmodel = session.execute(
                update(DagModel)
                .where(DagModel.dag_id.in_(to_deactivate))
                .values(is_stale=True)
                .execution_options(synchronize_session="fetch")
            )
            deactivated = getattr(deactivated_dagmodel, "rowcount", 0)
            if deactivated:
                self.log.info("Deactivated %i DAGs which are no longer present in file.", deactivated)

    def _run_parsing_loop(self):
        # initialize cache to mutualize calls to Variable.get in DAGs
        # needs to be done before this process is forked to create the DAG parsing processes.
        SecretCache.init()

        poll_time = 0.0

        known_files: dict[str, set[DagFileInfo]] = {}

        while True:
            loop_start_time = time.monotonic()

            self.heartbeat()

            self._kill_timed_out_processors()

            self._queue_requested_files_for_parsing()

            self._refresh_dag_bundles(known_files=known_files)

            if not self._file_queue:
                # Generate more file paths to process if we processed all the files already. Note for this to
                # clear down, we must have cleared all files found from scanning the dags dir _and_ have
                # cleared all files added as a result of callbacks
                self.prepare_file_queue(known_files=known_files)
                self.emit_metrics()
            else:
                # if new files found in dag dir, add them
                self.add_files_to_queue(known_files=known_files)

            self._start_new_processes()

            self._service_processor_sockets(timeout=poll_time)

            self._collect_results()

            for callback in self._fetch_callbacks():
                self._add_callback_to_queue(callback)
            self._scan_stale_dags()
            DagWarning.purge_inactive_dag_warnings()

            # Update number of loop iteration.
            self._num_run += 1

            self.print_stats(known_files=known_files)

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
            socket_handler, on_close = key.data

            # BrokenPipeError should be caught and treated as if the handler returned false, similar
            # to EOF case
            try:
                need_more = socket_handler(key.fileobj)
            except (BrokenPipeError, ConnectionResetError):
                need_more = False
            if not need_more:
                sock: socket = key.fileobj  # type: ignore[assignment]
                on_close(sock)
                sock.close()

    def _queue_requested_files_for_parsing(self) -> None:
        """Queue any files requested for parsing as requested by users via UI/API."""
        files = self._get_priority_files()
        bundles_to_refresh: set[str] = set()
        for file in files:
            # Try removing the file if already present
            with contextlib.suppress(ValueError):
                self._file_queue.remove(file)
            # enqueue file to the start of the queue.
            self._file_queue.appendleft(file)
            bundles_to_refresh.add(file.bundle_name)

        self._force_refresh_bundles |= bundles_to_refresh
        if self._force_refresh_bundles:
            self.log.info("Bundles being force refreshed: %s", ", ".join(self._force_refresh_bundles))

    @provide_session
    def _get_priority_files(self, session: Session = NEW_SESSION) -> list[DagFileInfo]:
        files: list[DagFileInfo] = []
        bundles = {b.name: b for b in self._dag_bundles}
        requests = session.scalars(
            select(DagPriorityParsingRequest).where(DagPriorityParsingRequest.bundle_name.in_(bundles.keys()))
        )
        for request in requests:
            bundle = bundles[request.bundle_name]
            files.append(
                DagFileInfo(
                    rel_path=Path(request.relative_fileloc), bundle_name=bundle.name, bundle_path=bundle.path
                )
            )
            session.delete(request)
        return files

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
            bundle_names = [bundle.name for bundle in self._dag_bundles]
            query: Select[tuple[DbCallbackRequest]] = select(DbCallbackRequest)
            query = query.order_by(DbCallbackRequest.priority_weight.desc()).limit(
                self.max_callbacks_per_loop
            )
            query = cast(
                "Select[tuple[DbCallbackRequest]]",
                with_row_locks(query, of=DbCallbackRequest, session=session, skip_locked=True),
            )
            callbacks = session.scalars(query)
            for callback in callbacks:
                req = callback.get_callback_request()
                if req.bundle_name not in bundle_names:
                    continue
                try:
                    callback_queue.append(req)
                    session.delete(callback)
                except Exception as e:
                    self.log.warning("Error adding callback for execution: %s, %s", callback, e)
            guard.commit()
        return callback_queue

    def _add_callback_to_queue(self, request: CallbackRequest):
        self.log.debug("Queuing %s CallbackRequest: %s", type(request).__name__, request)
        try:
            bundle = DagBundlesManager().get_bundle(name=request.bundle_name, version=request.bundle_version)
        except ValueError:
            # Bundle no longer configured
            self.log.error("Bundle %s no longer configured, skipping callback", request.bundle_name)
            return None

        file_info = DagFileInfo(
            rel_path=Path(request.filepath),
            bundle_path=bundle.path,
            bundle_name=request.bundle_name,
            bundle_version=request.bundle_version,
        )
        self._callback_to_execute[file_info].append(request)
        self._add_files_to_queue([file_info], True)
        Stats.incr("dag_processing.other_callback_count")

    def _refresh_dag_bundles(self, known_files: dict[str, set[DagFileInfo]]):
        """Refresh DAG bundles, if required."""
        now = timezone.utcnow()

        # we don't need to check if it's time to refresh every loop - that is way too often
        next_check = self._bundles_last_refreshed + self.bundle_refresh_check_interval
        now_seconds = time.monotonic()
        if now_seconds < next_check and not self._force_refresh_bundles:
            self.log.debug(
                "Not time to check if DAG Bundles need refreshed yet - skipping. Next check in %.2f seconds",
                next_check - now_seconds,
            )
            return

        self._bundles_last_refreshed = now_seconds

        for bundle in self._dag_bundles:
            # TODO: AIP-66 handle errors in the case of incomplete cloning? And test this.
            #  What if the cloning/refreshing took too long(longer than the dag processor timeout)
            if not bundle.is_initialized:
                try:
                    bundle.initialize()
                except AirflowException as e:
                    self.log.exception("Error initializing bundle %s: %s", bundle.name, e)
                    continue
            # TODO: AIP-66 test to make sure we get a fresh record from the db and it's not cached
            with create_session() as session:
                bundle_model = session.get(DagBundleModel, bundle.name)
                if bundle_model is None:
                    self.log.warning("Bundle model not found for %s", bundle.name)
                    continue
                elapsed_time_since_refresh = (
                    now - (bundle_model.last_refreshed or utc_epoch())
                ).total_seconds()
                if bundle.supports_versioning:
                    # we will also check the version of the bundle to see if another DAG processor has seen
                    # a new version
                    pre_refresh_version = (
                        self._bundle_versions.get(bundle.name) or bundle.get_current_version()
                    )
                    current_version_matches_db = pre_refresh_version == bundle_model.version
                else:
                    # With no versioning, it always "matches"
                    current_version_matches_db = True

                previously_seen = bundle.name in self._bundle_versions
                if (
                    elapsed_time_since_refresh < bundle.refresh_interval
                    and current_version_matches_db
                    and previously_seen
                    and bundle.name not in self._force_refresh_bundles
                ):
                    self.log.info("Not time to refresh bundle %s", bundle.name)
                    continue

                self.log.info("Refreshing bundle %s", bundle.name)

                try:
                    bundle.refresh()
                except Exception:
                    self.log.exception("Error refreshing bundle %s", bundle.name)
                    continue

                bundle_model.last_refreshed = now
                self._force_refresh_bundles.discard(bundle.name)

                if bundle.supports_versioning:
                    # We can short-circuit the rest of this if (1) bundle was seen before by
                    # this dag processor and (2) the version of the bundle did not change
                    # after refreshing it
                    version_after_refresh = bundle.get_current_version()
                    if previously_seen and pre_refresh_version == version_after_refresh:
                        self.log.debug(
                            "Bundle %s version not changed after refresh: %s",
                            bundle.name,
                            version_after_refresh,
                        )
                        continue

                    bundle_model.version = version_after_refresh

                    self.log.info(
                        "Version changed for %s, new version: %s", bundle.name, version_after_refresh
                    )
                else:
                    version_after_refresh = None

            self._bundle_versions[bundle.name] = version_after_refresh

            found_files = {
                DagFileInfo(rel_path=p, bundle_name=bundle.name, bundle_path=bundle.path)
                for p in self._find_files_in_bundle(bundle)
            }

            known_files[bundle.name] = found_files
            self.handle_removed_files(known_files=known_files)

            self.deactivate_deleted_dags(bundle_name=bundle.name, present=found_files)
            self.clear_orphaned_import_errors(
                bundle_name=bundle.name,
                observed_filelocs={str(x.rel_path) for x in found_files},  # todo: make relative
            )

    def _find_files_in_bundle(self, bundle: BaseDagBundle) -> list[Path]:
        """Get relative paths for dag files from bundle dir."""
        # Build up a list of Python files that could contain DAGs
        self.log.info("Searching for files in %s at %s", bundle.name, bundle.path)
        rel_paths = [Path(x).relative_to(bundle.path) for x in list_py_file_paths(bundle.path)]
        self.log.info("Found %s files for bundle %s", len(rel_paths), bundle.name)

        return rel_paths

    def deactivate_deleted_dags(self, bundle_name: str, present: set[DagFileInfo]) -> None:
        """Deactivate DAGs that come from files that are no longer present in bundle."""

        def find_zipped_dags(abs_path: os.PathLike) -> Iterator[str]:
            """
            Find dag files in zip file located at abs_path.

            We return the abs "paths" formed by joining the relative path inside the zip
            with the path to the zip.

            """
            try:
                with zipfile.ZipFile(abs_path) as z:
                    for info in z.infolist():
                        if might_contain_dag(info.filename, True, z):
                            yield os.path.join(abs_path, info.filename)
            except zipfile.BadZipFile:
                self.log.exception("There was an error accessing ZIP file %s", abs_path)

        rel_filelocs: list[str] = []
        for info in present:
            abs_path = str(info.absolute_path)
            if abs_path.endswith(".py") or not zipfile.is_zipfile(abs_path):
                rel_filelocs.append(str(info.rel_path))
            else:
                if TYPE_CHECKING:
                    assert info.bundle_path
                for abs_sub_path in find_zipped_dags(abs_path=info.absolute_path):
                    rel_sub_path = Path(abs_sub_path).relative_to(info.bundle_path)
                    rel_filelocs.append(str(rel_sub_path))

        with create_session() as session:
            any_deactivated = DagModel.deactivate_deleted_dags(
                bundle_name=bundle_name,
                rel_filelocs=rel_filelocs,
                session=session,
            )
            # Only run cleanup if we actually deactivated any DAGs
            # This avoids unnecessary DELETE queries in the common case where no DAGs were deleted
            if any_deactivated:
                remove_references_to_deleted_dags(session=session)

    def print_stats(self, known_files: dict[str, set[DagFileInfo]]):
        """Occasionally print out stats about how fast the files are getting processed."""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if known_files:
                self._log_file_processing_stats(known_files=known_files)
            self.last_stat_print_time = time.monotonic()

    @provide_session
    def clear_orphaned_import_errors(
        self, bundle_name: str, observed_filelocs: set[str], session: Session = NEW_SESSION
    ):
        """
        Clear import errors for files that no longer exist.

        :param session: session for ORM operations
        """
        self.log.debug("Removing old import errors")
        try:
            errors = session.scalars(
                select(ParseImportError)
                .where(ParseImportError.bundle_name == bundle_name)
                .options(load_only(ParseImportError.filename))
            )
            for error in errors:
                if error.filename not in observed_filelocs:
                    session.delete(error)
        except Exception:
            self.log.exception("Error removing old import errors")

    def _log_file_processing_stats(self, known_files: dict[str, set[DagFileInfo]]):
        """
        Print out stats about how files are getting processed.

        :param known_files: a list of file paths that may contain Airflow
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
            "Bundle",
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

        for files in known_files.values():
            for file in files:
                stat = self._file_stats[file]
                proc = self._processors.get(file)
                num_dags = stat.num_dags
                num_errors = stat.import_errors
                file_name = Path(file.rel_path).stem
                processor_pid = proc.pid if proc else None
                processor_start_time = proc.start_time if proc else None
                runtime = (now - processor_start_time) if processor_start_time else None
                last_run = stat.last_finish_time
                if last_run:
                    seconds_ago = (utcnow - last_run).total_seconds()
                    Stats.gauge(f"dag_processing.last_run.seconds_ago.{file_name}", seconds_ago)

                rows.append(
                    (
                        file.bundle_name,
                        file.rel_path,
                        processor_pid,
                        runtime,
                        num_dags,
                        num_errors,
                        stat.last_duration,
                        last_run,
                    )
                )

        # Sort by longest last runtime. (Can't sort None values in python3)
        rows.sort(key=lambda x: x[6] or 0.0, reverse=True)

        formatted_rows = []
        for (
            bundle_name,
            relative_path,
            pid,
            runtime,
            num_dags,
            num_errors,
            last_runtime,
            last_run,
        ) in rows:
            formatted_rows.append(
                (
                    bundle_name,
                    relative_path,
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

    def handle_removed_files(self, known_files: dict[str, set[DagFileInfo]]):
        """
        Remove from data structures the files that are missing.

        Also, terminate processes that may be running on those removed files.

        :param known_files: structure containing known files per-bundle
        :return: None
        """
        files_set: set[DagFileInfo] = set()
        """Set containing all observed files.

        We consolidate to one set for performance.
        """

        for v in known_files.values():
            files_set |= v

        self.purge_removed_files_from_queue(present=files_set)
        self.terminate_orphan_processes(present=files_set)
        self.remove_orphaned_file_stats(present=files_set)

    def purge_removed_files_from_queue(self, present: set[DagFileInfo]):
        """Remove from queue any files no longer observed locally."""
        self._file_queue = deque(x for x in self._file_queue if x in present)
        Stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def remove_orphaned_file_stats(self, present: set[DagFileInfo]):
        """Remove the stats for any dag files that don't exist anymore."""
        # todo: store stats by bundle also?
        stats_to_remove = set(self._file_stats).difference(present)
        for file in stats_to_remove:
            del self._file_stats[file]

    def terminate_orphan_processes(self, present: set[DagFileInfo]):
        """Stop processors that are working on deleted files."""
        for file in list(self._processors.keys()):
            if file not in present:
                processor = self._processors.pop(file, None)
                if not processor:
                    continue
                file_name = str(file.rel_path)
                self.log.warning("Stopping processor for %s", file_name)
                Stats.decr("dag_processing.processes", tags={"file_path": file_name, "action": "stop"})
                processor.kill(signal.SIGKILL)
                processor.logger_filehandle.close()
                self._file_stats.pop(file, None)

    @provide_session
    def _collect_results(self, session: Session = NEW_SESSION):
        # TODO: Use an explicit session in this fn
        finished = []
        for file, proc in self._processors.items():
            if not proc.is_ready:
                # This processor hasn't finished yet, or we haven't read all the output from it yet
                continue
            finished.append(file)

            # Detect if this was callback-only processing
            # For such-cases, we don't serialize the dags and hence send parsing_result as None.
            is_callback_only = proc.had_callbacks and proc.parsing_result is None
            if is_callback_only:
                self.log.debug("Detected callback-only processing for %s", file)

            # Collect the DAGS and import errors into the DB, emit metrics etc.
            self._file_stats[file] = process_parse_results(
                run_duration=time.monotonic() - proc.start_time,
                finish_time=timezone.utcnow(),
                run_count=self._file_stats[file].run_count,
                bundle_name=file.bundle_name,
                bundle_version=self._bundle_versions[file.bundle_name],
                parsing_result=proc.parsing_result,
                session=session,
                is_callback_only=is_callback_only,
                relative_fileloc=str(file.rel_path),
            )

        for file in finished:
            processor = self._processors.pop(file)
            processor.logger_filehandle.close()

    def _get_log_dir(self) -> str:
        return os.path.join(self.base_log_dir, timezone.utcnow().strftime("%Y-%m-%d"))

    def _symlink_latest_log_directory(self):
        """
        Create symbolic link to the current day's log directory.

        Allows easy access to the latest parsing log files.
        """
        log_directory = self._get_log_dir()
        latest_log_directory_path = os.path.join(self.base_log_dir, "latest")
        if os.path.isdir(log_directory):
            rel_link_target = Path(log_directory).relative_to(Path(latest_log_directory_path).parent)
            try:
                # if symlink exists but is stale, update it
                if os.path.islink(latest_log_directory_path):
                    if os.path.realpath(latest_log_directory_path) != log_directory:
                        os.unlink(latest_log_directory_path)
                        os.symlink(rel_link_target, latest_log_directory_path)
                elif os.path.isdir(latest_log_directory_path) or os.path.isfile(latest_log_directory_path):
                    self.log.warning(
                        "%s already exists as a dir/file. Skip creating symlink.", latest_log_directory_path
                    )
                else:
                    os.symlink(rel_link_target, latest_log_directory_path)
            except OSError:
                self.log.warning("OSError while attempting to symlink the latest log directory")

    def _render_log_filename(self, dag_file: DagFileInfo) -> str:
        """Return an absolute path of where to log for a given dagfile."""
        if self._latest_log_symlink_date < datetime.today():
            self._symlink_latest_log_directory()
            self._latest_log_symlink_date = datetime.today()

        bundle = next(b for b in self._dag_bundles if b.name == dag_file.bundle_name)
        relative_path = Path(dag_file.rel_path)
        return os.path.join(self._get_log_dir(), bundle.name, f"{relative_path}.log")

    def _get_logger_for_dag_file(self, dag_file: DagFileInfo):
        log_filename = self._render_log_filename(dag_file)
        log_file = init_log_file(log_filename)
        logger_filehandle = log_file.open("ab")
        underlying_logger = structlog.BytesLogger(logger_filehandle)
        processors = logging_processors(json_output=True)
        return structlog.wrap_logger(
            underlying_logger, processors=processors, logger_name="processor"
        ).bind(), logger_filehandle

    @functools.cached_property
    def client(self) -> Client:
        from airflow.sdk.api.client import Client

        client = Client(base_url=None, token="", dry_run=True, transport=self._api_server.transport)
        # Mypy is wrong -- the setter accepts a string on the property setter! `URLType = URL | str`
        client.base_url = "http://in-process.invalid./"
        return client

    def _create_process(self, dag_file: DagFileInfo) -> DagFileProcessorProcess:
        id = uuid7()

        callback_to_execute_for_file = self._callback_to_execute.pop(dag_file, [])
        logger, logger_filehandle = self._get_logger_for_dag_file(dag_file)

        return DagFileProcessorProcess.start(
            id=id,
            path=dag_file.absolute_path,
            bundle_path=cast("Path", dag_file.bundle_path),
            bundle_name=dag_file.bundle_name,
            callbacks=callback_to_execute_for_file,
            selector=self.selector,
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=self.client,
        )

    def _start_new_processes(self):
        """Start more processors if we have enough slots and files to process."""
        while self._parallelism > len(self._processors) and self._file_queue:
            file = self._file_queue.popleft()
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file in self._processors:
                continue

            processor = self._create_process(file)
            Stats.incr("dag_processing.processes", tags={"file_path": str(file.rel_path), "action": "start"})

            self._processors[file] = processor
            Stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def add_files_to_queue(self, known_files: dict[str, set[DagFileInfo]]):
        for files in known_files.values():
            for file in files:
                if file not in self._file_stats:  # todo: store stats by bundle also?
                    # We found new file after refreshing dir. add to parsing queue at start
                    self.log.info("Adding new file %s to parsing queue", file)
                    self._file_queue.appendleft(file)

    def _sort_by_mtime(self, files: Iterable[DagFileInfo]):
        files_with_mtime: dict[DagFileInfo, float] = {}
        changed_recently = set()
        for file in files:
            try:
                modified_timestamp = os.path.getmtime(file.absolute_path)
                modified_datetime = datetime.fromtimestamp(modified_timestamp, tz=timezone.utc)
                files_with_mtime[file] = modified_timestamp
                last_time = self._file_stats[file].last_finish_time
                if not last_time:
                    continue
                if modified_datetime > last_time:
                    changed_recently.add(file)
            except FileNotFoundError:
                self.log.warning("Skipping processing of missing file: %s", file)
                self._file_stats.pop(file, None)
                continue
        file_infos = [info for info, ts in sorted(files_with_mtime.items(), key=itemgetter(1), reverse=True)]
        return file_infos, changed_recently

    def processed_recently(self, now, file):
        last_time = self._file_stats[file].last_finish_time
        if not last_time:
            return False
        elapsed_ss = (now - last_time).total_seconds()
        if elapsed_ss < self._file_process_interval:
            return True
        return False

    def prepare_file_queue(self, known_files: dict[str, set[DagFileInfo]]):
        """
        Scan dags dir to generate more file paths to process.

        Note this method is only called when the file path queue is empty
        """
        self._parsing_start_time = time.perf_counter()
        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        in_progress = set(self._processors)
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        list_mode = conf.get("dag_processor", "file_parsing_sort_mode")
        recently_processed = set()
        files = []

        for bundle_files in known_files.values():
            for file in bundle_files:
                files.append(file)
                if self.processed_recently(now, file):
                    recently_processed.add(file)

        changed_recently: set[DagFileInfo] = set()
        if list_mode == "modified_time":
            files, changed_recently = self._sort_by_mtime(files=files)
        elif list_mode == "alphabetical":
            files.sort(key=attrgetter("rel_path"))
        elif list_mode == "random_seeded_by_host":
            # Shuffle the list seeded by hostname so multiple DAG processors can work on different
            # set of files. Since we set the seed, the sort order will remain same per host
            random.Random(get_hostname()).shuffle(files)

        at_run_limit = [info for info, stat in self._file_stats.items() if stat.run_count == self.max_runs]
        to_exclude = in_progress.union(at_run_limit)

        # exclude recently processed unless changed recently
        to_exclude |= recently_processed - changed_recently

        # Do not convert the following list to set as set does not preserve the order
        # and we need to maintain the order of files for `[dag_processor] file_parsing_sort_mode`
        to_queue = [x for x in files if x not in to_exclude]

        if self.log.isEnabledFor(logging.DEBUG):
            for path, processor in self._processors.items():
                self.log.debug(
                    "File path %s is still being processed (started: %s)", path, processor.start_time
                )

            self.log.debug(
                "Queuing the following files for processing:\n\t%s",
                "\n\t".join(str(f.rel_path) for f in to_queue),
            )
        self._add_files_to_queue(to_queue, False)
        Stats.incr("dag_processing.file_path_queue_update_count")

    def _kill_timed_out_processors(self):
        """Kill any file processors that timeout to defend against process hangs."""
        now = time.monotonic()
        processors_to_remove = []
        for file, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self.processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s started %d ago killing it.",
                    file,
                    processor.pid,
                    duration,
                )
                file_name = str(file.rel_path)
                Stats.decr("dag_processing.processes", tags={"file_path": file_name, "action": "timeout"})
                Stats.incr("dag_processing.processor_timeouts", tags={"file_path": file_name})
                processor.kill(signal.SIGKILL)

                processors_to_remove.append(file)

                stat = DagFileStat(
                    num_dags=0,
                    import_errors=1,
                    last_finish_time=timezone.utcnow(),
                    last_duration=duration,
                    run_count=self._file_stats[file].run_count + 1,
                    last_num_of_db_queries=0,
                )
                self._file_stats[file] = stat

        # Clean up `self._processors` after iterating over it
        for proc in processors_to_remove:
            processor = self._processors.pop(proc)
            processor.logger_filehandle.close()

    def _add_files_to_queue(self, files: list[DagFileInfo], add_at_front: bool):
        """Add stuff to the back or front of the file queue, unless it's already present."""
        new_files = list(f for f in files if f not in self._file_queue)
        if add_at_front:
            self._file_queue.extendleft(new_files)
        else:
            self._file_queue.extend(new_files)
        Stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def max_runs_reached(self):
        """:return: whether all file paths have been processed max_runs times."""
        if self.max_runs == -1:  # Unlimited runs.
            return False
        if self._num_run < self.max_runs:
            return False
        return all(stat.run_count >= self.max_runs for stat in self._file_stats.values())

    def terminate(self):
        """Stop all running processors."""
        for file, processor in self._processors.items():
            # todo: AIP-66 what to do about file_path tag? replace with bundle name and rel path?
            Stats.decr(
                "dag_processing.processes", tags={"file_path": str(file.rel_path), "action": "terminate"}
            )
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
        with DebugTrace.start_span(span_name="emit_metrics", component="DagFileProcessorManager") as span:
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


def process_parse_results(
    run_duration: float,
    finish_time: datetime,
    run_count: int,
    bundle_name: str,
    bundle_version: str | None,
    parsing_result: DagFileParsingResult | None,
    session: Session,
    *,
    is_callback_only: bool = False,
    relative_fileloc: str | None = None,
) -> DagFileStat:
    """Take the parsing result and stats about the parser process and convert it into a DagFileStat."""
    if is_callback_only:
        # Callback-only processing - don't update timestamps to avoid stale DAG detection issues
        stat = DagFileStat(
            last_duration=run_duration,
            run_count=run_count,  # Don't increment for callback-only processing
        )
        Stats.incr("dag_processing.callback_only_count")
    else:
        # Actual DAG parsing or import error
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
        # No DAGs were parsed - this happens for callback-only processing
        # Don't treat this as an import error when it's callback-only
        if not is_callback_only:
            stat.import_errors = 1
    else:
        # record DAGs and import errors to database
        import_errors = {}
        if parsing_result.import_errors:
            import_errors = {
                (bundle_name, rel_path): error for rel_path, error in parsing_result.import_errors.items()
            }

        # Build the set of files that were parsed. This includes the file that was parsed,
        # even if it no longer contains DAGs, so we can clear old import errors.
        files_parsed: set[tuple[str, str]] | None = None
        if relative_fileloc is not None:
            files_parsed = {(bundle_name, relative_fileloc)}
            files_parsed.update(import_errors.keys())

        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            dags=parsing_result.serialized_dags,
            import_errors=import_errors,
            parse_duration=run_duration,
            warnings=set(parsing_result.warnings or []),
            session=session,
            files_parsed=files_parsed,
        )
        stat.num_dags = len(parsing_result.serialized_dags)
        if parsing_result.import_errors:
            stat.import_errors = len(parsing_result.import_errors)
    return stat
