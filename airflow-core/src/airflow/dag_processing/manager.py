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
import gc
import inspect
import logging
import os
import random
import selectors
import signal
import sys
import time
import zipfile
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast

import attrs
import structlog
from tabulate import tabulate
from uuid6 import uuid7

from airflow._shared.observability.metrics import stats
from airflow._shared.observability.metrics.stats import normalize_name_for_stats
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.dag_processing.api_client import DagProcessingApiClient
from airflow.dag_processing.bundles.base import (
    BundleUsageTrackingManager,
    unpack_bundle_version,
)
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import get_execution_api_server_url
from airflow.observability.metrics import stats_utils
from airflow.sdk import SecretCache
from airflow.sdk.api.client import Client
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import GetConnection, GetVariable
from airflow.sdk.execution_time.request_handlers import handle_get_connection, handle_get_variable
from airflow.sdk.log import init_log_file, logging_processors
from airflow.typing_compat import assert_never
from airflow.utils.file import list_py_file_paths, might_contain_dag
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import (
    kill_child_processes_by_pids,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence
    from socket import socket

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.dag_processing.bundles.base import BaseDagBundle


log = logging.getLogger(__name__)


class DagParsingStat(NamedTuple):
    """Information on processing progress."""

    done: bool
    all_files_processed: bool


class BundleState(NamedTuple):
    """Persisted refresh state for a DAG bundle."""

    last_refreshed: datetime | None
    version: str | None


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

    @property
    def presence_key(self) -> tuple[str, Path]:
        """Return the stable file identity used for presence checks."""
        return self.bundle_name, self.rel_path


def _config_int_factory(section: str, key: str):
    return functools.partial(conf.getint, section, key)


def _config_bool_factory(section: str, key: str):
    return functools.partial(conf.getboolean, section, key)


def _config_get_factory(section: str, key: str):
    return functools.partial(conf.get, section, key)


def _dag_processing_api_server_url() -> str:
    """
    Resolve the DAG Processing API URL the processor persists through.

    Defaults to the ``/dag-processing`` mount on the configured API server (a sibling of the
    ``/execution`` mount), so a standard deployment that already runs the API server needs no
    extra configuration. Set ``[core] dag_processing_api_server_url`` to point at a different
    host.

    The default is derived from the resolved Execution API URL (``execution_api_server_url``),
    not just ``[api] base_url``, so a deployment that points the processor's Execution API at an
    internal service URL keeps both sibling clients on the same host.
    """
    explicit = conf.get("core", "dag_processing_api_server_url", fallback=None)
    if explicit:
        return explicit
    execution_url = get_execution_api_server_url().rstrip("/")
    if execution_url.endswith("/execution"):
        return f"{execution_url[: -len('/execution')]}/dag-processing"
    return f"{execution_url}/dag-processing"


def _api_token() -> str | None:
    """
    Return the token the DAG processor presents to the API server, or ``None``.

    The DAG processor parses (and forks) user code, so it must not hold the deployment signing
    key or be able to mint tokens. It only *carries* a token provisioned by a trusted component:
    read from the file at ``[dag_processor] api_token_path`` (written by the deployment / control
    plane). The same token authenticates both the DAG Processing and Execution API clients. The
    issuance of that token is intentionally left to the deployment (the AIP-92 non-task principal
    question); the processor never signs one itself.
    """
    token_path = conf.get("dag_processor", "api_token_path", fallback=None)
    if not token_path:
        return None
    try:
        return Path(token_path).read_text().strip() or None
    except OSError:
        log.warning("Could not read the DAG processor API token from %s", token_path)
        return None


class _DagProcessorSecretsComms:
    """
    Minimal ``SUPERVISOR_COMMS`` for the DAG processor process.

    Bundle initialization (e.g. ``GitDagBundle`` resolving its git connection) runs in the
    manager process, which holds no metadata-DB connection. Installing this as
    ``task_runner.SUPERVISOR_COMMS`` makes ``ensure_secrets_backend_loaded()`` select
    ``ExecutionAPISecretsBackend``; this shim then resolves the connection/variable lookups it
    sends through the manager's remote Execution API client -- the same path the worker and
    triggerer use -- so DB-stored bundle credentials resolve without direct DB access.
    """

    def __init__(self, client: Client) -> None:
        self._client = client

    def send(self, msg: Any, **kwargs: Any) -> Any:
        if isinstance(msg, GetConnection):
            return handle_get_connection(self._client, msg)[0]
        if isinstance(msg, GetVariable):
            return handle_get_variable(self._client, msg)[0]
        # Other messages (e.g. MaskSecret emitted while masking a fetched secret) do not apply to
        # the manager process; ignore them.
        return None


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


class _StubSelector(selectors.BaseSelector):
    """
    Stub to stand in until the real selector is created.

    This is used in DagFileProcessorManager to keep Mypy happy, and emit a
    slightly better error message than TypeError (if None is used) if a
    contributor accidentally initializes a selector in a wrong place in the
    future.

    Some selectors do not work well in daemon mode after fork (exact reason
    unknown; it's CPython internal). This stub allows us to delay creating a
    selector until after forking and work around the issue.
    """

    def __getattribute__(self, name):
        raise RuntimeError("Selector not initialized")

    def register(self, fileobj, events, data=None): ...
    def unregister(self, fileobj): ...
    def select(self, timeout=None): ...
    def get_map(self): ...


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
    selector: selectors.BaseSelector = attrs.field(factory=_StubSelector)

    _parallelism: int = attrs.field(factory=_config_int_factory("dag_processor", "parsing_processes"))

    parsing_cleanup_interval: float = attrs.field(
        factory=_config_int_factory("scheduler", "parsing_cleanup_interval")
    )
    stale_bundle_cleanup_interval: float = attrs.field(
        factory=_config_int_factory("dag_processor", "stale_bundle_cleanup_interval")
    )
    _file_process_interval: float = attrs.field(
        factory=_config_int_factory("dag_processor", "min_file_process_interval")
    )
    stale_dag_threshold: float = attrs.field(
        factory=_config_int_factory("dag_processor", "stale_dag_threshold")
    )

    _last_deactivate_stale_dags_time: float = attrs.field(default=0, init=False)
    _last_stale_bundle_cleanup_time: float = attrs.field(default=0, init=False)
    print_stats_interval: float = attrs.field(
        factory=_config_int_factory("dag_processor", "print_stats_interval")
    )
    last_stat_print_time: float = attrs.field(default=0, init=False)

    heartbeat: Callable[[], None] = attrs.field(default=lambda: None)
    """An overridable heartbeat called once every time around the loop"""

    _file_queue: OrderedDict[DagFileInfo, None] = attrs.field(factory=OrderedDict, init=False)
    _file_stats: dict[DagFileInfo, DagFileStat] = attrs.field(
        factory=lambda: defaultdict(DagFileStat), init=False
    )

    _dag_bundles: list[BaseDagBundle] = attrs.field(factory=list, init=False)
    _bundle_versions: dict[str, str | None] = attrs.field(factory=dict, init=False)
    _bundle_version_data: dict[str, dict | None] = attrs.field(factory=dict, init=False)

    _processors: dict[DagFileInfo, DagFileProcessorProcess] = attrs.field(factory=dict, init=False)

    _parsing_start_time: float | None = attrs.field(default=None, init=False)
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

    _file_parsing_sort_mode: str = attrs.field(
        factory=_config_get_factory("dag_processor", "file_parsing_sort_mode")
    )

    _dag_processing_client: DagProcessingApiClient = attrs.field(
        init=False,
        factory=lambda: DagProcessingApiClient(_dag_processing_api_server_url(), token_getter=_api_token),
    )
    """Client for the DAG Processing API. The DAG processor never reads or writes the metadata
    database directly; all persistence and metadata reads are routed through the API server."""

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

    def sync_bundles(self) -> None:
        """Sync configured DAG bundles to the metadata database via the DAG Processing API."""
        self._dag_processing_client.sync_bundles()

    def get_all_bundles(self) -> list[BaseDagBundle]:
        """Return configured DAG bundles filtered by ``bundle_names_to_parse`` if provided."""
        return list(DagBundlesManager().get_all_dag_bundles())

    def run(self):
        """
        Use multiple processes to parse and generate tasks for the DAGs in parallel.

        By processing them in separate processes, we can get parallelism and isolation
        from potentially harmful user code.
        """
        self.before_run()
        try:
            return self._run_parsing_loop()
        finally:
            self.after_run()

    def before_run(self) -> None:
        """Set up state required before the parsing loop starts. Default implementation; override to customize."""
        self.prepare_process_context()
        self.register_exit_signals()
        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        self._setup_secrets_comms()
        self.prepare_bundles()
        self._symlink_latest_log_directory()
        # To prevent COW in forked process parsing dag file
        gc.freeze()

    def after_run(self) -> None:
        """Tear down state after the parsing loop exits."""
        # Drop the secrets shim installed in before_run so it does not outlive the loop.
        if isinstance(getattr(task_runner, "SUPERVISOR_COMMS", None), _DagProcessorSecretsComms):
            del task_runner.SUPERVISOR_COMMS

    def _setup_secrets_comms(self) -> None:
        """
        Route the manager's own connection/variable lookups through the remote Execution API.

        Bundle initialization resolves credentials here (see :class:`_DagProcessorSecretsComms`).
        Child parser processes reset ``SUPERVISOR_COMMS`` to their own comms in
        ``_parse_file_entrypoint()``, so this only affects the manager process.
        """
        try:
            comms = _DagProcessorSecretsComms(self.client)
        except Exception:
            # Without a signing key the processor cannot authenticate to the Execution API;
            # skip the shim so bundle init still resolves env/external-backend credentials
            # instead of crashing. (DB-stored bundle credentials will not resolve in this case.)
            self.log.warning(
                "Could not initialize the Execution API client for bundle credentials; "
                "bundle-init connections will resolve only from non-database secrets backends",
                exc_info=True,
            )
            return
        # Duck-typed comms: the shim implements the .send() interface the secrets backend uses.
        task_runner.SUPERVISOR_COMMS = comms  # type: ignore[assignment]

    def prepare_process_context(self) -> None:
        """Initialize transport-neutral process state (selector, stats) before the parsing loop starts."""
        # Initialization is delayed until here to avoid fork issues in some
        # selector implementations. Also see _StubSelector documentation.
        self.selector = selectors.DefaultSelector()

        stats.initialize(
            factory=stats_utils.get_stats_factory(),
            export_legacy_names=conf.getboolean("metrics", "legacy_names_on"),
        )

    def prepare_bundles(self) -> None:
        """Sync bundle configuration to the DB and load bundles for parsing."""
        self.sync_bundles()
        self.load_dag_bundles()

    def load_dag_bundles(self) -> None:
        """Populate ``self._dag_bundles`` via ``get_all_bundles()`` (may hit the DB), filtered by ``bundle_names_to_parse``."""
        dag_bundles = self.get_all_bundles()
        if self.bundle_names_to_parse:
            dag_bundles = [b for b in dag_bundles if b.name in self.bundle_names_to_parse]
        self._dag_bundles = dag_bundles

        for bundle in self._dag_bundles:
            self.log.info(
                "Checking for new files in bundle %s every %s seconds", bundle.name, bundle.refresh_interval
            )

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

    def _cleanup_stale_bundle_versions(self):
        if self.stale_bundle_cleanup_interval <= 0:
            return
        now = time.monotonic()
        elapsed_time_since_cleanup = now - self._last_stale_bundle_cleanup_time
        if elapsed_time_since_cleanup < self.stale_bundle_cleanup_interval:
            return
        try:
            self.cleanup_stale_bundle_versions()
        except Exception:
            self.log.exception("Error removing stale bundle versions")
        finally:
            self._last_stale_bundle_cleanup_time = now

    def cleanup_stale_bundle_versions(self) -> None:
        """Clean up stale DAG bundle version usage records."""
        BundleUsageTrackingManager().remove_stale_bundle_versions()

    def deactivate_stale_dags(self, last_parsed: dict[DagFileInfo, datetime | None]) -> None:
        """Detect and deactivate DAGs which are no longer present in files, via the DAG Processing API."""
        entries = [
            {
                "bundle_name": file_info.bundle_name,
                "relative_fileloc": str(file_info.rel_path),
                "last_finish_time": last_finish_time.isoformat(),
            }
            for file_info, last_finish_time in last_parsed.items()
            if last_finish_time is not None
        ]
        try:
            self._dag_processing_client.deactivate_stale_dags(
                stale_dag_threshold=int(self.stale_dag_threshold), last_parsed=entries
            )
        except Exception:
            # A transient API outage must not crash the parse loop; the next cycle retries.
            self.log.exception("Error deactivating stale DAGs via the DAG Processing API")

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

            self._start_new_processes()

            self._service_processor_sockets(timeout=poll_time)

            self._collect_results()

            for callback in self.fetch_callbacks():
                self._add_callback_to_queue(callback)
            self._scan_stale_dags()
            self._cleanup_stale_bundle_versions()
            self.purge_inactive_dag_warnings()

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
        files = self.claim_priority_files()
        self._add_files_to_queue(files, mode="frontprio")
        self.request_bundle_refresh(file.bundle_name for file in files)
        if self._force_refresh_bundles:
            self.log.info("Bundles being force refreshed: %s", ", ".join(self._force_refresh_bundles))

    def claim_priority_files(self) -> list[DagFileInfo]:
        """Fetch and claim files requested for priority parsing, via the DAG Processing API."""
        bundles = {bundle.name: bundle for bundle in self._dag_bundles}
        try:
            claimed = self._dag_processing_client.claim_priority_files(list(bundles))
        except Exception:
            # A transient API outage must not crash the parse loop; the next cycle retries.
            self.log.exception("Error claiming priority parse requests via the DAG Processing API")
            return []
        return [
            DagFileInfo(
                rel_path=Path(entry["relative_fileloc"]),
                bundle_name=entry["bundle_name"],
                bundle_path=bundles[entry["bundle_name"]].path,
            )
            for entry in claimed
            if entry["bundle_name"] in bundles
        ]

    def request_bundle_refresh(self, bundle_names: str | Iterable[str]) -> None:
        """
        Request that the given bundles be refreshed on the next refresh tick.

        Use this from event handlers reacting to external signals to mark
        bundles as needing refresh; the next call to :meth:`_refresh_dag_bundles`
        will not skip them via :meth:`should_skip_refresh`.
        """
        if isinstance(bundle_names, str):
            self._force_refresh_bundles.add(bundle_names)
            return
        self._force_refresh_bundles.update(bundle_names)

    def should_skip_refresh(
        self,
        *,
        bundle: BaseDagBundle,
        elapsed_time_since_refresh: float,
        current_version_matches_db: bool,
        previously_seen: bool,
    ) -> bool:
        """Return ``True`` when a Dag bundle refresh should be skipped."""
        return (
            elapsed_time_since_refresh < bundle.refresh_interval
            and current_version_matches_db
            and previously_seen
            and bundle.name not in self._force_refresh_bundles
        )

    def fetch_callbacks(self) -> list[CallbackRequest]:
        """Fetch and claim callbacks for this manager's bundles, via the DAG Processing API."""
        try:
            return self._dag_processing_client.fetch_callbacks(
                bundle_names=[bundle.name for bundle in self._dag_bundles],
                limit=self.max_callbacks_per_loop,
            )
        except Exception:
            # A transient API outage must not crash the parse loop; the next cycle retries.
            self.log.exception("Error fetching callbacks via the DAG Processing API")
            return []

    def prepare_callback_bundle(self, request: CallbackRequest) -> BaseDagBundle | None:
        """
        Return the bundle to run the callback against, or ``None`` to skip the callback.

        Default implementation looks the bundle up via :class:`DagBundlesManager` and, for
        versioned requests on bundles that support versioning, calls ``bundle.initialize()``.
        Override to source the bundle from an API.
        """
        try:
            bundle = DagBundlesManager().get_bundle(name=request.bundle_name, version=request.bundle_version)
        except ValueError:
            self.log.error("Bundle %s no longer configured, skipping callback", request.bundle_name)
            return None
        if bundle.supports_versioning and request.bundle_version:
            try:
                bundle.initialize()
            except Exception:
                self.log.exception(
                    "Error initializing bundle %s version %s for callback, skipping",
                    request.bundle_name,
                    request.bundle_version,
                )
                return None
        return bundle

    def _add_callback_to_queue(self, request: CallbackRequest) -> None:
        self.log.debug("Queuing %s CallbackRequest: %s", type(request).__name__, request)
        bundle = self.prepare_callback_bundle(request)
        if bundle is None:
            return

        file_info = DagFileInfo(
            rel_path=Path(request.filepath),
            bundle_path=bundle.path,
            bundle_name=request.bundle_name,
            bundle_version=request.bundle_version,
        )
        self._callback_to_execute[file_info].append(request)
        self._add_files_to_queue([file_info], mode="front")
        stats.incr("dag_processing.other_callback_count")

    def get_bundle_state(self, bundle_name: str) -> BundleState | None:
        """
        Return the persisted refresh state for a bundle, via the DAG Processing API.

        Returns ``None`` if the bundle has no record.
        """
        data = self._dag_processing_client.get_bundle_state(bundle_name)
        if data is None:
            return None
        return BundleState(last_refreshed=data["last_refreshed"], version=data["version"])

    def update_bundle_state(self, bundle_name: str, *, last_refreshed: datetime, version: str | None) -> None:
        """
        Persist the post-refresh state for a bundle, via the DAG Processing API.

        Always updates ``last_refreshed``; updates ``version`` only when ``version`` is not
        ``None`` (pass ``None`` to leave the stored version unchanged).
        """
        self._dag_processing_client.update_bundle_state(
            bundle_name, last_refreshed=last_refreshed, version=version
        )

    def purge_inactive_dag_warnings(self) -> None:
        """Purge warnings for inactive/stale DAGs, via the DAG Processing API."""
        try:
            self._dag_processing_client.purge_inactive_dag_warnings()
        except Exception:
            # A transient API outage must not crash the parse loop; the next cycle retries.
            self.log.exception("Error purging inactive DAG warnings via the DAG Processing API")

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

        any_refreshed = False
        for bundle in self._dag_bundles:
            # TODO: AIP-66 handle errors in the case of incomplete cloning? And test this.
            #  What if the cloning/refreshing took too long(longer than the dag processor timeout)
            if not bundle.is_initialized:
                try:
                    bundle.initialize()
                    any_refreshed = True
                except AirflowException as e:
                    self.log.exception("Error initializing bundle %s: %s", bundle.name, e)
                    continue
            # TODO: AIP-66 test to make sure we get a fresh record from the db and it's not cached
            try:
                bundle_state = self.get_bundle_state(bundle.name)
            except Exception:
                self.log.exception("Error fetching state for bundle %s", bundle.name)
                continue
            if bundle_state is None:
                self.log.warning("Bundle model not found for %s", bundle.name)
                continue
            elapsed_time_since_refresh = (now - (bundle_state.last_refreshed or utc_epoch())).total_seconds()
            if bundle.supports_versioning:
                # we will also check the version of the bundle to see if another DAG processor has seen
                # a new version
                pre_refresh_version = self._bundle_versions.get(bundle.name)
                # Use `is None` (not falsy) so an empty-string version is treated as a valid cached value.
                if pre_refresh_version is None:
                    pre_refresh_version, _ = unpack_bundle_version(bundle.get_current_version(), bundle)
                current_version_matches_db = pre_refresh_version == bundle_state.version
            else:
                # With no versioning, it always "matches"
                current_version_matches_db = True

            previously_seen = bundle.name in self._bundle_versions
            if self.should_skip_refresh(
                bundle=bundle,
                elapsed_time_since_refresh=elapsed_time_since_refresh,
                current_version_matches_db=current_version_matches_db,
                previously_seen=previously_seen,
            ):
                self.log.info("Not time to refresh bundle %s", bundle.name)
                continue

            self.log.info("Refreshing bundle %s", bundle.name)

            try:
                bundle.refresh()
                any_refreshed = True
            except Exception:
                self.log.exception("Error refreshing bundle %s", bundle.name)
                continue

            self._force_refresh_bundles.discard(bundle.name)

            if bundle.supports_versioning:
                # We can short-circuit the rest of this if (1) bundle was seen before by
                # this dag processor and (2) the version of the bundle did not change
                # after refreshing it
                version_after_refresh, version_data_after_refresh = unpack_bundle_version(
                    bundle.get_current_version(), bundle
                )
                if previously_seen and pre_refresh_version == version_after_refresh:
                    self.log.debug(
                        "Bundle %s version not changed after refresh: %s",
                        bundle.name,
                        version_after_refresh,
                    )
                    try:
                        self.update_bundle_state(bundle.name, last_refreshed=now, version=None)
                    except Exception:
                        self.log.exception("Error persisting state for bundle %s", bundle.name)
                    continue

                self.log.info("Version changed for %s, new version: %s", bundle.name, version_after_refresh)
            else:
                version_after_refresh = None
                version_data_after_refresh = None

            # Persistence failure must not skip file scanning (bundle is already refreshed locally).
            # _bundle_versions is only advanced on success to stay consistent with the DB.
            try:
                self.update_bundle_state(bundle.name, last_refreshed=now, version=version_after_refresh)
            except Exception:
                self.log.exception("Error persisting state for bundle %s", bundle.name)
            else:
                self._bundle_versions[bundle.name] = version_after_refresh
                self._bundle_version_data[bundle.name] = version_data_after_refresh

            found_files = {
                DagFileInfo(rel_path=p, bundle_name=bundle.name, bundle_path=bundle.path)
                for p in self._find_files_in_bundle(bundle)
            }

            known_files[bundle.name] = found_files

            try:
                self._dag_processing_client.reconcile(
                    bundle_name=bundle.name,
                    observed_filelocs=self._get_observed_filelocs(found_files),
                )
            except Exception:
                self.log.exception(
                    "Error reconciling bundle %s via the DAG Processing API; "
                    "skipping stale reconciliation this cycle",
                    bundle.name,
                )

        if any_refreshed:
            self.handle_removed_files(known_files=known_files)
            self._resort_file_queue()
            self._add_new_files_to_queue(known_files=known_files)

    def _find_files_in_bundle(self, bundle: BaseDagBundle) -> list[Path]:
        """Get relative paths for dag files from bundle dir."""
        # Build up a list of Python files that could contain DAGs
        self.log.info("Searching for files in %s at %s", bundle.name, bundle.path)
        rel_paths = [Path(x).relative_to(bundle.path) for x in list_py_file_paths(bundle.path)]
        self.log.info("Found %s files for bundle %s", len(rel_paths), bundle.name)

        return rel_paths

    def _get_observed_filelocs(self, present: set[DagFileInfo]) -> set[str]:
        """
        Return observed DAG source paths for bundle entries.

        For regular files this includes the relative file path.
        For ZIP archives this includes DAG-like inner paths such as
        ``archive.zip/dag.py``.
        """

        def find_zipped_dags(abs_path: os.PathLike) -> Iterator[str]:
            """Yield absolute paths for DAG-like files inside a ZIP archive."""
            try:
                with zipfile.ZipFile(abs_path) as z:
                    for info in z.infolist():
                        if might_contain_dag(info.filename, True, z):
                            yield os.path.join(abs_path, info.filename)
            except zipfile.BadZipFile:
                self.log.exception("There was an error accessing ZIP file %s", abs_path)

        observed_filelocs: set[str] = set()
        for info in present:
            abs_path = str(info.absolute_path)
            if abs_path.endswith(".py") or not zipfile.is_zipfile(abs_path):
                observed_filelocs.add(str(info.rel_path))
            else:
                if TYPE_CHECKING:
                    assert info.bundle_path
                for abs_sub_path in find_zipped_dags(abs_path=info.absolute_path):
                    rel_sub_path = Path(abs_sub_path).relative_to(info.bundle_path)
                    observed_filelocs.add(str(rel_sub_path))

        return observed_filelocs

    def print_stats(self, known_files: dict[str, set[DagFileInfo]]):
        """Occasionally print out stats about how fast the files are getting processed."""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if known_files:
                self._log_file_processing_stats(known_files=known_files)
            self.last_stat_print_time = time.monotonic()

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
                    stats.gauge(f"dag_processing.last_run.seconds_ago.{file_name}", seconds_ago)

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
        present_keys = {file.presence_key for file in present}
        self._file_queue = OrderedDict((x, None) for x in self._file_queue if x.presence_key in present_keys)
        stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def remove_orphaned_file_stats(self, present: set[DagFileInfo]):
        """Remove the stats for any dag files that don't exist anymore."""
        present_keys = {file.presence_key for file in present}
        stats_to_remove = {file for file in self._file_stats if file.presence_key not in present_keys}
        for file in stats_to_remove:
            del self._file_stats[file]

    def terminate_orphan_processes(self, present: set[DagFileInfo]):
        """Stop processors that are working on deleted files."""
        present_keys = {file.presence_key for file in present}
        for file in list(self._processors.keys()):
            if file.presence_key not in present_keys:
                processor = self._processors.pop(file, None)
                if not processor:
                    continue
                file_name = str(file.rel_path)
                self.log.warning("Stopping processor for %s", file_name)
                stats.decr("dag_processing.processes", tags={"file_path": file_name, "action": "stop"})
                processor.kill(signal.SIGKILL)
                processor.logger_filehandle.close()
                self._file_stats.pop(file, None)

    def handle_parsing_result(
        self,
        file: DagFileInfo,
        proc: DagFileProcessorProcess,
    ) -> None:
        """
        Post-process a single finished parse result.

        Detects callback-only processing, updates file stats, emits metrics,
        and persists DAGs/import-errors via :meth:`persist_parsing_result`.
        Extracted from ``_collect_results`` to keep result handling and
        persistence separate.

        If persistence fails, the error is logged and the previous persisted
        DAG/import-error counts are preserved while a minimal timestamp update
        throttles immediate retries, so other files in the same
        ``_collect_results`` cycle still run.
        """
        is_callback_only = proc.had_callbacks and proc.parsing_result is None
        if is_callback_only:
            self.log.debug("Detected callback-only processing for %s", file)

        run_duration = time.monotonic() - proc.start_time
        finish_time = timezone.utcnow()
        next_stat = process_parse_results(
            run_duration=run_duration,
            finish_time=finish_time,
            run_count=self._file_stats[file].run_count,
            bundle_name=file.bundle_name,
            parsing_result=proc.parsing_result,
            is_callback_only=is_callback_only,
            relative_fileloc=str(file.rel_path),
        )

        if proc.parsing_result is not None:
            try:
                self.persist_parsing_result(
                    bundle_name=file.bundle_name,
                    bundle_version=self._bundle_versions[file.bundle_name],
                    version_data=self._bundle_version_data.get(file.bundle_name),
                    parsing_result=proc.parsing_result,
                    run_duration=run_duration,
                    relative_fileloc=str(file.rel_path),
                )
            except Exception:
                self.log.exception(
                    "Failed to persist parsing result for %s in bundle %s; "
                    "keeping previous persisted stats while throttling retries. "
                    "Other files in this cycle are still processed.",
                    str(file.rel_path),
                    file.bundle_name,
                )
                current_stat = self._file_stats[file]
                self._file_stats[file] = DagFileStat(
                    num_dags=current_stat.num_dags,
                    import_errors=current_stat.import_errors,
                    last_finish_time=finish_time,
                    last_duration=run_duration,
                    run_count=current_stat.run_count + 1,
                    last_num_of_db_queries=current_stat.last_num_of_db_queries,
                )
                return

        self._file_stats[file] = next_stat

    def persist_parsing_result(
        self,
        *,
        bundle_name: str,
        bundle_version: str | None,
        version_data: dict | None,
        parsing_result: DagFileParsingResult,
        run_duration: float,
        relative_fileloc: str | None,
    ) -> None:
        """Persist parsed DAG data via the DAG Processing API."""
        self._dag_processing_client.persist_parsing_result(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            version_data=version_data,
            parsing_result=parsing_result,
            run_duration=run_duration,
            relative_fileloc=relative_fileloc,
        )

    def _collect_results(self):
        finished = []
        for file, proc in self._processors.items():
            if not proc.is_ready:
                # This processor hasn't finished yet, or we haven't read all the output from it yet
                continue
            finished.append(file)
            self.handle_parsing_result(file, proc)

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
        # Parse-time connection/variable/xcom reads go to the remote Execution API, so the
        # processor holds no metadata-DB connection. It carries the externally-provisioned token
        # (see _api_token); it does not mint one, since it parses user code. Cached so the httpx
        # connection pool is reused across parses; the client refreshes its token in place from the
        # ``Refreshed-API-Token`` response header during use.
        return Client(
            base_url=get_execution_api_server_url(),
            token=_api_token() or "",
            dry_run=False,
        )

    def _create_process(self, dag_file: DagFileInfo) -> DagFileProcessorProcess:
        id = uuid7()

        callback_to_execute_for_file = self._callback_to_execute.pop(dag_file, [])
        logger, logger_filehandle = self._get_logger_for_dag_file(dag_file)

        return DagFileProcessorProcess.start(
            id=id,
            path=dag_file.absolute_path,
            bundle_path=cast("Path", dag_file.bundle_path),
            bundle_name=dag_file.bundle_name,
            dag_file_rel_path=str(dag_file.rel_path),
            callbacks=callback_to_execute_for_file,
            selector=self.selector,
            logger=logger,
            logger_filehandle=logger_filehandle,
            subprocess_logs_to_stdout=conf.get("logging", "dag_processor_log_target") == "stdout",
            client=self.client,
        )

    def _start_new_processes(self):
        """Start more processors if we have enough slots and files to process."""
        while self._parallelism > len(self._processors) and self._file_queue:
            file, _ = self._file_queue.popitem(last=False)
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file in self._processors:
                continue

            processor = self._create_process(file)
            stats.incr("dag_processing.processes", tags={"file_path": str(file.rel_path), "action": "start"})

            self._processors[file] = processor
            stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def _add_new_files_to_queue(self, known_files: dict[str, set[DagFileInfo]]):
        """
        Add new files to the front of the queue.

        A "new" file is a file that has not been processed yet and is not currently being processed.
        """
        new_files = []
        tracked_presence_keys = {file.presence_key for file in self._file_queue}
        tracked_presence_keys.update(file.presence_key for file in self._file_stats)
        tracked_presence_keys.update(file.presence_key for file in self._processors)
        for files in known_files.values():
            for file in files:
                if file.presence_key not in tracked_presence_keys:
                    new_files.append(file)
                    tracked_presence_keys.add(file.presence_key)

        if new_files:
            self.log.info("Adding %d new files to the front of the queue", len(new_files))
            self._add_files_to_queue(new_files, mode="front")

    def _resort_file_queue(self):
        if self._file_parsing_sort_mode == "modified_time" and self._file_queue:
            # Separate files with pending callbacks from regular files
            # Callbacks should stay at the front regardless of mtime
            callback_files = []
            regular_files = []
            for file in self._file_queue:
                if file in self._callback_to_execute:
                    callback_files.append(file)
                else:
                    regular_files.append(file)

            # Sort only the regular files by mtime
            sorted_regular_files, _ = self._sort_by_mtime(regular_files)

            # Put callback files at the front, then sorted regular files
            self._file_queue = OrderedDict.fromkeys(callback_files + sorted_regular_files)

    def _sort_by_mtime(self, files: Iterable[DagFileInfo]):
        file_stats_by_presence_key = {file.presence_key: stat for file, stat in self._file_stats.items()}
        files_with_mtime: dict[DagFileInfo, float] = {}
        changed_recently = set()
        for file in files:
            try:
                modified_timestamp = os.path.getmtime(file.absolute_path)
                modified_datetime = datetime.fromtimestamp(modified_timestamp, tz=timezone.utc)
                files_with_mtime[file] = modified_timestamp
                stat = file_stats_by_presence_key.get(file.presence_key)
                last_time = stat.last_finish_time if stat else None
                if not last_time:
                    continue
                if modified_datetime > last_time:
                    changed_recently.add(file)
            except FileNotFoundError:
                self.log.warning("Skipping processing of missing file: %s", file)
                stats_to_remove = [
                    tracked_file
                    for tracked_file in self._file_stats
                    if tracked_file.presence_key == file.presence_key
                ]
                for tracked_file in stats_to_remove:
                    self._file_stats.pop(tracked_file, None)
                continue
        file_infos = [info for info, ts in sorted(files_with_mtime.items(), key=itemgetter(1), reverse=True)]
        return file_infos, changed_recently

    def processed_recently(self, now, file):
        stat = next(
            (
                stat
                for tracked_file, stat in self._file_stats.items()
                if tracked_file.presence_key == file.presence_key
            ),
            None,
        )
        last_time = stat.last_finish_time if stat else None
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
        # We only emit metrics after processing all files in the queue. If `self._parsing_start_time` is None
        # when this method is called, no files have yet been added to the queue so we shouldn't emit metrics.
        if self._parsing_start_time is not None:
            emit_metrics(
                parse_time=time.perf_counter() - self._parsing_start_time,
                dag_file_stats=list(self._file_stats.values()),
            )
            self._parsing_start_time = None

        # If the file path is already being processed, or if a file was
        # processed recently, wait until the next batch
        in_progress_keys = {file.presence_key for file in self._processors}
        file_stats_by_presence_key = {file.presence_key: stat for file, stat in self._file_stats.items()}
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        recently_processed = set()
        files = []

        for bundle_files in known_files.values():
            for file in bundle_files:
                files.append(file)
                stat = file_stats_by_presence_key.get(file.presence_key)
                last_time = stat.last_finish_time if stat else None
                if last_time and (now - last_time).total_seconds() < self._file_process_interval:
                    recently_processed.add(file)

        changed_recently: set[DagFileInfo] = set()
        if self._file_parsing_sort_mode == "modified_time":
            files, changed_recently = self._sort_by_mtime(files=files)
        elif self._file_parsing_sort_mode == "alphabetical":
            files.sort(key=attrgetter("rel_path"))
        elif self._file_parsing_sort_mode == "random_seeded_by_host":
            # Shuffle the list seeded by hostname so multiple DAG processors can work on different
            # set of files. Since we set the seed, the sort order will remain same per host
            random.Random(get_hostname()).shuffle(files)

        at_run_limit_keys = {
            presence_key
            for presence_key, stat in file_stats_by_presence_key.items()
            if stat.run_count == self.max_runs
        }
        to_exclude = in_progress_keys.union(at_run_limit_keys)

        # exclude recently processed unless changed recently
        to_exclude |= {file.presence_key for file in recently_processed - changed_recently}

        # Do not convert the following list to set as set does not preserve the order
        # and we need to maintain the order of files for `[dag_processor] file_parsing_sort_mode`
        to_queue = [x for x in files if x.presence_key not in to_exclude]

        if self.log.isEnabledFor(logging.DEBUG):
            for path, processor in self._processors.items():
                now_monotonic = time.monotonic()
                self.log.debug(
                    "File path %s is still being processed (duration: %.2fs)",
                    path,
                    now_monotonic - processor.start_time,
                )

            self.log.debug(
                "Queuing the following files for processing:\n\t%s",
                "\n\t".join(str(f.rel_path) for f in to_queue),
            )
        self._add_files_to_queue(to_queue, mode="back")
        stats.incr("dag_processing.file_path_queue_update_count")

    def _kill_timed_out_processors(self):
        """Kill any file processors that timeout to defend against process hangs."""
        now = time.monotonic()
        processors_to_remove = []
        for file, processor in self._processors.items():
            duration = now - processor.start_time
            if duration > self.processor_timeout:
                self.log.error(
                    "Processor for %s with PID %s has been running for %.2f seconds, exceeding the timeout of %.2f seconds. Killing it!",
                    file,
                    processor.pid,
                    duration,
                    self.processor_timeout,
                )
                file_name = str(file.rel_path)
                stats.decr("dag_processing.processes", tags={"file_path": file_name, "action": "timeout"})
                stats.incr("dag_processing.processor_timeouts", tags={"file_path": file_name})
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

    def _add_files_to_queue(
        self,
        files: list[DagFileInfo],
        *,
        mode: Literal["front", "back", "frontprio"],
    ):
        """Add stuff to the back or front of the file queue, unless it's already present."""
        if mode == "frontprio":
            for file in files:
                self._file_queue.pop(file, None)
                self._file_queue[file] = None
                self._file_queue.move_to_end(file, last=False)
        elif mode == "front":
            for file in files:
                if file not in self._file_queue:
                    self._file_queue[file] = None
                    self._file_queue.move_to_end(file, last=False)
        elif mode == "back":
            for file in files:
                if file not in self._file_queue:
                    self._file_queue[file] = None
        else:
            assert_never(mode)

        # If we've just added files to the queue for the first time since metrics were last emitted, reset the
        # parse time counter.
        if self._parsing_start_time is None and self._file_queue:
            self._parsing_start_time = time.perf_counter()

        stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

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
            stats.decr(
                "dag_processing.processes", tags={"file_path": str(file.rel_path), "action": "terminate"}
            )
            # SIGTERM, wait 5s, SIGKILL if still alive
            processor.kill(signal.SIGTERM, escalation_delay=5.0)

    def end(self):
        """Kill all child processes on exit since we don't want to leave them as orphaned."""
        pids_to_kill = [p.pid for p in self._processors.values()]
        if pids_to_kill:
            kill_child_processes_by_pids(pids_to_kill)


def emit_metrics(*, parse_time: float, dag_file_stats: Sequence[DagFileStat]):
    """
    Emit metrics about dag parsing summary.

    This is called once every time around the parsing "loop" - i.e. after
    all files have been parsed.
    """
    stats.gauge("dag_processing.total_parse_time", parse_time)
    stats.gauge("dagbag_size", sum(stat.num_dags for stat in dag_file_stats))
    stats.gauge("dag_processing.import_errors", sum(stat.import_errors for stat in dag_file_stats))


def process_parse_results(
    run_duration: float,
    finish_time: datetime,
    run_count: int,
    bundle_name: str,
    parsing_result: DagFileParsingResult | None,
    *,
    is_callback_only: bool = False,
    relative_fileloc: str | None = None,
) -> DagFileStat:
    """
    Create a DagFileStat from parsing results and emit metrics.

    This function handles stat creation and metrics only — database persistence
    is handled separately by ``DagFileProcessorManager.persist_parsing_result``.
    """
    if is_callback_only:
        # Callback-only processing - don't update timestamps to avoid stale DAG detection issues
        stat = DagFileStat(
            last_duration=run_duration,
            run_count=run_count,  # Don't increment for callback-only processing
        )
        stats.incr("dag_processing.callback_only_count")
    else:
        # Actual DAG parsing or import error
        stat = DagFileStat(
            last_finish_time=finish_time,
            last_duration=run_duration,
            run_count=run_count + 1,
        )

    # Note: relative_fileloc has a None default. In practice it is always provided but code defensively here in case
    if relative_fileloc is not None and stat.last_duration is not None:
        # Normalize names to ensure they only contain valid characters for stats (alphanumeric, underscore, dot, dash)
        file_name = normalize_name_for_stats(Path(relative_fileloc).stem)
        # bundle_name is included to distinguish files with the same name across different bundles
        normalized_bundle = normalize_name_for_stats(bundle_name)
        stats.timing(
            "dag_processing.last_duration",
            stat.last_duration,
            tags={"bundle_name": normalized_bundle, "file_name": file_name},
        )

    if parsing_result is None:
        # No DAGs were parsed - this happens for callback-only processing
        # Don't treat this as an import error when it's callback-only
        if not is_callback_only:
            stat.import_errors = 1
    else:
        stat.num_dags = len(parsing_result.serialized_dags)
        if parsing_result.import_errors:
            stat.import_errors = len(parsing_result.import_errors)
    return stat
