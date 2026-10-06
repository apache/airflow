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
from collections import OrderedDict, defaultdict
from contextlib import AbstractContextManager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast
from uuid import UUID

import attrs
import httpx
import structlog
from pydantic import TypeAdapter
from sqlalchemy import or_, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from tabulate import tabulate
from uuid6 import uuid7

from airflow._shared.observability.metrics import stats
from airflow._shared.observability.metrics.stats import normalize_name_for_stats
from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    DagBundleInventoryBody,
    DagParseResultBody,
    ParseSourceCode,
    ParseWarning,
    ProcessorWorkItem,
)
from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.bundles.base import (
    BundleUsageTrackingManager,
    unpack_bundle_version,
)
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.importer_routing import get_claiming_importer
from airflow.dag_processing.lang_sdk_processor import LangSDKDagFileProcessorProcess
from airflow.dag_processing.processor import (
    BaseDagFileProcessorProcess,
    DagFileParsingResult,
    DagFileProcessorProcess,
    DagParseSource,
)
from airflow.jobs.job import Job
from airflow.models.asset import remove_references_to_deleted_dags
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ParseImportError
from airflow.observability.metrics import stats_utils
from airflow.sdk import SecretCache
from airflow.sdk.importers import DagImportError, get_importer_registry
from airflow.sdk.log import init_log_file, logging_processors
from airflow.typing_compat import assert_never
from airflow.utils.file import find_enclosing_file
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.process_utils import (
    kill_child_processes_by_pids,
)
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import (
    is_lock_not_available_error,
    prohibit_commit,
    with_db_lock_timeout,
    with_row_locks,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Sequence
    from socket import socket

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
    from airflow.dag_processing.api_client import DagProcessorAPIClient
    from airflow.dag_processing.bundles.base import BaseDagBundle
    from airflow.sdk.api.client import Client


def _make_execution_api() -> InProcessExecutionAPI:
    # This is a seriously weighty import, pulling in svcs, cadwyn, fastapi, aiohttp, etc.
    #
    # Defer it so that an import of this module for types (e.g. DagFileStat, DagFileInto) doesn't need to pay
    # that cost.
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    return InProcessExecutionAPI()


class BundleState(NamedTuple):
    """Persisted refresh state for a DAG bundle."""

    last_refreshed: datetime | None
    version: str | None
    revision: UUID | None = None


@attrs.define
class DagFileStat:
    """Information about single processing of one file."""

    num_dags: int = 0
    import_errors: int = 0
    last_finish_time: datetime | None = None
    last_duration: float | None = None
    run_count: int = 0
    last_num_of_db_queries: int = 0
    last_attempt_time: datetime | None = None


@attrs.define
class PendingDagPublication:
    """A completed import retained until publication succeeds or exhausts its retries."""

    body: DagParseResultBody
    stat: DagFileStat
    refresh_generation: int
    attempts: int = 0
    next_attempt_time: float = 0.0


@attrs.define
class DeferredCallback:
    """A claimed callback whose bundle could not be prepared, retried until it expires."""

    expires_at: float
    attempts: int = 0
    next_attempt_time: float = 0.0


_MAX_CALLBACK_RETRY_DELAY = 60.0


@dataclass(frozen=True)
class DagFileInfo:
    """Information about a DAG file."""

    rel_path: Path
    bundle_name: str
    bundle_path: Path | None = field(compare=False, default=None)
    bundle_version: str | None = None
    definition_locs: frozenset[str] = field(compare=False, default=frozenset())

    @property
    def absolute_path(self) -> Path:
        if not self.bundle_path:
            raise ValueError("bundle_path not set")
        return self.bundle_path / self.rel_path

    @property
    def presence_key(self) -> tuple[str, Path]:
        """Return the stable file identity used for presence checks."""
        return self.bundle_name, self.rel_path

    @property
    def normalized_file_path_for_stats(self) -> str:
        """Return the relative file path normalized for use in stats tags."""
        return normalize_name_for_stats(str(self.rel_path), log_warning=False)


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
    _bundle_parse_sources: dict[str, DagParseSource] = attrs.field(factory=dict, init=False)
    _bundle_refresh_generations: dict[str, int] = attrs.field(factory=lambda: defaultdict(int), init=False)
    _bundles_waiting_for_refresh: set[str] = attrs.field(factory=set, init=False)
    _dispatch_sequence: int = attrs.field(default=0, init=False)
    _multi_team: bool = attrs.field(factory=lambda: conf.getboolean("core", "multi_team"), init=False)
    _bundle_name_to_team_name: dict[str, str | None] = attrs.field(factory=dict, init=False)

    _processors: dict[DagFileInfo, BaseDagFileProcessorProcess] = attrs.field(factory=dict, init=False)
    _pending_publications: dict[DagFileInfo, PendingDagPublication] = attrs.field(factory=dict, init=False)
    _pending_inventories: dict[str, tuple[DagBundleInventoryBody, set[DagFileInfo], DagParseSource]] = (
        attrs.field(factory=dict, init=False)
    )

    _parsing_start_time: float | None = attrs.field(default=None, init=False)
    _callback_claims: dict[int, ProcessorWorkItem] = attrs.field(factory=dict, init=False)
    _deferred_api_callbacks: list[CallbackRequest] = attrs.field(factory=list, init=False)
    _deferred_callback_retries: dict[int, DeferredCallback] = attrs.field(factory=dict, init=False)
    _callback_attempts: dict[UUID, list[ProcessorWorkItem]] = attrs.field(factory=dict, init=False)
    _priority_claims: dict[str, ProcessorWorkItem] = attrs.field(factory=dict, init=False)
    _pending_work_acks: list[tuple[Literal["callbacks", "priority"], ProcessorWorkItem, bool]] = attrs.field(
        factory=list, init=False
    )
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

    dag_discovery_safe_mode: bool = attrs.field(
        factory=_config_bool_factory("core", "dag_discovery_safe_mode")
    )
    """Resolved once per process so file discovery and the deactivation scan use the same value."""

    api_client: DagProcessorAPIClient | None = None
    """
    Registered HTTP client for this manager's API requests, used instead of the in-process API.

    Bundle credentials and parse-time requests then go through it, on behalf of the bundle they are for.
    """

    _api_server: InProcessExecutionAPI | None = attrs.field(init=False, default=None)
    """In-process API server, created only when there is no :attr:`api_client`."""

    def register_exit_signals(self):
        """Register signals that stop child processes."""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)
        # So that we ignore the debug dump signal, making it easier to send
        signal.signal(signal.SIGUSR2, signal.SIG_IGN)

    def _get_team_names(self, bundle_names: Collection[str]) -> dict[str, str | None]:
        if not self._multi_team or not bundle_names:
            return {}
        missing = [name for name in bundle_names if name not in self._bundle_name_to_team_name]
        if missing:
            try:
                if self.api_client is not None:
                    queried = {bundle.name: bundle.team_name for bundle in self.api_client.get_bundles()}
                else:
                    queried = DagBundleModel.get_team_names(missing)
            except httpx.HTTPError:
                # Team names only tag metrics, so parsing continues and the lookup is retried later.
                self.log.warning("Unable to look up the teams of bundles %s; retrying later", missing)
            else:
                for name in missing:
                    self._bundle_name_to_team_name[name] = queried.get(name)
        return {name: self._bundle_name_to_team_name.get(name) for name in bundle_names}

    def _get_team_name(self, bundle_name: str) -> str | None:
        return self._get_team_names({bundle_name}).get(bundle_name)

    def _exit_gracefully(self, signum, frame):
        """Clean up DAG file processors to avoid leaving orphan processes."""
        self.log.info("Exiting gracefully upon receiving signal %s", signum)
        self.log.debug("Current Stacktrace is: %s", "\n".join(map(str, inspect.stack())))
        self.terminate()
        self.end()
        self.log.debug("Finished terminating DAG processors.")
        sys.exit(os.EX_OK)

    def _create_bundle_manager(self) -> DagBundlesManager:
        if self.api_client is None:
            return DagBundlesManager()
        return DagBundlesManager(
            bundle_names=self.bundle_names_to_parse,
            bundle_context=self.api_client.use_bundle,
        )

    def sync_bundles(self, *, include_bundle_urls: bool = True) -> None:
        """Sync configured DAG bundles to the metadata database."""
        if self.api_client is not None:
            provisioned = {bundle.name for bundle in self.api_client.get_bundles()}
            configured = set(
                self.bundle_names_to_parse or self._create_bundle_manager().get_all_bundle_names()
            )
            if missing := configured - provisioned:
                raise ValueError(
                    f"Bundles must be provisioned before starting the processor: {sorted(missing)}"
                )
            return
        # When this processor only parses a subset of bundles, it does not see the full
        # bundle configuration and must not deactivate bundles owned by other processors.
        dag_bundle_manager = self._create_bundle_manager()
        dag_bundle_manager.sync_bundles_to_db(
            deactivate_missing=not self.bundle_names_to_parse, include_bundle_urls=include_bundle_urls
        )
        if not include_bundle_urls:
            return
        # Best-effort legacy repair: a failure here must not crash DFP startup.
        # Affected Dags self-heal on the next successful parse.
        try:
            dag_bundle_manager.reassign_dags_with_unconfigured_bundles()
        except Exception:
            self.log.exception("Failed to reassign Dags with unconfigured bundles during startup")

    def get_all_bundles(self) -> list[BaseDagBundle]:
        """Return configured DAG bundles filtered by ``bundle_names_to_parse`` if provided."""
        return list(self._create_bundle_manager().get_all_dag_bundles())

    def run(self):
        """
        Use multiple processes to parse and generate tasks for the DAGs in parallel.

        By processing them in separate processes, we can get parallelism and isolation
        from potentially harmful user code.
        """
        try:
            self.before_run()
            return self._run_parsing_loop()
        finally:
            self.after_run()

    def before_run(self) -> None:
        """Set up state required before the parsing loop starts. Default implementation; override to customize."""
        if self.api_client is None:
            self.prepare_server_process_context()
        else:
            self.prepare_api_secrets_context(self.api_client)
        self.prepare_process_context()
        self.register_exit_signals()
        self.log.info("Processing files using up to %s processes at a time ", self._parallelism)
        self.log.info("Process each file at most once every %s seconds", self._file_process_interval)
        self.prepare_bundles()
        self._symlink_latest_log_directory()
        self.warm_importers()
        # To prevent COW in forked process parsing dag file
        gc.freeze()

    def after_run(self) -> None:
        """Tear down state after the parsing loop exits. Default implementation; override to customize."""
        if self.api_client is None:
            return
        from airflow.dag_processing.api_client import DagProcessorSecretsComms
        from airflow.sdk.execution_time import task_runner

        if isinstance(getattr(task_runner, "SUPERVISOR_COMMS", None), DagProcessorSecretsComms):
            del task_runner.SUPERVISOR_COMMS

    def warm_importers(self) -> None:
        """Build each bundle's Dag importers, so parse processes forked later share them."""
        for bundle in self._dag_bundles:
            try:
                with self._use_bundle(bundle.name):
                    get_importer_registry(bundle.name).warm_importers()
            except Exception:
                # The importer fails again when the bundle is listed, which reports it per refresh.
                self.log.exception("Error loading Dag importers for bundle %s", bundle.name)

    def prepare_server_process_context(self) -> None:
        """
        Mark this process as running in "server" context so MetastoreBackend is available.

        Override to a no-op in subclasses that do not require direct DB access (e.g. API-backed
        deployments under AIP-92).
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

    def prepare_api_secrets_context(self, api_client: DagProcessorAPIClient) -> None:
        """
        Resolve this process's own connection and variable lookups through ``api_client``.

        Bundle code runs in this process, for example a Git bundle reading its connection. Its secret
        lookups use the API. Parse processes replace
        ``SUPERVISOR_COMMS`` with their own channel before running any Dag code.
        """
        from airflow.dag_processing.api_client import DagProcessorSecretsComms
        from airflow.sdk.execution_time import task_runner

        # SDK cache keys do not carry the bundle identity used to authorize these requests.
        SecretCache.reset()
        os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"
        task_runner.SUPERVISOR_COMMS = DagProcessorSecretsComms(api_client)  # type: ignore[assignment]

    def _use_bundle(self, bundle_name: str) -> AbstractContextManager[object]:
        """Make API requests on behalf of the bundle; a no-op with the in-process API."""
        if self.api_client is None:
            return nullcontext()
        return self.api_client.use_bundle(bundle_name)

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
        if self.api_client is not None:
            return
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
        if self.api_client is not None:
            manager = BundleUsageTrackingManager()
            for bundle in self._dag_bundles:
                if bundle.supports_versioning:
                    manager._remove_stale_bundle_versions_for_bundle(bundle.name)
            return
        BundleUsageTrackingManager().remove_stale_bundle_versions()

    @provide_session
    def deactivate_stale_dags(
        self,
        last_parsed: dict[DagFileInfo, datetime | None],
        *,
        session: Session = NEW_SESSION,
    ):
        """Detect and deactivate DAGs which are no longer present in files."""
        to_deactivate = set()
        inactive_bundles = set(
            session.scalars(select(DagBundleModel.name).where(DagBundleModel.active.is_(False))).all()
        )
        query = select(
            DagModel.dag_id,
            DagModel.bundle_name,
            DagModel.fileloc,
            DagModel.last_parsed_time,
            DagModel.relative_fileloc,
        ).where(~DagModel.is_stale)
        dags_parsed = session.execute(query)

        stuck_legacy_rows = 0
        for dag in dags_parsed:
            if dag.bundle_name is None or dag.bundle_name in inactive_bundles:
                continue
            # A Dag upgraded from Airflow 2.x can still have a NULL relative_fileloc:
            # the 0082 migration adds the column as nullable, and the startup repair
            # in DagBundlesManager only backfills it when the Dag's fileloc resolves to
            # a configured bundle. Rows whose fileloc matches no bundle stay NULL, so
            # the time-based stale check below would build Path(None) and crash. Skip
            # them here and count them so the total is surfaced after the loop.
            # See https://github.com/apache/airflow/issues/63323.
            if dag.relative_fileloc is None:
                stuck_legacy_rows += 1
                continue
            # When the Dag's last_parsed_time is more than the stale_dag_threshold older than the
            # Dag file's last_finish_time, the Dag is considered stale as has apparently been removed from the file,
            # This is especially relevant for Dag files that generate Dags in a dynamic manner.
            rel_path = Path(dag.relative_fileloc)
            # A Dag nested in a container (``archive.zip/sub/dag.py``) is parsed under the container.
            last_finish_time = next(
                (
                    last_parsed[file_info]
                    for candidate in (rel_path, *rel_path.parents)
                    if (file_info := DagFileInfo(rel_path=candidate, bundle_name=dag.bundle_name))
                    in last_parsed
                ),
                None,
            )
            if last_finish_time:
                if dag.last_parsed_time + timedelta(seconds=self.stale_dag_threshold) < last_finish_time:
                    self.log.info(
                        "Deactivating stale DAG %s. Not parsed for %s seconds (last parsed: %s).",
                        dag.dag_id,
                        int((last_finish_time - dag.last_parsed_time).total_seconds()),
                        dag.last_parsed_time,
                    )
                    to_deactivate.add(dag.dag_id)

        if to_deactivate:
            try:
                with with_db_lock_timeout(session=session, lock_timeout=30):
                    deactivated_dagmodel = session.execute(
                        update(DagModel)
                        .where(DagModel.dag_id.in_(to_deactivate))
                        .values(is_stale=True)
                        .execution_options(synchronize_session="fetch")
                    )
                    deactivated = getattr(deactivated_dagmodel, "rowcount", 0)
                    if deactivated:
                        self.log.info("Deactivated %i DAGs which are no longer present in file.", deactivated)
            except OperationalError as e:
                if is_lock_not_available_error(e):
                    self.log.warning(
                        "Lock not available when deactivating stale DAGs. "
                        "Skipping this iteration to prevent processor hang."
                    )
                    session.rollback()
                else:
                    raise

        if stuck_legacy_rows:
            # Surface how many legacy rows the startup repair could not route;
            # each one keeps raising "Requested bundle is not configured." until
            # a matching bundle is added to dag_bundle_config_list.
            self.log.info(
                "Skipped stale check for %d legacy Dag(s) with NULL relative_fileloc.",
                stuck_legacy_rows,
            )

    def _run_parsing_loop(self):
        # initialize cache to mutualize calls to Variable.get in DAGs
        # needs to be done before this process is forked to create the DAG parsing processes.
        if self.api_client is None:
            SecretCache.init()

        poll_time = 0.0

        known_files: dict[str, set[DagFileInfo]] = {}

        while True:
            loop_start_time = time.monotonic()

            self.heartbeat()

            self._kill_timed_out_processors()

            self._queue_requested_files_for_parsing()

            self._service_processor_sockets(timeout=0)
            self._collect_results()
            self._refresh_dag_bundles(known_files=known_files)

            if not self._file_queue:
                # Generate more file paths to process if we processed all the files already. Note for this to
                # clear down, we must have cleared all files found from scanning the dags dir _and_ have
                # cleared all files added as a result of callbacks
                self.prepare_file_queue(known_files=known_files)

            self._start_new_processes()

            self._service_processor_sockets(timeout=poll_time)

            self._collect_results()

            self._publish_pending_results()
            self._acknowledge_requested_work()
            for callback in self.fetch_callbacks():
                self._add_callback_to_queue(callback)
            self._scan_stale_dags()
            self._cleanup_stale_bundle_versions()

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
        """
        Fetch and claim files requested for priority parsing.

        Default implementation reads from the metadata DB; override to source requests from an API.
        """
        if self.api_client is None:
            return self._claim_priority_files()
        available = (
            self._parallelism
            - len(self._priority_claims)
            - sum(kind == "priority" for kind, _, _ in self._pending_work_acks)
        )
        if available <= 0:
            return []
        bundles = {bundle.name: bundle for bundle in self._dag_bundles}
        try:
            work = self.api_client.claim_work("priority", list(bundles), available)
        except httpx.HTTPError:
            self.log.warning("Unable to claim priority parses; retrying on a later loop")
            return []
        files = []
        for item in work:
            self._priority_claims[item.id] = item
            bundle = bundles[item.bundle_name]
            path = find_enclosing_file(bundle.path / item.relative_fileloc)
            files.append(
                DagFileInfo(
                    rel_path=path.relative_to(bundle.path) if path else Path(item.relative_fileloc),
                    bundle_name=bundle.name,
                    bundle_path=bundle.path,
                )
            )
        return files

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

    @provide_session
    def _claim_priority_files(self, *, session: Session = NEW_SESSION) -> list[DagFileInfo]:
        """Fetch priority parsing requests from the metadata database."""
        files: list[DagFileInfo] = []
        bundles = {b.name: b for b in self._dag_bundles}
        requests = session.scalars(
            select(DagPriorityParsingRequest).where(
                DagPriorityParsingRequest.bundle_name.in_(bundles.keys()),
                or_(
                    DagPriorityParsingRequest.processor_job_id.is_(None),
                    DagPriorityParsingRequest.processor_job_id.in_(
                        select(Job.id).where(Job.end_date.is_not(None))
                    ),
                ),
            )
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

    def fetch_callbacks(self) -> list[CallbackRequest]:
        """
        Fetch and claim callbacks for this manager's bundles.

        Default implementation reads from the metadata DB; override to source callbacks from an API.
        """
        if self.api_client is None:
            return self._fetch_callbacks_from_db()
        available = (
            self.max_callbacks_per_loop
            - len(self._callback_claims)
            - sum(len(work) for work in self._callback_attempts.values())
            - sum(kind == "callbacks" for kind, _, _ in self._pending_work_acks)
        )
        if available <= 0:
            return list(self._deferred_api_callbacks)
        bundles = [b.name for b in self._dag_bundles if not b.supports_versioning or b.is_initialized]
        try:
            work = self.api_client.claim_work("callbacks", bundles, available)
        except httpx.HTTPError:
            self.log.warning("Unable to claim callbacks; retrying on a later loop")
            return list(self._deferred_api_callbacks)
        for item in work:
            if item.callback is None:
                raise ValueError("Callback claim returned no request")
            request: CallbackRequest = TypeAdapter(CallbackRequest).validate_json(item.callback)
            self._callback_claims[id(request)] = item
            self._deferred_api_callbacks.append(request)
        return list(self._deferred_api_callbacks)

    def _acknowledge_requested_work(self) -> None:
        if self.api_client is None or not self._pending_work_acks:
            return
        kind, item, failed = self._pending_work_acks[0]
        try:
            self.api_client.acknowledge_work(kind, item, failed=failed)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 409:
                self.log.warning("Unable to acknowledge requested work; retaining the claim")
                return
        except httpx.RequestError:
            self.log.warning("Unable to acknowledge requested work; retaining the claim")
            return
        self._pending_work_acks.pop(0)

    def _finish_callback_attempt(self, proc: BaseDagFileProcessorProcess) -> None:
        for item in self._callback_attempts.pop(proc.id, []):
            self._pending_work_acks.append(("callbacks", item, proc._exit_code != 0))

    def _finish_priority_claims(self, file: DagFileInfo, *, failed: bool) -> None:
        """Acknowledge the priority requests a parse of *file* served, as the database path consumes them."""
        for key, item in list(self._priority_claims.items()):
            if item.bundle_name == file.bundle_name and (
                item.relative_fileloc == str(file.rel_path)
                or item.relative_fileloc.startswith(str(file.rel_path) + "/")
            ):
                self._pending_work_acks.append(("priority", item, failed))
                del self._priority_claims[key]

    @provide_session
    @retry_db_transaction
    def _fetch_callbacks_from_db(
        self,
        *,
        session: Session = NEW_SESSION,
    ) -> list[CallbackRequest]:
        """Claim callbacks for ready bundles, leaving the rest pending."""
        self.log.debug("Fetching callbacks from the database.")

        callback_queue: list[CallbackRequest] = []
        with prohibit_commit(session) as guard:
            # Claiming deletes rows, so defer unavailable bundles before applying the limit.
            bundle_names = [
                bundle.name
                for bundle in self._dag_bundles
                if not bundle.supports_versioning or bundle.is_initialized
            ]
            if unready_bundles := [
                bundle.name
                for bundle in self._dag_bundles
                if bundle.supports_versioning and not bundle.is_initialized
            ]:
                self.log.debug("Skipping callback fetch for uninitialized bundles: %s", unready_bundles)
            query: Select[tuple[DbCallbackRequest]] = with_row_locks(
                select(DbCallbackRequest)
                .where(
                    DbCallbackRequest.bundle_name.in_(bundle_names),
                    or_(
                        DbCallbackRequest.processor_job_id.is_(None),
                        DbCallbackRequest.processor_job_id.in_(
                            select(Job.id).where(Job.end_date.is_not(None))
                        ),
                    ),
                )
                .order_by(DbCallbackRequest.priority_weight.desc())
                .limit(self.max_callbacks_per_loop),
                of=DbCallbackRequest,
                session=session,
                skip_locked=True,
            )
            callbacks: Sequence[DbCallbackRequest] = [
                cb[0] if isinstance(cb, tuple) else cb for cb in session.scalars(query)
            ]
            for callback in callbacks:
                req = callback.get_callback_request()
                try:
                    callback_queue.append(req)
                    session.delete(callback)
                except Exception as e:
                    self.log.warning("Error adding callback for execution: %s, %s", callback, e)
            guard.commit()
        return callback_queue

    def prepare_callback_bundle(self, request: CallbackRequest) -> BaseDagBundle | None:
        """
        Return a usable bundle or ``None`` to skip; override for API-backed bundles.

        Reuse loaded bundles for unversioned requests; versioning bundles must be initialized.
        """
        if request.bundle_version is None:
            # Reuse the scan path without fetching or checking out per callback.
            loaded = next((b for b in self._dag_bundles if b.name == request.bundle_name), None)
            if loaded is None:
                self.log.error(
                    "Bundle %s is not parsed by this processor, skipping callback", request.bundle_name
                )
                return None
            if loaded.supports_versioning and not loaded.is_initialized:
                self.log.error("Bundle %s is not initialized, skipping callback", request.bundle_name)
                return None
            return loaded
        with self._use_bundle(request.bundle_name):
            try:
                bundle = self._create_bundle_manager().get_bundle(
                    name=request.bundle_name,
                    version=request.bundle_version,
                    version_data=request.version_data,
                )
            except ValueError:
                self.log.error("Bundle %s no longer configured, skipping callback", request.bundle_name)
                return None
            if bundle.supports_versioning:
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
        if get_claiming_importer(request.filepath, request.bundle_name) is not None:
            self._log_dropped_lang_sdk_callback(request)
            if item := self._callback_claims.pop(id(request), None):
                self._pending_work_acks.append(("callbacks", item, True))
                self._deferred_api_callbacks.remove(request)
            return
        retry = self._deferred_callback_retries.get(id(request))
        if retry is not None and time.monotonic() < retry.next_attempt_time:
            return
        self.heartbeat()
        bundle = self.prepare_callback_bundle(request)
        if bundle is None:
            if id(request) in self._callback_claims:
                self._defer_claimed_callback(request)
            return
        self._deferred_callback_retries.pop(id(request), None)

        file_info = DagFileInfo(
            rel_path=Path(request.filepath),
            bundle_path=bundle.path,
            bundle_name=request.bundle_name,
            bundle_version=request.bundle_version,
        )
        self._callback_to_execute[file_info].append(request)
        if id(request) in self._callback_claims:
            self._deferred_api_callbacks.remove(request)
        self._add_files_to_queue([file_info], mode="front")
        team_name = self._get_team_name(file_info.bundle_name)
        stats.incr("dag_processing.other_callback_count", tags=prune_dict({"team_name": team_name}))

    def _defer_claimed_callback(self, request: CallbackRequest) -> None:
        """Retry preparing a claimed callback's bundle with backoff, then acknowledge it as failed."""
        now = time.monotonic()
        timeout = conf.getint("dag_processor", "job_heartbeat_timeout")
        retry = self._deferred_callback_retries.setdefault(id(request), DeferredCallback(now + timeout))
        if now >= retry.expires_at:
            self.log.error(
                "Dropping %s for %s in bundle %s: its bundle version %s could not be prepared within %s seconds",
                type(request).__name__,
                request.filepath,
                request.bundle_name,
                request.bundle_version,
                timeout,
            )
            self._pending_work_acks.append(("callbacks", self._callback_claims.pop(id(request)), True))
            self._deferred_api_callbacks.remove(request)
            del self._deferred_callback_retries[id(request)]
            return
        retry.attempts += 1
        retry.next_attempt_time = min(
            now + min(2 ** (retry.attempts - 1), _MAX_CALLBACK_RETRY_DELAY), retry.expires_at
        )

    def _log_dropped_lang_sdk_callback(self, request: CallbackRequest) -> None:
        if isinstance(request, DagCallbackRequest):
            target = f"dag_id={request.dag_id} run_id={request.run_id}"
        else:
            ti = request.ti
            target = f"dag_id={ti.dag_id} run_id={ti.run_id} task_id={ti.task_id}"
        self.log.warning(
            "Dropping %s for %s (%s): Lang-SDK runtimes do not run callbacks",
            type(request).__name__,
            request.filepath,
            target,
        )

    @provide_session
    def get_bundle_state(self, bundle_name: str, *, session: Session = NEW_SESSION) -> BundleState | None:
        """
        Return the persisted refresh state for a bundle.

        Returns ``None`` if the bundle has no database record.
        """
        if self.api_client is not None:
            for bundle in self.api_client.get_bundles():
                if bundle.name == bundle_name:
                    return BundleState(bundle.last_refreshed, bundle.version, bundle.revision)
            return None
        row = session.execute(
            select(DagBundleModel.last_refreshed, DagBundleModel.version).where(
                DagBundleModel.name == bundle_name
            )
        ).one_or_none()
        if row is None:
            return None
        return BundleState(last_refreshed=row.last_refreshed, version=row.version)

    @provide_session
    def update_bundle_state(
        self,
        bundle_name: str,
        *,
        last_refreshed: datetime,
        version: str | None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Persist the post-refresh state for a bundle.

        Always updates ``last_refreshed``. Updates ``version`` only when ``version`` is not
        ``None`` — pass ``None`` to leave the stored version unchanged (e.g. for non-versioned
        bundles or when the version did not change after a refresh).
        """
        values: dict[str, Any] = {"last_refreshed": last_refreshed}
        if version is not None:
            values["version"] = version
        session.execute(update(DagBundleModel).where(DagBundleModel.name == bundle_name).values(**values))

    def purge_inactive_dag_warnings(self) -> None:
        """
        Purge warnings for inactive/stale DAGs.

        Default implementation deletes records from the metadata DB; override to
        source warnings from an API or skip the cleanup entirely.
        """
        if self.api_client is None:
            DagWarning.purge_inactive_dag_warnings()

    def _publish_bundle_inventory(self, bundle_name: str, known_files: dict[str, set[DagFileInfo]]) -> None:
        if self.api_client is None:
            raise ValueError("Inventory publication requires an API client")
        body, found_files, source = self._pending_inventories[bundle_name]
        try:
            receipt = self.api_client.publish_inventory(bundle_name, body)
        except httpx.HTTPStatusError as error:
            if error.response.status_code == 409:
                del self._pending_inventories[bundle_name]
                self.request_bundle_refresh(bundle_name)
            elif error.response.status_code < 500:
                raise
            self.log.warning("Inventory publication was rejected for %s", bundle_name)
            return
        except httpx.RequestError:
            self.log.warning("Unable to publish inventory for %s; retaining it for retry", bundle_name)
            return
        del self._pending_inventories[bundle_name]
        self._bundle_parse_sources[bundle_name] = attrs.evolve(source, bundle_revision=receipt.revision)
        self._bundle_versions[bundle_name] = source.bundle_version
        known_files[bundle_name] = found_files
        for key, item in list(self._priority_claims.items()):
            if item.bundle_name == bundle_name and item.relative_fileloc not in body.files:
                self._pending_work_acks.append(("priority", item, True))
                del self._priority_claims[key]

    def _invalidate_bundle_parse_source(self, bundle_name: str) -> None:
        # Initialization or refresh may change files even when the provider raises afterwards.
        self._bundle_refresh_generations[bundle_name] += 1
        self._bundle_parse_sources.pop(bundle_name, None)

    def _refresh_dag_bundles(self, known_files: dict[str, set[DagFileInfo]]):
        """Refresh DAG bundles, if required."""
        now = timezone.utcnow()

        # we don't need to check if it's time to refresh every loop - that is way too often
        next_check = self._bundles_last_refreshed + self.bundle_refresh_check_interval
        now_seconds = time.monotonic()
        if (
            now_seconds < next_check
            and not self._force_refresh_bundles
            and not self._bundles_waiting_for_refresh
            and not self._pending_inventories
        ):
            self.log.debug(
                "Not time to check if DAG Bundles need refreshed yet - skipping. Next check in %.2f seconds",
                next_check - now_seconds,
            )
            return

        self._bundles_last_refreshed = now_seconds

        any_refreshed = False
        active_bundles = {
            file.bundle_name
            for file in self._processors.keys() | self._pending_publications.keys()
            if file.bundle_version is None
        }
        for bundle in self._dag_bundles:
            # Bundle operations can block for minutes; an API-mode Job ends after job_heartbeat_timeout.
            self.heartbeat()
            with self._use_bundle(bundle.name):
                if bundle.name in self._pending_inventories:
                    self._publish_bundle_inventory(bundle.name, known_files)
                    any_refreshed = True
                    continue
                # TODO: AIP-66 handle errors in the case of incomplete cloning? And test this.
                #  What if the cloning/refreshing took too long(longer than the dag processor timeout)
                if not bundle.is_initialized:
                    self._invalidate_bundle_parse_source(bundle.name)
                    try:
                        bundle.initialize()
                        any_refreshed = True
                    except Exception as e:
                        self.log.exception("Error initializing bundle %s: %s", bundle.name, e)
                        continue
                try:
                    bundle_state = self.get_bundle_state(bundle.name)
                except httpx.HTTPError:
                    self.log.warning("Unable to read metadata for bundle %s; retrying later", bundle.name)
                    continue
                except Exception:
                    self.log.exception("Error fetching state for bundle %s", bundle.name)
                    continue
                if bundle_state is None:
                    self.log.warning("Bundle model not found for %s", bundle.name)
                    continue
                elapsed_time_since_refresh = (
                    now - (bundle_state.last_refreshed or utc_epoch())
                ).total_seconds()
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
                if (
                    bundle.name not in self._bundles_waiting_for_refresh
                    and bundle.name in self._bundle_parse_sources
                    and self.should_skip_refresh(
                        bundle=bundle,
                        elapsed_time_since_refresh=elapsed_time_since_refresh,
                        current_version_matches_db=current_version_matches_db,
                        previously_seen=previously_seen,
                    )
                ):
                    self.log.debug("Not time to refresh bundle %s", bundle.name)
                    continue

                if bundle.name in active_bundles:
                    # Forced refreshes wait too: refreshing under an import would discard its result.
                    self._bundles_waiting_for_refresh.add(bundle.name)
                    continue

                self._bundles_waiting_for_refresh.discard(bundle.name)
                self.log.info("Refreshing bundle %s", bundle.name)

                self._invalidate_bundle_parse_source(bundle.name)
                try:
                    bundle.refresh()
                    any_refreshed = True
                except Exception:
                    self.log.exception("Error refreshing bundle %s", bundle.name)
                    continue

                self._force_refresh_bundles.discard(bundle.name)

                if bundle.supports_versioning:
                    version_after_refresh, version_data_after_refresh = unpack_bundle_version(
                        bundle.get_current_version(), bundle
                    )
                else:
                    version_after_refresh = None
                    version_data_after_refresh = None

                self._bundle_parse_sources[bundle.name] = DagParseSource(
                    bundle_version=version_after_refresh,
                    version_data=deepcopy(version_data_after_refresh),
                    refresh_generation=self._bundle_refresh_generations[bundle.name],
                )

                if bundle.supports_versioning:
                    # We can short-circuit the rest of this if (1) bundle was seen before by
                    # this dag processor and (2) the version of the bundle did not change
                    # after refreshing it
                    if (
                        self.api_client is None
                        and previously_seen
                        and pre_refresh_version == version_after_refresh
                    ):
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

                    self.log.info(
                        "Version changed for %s, new version: %s", bundle.name, version_after_refresh
                    )
                try:
                    found_files = self._find_files_in_bundle(bundle)
                except Exception:
                    # Keep the bundle's known files and Dags, and leave its version unadvanced so the
                    # next refresh lists it again.
                    self.log.exception("Error listing Dag definitions in bundle %s", bundle.name)
                    if self.api_client is not None:
                        self._bundle_parse_sources.pop(bundle.name, None)
                    continue

                if self.api_client is not None:
                    self._dispatch_sequence += 1
                    self._pending_inventories[bundle.name] = (
                        DagBundleInventoryBody(
                            attempt_id=uuid7(),
                            dispatch_sequence=self._dispatch_sequence,
                            expected_revision=bundle_state.revision,
                            version=version_after_refresh,
                            files=sorted(self._get_observed_filelocs(found_files)),
                        ),
                        found_files,
                        self._bundle_parse_sources.pop(bundle.name),
                    )
                    self._publish_bundle_inventory(bundle.name, known_files)
                    continue

                # Persistence failure must not skip file scanning (bundle is already refreshed locally).
                # _bundle_versions is only advanced on success to stay consistent with the DB.
                try:
                    self.update_bundle_state(bundle.name, last_refreshed=now, version=version_after_refresh)
                except Exception:
                    self.log.exception("Error persisting state for bundle %s", bundle.name)
                else:
                    self._bundle_versions[bundle.name] = version_after_refresh

                known_files[bundle.name] = found_files

                self.deactivate_deleted_dags(bundle_name=bundle.name, present=found_files)
                self.clear_orphaned_import_errors(
                    bundle_name=bundle.name,
                    observed_filelocs=self._get_observed_filelocs(found_files),
                )

        if any_refreshed:
            # Bundle-to-team assignments can only change on bundle refresh, so clear the cache.
            self._bundle_name_to_team_name = {}
            self.handle_removed_files(known_files=known_files)
            self._resort_file_queue()
            self._add_new_files_to_queue(known_files=known_files)

    def _find_files_in_bundle(self, bundle: BaseDagBundle) -> set[DagFileInfo]:
        """
        List the files to parse in a bundle through its importers.

        A file holding several Dag definitions (a zip archive, for instance) is parsed as one.
        """
        self.log.info("Searching for Dag definitions in %s at %s", bundle.name, bundle.path)
        registry = get_importer_registry(bundle.name)
        definition_locs: defaultdict[Path, set[str]] = defaultdict(set)
        for _, item in registry.list_dag_definitions(bundle, safe_mode=self.dag_discovery_safe_mode):
            if isinstance(item, DagImportError):
                # An unreadable source stays present, so its Dags are kept and parsing reports the error.
                # Importers report a source either absolutely or relative to the bundle.
                rel_fileloc = os.path.relpath(bundle.path / item.source_reference, bundle.path)
            else:
                rel_fileloc = item.get_relative_loc(bundle.path)
            loc = Path(os.path.normpath(bundle.path / rel_fileloc))
            if not loc.is_relative_to(bundle.path):
                self.log.warning(
                    "Ignoring %r listed in bundle %s: it resolves outside the bundle", item, bundle.name
                )
                continue
            if (path := find_enclosing_file(loc)) is None:
                self.log.warning(
                    "Ignoring %r listed in bundle %s: no file in the bundle holds it", item, bundle.name
                )
                continue
            definition_locs[path.relative_to(bundle.path)].add(rel_fileloc)
        self.log.info(
            "Found %s files for bundle %s (dag_discovery_safe_mode=%s)",
            len(definition_locs),
            bundle.name,
            self.dag_discovery_safe_mode,
        )
        return {
            DagFileInfo(
                rel_path=rel_path,
                bundle_name=bundle.name,
                bundle_path=bundle.path,
                definition_locs=frozenset(locs),
            )
            for rel_path, locs in definition_locs.items()
        }

    @staticmethod
    def _get_observed_filelocs(present: set[DagFileInfo]) -> set[str]:
        """Return the bundle-relative locations of the files and of the definitions found in them."""
        return {loc for file in present for loc in (str(file.rel_path), *file.definition_locs)}

    def deactivate_deleted_dags(self, bundle_name: str, present: set[DagFileInfo]) -> None:
        """Deactivate DAGs that come from files that are no longer present in bundle."""
        observed_filelocs = self._get_observed_filelocs(present)
        with create_session() as session:
            try:
                with with_db_lock_timeout(session=session, lock_timeout=30):
                    any_deactivated = DagModel.deactivate_deleted_dags(
                        bundle_name=bundle_name,
                        rel_filelocs=observed_filelocs,
                        session=session,
                    )
                    # Only run cleanup if we actually deactivated any DAGs
                    # This avoids unnecessary DELETE queries in the common case where no DAGs were deleted
                    if any_deactivated:
                        remove_references_to_deleted_dags(session=session)
                    session.flush()
            except OperationalError as e:
                if is_lock_not_available_error(e):
                    self.log.warning(
                        "Lock not available when deactivating deleted DAGs for bundle %s. "
                        "Skipping this iteration to prevent processor hang.",
                        bundle_name,
                    )
                    session.rollback()
                else:
                    raise

    def print_stats(self, known_files: dict[str, set[DagFileInfo]]):
        """Occasionally print out stats about how fast the files are getting processed."""
        if 0 < self.print_stats_interval < time.monotonic() - self.last_stat_print_time:
            if known_files:
                self._log_file_processing_stats(known_files=known_files)
            self.last_stat_print_time = time.monotonic()

    @provide_session
    def clear_orphaned_import_errors(
        self, bundle_name: str, observed_filelocs: set[str], *, session: Session = NEW_SESSION
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

        bundle_to_team = self._get_team_names({bundle_name for bundle_name in known_files})

        for files in known_files.values():
            for file in files:
                stat = self._file_stats[file]
                proc = self._processors.get(file)
                num_dags = stat.num_dags
                num_errors = stat.import_errors
                file_name = normalize_name_for_stats(Path(file.rel_path).stem)
                processor_pid = proc.pid if proc else None
                processor_start_time = proc.start_time if proc else None
                runtime = (now - processor_start_time) if processor_start_time else None
                last_run = stat.last_finish_time
                if last_run:
                    seconds_ago = (utcnow - last_run).total_seconds()
                    # file_path and bundle_name uniquely identify a file (the same file name can
                    # exist in different folders or bundles). file_name is kept as a tag to ease
                    # migration from the legacy interpolated metric, which is emitted automatically
                    # from the registry (controlled by the export_legacy_names config).
                    stats.gauge(
                        "dag_processing.last_run.seconds_ago",
                        seconds_ago,
                        tags=prune_dict(
                            {
                                "file_path": file.normalized_file_path_for_stats,
                                "bundle_name": normalize_name_for_stats(file.bundle_name),
                                "file_name": file_name,
                                "team_name": bundle_to_team.get(file.bundle_name),
                            }
                        ),
                    )

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
        self._file_queue = OrderedDict(
            (x, None)
            for x in self._file_queue
            if x.presence_key in present_keys
            or (self.api_client is not None and self._callback_to_execute.get(x))
        )
        stats.gauge("dag_processing.file_path_queue_size", len(self._file_queue))

    def remove_orphaned_file_stats(self, present: set[DagFileInfo]):
        """Remove the stats for any dag files that don't exist anymore."""
        present_keys = {file.presence_key for file in present}
        stats_to_remove = {file for file in self._file_stats if file.presence_key not in present_keys}
        for file in stats_to_remove:
            del self._file_stats[file]
        for file in list(self._pending_publications):
            if file.presence_key not in present_keys:
                del self._pending_publications[file]

    def terminate_orphan_processes(self, present: set[DagFileInfo]):
        """Stop processors that are working on deleted files."""
        present_keys = {file.presence_key for file in present}

        bundle_to_team = self._get_team_names({file.bundle_name for file in self._processors})

        for file in list(self._processors.keys()):
            if self.api_client is not None and self._processors[file].id in self._callback_attempts:
                continue
            if file.presence_key not in present_keys:
                processor = self._processors.pop(file, None)
                if not processor:
                    continue
                file_name = str(file.rel_path)
                self.log.warning("Stopping processor for %s", file_name)
                stats.decr(
                    "dag_processing.processes",
                    tags=prune_dict(
                        {
                            "file_path": file.normalized_file_path_for_stats,
                            "bundle_name": normalize_name_for_stats(file.bundle_name),
                            "action": "stop",
                            "team_name": bundle_to_team.get(file.bundle_name),
                        }
                    ),
                )
                processor.kill(signal.SIGKILL)
                processor.close()
                self._file_stats.pop(file, None)

    @provide_session
    def handle_parsing_result(
        self,
        file: DagFileInfo,
        proc: BaseDagFileProcessorProcess,
        *,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Post-process a single finished parse result.

        Detects callback-only processing, updates file stats, emits metrics,
        and persists DAGs/import-errors via :meth:`persist_parsing_result`.
        Extracted from ``_collect_results`` to keep result handling and
        persistence separate.

        Owns its own DB session via ``@provide_session`` so subclasses that
        forward results without touching the metadata DB (e.g. AIP-92 API-backed
        deployments) can override this method without inheriting a session
        created by the caller.

        If persistence fails, the error is logged and the previous persisted
        DAG/import-error counts are preserved while a minimal timestamp update
        throttles immediate retries, so other files in the same
        ``_collect_results`` cycle still run.
        """
        is_callback_only = proc.had_callbacks and proc.parsing_result is None
        if is_callback_only:
            self.log.debug("Detected callback-only processing for %s", file)

        if (
            proc.parsing_result is not None
            and proc.parse_source.refresh_generation
            != self._bundle_refresh_generations.get(file.bundle_name, 0)
        ):
            self.log.info("Discarding parse of %s after its bundle refreshed; requeuing", file.rel_path)
            stats.incr(
                "dag_processing.results_discarded_on_refresh",
                tags={"bundle_name": normalize_name_for_stats(file.bundle_name)},
            )
            self._add_files_to_queue([file], mode="front")
            return

        run_duration = time.monotonic() - proc.start_time
        finish_time = timezone.utcnow()
        team_name = self._get_team_name(file.bundle_name)
        next_stat = process_parse_results(
            run_duration=run_duration,
            finish_time=finish_time,
            run_count=self._file_stats[file].run_count,
            bundle_name=file.bundle_name,
            parsing_result=proc.parsing_result,
            is_callback_only=is_callback_only,
            relative_fileloc=str(file.rel_path),
            team_name=team_name,
        )

        if proc.parsing_result is not None:
            try:
                if self.api_client is not None:
                    self._pending_publications[file] = PendingDagPublication(
                        body=self._build_parse_result(file, proc, run_duration),
                        stat=next_stat,
                        refresh_generation=proc.parse_source.refresh_generation,
                    )
                    return
                self.persist_parsing_result(
                    bundle_name=file.bundle_name,
                    bundle_version=proc.parse_source.bundle_version,
                    version_data=proc.parse_source.version_data,
                    parsing_result=proc.parsing_result,
                    run_duration=run_duration,
                    relative_fileloc=str(file.rel_path),
                    session=session,
                )
            except Exception:
                self.log.exception(
                    "Failed to persist parsing result for %s in bundle %s; "
                    "keeping previous persisted stats while throttling retries. "
                    "Other files in this cycle are still processed.",
                    str(file.rel_path),
                    file.bundle_name,
                )
                self._record_failed_publication(file, next_stat)
                self._finish_priority_claims(file, failed=True)
                return

        if proc.parsing_result is None and not is_callback_only:
            self._finish_priority_claims(file, failed=True)
        self._file_stats[file] = next_stat

    def _record_failed_publication(self, file: DagFileInfo, stat: DagFileStat) -> None:
        self._file_stats[file] = attrs.evolve(
            self._file_stats[file],
            last_attempt_time=timezone.utcnow(),
            last_duration=stat.last_duration,
            run_count=stat.run_count,
        )

    def _publish_pending_results(self) -> None:
        """Make at most one HTTP attempt per loop, leaving supervision running between retries."""
        if self.api_client is None:
            return
        for file, pending in list(self._pending_publications.items()):
            if pending.refresh_generation != self._bundle_refresh_generations.get(file.bundle_name, 0):
                del self._pending_publications[file]
                self._add_files_to_queue([file], mode="front")
                continue
            if time.monotonic() < pending.next_attempt_time:
                continue
            pending.attempts += 1
            try:
                self.api_client.publish_parse_result(pending.body)
            except Exception as error:
                from airflow.dag_processing.api_client import get_error_reason

                if isinstance(error, httpx.HTTPStatusError) and get_error_reason(error) == "source_changed":
                    # Another processor's inventory moved the revision; refreshing fetches it.
                    self.log.info(
                        "Bundle %s changed before %s was published; reparsing", file.bundle_name, file
                    )
                    del self._pending_publications[file]
                    self.request_bundle_refresh(file.bundle_name)
                    self._add_files_to_queue([file], mode="front")
                    return
                retryable = isinstance(error, httpx.RequestError) or (
                    isinstance(error, httpx.HTTPStatusError) and error.response.status_code >= 500
                )
                if retryable and pending.attempts < conf.getint("workers", "execution_api_retries"):
                    pending.next_attempt_time = time.monotonic() + min(
                        conf.getfloat("workers", "execution_api_retry_wait_max"),
                        max(
                            conf.getfloat("workers", "execution_api_retry_wait_min"),
                            2 ** (pending.attempts - 1),
                        ),
                    )
                    self.log.warning("Unable to publish %s; retrying on a later loop", file)
                    del self._pending_publications[file]
                    self._pending_publications[file] = pending
                    return
                self.log.exception("Failed to publish parsing result for %s", file)
                self._record_failed_publication(file, pending.stat)
                self._finish_priority_claims(file, failed=True)
            else:
                self._file_stats[file] = pending.stat
                self._finish_priority_claims(file, failed=False)
            del self._pending_publications[file]
            return

    def _build_parse_result(
        self, file: DagFileInfo, proc: BaseDagFileProcessorProcess, run_duration: float
    ) -> DagParseResultBody:
        result = proc.parsing_result
        if result is None or self.api_client is None:
            raise ValueError("API publication requires a client and completed parse result")
        published_dag_ids = {dag.dag_id for dag in result.serialized_dags}
        warnings = [
            ParseWarning(
                **{
                    key: warning[key] if isinstance(warning, dict) else getattr(warning, key)
                    for key in ("dag_id", "warning_type", "message")
                }
            )
            for warning in result.warnings or []
        ]
        # The API accepts warnings only for published Dags; a Dag that failed to serialize has an import error.
        warnings = [warning for warning in warnings if warning.dag_id in published_dag_ids]
        source_codes = {}
        for dag in result.serialized_dags:
            source = result.dag_source_codes.get(dag.fileloc)
            source_codes[dag.fileloc] = ParseSourceCode(
                source_code=source.source_code if source else None,
                language=source.language if source else "python",
            )
        return DagParseResultBody(
            attempt_id=proc.id,
            dispatch_sequence=proc.dispatch_sequence,
            bundle_name=file.bundle_name,
            relative_fileloc=str(file.rel_path),
            bundle_version=proc.parse_source.bundle_version,
            bundle_revision=proc.parse_source.bundle_revision,
            version_data=proc.parse_source.version_data,
            parse_duration=run_duration,
            serialized_dags=[dag.data for dag in result.serialized_dags],
            import_errors=result.import_errors or {},
            parsed_definitions=result.parsed_definitions,
            warnings=warnings,
            source_codes=source_codes,
        )

    def persist_parsing_result(
        self,
        *,
        bundle_name: str,
        bundle_version: str | None,
        version_data: dict | None,
        parsing_result: DagFileParsingResult,
        run_duration: float,
        relative_fileloc: str | None,
        session: Session,
    ) -> None:
        """Persist parsed DAG data to the metadata database."""
        import_errors: dict[tuple[str, str], str] = {}
        if parsing_result.import_errors:
            import_errors = {
                (bundle_name, rel_path): error for rel_path, error in parsing_result.import_errors.items()
            }

        # Build the set of files that were parsed. This includes the file that was parsed,
        # even if it no longer contains DAGs, so we can clear old import errors.
        files_parsed: set[tuple[str, str]] | None = None
        if relative_fileloc is not None:
            files_parsed = {(bundle_name, relative_fileloc)}
            files_parsed.update((bundle_name, rel_path) for rel_path in parsing_result.parsed_definitions)
            files_parsed.update(import_errors.keys())

        warnings = parsing_result.warnings or []
        if warnings and isinstance(warnings[0], dict):
            warnings = [DagWarning(**warn) for warn in warnings]

        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            version_data=version_data,
            dags=parsing_result.serialized_dags,
            import_errors=import_errors,
            parse_duration=run_duration,
            warnings=set(warnings),
            session=session,
            files_parsed=files_parsed,
            dag_source_codes=parsing_result.dag_source_codes,
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
            self._finish_callback_attempt(processor)
            processor.close()

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

        relative_path = Path(dag_file.rel_path)
        return os.path.join(self._get_log_dir(), dag_file.bundle_name, f"{relative_path}.log")

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
        if self.api_client is not None:
            return self.api_client

        from airflow.sdk.api.client import Client

        self._api_server = _make_execution_api()
        client = Client(base_url=None, token="", dry_run=True, transport=self._api_server.transport)
        # Mypy is wrong -- the setter accepts a string on the property setter! `URLType = URL | str`
        client.base_url = "http://in-process.invalid./"
        return client

    def _create_process(self, dag_file: DagFileInfo) -> BaseDagFileProcessorProcess:
        process_id = uuid7()

        callback_to_execute_for_file = self._callback_to_execute.pop(dag_file, [])
        claims = [
            self._callback_claims.pop(id(request))
            for request in callback_to_execute_for_file
            if id(request) in self._callback_claims
        ]
        if claims:
            self._callback_attempts[process_id] = claims
        logger, logger_filehandle = self._get_logger_for_dag_file(dag_file)
        subprocess_logs_to_stdout = conf.get("logging", "dag_processor_log_target") == "stdout"

        if get_claiming_importer(dag_file.absolute_path, dag_file.bundle_name) is not None:
            return LangSDKDagFileProcessorProcess.start(
                id=process_id,
                path=dag_file.absolute_path,
                bundle_path=cast("Path", dag_file.bundle_path),
                bundle_name=dag_file.bundle_name,
                dag_file_rel_path=str(dag_file.rel_path),
                selector=self.selector,
                logger=logger,
                logger_filehandle=logger_filehandle,
                subprocess_logs_to_stdout=subprocess_logs_to_stdout,
                client=self.client,
            )

        return DagFileProcessorProcess.start(
            id=process_id,
            path=dag_file.absolute_path,
            bundle_path=cast("Path", dag_file.bundle_path),
            bundle_name=dag_file.bundle_name,
            dag_file_rel_path=str(dag_file.rel_path),
            callbacks=callback_to_execute_for_file,
            selector=self.selector,
            logger=logger,
            logger_filehandle=logger_filehandle,
            subprocess_logs_to_stdout=subprocess_logs_to_stdout,
            client=self.client,
        )

    def _start_new_processes(self):
        """Start more processors if we have enough slots and files to process."""
        bundle_to_team = self._get_team_names({file.bundle_name for file in self._file_queue})

        for _ in range(len(self._file_queue)):
            if len(self._processors) + len(self._pending_publications) >= self._parallelism:
                break
            file, _ = self._file_queue.popitem(last=False)
            # Stop creating duplicate processor i.e. processor with the same filepath
            if file in self._processors or file in self._pending_publications:
                continue

            source = self._bundle_parse_sources.get(file.bundle_name)
            if file.bundle_name in self._bundles_waiting_for_refresh:
                source = None
            if file.bundle_version is not None and file in self._callback_to_execute:
                source = DagParseSource(bundle_version=file.bundle_version)
            if source is None:
                self._file_queue[file] = None
                continue
            source = deepcopy(source)
            processor = self._create_process(file)
            processor.parse_source = source
            self._dispatch_sequence += 1
            processor.dispatch_sequence = self._dispatch_sequence
            stats.incr(
                "dag_processing.processes",
                tags=prune_dict(
                    {
                        "file_path": file.normalized_file_path_for_stats,
                        "bundle_name": normalize_name_for_stats(file.bundle_name),
                        "action": "start",
                        "team_name": bundle_to_team.get(file.bundle_name),
                    }
                ),
            )

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
        tracked_presence_keys.update(file.presence_key for file in self._pending_publications)
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
                last_time = (stat.last_attempt_time or stat.last_finish_time) if stat else None
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
        last_time = (stat.last_attempt_time or stat.last_finish_time) if stat else None
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
        in_progress_keys.update(file.presence_key for file in self._pending_publications)
        file_stats_by_presence_key = {file.presence_key: stat for file, stat in self._file_stats.items()}
        now = timezone.utcnow()

        # Sort the file paths by the parsing order mode
        recently_processed = set()
        files = []

        for bundle_files in known_files.values():
            for file in bundle_files:
                files.append(file)
                stat = file_stats_by_presence_key.get(file.presence_key)
                last_time = (stat.last_attempt_time or stat.last_finish_time) if stat else None
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

        bundle_to_team = self._get_team_names({file.bundle_name for file in self._processors})

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
                file_path_tag = file.normalized_file_path_for_stats
                bundle_name_tag = normalize_name_for_stats(file.bundle_name)
                team_name = bundle_to_team.get(file.bundle_name)
                stats.decr(
                    "dag_processing.processes",
                    tags=prune_dict(
                        {
                            "file_path": file_path_tag,
                            "bundle_name": bundle_name_tag,
                            "action": "timeout",
                            "team_name": team_name,
                        }
                    ),
                )
                stats.incr(
                    "dag_processing.processor_timeouts",
                    tags=prune_dict(
                        {
                            "file_path": file_path_tag,
                            "bundle_name": bundle_name_tag,
                            "team_name": team_name,
                        }
                    ),
                )
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
                if not processor.had_callbacks:
                    self._finish_priority_claims(file, failed=True)

        # Clean up `self._processors` after iterating over it
        for proc in processors_to_remove:
            processor = self._processors.pop(proc)
            self._finish_callback_attempt(processor)
            processor.close()

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
        if self._pending_publications:
            return False
        if self._pending_inventories:
            return False
        if (
            self._pending_work_acks
            or self._callback_claims
            or self._callback_attempts
            or self._priority_claims
        ):
            return False
        if any(file not in self._callback_to_execute for file in self._file_queue) or any(
            not proc.had_callbacks for proc in self._processors.values()
        ):
            return False
        return all(stat.run_count >= self.max_runs for stat in self._file_stats.values())

    def terminate(self):
        """Stop all running processors."""
        bundle_to_team = self._get_team_names({file.bundle_name for file in self._processors})

        for file, processor in self._processors.items():
            stats.decr(
                "dag_processing.processes",
                tags=prune_dict(
                    {
                        "file_path": file.normalized_file_path_for_stats,
                        "bundle_name": normalize_name_for_stats(file.bundle_name),
                        "action": "terminate",
                        "team_name": bundle_to_team.get(file.bundle_name),
                    }
                ),
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
    team_name: str | None = None,
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
        stats.incr("dag_processing.callback_only_count", tags=prune_dict({"team_name": team_name}))
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
            tags=prune_dict(
                {"bundle_name": normalized_bundle, "file_name": file_name, "team_name": team_name}
            ),
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
