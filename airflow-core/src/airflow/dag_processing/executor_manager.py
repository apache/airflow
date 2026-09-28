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
"""Opt-in, single-host LocalExecutor parsing through the normal Dag processor command."""

from __future__ import annotations

import fcntl
import json
import multiprocessing
import os
import signal
import socket
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import httpx
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.engine import make_url

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.parsing import (
    TOKEN_AUDIENCE,
    TOKEN_ISSUER,
    TOKEN_KEY_ID,
    TOKEN_SCOPE,
    create_app,
)
from airflow.configuration import conf
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.discovery import discover_python_bundle
from airflow.dag_processing.executor_runner import ParsingExecutorRunner
from airflow.dag_processing.orchestrator import ParseOrchestrator
from airflow.dag_processing.parsing_metadata import MetadataOrchestrationStore
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from collections.abc import Callable
    from multiprocessing.connection import Connection
    from multiprocessing.process import BaseProcess

ROUTE = "local-dag-parsing"


def _serve_api(store_path: str, public_key: Path, listener: socket.socket) -> None:
    uvicorn.Server(
        uvicorn.Config(
            create_app(store_path, public_key, persist_metadata=True, orchestrated=True),
            log_level="warning",
        )
    ).run(sockets=[listener])


def _run_executor(
    store_path: str,
    key_bytes: bytes,
    stop: Connection,
    bundle_roots: dict,
    api_url: str,
    parallelism: int,
    log_dir: str,
) -> None:
    os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] = api_url + "/execution/"
    os.environ["AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS"] = json.dumps(bundle_roots)
    os.environ["AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE"] = "1"
    os.environ["AIRFLOW_DAG_PARSING_POC_LOG_DIR"] = log_dir
    generator = JWTGenerator(
        private_key=Ed25519PrivateKey.from_private_bytes(key_bytes),
        kid=TOKEN_KEY_ID,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm="EdDSA",
        valid_for=600,
    )

    def issue_token(manifest: dict) -> str:
        remaining = (
            datetime.fromisoformat(manifest["stop_deadline"].replace("Z", "+00:00"))
            - datetime.now(timezone.utc)
        ).total_seconds()
        return generator.generate(
            {
                "sub": manifest["workload_id"],
                "scope": TOKEN_SCOPE,
                "attempt_ids": [item["attempt_id"] for item in manifest["definitions"]],
            },
            valid_for=max(1, remaining) + 60,
        )

    executor = LocalExecutor(parallelism=parallelism)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    runner = ParsingExecutorRunner(
        MetadataOrchestrationStore(store_path), executor, route=ROUTE, token_issuer=issue_token
    )
    runner.start()
    try:
        while not stop.poll(0.05):
            runner.tick()
    finally:
        runner.close()
        stop.close()


def _wait_for_api(url: str, process: BaseProcess) -> None:
    deadline = time.monotonic() + 30
    with httpx.Client(timeout=1, trust_env=False) as client:
        while time.monotonic() < deadline:
            if not process.is_alive():
                raise RuntimeError(f"Parsing API exited during startup: {process.exitcode}")
            try:
                if client.get(f"{url}/health").status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.05)
    raise TimeoutError("Parsing API did not become ready")


def _stop_process(process: BaseProcess, *, graceful: bool) -> None:
    if graceful:
        process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
    if process.is_alive():
        process.kill()
        process.join(timeout=5)
    process.close()


class ExecutorDagProcessor(LoggingMixin):
    """
    Host the experimental orchestrator with a dedicated local parsing route.

    This uses SQLite auxiliary tables without migrations. It is for disposable development
    databases, and must not run alongside the regular Dag processor for the same bundles.
    """

    def __init__(self, *, max_runs: int = -1, bundle_names_to_parse: list[str] | None = None):
        if max_runs < -1:
            raise ValueError("num-runs must be -1 or nonnegative")
        self.max_runs = max_runs
        self.bundle_names_to_parse = bundle_names_to_parse
        self.heartbeat: Callable[[], None] = lambda: None
        self._stopping = False

    def terminate(self) -> None:
        """Request a graceful stop at the next host iteration."""
        self._stopping = True

    def end(self) -> None:
        """Children are joined by run() before the job ends."""

    def _handle_signal(self, signum, frame) -> None:
        self.terminate()

    def _get_store_path(self) -> Path:
        url = make_url(conf.get("database", "sql_alchemy_conn"))
        if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:" or url.query:
            raise ValueError(
                "--executor-parsing currently requires a file-backed SQLite development database"
            )
        path = Path(url.database).resolve(strict=True)
        return path

    def _get_bundles(self) -> list[LocalDagBundle]:
        manager = DagBundlesManager()
        names = self.bundle_names_to_parse or list(manager.get_all_bundle_names())
        bundles = [manager.get_bundle(name) for name in names]
        local_bundles = []
        for bundle in bundles:
            if not isinstance(bundle, LocalDagBundle):
                raise ValueError(f"--executor-parsing currently requires LocalDagBundle: {bundle.name}")
            bundle.initialize()
            if not bundle.path.is_dir():
                raise ValueError(f"Local bundle directory does not exist: {bundle.path}")
            local_bundles.append(bundle)
        manager.sync_bundles_to_db(deactivate_missing=not self.bundle_names_to_parse)
        return local_bundles

    def run(self) -> None:
        """Run normal job heartbeats, inventory discovery and bounded admission steps."""
        if self.max_runs == 0:
            return
        store_path = self._get_store_path()
        parallelism = conf.getint("dag_processor", "parsing_processes")
        if parallelism < 1:
            raise ValueError("Executor parsing requires positive dag_processor.parsing_processes")
        with ExitStack() as stack:
            lock = stack.enter_context(store_path.with_name(store_path.name + ".parsing.lock").open("a"))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Another executor parsing processor owns this database") from None
            store = MetadataOrchestrationStore(store_path)
            stack.callback(store.engine.dispose)
            admissions = store.restore_admissions(ROUTE)
            if any(row["state"] == "submitted" for row in admissions):
                raise RuntimeError(
                    "Unresolved submitted parsing work requires confirmed termination and recovery; "
                    "refusing to redispatch it"
                )
            bundles = self._get_bundles()
            selected = {bundle.name for bundle in bundles}
            if any(row["manifest"]["bundle_info"]["name"] not in selected for row in admissions):
                raise RuntimeError("Pending parsing reservations belong to bundles outside this selection")
            # Rediscover unsent work before a new runner can dispatch stale source references.
            for row in admissions:
                if not store.retire_unsubmitted_reservation(row["workload_id"]):
                    raise RuntimeError("Parsing reservation entered submission during startup")
            temp = Path(stack.enter_context(TemporaryDirectory(prefix="airflow-parsing-")))
            key = Ed25519PrivateKey.generate()
            public = temp / "public.pem"
            public.write_bytes(
                key.public_key().public_bytes(
                    serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
                )
            )
            context = multiprocessing.get_context("spawn")
            listener = stack.enter_context(socket.socket())
            listener.bind(("127.0.0.1", 0))
            api_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
            api = context.Process(target=_serve_api, args=(str(store_path), public, listener))
            api.start()
            stack.callback(_stop_process, api, graceful=False)
            _wait_for_api(api_url, api)
            receiver, sender = context.Pipe(duplex=False)
            stack.callback(sender.close)
            stack.callback(receiver.close)
            runner = context.Process(
                target=_run_executor,
                args=(
                    str(store_path),
                    key.private_bytes(
                        serialization.Encoding.Raw,
                        serialization.PrivateFormat.Raw,
                        serialization.NoEncryption(),
                    ),
                    receiver,
                    {
                        bundle.name: {"path": str(bundle.path.resolve()), "version": None}
                        for bundle in bundles
                    },
                    api_url,
                    parallelism,
                    conf.get("logging", "dag_processor_child_process_log_directory"),
                ),
            )
            runner.start()
            stack.callback(_stop_process, runner, graceful=True)
            # Closing the only writing end wakes the runner even when the parent raises.
            stack.callback(sender.close)
            receiver.close()
            for sig in (signal.SIGINT, signal.SIGTERM):
                old_handler = signal.signal(sig, self._handle_signal)
                stack.callback(signal.signal, sig, old_handler)
            self.log.warning(
                "Experimental executor parsing enabled: LocalExecutor, SQLite, route=%s, capacity=%s",
                ROUTE,
                parallelism,
            )
            self._run_loop(store, bundles, parallelism, [api, runner])

    def _run_loop(
        self,
        store: MetadataOrchestrationStore,
        bundles: list[LocalDagBundle],
        parallelism: int,
        processes: list[BaseProcess],
    ) -> None:
        timeout = conf.getfloat("dag_processor", "dag_file_processor_timeout")
        interval = max(0.1, conf.getfloat("dag_processor", "min_file_process_interval"))
        refresh = max(0.1, conf.getfloat("dag_processor", "refresh_interval"))
        orchestrators = {
            bundle.name: ParseOrchestrator(
                store,
                route=ROUTE,
                bundle=bundle.name,
                capacity=parallelism,
                batch_size=10,
                parse_interval=interval,
                execution_window=10 * timeout + 30,
            )
            for bundle in bundles
        }
        baselines: dict[tuple[str, str], int] = {}
        next_refresh: dict[str, float] = {}
        bundle_order = list(bundles)
        while not self._stopping:
            self.heartbeat()
            for process in processes:
                if not process.is_alive():
                    raise RuntimeError(
                        f"Parsing child exited unexpectedly: {process.name} ({process.exitcode})"
                    )
            admissions = store.get_admissions(ROUTE)
            for row in admissions:
                deadline = datetime.fromisoformat(row["manifest"]["stop_deadline"].replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - deadline).total_seconds() > 30:
                    raise RuntimeError(
                        "Parsing work did not reconcile after its deadline; admission remains reserved "
                        "until termination is confirmed"
                    )
            active_bundles = {row["manifest"]["bundle_info"]["name"] for row in admissions}
            finished = True
            for bundle in tuple(bundle_order):
                orchestrator = orchestrators[bundle.name]
                # Do not invalidate the current local batch while its results are being published.
                if bundle.name not in active_bundles and time.monotonic() >= next_refresh.get(bundle.name, 0):
                    definitions = [
                        definition.model_copy(update={"timeout_seconds": timeout})
                        for definition in discover_python_bundle(bundle.path, bundle_name=bundle.name)
                    ]
                    orchestrator.update_inventory(BundleInfo(name=bundle.name, version=None), definitions)
                    next_refresh[bundle.name] = time.monotonic() + refresh
                eligible_paths = None
                if self.max_runs != -1:
                    sources = [row for row in store.get_sources(ROUTE, bundle.name) if row["present"]]
                    for row in sources:
                        baselines.setdefault((bundle.name, row["path"]), row["accepted_count"])
                    eligible_paths = {
                        row["path"]
                        for row in sources
                        if row["accepted_count"] < baselines[bundle.name, row["path"]] + self.max_runs
                    }
                if eligible_paths is None or eligible_paths:
                    finished = False
                    result = orchestrator.step(eligible_paths=eligible_paths)
                    if result.workload_id is not None:
                        bundle_order.remove(bundle)
                        bundle_order.append(bundle)
            if finished and not admissions:
                return
            time.sleep(0.1)
