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
"""Dedicated LocalExecutor runner for the standalone orchestration checkpoint."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from airflow.dag_processing.parsing_state import ReceiptExpiredError
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import ParseDagDefinitions, ParseDagDefinitionsState

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.dag_processing.orchestrator import OrchestrationStore
    from airflow.executors.local_executor import LocalExecutor


class LocalParsingRunner:
    """
    Own provider I/O separately from ParseOrchestrator.step().

    A restarted runner retains submitted charges but cannot recover its predecessor's
    processes. That requires external termination evidence, as in the Celery checkpoint.
    """

    def __init__(
        self,
        store: OrchestrationStore,
        executor: LocalExecutor,
        *,
        route: str,
        token_issuer: Callable[[dict], str],
    ):
        if executor.supported_workload_types != frozenset({WorkloadType.PARSE_DAG_DEFINITIONS}):
            raise ValueError("Runner requires a dedicated parsing-only executor")
        if not route.strip() or route in {"default", "celery"} or executor.parallelism <= 0:
            raise ValueError("Runner requires an explicit route and finite positive capacity")
        self.store, self.executor, self.route, self.token_issuer = store, executor, route, token_issuer
        self._submitted: dict[str, ParseDagDefinitions] = {}
        self._terminal: set[str] = set()
        self._started = False

    def start(self) -> None:
        if self._started:
            raise RuntimeError("Runner already started")
        self.store.restore_admissions(self.route)
        self.executor.start()
        self._started = True

    def tick(self) -> None:
        if not self._started:
            raise RuntimeError("Start the runner before ticking")
        admissions = self.store.get_admissions(self.route)
        active = {row["workload_id"] for row in admissions}
        for workload_id in tuple(self._submitted):
            if workload_id not in active:
                del self._submitted[workload_id]
                self._terminal.discard(workload_id)
        submitted = sum(row["state"] == "submitted" for row in admissions)
        for row in admissions:
            if row["state"] != "reserved":
                continue
            if self.store.retire_expired_reservation(row["workload_id"]):
                continue
            if submitted >= self.executor.parallelism:
                break
            workload = ParseDagDefinitions.model_validate(
                row["manifest"] | {"token": self.token_issuer(row["manifest"])}
            )
            try:
                self.store.mark_submitted(workload.workload_id)
            except ReceiptExpiredError:
                self.store.retire_expired_reservation(workload.workload_id)
                continue
            # A crash after this commit retains capacity; never automatically republish submitted work.
            # LocalExecutor only enqueues; this session must remain unbound to the metadata engine.
            with Session() as session:
                self.executor.queue_workload(workload, session=session)
            self._submitted[str(workload.workload_id)] = workload
            submitted += 1
        self.executor.heartbeat()
        self._reconcile_returned()

    def _reconcile_returned(self) -> None:
        for key, (state, _) in self.executor.get_event_buffer().items():
            if state not in {ParseDagDefinitionsState.SUCCESS, ParseDagDefinitionsState.FAILED}:
                continue
            if str(key) in self._submitted:
                self._terminal.add(str(key))
        for workload_id in tuple(self._terminal):
            workload = self._submitted[workload_id]
            attempts = self.store.get_attempts(workload.workload_id)
            results = self.store.get_results(workload.workload_id)
            if len(results) != len(attempts) or any(
                result["outcome"] not in {"success", "import_error"} for result in results
            ):
                continue
            # Success/import_error require an observed importer exit. kill() can return without
            # termination, so timeouts, worker errors and missing receipts need external evidence.
            # Only this runner's single local delivery qualifies; never apply this to remote events.
            self.store.retire_and_replace(
                workload.workload_id,
                termination={
                    "kind": "confirmed_worker_termination",
                    "workload_id": str(workload.workload_id),
                    "execution_ids": [
                        attempt["execution_id"] for attempt in attempts if attempt["execution_id"]
                    ],
                    "evidence": {"local_execution_returned": str(workload.workload_id)},
                },
                start_deadline=datetime.now(timezone.utc) + timedelta(seconds=1),
                stop_deadline=datetime.now(timezone.utc) + timedelta(seconds=2),
            )
            del self._submitted[workload_id]
            self._terminal.remove(workload_id)

    def close(self) -> None:
        if self._started:
            self.executor.end()
            self._reconcile_returned()
            self._started = False
