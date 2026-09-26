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
"""Dedicated executor runner, separated from either orchestration host."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from airflow.dag_processing.executor_worker import ParsingPublicationError
from airflow.dag_processing.parsing_state import ReceiptExpiredError
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import ParseDagDefinitions, ParseDagDefinitionsState

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.dag_processing.orchestrator import OrchestrationStore
    from airflow.executors.base_executor import BaseExecutor


class ParsingExecutorRunner:
    """
    Own provider I/O separately from ParseOrchestrator.step().

    A restarted runner retains submitted charges but cannot recover its predecessor's
    processes. That requires external termination evidence, as in the Celery checkpoint.
    """

    def __init__(
        self,
        store: OrchestrationStore,
        executor: BaseExecutor,
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
        self._terminal: dict[str, str | None] = {}
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
                self._terminal.pop(workload_id, None)
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
            # Parsing enqueue must not use a task instance or a metadata-bound session.
            with Session() as session:
                self.executor.queue_workload(workload, session=session)
            self._submitted[str(workload.workload_id)] = workload
            submitted += 1
        self.executor.heartbeat()
        self._reconcile_returned()

    def _reconcile_returned(self) -> None:
        for key, (state, info) in self.executor.get_event_buffer().items():
            if state not in {ParseDagDefinitionsState.SUCCESS, ParseDagDefinitionsState.FAILED}:
                continue
            if not isinstance(self.executor, LocalExecutor) and state != ParseDagDefinitionsState.SUCCESS:
                continue
            if str(key) in self._submitted:
                self._terminal[str(key)] = (
                    info.execution_id
                    if isinstance(self.executor, LocalExecutor) and isinstance(info, ParsingPublicationError)
                    else None
                )
        for workload_id, finished_execution in tuple(self._terminal.items()):
            workload = self._submitted[workload_id]
            attempts = self.store.get_attempts(workload.workload_id)
            results = self.store.get_results(workload.workload_id)
            if len(results) != len(attempts) and finished_execution is None:
                continue
            if finished_execution is not None and any(
                attempt["status"] == "claimed" and attempt["execution_id"] != finished_execution
                for attempt in attempts
            ):
                continue
            if any(result["outcome"] not in {"success", "import_error"} for result in results):
                continue
            # Remote success needs every importer-exit receipt. Partial results or generic remote
            # failures need external evidence; only a local delivery can attest a publication failure.
            self.store.retire_and_replace(
                workload.workload_id,
                termination={
                    "kind": "confirmed_worker_termination",
                    "workload_id": str(workload.workload_id),
                    "execution_ids": (
                        [finished_execution]
                        if finished_execution is not None
                        else [attempt["execution_id"] for attempt in attempts if attempt["execution_id"]]
                    ),
                    "evidence": {"executor_completed_imports": str(workload.workload_id)},
                },
                start_deadline=datetime.now(timezone.utc) + timedelta(seconds=1),
                stop_deadline=datetime.now(timezone.utc) + timedelta(seconds=2),
            )
            del self._submitted[workload_id]
            del self._terminal[workload_id]

    def close(self) -> None:
        if self._started:
            self.executor.end()
            self._reconcile_returned()
            self._started = False
