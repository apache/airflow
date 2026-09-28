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
"""Single-owner recovery experiment; not an HA scheduler or production parsing service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from airflow.dag_processing.parsing_state import ReceiptExpiredError
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import ParseDagDefinitions

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime
    from uuid import UUID

    from airflow.dag_processing.parsing_state import ReceiptStore
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor


@dataclass(frozen=True)
class PublicationOutcome:
    """Provider publication outcome; this does not establish execution or termination."""

    workload_id: str
    status: Literal["published", "uncertain", "expired"]


class ParsingRecoveryCoordinator:
    """Own a dedicated executor, driving it only through durable admission decisions."""

    def __init__(
        self,
        store: ReceiptStore,
        executor: CeleryExecutor,
        *,
        route: str,
        capacity: int,
        token_issuer: Callable[[dict], str],
        termination_validator: Callable[[dict], None],
    ):
        if (
            not route.strip()
            or route in {"default", "celery"}
            or not isinstance(capacity, int)
            or isinstance(capacity, bool)
            or capacity <= 0
            or executor.parallelism != capacity
        ):
            raise ValueError("Use an explicit parsing route and capacity matching executor parallelism")
        if executor.supported_workload_types != frozenset({WorkloadType.PARSE_DAG_DEFINITIONS}):
            raise ValueError("Recovery requires a dedicated parsing-only executor")
        if executor.running or executor.workloads or any(executor.executor_queues.values()):
            raise ValueError("Recovery requires a fresh executor instance")
        self.store = store
        self.executor = executor
        self.route = route
        self.capacity = capacity
        self.token_issuer = token_issuer
        self.termination_validator = termination_validator
        self._started = False

    def _require_started(self) -> None:
        if not self._started:
            raise RuntimeError("Restore durable admissions with start() before coordinating work")

    def _restore_tracking(self, admissions: list[dict] | None = None) -> None:
        rows = admissions if admissions is not None else self.store.get_admissions(self.route)
        restored = []
        for row in rows:
            workload = ParseDagDefinitions.model_validate(
                {**row["manifest"], "token": "unissued-coordinator-token"}
            )
            if row["route"] != self.route or workload.queue != self.route:
                raise ValueError("Durable admission belongs to another parsing route")
            if row["state"] not in {"reserved", "submitted"}:
                raise ValueError("Cannot restore an inactive admission")
            restored.append((row["state"], workload))
        self.executor.running.clear()
        self.executor.workloads.clear()
        self.executor.executor_queues.clear()
        self.executor.event_buffer.clear()
        self.executor.workload_publish_retries.clear()
        for state, workload in restored:
            # Unsigned reservations must never enter the executor's heartbeat dispatch queue.
            if state == "submitted":
                self.executor.running.add(workload.key)
                self.executor.workloads[workload.key] = self.executor.celery_app.AsyncResult(
                    str(workload.workload_id)
                )

    def start(self) -> None:
        if self._started:
            raise RuntimeError("Coordinator already started")
        admissions = self.store.restore_admissions(self.route)
        self.executor.start()
        self._restore_tracking(admissions)
        self._started = True

    @property
    def available_slots(self) -> int:
        self._require_started()
        return max(0, self.capacity - len(self.store.get_admissions(self.route)))

    def admit(self, workload: ParseDagDefinitions) -> dict:
        self._require_started()
        if workload.queue != self.route:
            raise ValueError("Workload belongs to another parsing route")
        reservation = self.store.reserve_workload(workload, route=self.route, capacity=self.capacity)
        self._restore_tracking()
        return reservation

    def dispatch_reserved(self, *, limit: int = 1) -> list[PublicationOutcome]:
        """Inspect at most limit reservations and report publication or unsent expiry."""
        self._require_started()
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("Dispatch limit must be a positive integer")
        admissions = self.store.get_admissions(self.route)
        self._restore_tracking(admissions)
        submitted_count = sum(row["state"] == "submitted" for row in admissions)
        remaining = max(0, self.capacity - submitted_count)
        reserved = [row for row in admissions if row["state"] == "reserved"][:limit]
        outcomes = []
        for row in reserved:
            manifest = row["manifest"]
            workload_id = row["workload_id"]
            try:
                if self.store.retire_expired_reservation(workload_id):
                    outcomes.append(PublicationOutcome(workload_id, "expired"))
                    continue
                if not remaining:
                    continue
                workload = ParseDagDefinitions.model_validate(
                    {**manifest, "token": self.token_issuer(manifest)}
                )
                try:
                    self.store.mark_submitted(workload.workload_id)
                except ReceiptExpiredError:
                    # Token issuance may consume the remaining start window.
                    if not self.store.retire_expired_reservation(workload_id):
                        raise
                    outcomes.append(PublicationOutcome(workload_id, "expired"))
                    continue
                # A crash or broker timeout after this commit must never trigger automatic republishing.
                self.executor._process_workloads([workload])
                outcomes.append(
                    PublicationOutcome(
                        workload_id, "published" if workload.key in self.executor.workloads else "uncertain"
                    )
                )
                remaining -= 1
            finally:
                self._restore_tracking()
        return outcomes

    def sync(self) -> dict:
        self._require_started()
        try:
            self.executor.sync()
            return self.executor.get_event_buffer()
        finally:
            # Provider terminal events do not prove every claimed execution has stopped.
            self._restore_tracking()

    def recover(
        self,
        workload_id: UUID | str,
        *,
        termination: dict,
        start_deadline: datetime,
        stop_deadline: datetime,
    ) -> dict:
        self._require_started()
        manifest = self.store.get_manifest(workload_id)
        if manifest["queue"] != self.route:
            raise ValueError("Recovery workload belongs to another parsing route")
        if termination.get("workload_id") != str(workload_id):
            raise ValueError("Termination evidence belongs to another workload")
        self.termination_validator(termination)
        decision = self.store.retire_and_replace(
            workload_id,
            termination=termination,
            start_deadline=start_deadline,
            stop_deadline=stop_deadline,
        )
        self._restore_tracking()
        return decision
