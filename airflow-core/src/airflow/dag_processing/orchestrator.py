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
"""Single-owner periodic scheduling experiment; no discovery or executor I/O in step()."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, model_validator

from airflow.dag_processing.parsing_state import (
    ReceiptCapacityError,
    ReceiptConflictError,
    ReceiptStore,
    _encode_json,
)
from airflow.executors.workloads import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, ParseDagDefinitions

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    from airflow.executors.workloads.parsing import DagDefinitionResult


class DiscoveredDefinition(BaseModel):
    """Trusted discovery data; attempts are allocated only when work is admitted."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    relative_path: str
    source_revision: str
    timeout_seconds: float = 30
    archive_path: str | None = None
    archive_revision: str | None = None

    @model_validator(mode="after")
    def validate_reference(self) -> DiscoveredDefinition:
        self.create_attempt()
        if str(PurePosixPath(self.relative_path)) != self.relative_path:
            raise ValueError("Definition paths must be canonical")
        return self

    def create_attempt(self) -> DagDefinitionAttempt:
        return DagDefinitionAttempt(attempt_id=uuid4(), **self.model_dump())


@dataclass(frozen=True)
class StepResult:
    """Report bounded reconciliation and admission to the host."""

    reconciled: int
    workload_id: str | None = None
    capacity_blocked: bool = False


class OrchestrationStore(ReceiptStore):
    """Keep source scheduling and admission in the same experimental SQLite database."""

    def __init__(self, path: str | Path):
        super().__init__(path)
        with self._open_transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS parse_sources ("
                "route TEXT NOT NULL, bundle TEXT NOT NULL, path TEXT NOT NULL, "
                "version TEXT, definition_json TEXT NOT NULL, generation INTEGER NOT NULL, "
                "present INTEGER NOT NULL, next_due REAL NOT NULL, "
                "parse_interval REAL NOT NULL, retry_interval REAL NOT NULL, "
                "active_workload TEXT REFERENCES workloads(workload_id), active_attempt TEXT, "
                "last_outcome TEXT, accepted_count INTEGER NOT NULL DEFAULT 0, "
                "PRIMARY KEY (route, bundle, path))"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS parse_sources_due "
                "ON parse_sources (route, bundle, present, active_workload, next_due, path)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS parse_sources_attempt "
                "ON parse_sources (active_workload, active_attempt)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scheduled_attempts ("
                "workload_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
                "route TEXT NOT NULL, bundle TEXT NOT NULL, path TEXT NOT NULL, generation INTEGER NOT NULL, "
                "PRIMARY KEY (workload_id, attempt_id), "
                "FOREIGN KEY (workload_id, attempt_id) REFERENCES attempts(workload_id, attempt_id))"
            )

    def get_sources(self, route: str, bundle: str) -> list[dict]:
        """Inspect scheduling state for the development runner, outside the bounded step."""
        with self._open_transaction() as connection:
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM parse_sources WHERE route = ? AND bundle = ? ORDER BY path",
                    (route, bundle),
                )
            ]

    def _get_attempt(self, connection: sqlite3.Connection, workload_id: str, attempt_id: str) -> sqlite3.Row:
        attempt = super()._get_attempt(connection, workload_id, attempt_id)
        if attempt["result_json"] is not None:
            return attempt
        current = connection.execute(
            "SELECT 1 FROM scheduled_attempts t JOIN parse_sources s "
            "ON (s.route, s.bundle, s.path) = (t.route, t.bundle, t.path) "
            "WHERE t.workload_id = ? AND t.attempt_id = ? AND s.present = 1 "
            "AND s.generation = t.generation AND s.active_workload = t.workload_id "
            "AND s.active_attempt = t.attempt_id",
            (workload_id, attempt_id),
        ).fetchone()
        if current is None:
            raise ReceiptConflictError("Definition was removed or superseded")
        return attempt

    def _persist_result(
        self, connection: sqlite3.Connection, workload_id: str, result: DagDefinitionResult
    ) -> None:
        super()._persist_result(connection, workload_id, result)
        connection.execute(
            "UPDATE parse_sources SET last_outcome = ?, accepted_count = accepted_count + 1, "
            "next_due = ? + CASE WHEN ? IN ('worker_error', 'timeout') "
            "THEN retry_interval ELSE parse_interval END "
            "WHERE active_workload = ? AND active_attempt = ?",
            (
                result.outcome,
                datetime.now(timezone.utc).timestamp(),
                result.outcome,
                workload_id,
                str(result.attempt_id),
            ),
        )

    def _get_retry_definitions(
        self, connection: sqlite3.Connection, workload_id: str, definitions: list[dict]
    ) -> list[dict]:
        # Retire through the existing recovery transaction; step() retries the current inventory
        # after backoff. Eager replacement here would bypass scheduling and source generations.
        return []


class ParseOrchestrator:
    """
    Admit at most one batch and reconcile at most limit definitions per step.

    The host supplies complete trusted snapshots separately. Scheduler hosting must
    supply a store with bounded transactions; the default store permits lock waits.
    """

    def __init__(
        self,
        store: OrchestrationStore,
        *,
        route: str,
        bundle: str,
        capacity: int = 1,
        batch_size: int = 10,
        parse_interval: float = 30,
        retry_interval: float = 5,
        start_window: float = 60,
        execution_window: float = 300,
    ):
        if not route.strip() or route in {"default", "celery"} or not bundle.strip():
            raise ValueError("Use an explicit parsing route and bundle")
        if (
            type(capacity) is not int
            or capacity < 1
            or type(batch_size) is not int
            or not 1 <= batch_size <= 100
        ):
            raise ValueError("Capacity must be positive and batch_size must be between 1 and 100")
        if any(
            not math.isfinite(value) or value <= 0
            for value in (
                parse_interval,
                retry_interval,
                start_window,
                execution_window,
            )
        ):
            raise ValueError("Intervals and windows must be finite and positive")
        self.store, self.route, self.bundle = store, route, bundle
        self.capacity, self.batch_size = capacity, batch_size
        self.parse_interval, self.retry_interval = parse_interval, retry_interval
        self.start_window, self.execution_window = start_window, execution_window

    def update_inventory(self, bundle: BundleInfo, definitions: list[DiscoveredDefinition]) -> None:
        """Apply a complete snapshot outside step(); absence stops future admission, not metadata Dags."""
        if bundle.name != self.bundle:
            raise ValueError("Snapshot belongs to another bundle")
        items = {
            definition.relative_path: _encode_json(definition.model_dump()) for definition in definitions
        }
        if len(items) != len(definitions):
            raise ValueError("Snapshot contains duplicate definitions")
        with self.store._open_transaction() as connection:
            existing = {
                row["path"]: row
                for row in connection.execute(
                    "SELECT * FROM parse_sources WHERE route = ? AND bundle = ?", (self.route, self.bundle)
                )
            }
            for path, definition_json in items.items():
                old = existing.get(path)
                if old is None:
                    connection.execute(
                        "INSERT INTO parse_sources (route, bundle, path, version, definition_json, "
                        "generation, present, next_due, parse_interval, retry_interval) "
                        "VALUES (?, ?, ?, ?, ?, 1, 1, 0, ?, ?)",
                        (
                            self.route,
                            self.bundle,
                            path,
                            bundle.version,
                            definition_json,
                            self.parse_interval,
                            self.retry_interval,
                        ),
                    )
                else:
                    changed = (
                        not old["present"]
                        or old["version"] != bundle.version
                        or old["definition_json"] != definition_json
                    )
                    connection.execute(
                        "UPDATE parse_sources SET version = ?, definition_json = ?, generation = ?, "
                        "present = 1, next_due = ?, parse_interval = ?, retry_interval = ? "
                        "WHERE route = ? AND bundle = ? AND path = ?",
                        (
                            bundle.version,
                            definition_json,
                            old["generation"] + int(changed),
                            0 if changed else old["next_due"],
                            self.parse_interval,
                            self.retry_interval,
                            self.route,
                            self.bundle,
                            path,
                        ),
                    )
            for path, old in existing.items():
                if path not in items and old["present"]:
                    connection.execute(
                        "UPDATE parse_sources SET present = 0, generation = generation + 1 "
                        "WHERE route = ? AND bundle = ? AND path = ?",
                        (self.route, self.bundle, path),
                    )

    def step(self, *, limit: int = 100, eligible_paths: set[str] | None = None) -> StepResult:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Step limit must be between 1 and 100")
        eligible_json = None if eligible_paths is None else _encode_json(sorted(eligible_paths))
        now = datetime.now(timezone.utc)
        with self.store._open_transaction() as connection:
            released = connection.execute(
                "SELECT s.path, s.generation, t.generation AS attempted_generation, "
                "(a.result_json IS NOT NULL) AS accepted "
                "FROM parse_sources s JOIN admissions r ON r.workload_id = s.active_workload "
                "JOIN scheduled_attempts t ON t.workload_id = s.active_workload AND t.attempt_id = s.active_attempt "
                "JOIN attempts a ON a.workload_id = t.workload_id AND a.attempt_id = t.attempt_id "
                "WHERE s.route = ? AND s.bundle = ? AND r.state = 'released' LIMIT ?",
                (self.route, self.bundle, limit),
            ).fetchall()
            for row in released:
                backoff = not row["accepted"] and row["generation"] == row["attempted_generation"]
                connection.execute(
                    "UPDATE parse_sources SET active_workload = NULL, active_attempt = NULL, "
                    "next_due = CASE WHEN ? THEN ? + retry_interval ELSE next_due END "
                    "WHERE route = ? AND bundle = ? AND path = ?",
                    (backoff, now.timestamp(), self.route, self.bundle, row["path"]),
                )
            rows = connection.execute(
                "SELECT * FROM parse_sources WHERE route = ? AND bundle = ? AND present = 1 "
                "AND active_workload IS NULL AND next_due <= ? "
                "AND (? IS NULL OR path IN (SELECT value FROM json_each(?))) "
                "ORDER BY next_due, path LIMIT ?",
                (
                    self.route,
                    self.bundle,
                    now.timestamp(),
                    eligible_json,
                    eligible_json,
                    min(limit, self.batch_size),
                ),
            ).fetchall()
            if not rows:
                return StepResult(len(released))
            workload = ParseDagDefinitions(
                workload_id=uuid4(),
                token="not-issued",
                queue=self.route,
                bundle_info=BundleInfo(name=self.bundle, version=rows[0]["version"]),
                definitions=tuple(
                    DiscoveredDefinition.model_validate_json(row["definition_json"]).create_attempt()
                    for row in rows
                ),
                start_deadline=now + timedelta(seconds=self.start_window),
                stop_deadline=now + timedelta(seconds=self.start_window + self.execution_window),
            )
            try:
                self.store._reserve_workload(connection, workload, route=self.route, capacity=self.capacity)
            except ReceiptCapacityError:
                return StepResult(len(released), capacity_blocked=True)
            for row, definition in zip(rows, workload.definitions):
                connection.execute(
                    "INSERT INTO scheduled_attempts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        str(workload.workload_id),
                        str(definition.attempt_id),
                        self.route,
                        self.bundle,
                        row["path"],
                        row["generation"],
                    ),
                )
                connection.execute(
                    "UPDATE parse_sources SET active_workload = ?, active_attempt = ? "
                    "WHERE route = ? AND bundle = ? AND path = ?",
                    (
                        str(workload.workload_id),
                        str(definition.attempt_id),
                        self.route,
                        self.bundle,
                        row["path"],
                    ),
                )
            return StepResult(len(released), str(workload.workload_id))
