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

"""Durable PoC receipts, deliberately separate from Airflow metadata ingestion."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from airflow.executors.workloads.parsing import ParseDagDefinitions

if TYPE_CHECKING:
    from collections.abc import Iterator
    from uuid import UUID

    from airflow.executors.workloads.parsing import DagDefinitionResult


class ReceiptNotFoundError(ValueError):
    """The requested attempt was never registered."""


class ReceiptConflictError(ValueError):
    """A claim or result conflicts with a durable identity."""


class ReceiptExpiredError(ValueError):
    """The registered deadline no longer permits new work."""


class ReceiptInvalidResultError(ValueError):
    """A result cannot be represented as finite JSON."""


class ReceiptCapacityError(ValueError):
    """The route has no unreserved parsing capacity."""


def _encode_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class ReceiptStore:
    """Register manifests and atomically fence claims and per-definition results."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("The PoC requires a durable file-backed receipt store")
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._open_transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS workloads ("
                "workload_id TEXT PRIMARY KEY, manifest_json TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS attempts ("
                "workload_id TEXT NOT NULL REFERENCES workloads(workload_id), "
                "attempt_id TEXT NOT NULL, definition_json TEXT NOT NULL, "
                "start_deadline REAL NOT NULL, stop_deadline REAL NOT NULL, "
                "execution_id TEXT, result_json TEXT, digest TEXT, "
                "PRIMARY KEY (workload_id, attempt_id))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS admissions ("
                "workload_id TEXT PRIMARY KEY REFERENCES workloads(workload_id), "
                "route TEXT NOT NULL, state TEXT NOT NULL "
                "CHECK (state IN ('reserved', 'submitted', 'released')))"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS admissions_route_state ON admissions (route, state)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS retired_attempts ("
                "workload_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
                "PRIMARY KEY (workload_id, attempt_id), "
                "FOREIGN KEY (workload_id, attempt_id) REFERENCES attempts(workload_id, attempt_id))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS recovery_decisions ("
                "workload_id TEXT PRIMARY KEY REFERENCES workloads(workload_id), "
                "termination_json TEXT NOT NULL, decision_json TEXT NOT NULL)"
            )
            if not connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pending_admissions'"
            ).fetchone():
                connection.execute(
                    "CREATE TABLE pending_admissions ("
                    "workload_id TEXT PRIMARY KEY REFERENCES workloads(workload_id), route TEXT NOT NULL)"
                )
                connection.execute("CREATE INDEX pending_admissions_route ON pending_admissions (route)")
                # Scan pre-ledger history only when installing the pending-record index.
                for row in connection.execute(
                    "SELECT w.workload_id, w.manifest_json FROM workloads w "
                    "LEFT JOIN admissions a ON a.workload_id = w.workload_id WHERE a.workload_id IS NULL"
                ).fetchall():
                    route = json.loads(row["manifest_json"]).get("queue")
                    if isinstance(route, str) and route.strip():
                        connection.execute(
                            "INSERT INTO pending_admissions VALUES (?, ?)", (row["workload_id"], route)
                        )

    @contextmanager
    def _open_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            with connection:
                yield connection
        finally:
            connection.close()

    def register_workload(self, workload: ParseDagDefinitions) -> None:
        """Register trusted dispatch metadata; this operation is not exposed over HTTP."""
        with self._open_transaction() as connection:
            self._register_workload(connection, workload)

    def _register_workload(self, connection: sqlite3.Connection, workload: ParseDagDefinitions) -> None:
        manifest = _encode_json(workload.model_dump(mode="json", exclude={"token"}))
        workload_id = str(workload.workload_id)
        existing = connection.execute(
            "SELECT manifest_json FROM workloads WHERE workload_id = ?", (workload_id,)
        ).fetchone()
        if existing:
            if existing["manifest_json"] != manifest:
                raise ReceiptConflictError("Workload identity already has a different manifest")
            return
        connection.execute("INSERT INTO workloads VALUES (?, ?)", (workload_id, manifest))
        if workload.queue and workload.queue.strip():
            connection.execute("INSERT INTO pending_admissions VALUES (?, ?)", (workload_id, workload.queue))
        connection.executemany(
            "INSERT INTO attempts "
            "(workload_id, attempt_id, definition_json, start_deadline, stop_deadline) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                (
                    workload_id,
                    str(definition.attempt_id),
                    _encode_json(definition.model_dump(mode="json")),
                    workload.start_deadline.timestamp(),
                    workload.stop_deadline.timestamp(),
                )
                for definition in workload.definitions
            ],
        )

    def _get_manifest(self, connection: sqlite3.Connection, workload_id: str) -> dict:
        row = connection.execute(
            "SELECT manifest_json FROM workloads WHERE workload_id = ?", (workload_id,)
        ).fetchone()
        if row is None:
            raise ReceiptNotFoundError("Workload is not registered")
        return json.loads(row["manifest_json"])

    def get_manifest(self, workload_id: UUID | str) -> dict:
        """Read a registered manifest without a workload token."""
        with self._open_transaction() as connection:
            return self._get_manifest(connection, str(workload_id))

    def _restore_admissions(self, connection: sqlite3.Connection, route: str) -> None:
        legacy = connection.execute(
            "SELECT workload_id FROM pending_admissions WHERE route = ?", (route,)
        ).fetchall()
        for row in legacy:
            # A legacy manifest may have been dispatched, even when every result was accepted.
            self._insert_admission(connection, row["workload_id"], route, "submitted")

    def _insert_admission(
        self, connection: sqlite3.Connection, workload_id: str, route: str, state: str
    ) -> None:
        connection.execute(
            "INSERT INTO admissions (workload_id, route, state) VALUES (?, ?, ?)",
            (workload_id, route, state),
        )
        connection.execute("DELETE FROM pending_admissions WHERE workload_id = ?", (workload_id,))

    def _get_admissions(
        self, connection: sqlite3.Connection, route: str, *, include_released: bool = False
    ) -> list[dict]:
        rows = connection.execute(
            "SELECT a.workload_id, a.route, a.state, w.manifest_json FROM admissions a "
            "JOIN workloads w ON w.workload_id = a.workload_id WHERE a.route = ? "
            + ("" if include_released else "AND a.state IN ('reserved', 'submitted') ")
            + "ORDER BY a.rowid",
            (route,),
        ).fetchall()
        return [
            {
                "workload_id": row["workload_id"],
                "route": row["route"],
                "state": row["state"],
                "manifest": json.loads(row["manifest_json"]),
            }
            for row in rows
        ]

    def get_admissions(self, route: str, include_released: bool = False) -> list[dict]:
        """Read durable admission records for a route."""
        with self._open_transaction() as connection:
            return self._get_admissions(connection, route, include_released=include_released)

    def restore_admissions(self, route: str) -> list[dict]:
        """Conservatively charge legacy manifests before reconstructing route capacity."""
        if not isinstance(route, str) or not route.strip():
            raise ValueError("Parsing admission requires an explicit route")
        with self._open_transaction() as connection:
            self._restore_admissions(connection, route)
            return self._get_admissions(connection, route)

    def reserve_workload(self, workload: ParseDagDefinitions, *, route: str, capacity: int) -> dict:
        """Atomically register and charge one batch against the trusted route capacity."""
        if not isinstance(route, str) or not route.strip() or workload.queue != route:
            raise ReceiptConflictError("Parsing route must match the workload's explicit queue")
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("Parsing capacity must be a positive integer")
        workload_id = str(workload.workload_id)
        with self._open_transaction() as connection:
            self._restore_admissions(connection, route)
            existing = connection.execute(
                "SELECT route, state FROM admissions WHERE workload_id = ?", (workload_id,)
            ).fetchone()
            if existing:
                if existing["route"] != route or existing["state"] == "released":
                    raise ReceiptConflictError("Workload admission cannot change route or reuse a release")
                self._register_workload(connection, workload)
            else:
                active = connection.execute(
                    "SELECT COUNT(*) FROM admissions WHERE route = ? AND state IN ('reserved', 'submitted')",
                    (route,),
                ).fetchone()[0]
                if active >= capacity:
                    raise ReceiptCapacityError("Parsing route has no unreserved capacity")
                self._register_workload(connection, workload)
                self._insert_admission(connection, workload_id, route, "reserved")
            return next(
                row for row in self._get_admissions(connection, route) if row["workload_id"] == workload_id
            )

    def mark_submitted(self, workload_id: UUID | str) -> dict:
        """Persist possible submission before provider I/O; a restart must not redispatch it."""
        workload_id = str(workload_id)
        with self._open_transaction() as connection:
            admission = connection.execute(
                "SELECT route, state FROM admissions WHERE workload_id = ?", (workload_id,)
            ).fetchone()
            if admission is None:
                raise ReceiptNotFoundError("Workload has no admission record")
            if admission["state"] == "released":
                raise ReceiptConflictError("Released admission cannot be submitted again")
            manifest = self._get_manifest(connection, workload_id)
            if admission["state"] == "reserved" and datetime.now(timezone.utc) >= datetime.fromisoformat(
                manifest["start_deadline"].replace("Z", "+00:00")
            ):
                raise ReceiptExpiredError("Unsubmitted workload start deadline has elapsed")
            connection.execute(
                "UPDATE admissions SET state = 'submitted' WHERE workload_id = ?", (workload_id,)
            )
            return next(
                row
                for row in self._get_admissions(connection, admission["route"])
                if row["workload_id"] == workload_id
            )

    def retire_expired_reservation(self, workload_id: UUID | str) -> bool:
        """Fence expired, unclaimed work that has never entered the submission path."""
        workload_id = str(workload_id)
        with self._open_transaction() as connection:
            admission = connection.execute(
                "SELECT state FROM admissions WHERE workload_id = ?", (workload_id,)
            ).fetchone()
            if admission is None:
                raise ReceiptNotFoundError("Workload has no admission record")
            if admission["state"] != "reserved":
                return False
            manifest = self._get_manifest(connection, workload_id)
            if datetime.now(timezone.utc) < datetime.fromisoformat(
                manifest["start_deadline"].replace("Z", "+00:00")
            ):
                return False
            if connection.execute(
                "SELECT 1 FROM attempts WHERE workload_id = ? AND execution_id IS NOT NULL LIMIT 1",
                (workload_id,),
            ).fetchone():
                raise ReceiptConflictError("Claimed work requires confirmed termination before release")
            connection.execute(
                "INSERT INTO retired_attempts (workload_id, attempt_id) "
                "SELECT workload_id, attempt_id FROM attempts WHERE workload_id = ?",
                (workload_id,),
            )
            connection.execute(
                "UPDATE admissions SET state = 'released' WHERE workload_id = ?", (workload_id,)
            )
            return True

    def retire_and_replace(
        self,
        workload_id: UUID | str,
        *,
        termination: dict,
        start_deadline: datetime,
        stop_deadline: datetime,
    ) -> dict:
        """Fence unfinished attempts after trusted termination and transfer their capacity.

        This PoC bounds fresh queue wait and execution window by the original execution window.
        Repeated recovery returns the persisted decision without extending either deadline.
        """
        workload_id = str(workload_id)
        if (
            not isinstance(termination, dict)
            or termination.get("kind") != "confirmed_worker_termination"
            or termination.get("workload_id") != workload_id
            or not isinstance(termination.get("execution_ids"), list)
            or any(not isinstance(value, str) or not value for value in termination["execution_ids"])
            or not isinstance(termination.get("evidence"), dict)
            or not termination["evidence"]
        ):
            raise ReceiptConflictError("Recovery requires matching confirmed worker termination evidence")
        termination_json = _encode_json(termination)
        with self._open_transaction() as connection:
            previous = connection.execute(
                "SELECT decision_json FROM recovery_decisions WHERE workload_id = ?", (workload_id,)
            ).fetchone()
            if previous:
                return json.loads(previous["decision_json"])
            manifest = self._get_manifest(connection, workload_id)
            route = manifest.get("queue")
            if not isinstance(route, str) or not route.strip():
                raise ReceiptConflictError("Recovery requires an explicit parsing route")
            self._restore_admissions(connection, route)
            admission = connection.execute(
                "SELECT route, state FROM admissions WHERE workload_id = ?", (workload_id,)
            ).fetchone()
            if admission["route"] != route or admission["state"] == "released":
                raise ReceiptConflictError("Recovery requires an active admission on the registered route")
            attempts = connection.execute(
                "SELECT attempt_id, execution_id, result_json FROM attempts WHERE workload_id = ? ORDER BY rowid",
                (workload_id,),
            ).fetchall()
            unfinished = [row for row in attempts if row["result_json"] is None]
            owners = {row["execution_id"] for row in unfinished if row["execution_id"]}
            if not owners.issubset(set(termination["execution_ids"])):
                raise ReceiptConflictError("Termination evidence does not cover every unfinished execution")
            replacement = None
            retired = {row["attempt_id"] for row in unfinished}
            retry_definitions = self._get_retry_definitions(
                connection,
                workload_id,
                [definition for definition in manifest["definitions"] if definition["attempt_id"] in retired],
            )
            if retry_definitions:
                original = ParseDagDefinitions.model_validate(manifest | {"token": "not-issued"})
                window = original.stop_deadline - original.start_deadline
                now = datetime.now(timezone.utc)
                if (
                    start_deadline.utcoffset() is None
                    or stop_deadline.utcoffset() is None
                    or not now < start_deadline < stop_deadline
                    or start_deadline - now > window
                    or stop_deadline - start_deadline > window
                ):
                    raise ReceiptConflictError("Replacement deadlines must be future, aware and bounded")
                replacement_workload = ParseDagDefinitions.model_validate(
                    manifest
                    | {
                        "workload_id": uuid4(),
                        "token": "not-issued",
                        "start_deadline": start_deadline,
                        "stop_deadline": stop_deadline,
                        "definitions": [
                            definition | {"attempt_id": uuid4()} for definition in retry_definitions
                        ],
                    }
                )
                self._register_workload(connection, replacement_workload)
                replacement = replacement_workload.model_dump(mode="json", exclude={"token"})
                self._insert_admission(connection, str(replacement_workload.workload_id), route, "reserved")
            if unfinished:
                connection.executemany(
                    "INSERT INTO retired_attempts (workload_id, attempt_id) VALUES (?, ?)",
                    [(workload_id, row["attempt_id"]) for row in unfinished],
                )
            connection.execute(
                "UPDATE admissions SET state = 'released' WHERE workload_id = ?", (workload_id,)
            )
            decision = {
                "workload_id": workload_id,
                "replacement": replacement,
                "retired_attempt_ids": [row["attempt_id"] for row in unfinished],
            }
            connection.execute(
                "INSERT INTO recovery_decisions (workload_id, termination_json, decision_json) VALUES (?, ?, ?)",
                (workload_id, termination_json, _encode_json(decision)),
            )
            return decision

    def _get_retry_definitions(
        self, connection: sqlite3.Connection, workload_id: str, definitions: list[dict]
    ) -> list[dict]:
        """Allow a metadata sink to exclude definitions superseded before recovery."""
        return definitions

    def _get_attempt(self, connection: sqlite3.Connection, workload_id: str, attempt_id: str) -> sqlite3.Row:
        attempt = connection.execute(
            "SELECT a.*, r.attempt_id AS retired FROM attempts a LEFT JOIN retired_attempts r "
            "ON a.workload_id = r.workload_id AND a.attempt_id = r.attempt_id "
            "WHERE a.workload_id = ? AND a.attempt_id = ?",
            (workload_id, attempt_id),
        ).fetchone()
        if attempt is None:
            raise ReceiptNotFoundError("Attempt is not registered for this workload")
        return attempt

    def claim(self, workload_id: UUID | str, attempt_id: UUID | str, execution_id: UUID | str) -> dict:
        workload_id, attempt_id, execution_id = map(str, (workload_id, attempt_id, execution_id))
        with self._open_transaction() as connection:
            attempt = self._get_attempt(connection, workload_id, attempt_id)
            if attempt["result_json"] is not None:
                return {"execution_id": attempt["execution_id"], "status": "accepted"}
            if attempt["retired"]:
                raise ReceiptConflictError("Attempt was retired by recovery")
            if attempt["execution_id"] not in (None, execution_id):
                raise ReceiptConflictError("Another execution already claimed this attempt")
            now = datetime.now(timezone.utc).timestamp()
            if now >= attempt["stop_deadline"]:
                raise ReceiptExpiredError("Attempt stop deadline has elapsed")
            if attempt["execution_id"] == execution_id:
                return {"execution_id": execution_id, "status": "already_claimed"}
            if now >= attempt["start_deadline"]:
                raise ReceiptExpiredError("Attempt start deadline has elapsed")
            connection.execute(
                "UPDATE attempts SET execution_id = ? WHERE workload_id = ? AND attempt_id = ?",
                (execution_id, workload_id, attempt_id),
            )
            return {"execution_id": execution_id, "status": "claimed"}

    def accept_result(
        self,
        workload_id: UUID | str,
        attempt_id: UUID | str,
        execution_id: UUID | str,
        result: DagDefinitionResult,
    ) -> dict:
        workload_id, attempt_id, execution_id = map(str, (workload_id, attempt_id, execution_id))
        try:
            result_json = _encode_json(result.model_dump(mode="json"))
        except ValueError as error:
            raise ReceiptInvalidResultError(
                "Serialized results must contain only finite JSON values"
            ) from error
        digest = hashlib.sha256(result_json.encode()).hexdigest()
        with self._open_transaction() as connection:
            attempt = self._get_attempt(connection, workload_id, attempt_id)
            if attempt["execution_id"] != execution_id:
                raise ReceiptConflictError("Result does not belong to the claimed execution")
            expected = json.loads(attempt["definition_json"])
            if (
                str(result.attempt_id) != attempt_id
                or result.relative_path != expected["relative_path"]
                or result.source_revision != expected["source_revision"]
            ):
                raise ReceiptConflictError("Result identity or source revision differs from registration")
            if attempt["result_json"] is not None:
                if attempt["result_json"] != result_json:
                    raise ReceiptConflictError("An accepted result cannot be changed")
            else:
                if attempt["retired"]:
                    raise ReceiptConflictError("Attempt was retired by recovery")
                if datetime.now(timezone.utc).timestamp() >= attempt["stop_deadline"]:
                    raise ReceiptExpiredError("Attempt stop deadline has elapsed")
                self._persist_result(connection, workload_id, result)
                connection.execute(
                    "UPDATE attempts SET result_json = ?, digest = ? WHERE workload_id = ? AND attempt_id = ?",
                    (result_json, digest, workload_id, attempt_id),
                )
            return {"attempt_id": attempt_id, "status": "accepted", "digest": digest}

    def _persist_result(
        self, connection: sqlite3.Connection, workload_id: str, result: DagDefinitionResult
    ) -> None:
        """Optional metadata sink, called only on first acceptance within the receipt transaction."""

    def get_results(self, workload_id: UUID | str) -> list[dict]:
        """Read accepted result envelopes for the local experiment driver."""
        with self._open_transaction() as connection:
            results = connection.execute(
                "SELECT result_json FROM attempts WHERE workload_id = ? AND result_json IS NOT NULL "
                "ORDER BY rowid",
                (str(workload_id),),
            ).fetchall()
            return [json.loads(row["result_json"]) for row in results]

    def get_attempts(self, workload_id: UUID | str) -> list[dict]:
        """Report attempt authority and receipts without exposing workload credentials."""
        with self._open_transaction() as connection:
            attempts = connection.execute(
                "SELECT a.attempt_id, a.execution_id, a.digest, r.attempt_id AS retired FROM attempts a "
                "LEFT JOIN retired_attempts r ON a.workload_id = r.workload_id AND a.attempt_id = r.attempt_id "
                "WHERE a.workload_id = ? ORDER BY a.rowid",
                (str(workload_id),),
            ).fetchall()
            return [
                {
                    "attempt_id": row["attempt_id"],
                    "execution_id": row["execution_id"],
                    "status": "accepted"
                    if row["digest"]
                    else "retired"
                    if row["retired"]
                    else "claimed"
                    if row["execution_id"]
                    else "pending",
                    "digest": row["digest"],
                }
                for row in attempts
            ]
