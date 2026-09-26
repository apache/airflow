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
"""SQLite-only metadata ingestion experiment; not a production Execution API."""

from __future__ import annotations

import copy
import hashlib
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, cast

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.dag_processing.orchestrator import OrchestrationStore
from airflow.dag_processing.parsing_state import ReceiptConflictError, ReceiptInvalidResultError, ReceiptStore
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagwarning import DagWarning
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.sqlalchemy import prohibit_commit

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.executors.workloads.parsing import DagDefinitionResult, ParseDagDefinitions


class MetadataConnection(sqlite3.Connection):
    """Receipt SQL and the ORM session share this physical connection."""

    session: Session


class MetadataReceiptStore(ReceiptStore):
    """Commit result receipts and Airflow metadata together in one SQLite file."""

    def __init__(self, path: str | Path):
        self.engine = create_engine(
            "sqlite:///" + str(Path(path).resolve()),
            connect_args={"factory": MetadataConnection, "check_same_thread": False, "timeout": 5},
            poolclass=NullPool,
        )
        super().__init__(path)
        with self._open_transaction() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS current_definitions ("
                "bundle_name TEXT NOT NULL, relative_path TEXT NOT NULL, "
                "workload_id TEXT NOT NULL, attempt_id TEXT NOT NULL, "
                "PRIMARY KEY (bundle_name, relative_path), "
                "FOREIGN KEY (workload_id, attempt_id) REFERENCES attempts(workload_id, attempt_id))"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS current_definitions_workload ON current_definitions(workload_id)"
            )

    @contextmanager
    def _open_transaction(self) -> Iterator[MetadataConnection]:
        with self.engine.connect() as connection:
            raw = cast("MetadataConnection", connection.connection.driver_connection)
            raw.row_factory = sqlite3.Row
            connection.exec_driver_sql("PRAGMA foreign_keys = ON")
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            with Session(bind=connection, expire_on_commit=False) as session, prohibit_commit(session):
                raw.session = session
                try:
                    yield raw
                    session.flush()
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise

    def _register_workload(self, connection: sqlite3.Connection, workload: ParseDagDefinitions) -> None:
        existing = connection.execute(
            "SELECT 1 FROM workloads WHERE workload_id = ?", (str(workload.workload_id),)
        ).fetchone()
        if len({item.relative_path for item in workload.definitions}) != len(workload.definitions):
            raise ReceiptConflictError("Metadata registration requires distinct definition paths")
        super()._register_workload(connection, workload)
        if existing:
            return
        for definition in workload.definitions:
            connection.execute(
                "INSERT INTO current_definitions VALUES (?, ?, ?, ?) "
                "ON CONFLICT (bundle_name, relative_path) DO UPDATE SET "
                "workload_id = excluded.workload_id, attempt_id = excluded.attempt_id",
                (
                    workload.bundle_info.name,
                    definition.relative_path,
                    str(workload.workload_id),
                    str(definition.attempt_id),
                ),
            )

    def _get_retry_definitions(
        self, connection: sqlite3.Connection, workload_id: str, definitions: list[dict]
    ) -> list[dict]:
        current = {
            row["attempt_id"]
            for row in connection.execute(
                "SELECT attempt_id FROM current_definitions WHERE workload_id = ?", (workload_id,)
            )
        }
        return [definition for definition in definitions if definition["attempt_id"] in current]

    def _persist_result(
        self, connection: sqlite3.Connection, workload_id: str, result: DagDefinitionResult
    ) -> None:
        manifest = self._get_manifest(connection, workload_id)
        bundle_name, bundle_version = manifest["bundle_info"]["name"], manifest["bundle_info"]["version"]
        current = connection.execute(
            "SELECT workload_id, attempt_id FROM current_definitions "
            "WHERE bundle_name = ? AND relative_path = ?",
            (bundle_name, result.relative_path),
        ).fetchone()
        if current is None or (current["workload_id"], current["attempt_id"]) != (
            workload_id,
            str(result.attempt_id),
        ):
            raise ReceiptConflictError("A newer registration superseded this definition")
        session = cast("MetadataConnection", connection).session
        bundle = session.get(DagBundleModel, bundle_name)
        if bundle is None or not bundle.active or bundle.version != bundle_version:
            raise ReceiptConflictError("Bundle is absent, inactive or no longer at the registered version")
        if result.serialized_dags:
            if (
                result.source_code is None
                or hashlib.sha256(result.source_code.encode()).hexdigest() != result.source_revision
            ):
                raise ReceiptInvalidResultError(
                    "Serialized Dags require source text matching the registered revision"
                )
        dags = []
        for payload in result.serialized_dags:
            data = copy.deepcopy(payload)
            dag_data = data.get("dag", {})
            if (
                not isinstance(dag_data, dict)
                or dag_data.get("relative_fileloc") != result.relative_path
                or not dag_data.get("dag_id")
            ):
                raise ReceiptInvalidResultError("Serialized Dag does not identify the registered definition")
            dag_data["fileloc"] = result.relative_path
            dags.append(LazyDeserializedDAG(data=data))
        if len({dag.dag_id for dag in dags}) != len(dags):
            raise ReceiptInvalidResultError("Serialized Dag IDs must be distinct")
        for existing in session.scalars(
            select(DagModel).where(DagModel.dag_id.in_([dag.dag_id for dag in dags]))
        ):
            if (existing.bundle_name, existing.relative_fileloc) != (bundle_name, result.relative_path):
                raise ReceiptConflictError("Dag ID belongs to another registered definition")
        if any(path != result.relative_path for path in result.import_errors):
            raise ReceiptInvalidResultError("Import errors must belong to the registered definition")
        import_errors = {(bundle_name, path): error for path, error in result.import_errors.items()}
        if result.outcome in {"timeout", "worker_error"}:
            import_errors[(bundle_name, result.relative_path)] = (
                "\n".join(result.diagnostics) or result.outcome
            )
        try:
            warnings = set()
            for warning in result.warnings:
                if not isinstance(warning, dict) or any(
                    not isinstance(value, str) for value in warning.values()
                ):
                    raise TypeError("Serialized Dag warning must contain string fields")
                warnings.add(DagWarning(**cast("dict[str, str]", warning)))
        except (TypeError, ValueError) as error:
            raise ReceiptInvalidResultError("Invalid serialized Dag warning") from error
        if any(warning.dag_id not in {dag.dag_id for dag in dags} for warning in warnings):
            raise ReceiptInvalidResultError("Warning belongs to another definition")
        update_dag_parsing_results_in_db(
            bundle_name,
            bundle_version,
            dags,
            import_errors,
            result.duration_seconds,
            warnings,
            session=session,
            version_data=manifest["bundle_info"].get("version_data"),
            files_parsed={(bundle_name, result.relative_path)},
            atomic=True,
            source_codes={dag.dag_id: result.source_code for dag in dags if result.source_code is not None},
        )


class MetadataOrchestrationStore(OrchestrationStore, MetadataReceiptStore):
    """Commit source scheduling, receipts and Airflow metadata in one SQLite transaction."""
