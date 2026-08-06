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

from __future__ import annotations

import importlib.util
import json
import uuid
from pathlib import Path

import sqlalchemy as sa

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow"
    / "migrations"
    / "versions"
    / "0132_3_4_0_add_dagrun_id_to_callback.py"
)

_spec = importlib.util.spec_from_file_location("migration_0132", _MIGRATION_PATH)
assert _spec is not None
_migration = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_migration)


def _make_engine():
    return sa.create_engine("sqlite:///:memory:")


def _create_tables(conn):
    conn.execute(sa.text("CREATE TABLE dag_run (id INTEGER PRIMARY KEY)"))
    conn.execute(
        sa.text(
            """
            CREATE TABLE callback (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                data TEXT NOT NULL,
                dagrun_id INTEGER
            )
            """
        )
    )
    conn.execute(
        sa.text(
            """
            CREATE TABLE deadline (
                id TEXT PRIMARY KEY,
                callback_id TEXT NOT NULL,
                dagrun_id INTEGER
            )
            """
        )
    )


def _insert_callback(conn, callback_id: str, data: dict, callback_type: str = "executor"):
    conn.execute(
        sa.text("INSERT INTO callback (id, type, data) VALUES (:id, :type, :data)"),
        {"id": callback_id, "type": callback_type, "data": json.dumps(data)},
    )


def _insert_deadline(conn, callback_id: str, dagrun_id: int):
    conn.execute(
        sa.text("INSERT INTO deadline (id, callback_id, dagrun_id) VALUES (:id, :callback_id, :dagrun_id)"),
        {"id": uuid.uuid4().hex, "callback_id": callback_id, "dagrun_id": dagrun_id},
    )


def test_backfill_dagrun_id_from_legacy_callback_data():
    engine = _make_engine()
    extended_id = uuid.uuid4().hex
    plain_id = uuid.uuid4().hex
    deadline_id = uuid.uuid4().hex
    invalid_id = uuid.uuid4().hex
    triggerer_id = uuid.uuid4().hex
    malformed_id = uuid.uuid4().hex

    with engine.begin() as conn:
        _create_tables(conn)
        conn.execute(sa.text("INSERT INTO dag_run (id) VALUES (1), (2)"))
        _insert_callback(conn, extended_id, {"__var": {"dag_run_id": "1"}, "__type": "dict"})
        _insert_callback(conn, plain_id, {"dag_run_id": 2})
        _insert_callback(conn, deadline_id, {"path": "callback.without.dagrun.id"})
        _insert_deadline(conn, deadline_id, 1)
        _insert_callback(conn, invalid_id, {"__var": {"dag_run_id": "missing"}, "__type": "dict"})
        _insert_callback(conn, triggerer_id, {"__var": {"dag_run_id": "1"}, "__type": "dict"}, "triggerer")
        conn.execute(
            sa.text("INSERT INTO callback (id, type, data) VALUES (:id, :type, :data)"),
            {"id": malformed_id, "type": "executor", "data": "not-json"},
        )

        _migration._backfill_dagrun_id_from_deadline(conn, batch_size=2)
        _migration._backfill_dagrun_id(conn, batch_size=2)

        rows = conn.execute(sa.text("SELECT id, dagrun_id FROM callback")).mappings().all()

    dagrun_id_by_callback_id = {row["id"]: row["dagrun_id"] for row in rows}
    assert dagrun_id_by_callback_id[extended_id] == 1
    assert dagrun_id_by_callback_id[plain_id] == 2
    assert dagrun_id_by_callback_id[deadline_id] == 1
    assert dagrun_id_by_callback_id[invalid_id] is None
    assert dagrun_id_by_callback_id[triggerer_id] is None
    assert dagrun_id_by_callback_id[malformed_id] is None
