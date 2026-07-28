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

"""
Regression tests for migration 0094 (e812941398f4).

These tests focus on the defensive NULL-callback path: legacy MySQL
deployments that ran the original (buggy) 0080 left ``deadline.callback``
rows as NULL. 0094 must heal those rows instead of crashing on
``json.loads(None)``.
"""

from __future__ import annotations

import importlib.util
import json
import uuid
from pathlib import Path

import sqlalchemy as sa

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0094_3_2_0_replace_deadline_inline_callback_with_fkey.py"
)
_spec = importlib.util.spec_from_file_location("migration_0094", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]


# Minimal post-0080 / pre-0094 schema. 0094 adds ``missed`` and ``callback_id``
# itself before invoking ``_upgrade_mysql_sqlite``; we mimic that here so we
# can call the inner helper directly without driving the full alembic chain.
_POST_0080_DDL = [
    """
    CREATE TABLE dag_run (
        id      INTEGER PRIMARY KEY,
        dag_id  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE deadline (
        id              TEXT    PRIMARY KEY,
        dagrun_id       INTEGER NOT NULL,
        deadline_time   TEXT    NOT NULL,
        callback        TEXT,
        callback_state  TEXT,
        trigger_id      INTEGER,
        callback_id     TEXT,
        missed          BOOLEAN
    )
    """,
    """
    CREATE TABLE callback (
        id              TEXT    PRIMARY KEY,
        type            TEXT    NOT NULL,
        fetch_method    TEXT    NOT NULL,
        data            TEXT    NOT NULL,
        state           TEXT    NOT NULL,
        priority_weight INTEGER NOT NULL,
        created_at      TEXT    NOT NULL
    )
    """,
]


def _make_engine():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        for ddl in _POST_0080_DDL:
            conn.execute(sa.text(ddl))
        conn.commit()
    return engine


def _insert_dagrun(conn, dagrun_id: int = 1, dag_id: str = "test_dag"):
    conn.execute(
        sa.text("INSERT INTO dag_run (id, dag_id) VALUES (:id, :dag_id)"),
        {"id": dagrun_id, "dag_id": dag_id},
    )


def _insert_deadline(conn, deadline_id: str, callback, callback_state: str | None = None):
    conn.execute(
        sa.text(
            "INSERT INTO deadline (id, dagrun_id, deadline_time, callback, callback_state)"
            " VALUES (:id, 1, '2025-01-01', :cb, :state)"
        ),
        {"id": deadline_id, "cb": callback, "state": callback_state},
    )


class TestMigration0094NullCallbackRepair:
    """A NULL callback row from a buggy 0080 must not crash 0094's upgrade."""

    def test_null_callback_does_not_crash(self):
        engine = _make_engine()
        # `_upgrade_mysql_sqlite` declares ``id`` as ``sa.Uuid()``; on SQLite the
        # write path emits the hex (no-dash) form. Insert IDs in that same form so
        # the UPDATE in the migration loop matches the row we created.
        deadline_id = uuid.uuid4().hex
        with engine.begin() as conn:
            _insert_dagrun(conn)
            _insert_deadline(conn, deadline_id, callback=None)

        # _upgrade_mysql_sqlite reads from `deadline` and writes to `callback`;
        # it does not depend on alembic's batch_alter_table prelude.
        with engine.begin() as conn:
            _migration._upgrade_mysql_sqlite(conn, batch_size=10)

        with engine.connect() as conn:
            deadline_rows = conn.execute(sa.text("SELECT * FROM deadline")).mappings().all()
            callback_rows = conn.execute(sa.text("SELECT * FROM callback")).mappings().all()

        assert len(deadline_rows) == 1
        assert len(callback_rows) == 1
        assert deadline_rows[0]["callback_id"] == callback_rows[0]["id"]
        assert deadline_rows[0]["missed"] == 0  # SQLite: False -> 0

        cb_data = json.loads(callback_rows[0]["data"])
        assert cb_data["path"] == ""
        assert cb_data["kwargs"] == {}
        assert cb_data["dag_id"] == "test_dag"

    def test_mixed_null_and_valid_callbacks(self):
        """A batch with both NULL and well-formed rows must migrate both."""
        engine = _make_engine()
        null_id = uuid.uuid4().hex
        valid_id = uuid.uuid4().hex
        valid_callback = json.dumps(
            {
                "__data__": {"path": "mymodule.cb", "kwargs": {"k": "v"}},
                "__classname__": "airflow.sdk.definitions.deadline.AsyncCallback",
                "__version__": 0,
            }
        )
        with engine.begin() as conn:
            _insert_dagrun(conn)
            _insert_deadline(conn, null_id, callback=None)
            _insert_deadline(conn, valid_id, callback=valid_callback)

        with engine.begin() as conn:
            _migration._upgrade_mysql_sqlite(conn, batch_size=10)

        with engine.connect() as conn:
            rows = (
                conn.execute(
                    sa.text(
                        "SELECT d.id AS deadline_id, c.data"
                        " FROM deadline d JOIN callback c ON d.callback_id = c.id"
                    )
                )
                .mappings()
                .all()
            )

        by_id = {r["deadline_id"]: json.loads(r["data"]) for r in rows}
        assert by_id[null_id]["path"] == ""
        assert by_id[null_id]["kwargs"] == {}
        assert by_id[valid_id]["path"] == "mymodule.cb"
        assert by_id[valid_id]["kwargs"] == {"k": "v"}
