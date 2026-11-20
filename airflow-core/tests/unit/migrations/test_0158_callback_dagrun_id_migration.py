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
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow"
    / "migrations"
    / "versions"
    / "0158_3_5_0_add_dagrun_id_to_callback.py"
)

_spec = importlib.util.spec_from_file_location("migration_0158", _MIGRATION_PATH)
assert _spec is not None
_migration = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_migration)

REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
_DATABASE_CALLBACK_ID = uuid.UUID("00000000-0000-0000-0000-000000001580")
_DATABASE_DAG_ID = "test_0158_callback_dagrun_id"
_DATABASE_RUN_ID = "test_0158_callback_dagrun_id_run"


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
    deadline_triggerer_id = uuid.uuid4().hex
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
        _insert_callback(conn, deadline_triggerer_id, {"path": "callback.without.dagrun.id"}, "triggerer")
        _insert_deadline(conn, deadline_triggerer_id, 2)
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
    assert dagrun_id_by_callback_id[deadline_triggerer_id] == 2
    assert dagrun_id_by_callback_id[invalid_id] is None
    missing_dagrun_id = uuid.uuid4().hex
    with engine.begin() as conn:
        _insert_callback(conn, missing_dagrun_id, {"dag_run_id": 999})
        _migration._backfill_dagrun_id(conn, batch_size=2)
        assert (
            conn.scalar(sa.text("SELECT dagrun_id FROM callback WHERE id = :id"), {"id": missing_dagrun_id})
            is None
        )
    assert dagrun_id_by_callback_id[triggerer_id] is None
    assert dagrun_id_by_callback_id[malformed_id] is None


def test_restore_legacy_dagrun_id_for_downgrade():
    engine = _make_engine()
    wrapped_id = uuid.uuid4().hex
    plain_id = uuid.uuid4().hex

    with engine.begin() as conn:
        _create_tables(conn)
        _insert_callback(conn, wrapped_id, {"__var": {"path": "callback.path"}, "__type": "dict"})
        _insert_callback(conn, plain_id, {"path": "callback.path"})
        conn.execute(
            sa.text("UPDATE callback SET dagrun_id = CASE id WHEN :wrapped_id THEN 1 ELSE 2 END"),
            {"wrapped_id": wrapped_id},
        )

        _migration._restore_legacy_dagrun_id(conn, batch_size=1)
        rows = conn.execute(sa.text("SELECT id, data FROM callback")).mappings().all()

    data_by_callback_id = {row["id"]: json.loads(row["data"]) for row in rows}
    assert data_by_callback_id[wrapped_id]["__var"]["dag_run_id"] == "1"
    assert data_by_callback_id[plain_id]["dag_run_id"] == "2"


def test_restore_legacy_dagrun_id_uses_jsonb_with_psycopg():
    callback = sa.table(
        "callback",
        sa.column("id", sa.Uuid()),
        sa.column("data", _migration._CALLBACK_DATA_TYPE),
    )

    statement = callback.update().where(callback.c.id == uuid.uuid4()).values(data={"dag_run_id": "1"})

    assert "::JSONB" in str(statement.compile(dialect=PGDialect_psycopg()))


@pytest.mark.parametrize(
    ("migration_name", "warning"),
    [
        (
            "upgrade",
            "-- WARNING: callback.dagrun_id backfill is skipped in offline mode; "
            "existing pending executor callbacks without dagrun_id will not run.",
        ),
        (
            "downgrade",
            "-- WARNING: callback.data restoration of dag_run_id is skipped in offline mode; "
            "downgrading loses dagrun_id from existing executor callbacks.",
        ),
    ],
)
def test_offline_migration_emits_callback_data_warning(migration_name, warning):
    output = StringIO()
    operations = Operations(
        MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    )

    with (
        patch.object(_migration, "op", operations),
        patch.object(_migration.context, "is_offline_mode", return_value=True),
    ):
        getattr(_migration, migration_name)()

    assert warning in output.getvalue()


def _delete_database_test_rows(engine) -> None:
    metadata = sa.MetaData()
    callback = sa.Table("callback", metadata, autoload_with=engine)
    dag_run = sa.Table("dag_run", metadata, autoload_with=engine)
    with engine.begin() as conn:
        conn.execute(callback.delete().where(callback.c.id == _DATABASE_CALLBACK_ID))
        conn.execute(dag_run.delete().where(dag_run.c.dag_id == _DATABASE_DAG_ID))


@pytest.fixture
def database_at_previous_revision():
    """Move the database to the revision before this migration, then restore the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        _delete_database_test_rows(settings.engine)
        yield settings.engine
    finally:
        try:
            upgradedb()
            _delete_database_test_rows(settings.engine)
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


@pytest.mark.db_test
@pytest.mark.backend("postgres", "mysql")
def test_downgrade_restores_callback_data(database_at_previous_revision):
    metadata = sa.MetaData()
    callback = sa.Table("callback", metadata, autoload_with=database_at_previous_revision)
    dag_run = sa.Table("dag_run", metadata, autoload_with=database_at_previous_revision)
    with database_at_previous_revision.begin() as conn:
        dagrun_id = conn.execute(
            dag_run.insert().values(
                dag_id=_DATABASE_DAG_ID,
                run_id=_DATABASE_RUN_ID,
                run_type="manual",
                run_after=timezone.utcnow(),
            )
        ).inserted_primary_key[0]
        conn.execute(
            callback.insert().values(
                id=_DATABASE_CALLBACK_ID,
                type="executor",
                fetch_method="import_path",
                data={"__var": {"path": "callback.path", "dag_run_id": str(dagrun_id)}},
                state="pending",
                priority_weight=1,
                created_at=timezone.utcnow(),
            )
        )

    upgradedb(to_revision=REVISION)
    downgrade(to_revision=PREVIOUS_REVISION)

    metadata = sa.MetaData()
    callback = sa.Table("callback", metadata, autoload_with=database_at_previous_revision)
    with database_at_previous_revision.connect() as conn:
        data = conn.scalar(sa.select(callback.c.data).where(callback.c.id == _DATABASE_CALLBACK_ID))

    if isinstance(data, str):
        data = json.loads(data)
    assert data == {"__var": {"path": "callback.path", "dag_run_id": str(dagrun_id)}}


def test_sqlite_upgrade_and_downgrade_preserve_deadline_callback():
    engine = _make_engine()
    callback_id = uuid.uuid4()
    deadline_id = uuid.uuid4()

    with engine.connect() as conn:
        conn.execute(sa.text("PRAGMA foreign_keys=ON"))
        conn.execute(sa.text("CREATE TABLE dag_run (id INTEGER PRIMARY KEY)"))
        conn.execute(
            sa.text("CREATE TABLE callback (id TEXT PRIMARY KEY, type TEXT NOT NULL, data TEXT NOT NULL)")
        )
        conn.execute(
            sa.text(
                "CREATE TABLE deadline ("
                "id TEXT PRIMARY KEY, "
                "callback_id TEXT NOT NULL REFERENCES callback(id) ON DELETE CASCADE, "
                "dagrun_id INTEGER"
                ")"
            )
        )
        conn.execute(sa.text("INSERT INTO dag_run (id) VALUES (1)"))
        conn.execute(
            sa.text("INSERT INTO callback (id, type, data) VALUES (:id, :type, :data)"),
            {"id": callback_id.hex, "type": "executor", "data": json.dumps({"dag_run_id": "1"})},
        )
        conn.execute(
            sa.text("INSERT INTO deadline (id, callback_id, dagrun_id) VALUES (:id, :callback_id, 1)"),
            {"id": str(deadline_id), "callback_id": callback_id.hex},
        )
        conn.commit()

        operations = Operations(MigrationContext.configure(conn))
        with (
            patch.object(_migration, "op", operations),
            patch.object(_migration.context, "is_offline_mode", return_value=False),
        ):
            _migration.upgrade()
            assert (
                conn.scalar(sa.text("SELECT COUNT(*) FROM deadline WHERE id = :id"), {"id": str(deadline_id)})
                == 1
            )
            assert (
                conn.scalar(sa.text("SELECT dagrun_id FROM callback WHERE id = :id"), {"id": callback_id.hex})
                == 1
            )
            assert "WHERE dagrun_id IS NOT NULL" in conn.scalar(
                sa.text(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'callback_dagrun_id_idx'"
                )
            )

            _migration.downgrade()

        assert (
            conn.scalar(sa.text("SELECT COUNT(*) FROM deadline WHERE id = :id"), {"id": str(deadline_id)})
            == 1
        )
