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
Tests for the stored map index rename migration, written against the schema as it stood at that revision.

The tables are declared here instead of taken from ``airflow.models``, and alembic moves the database
to the revision, so later migrations and model changes cannot change what these tests exercise.
"""

from __future__ import annotations

import sqlite3
from importlib import import_module
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import event, inspect

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0144_3_4_0_rename_stored_map_index")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
DAG_ID = "migration_0144"
RUN_ID = "run"
REGION_ID = UUID(int=7)
CHILD_REGION_ID = UUID(int=8)

metadata = sa.MetaData()
dag_run = sa.Table(
    "dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("run_type", sa.String(50)),
    sa.Column("run_after", UtcDateTime),
)
task_instance = sa.Table(
    "task_instance",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("task_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("region_index", sa.Integer),
    sa.Column("region_id", CompactUUID),
    sa.Column("working_set", sa.Boolean),
    sa.Column("try_number", sa.Integer),
    sa.Column("pool", sa.String(256)),
    sa.Column("pool_slots", sa.Integer),
)
task_state_store = sa.Table(
    "task_state_store",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_run_id", sa.Integer),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("task_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("region_index", sa.Integer),
    sa.Column("region_id", CompactUUID),
    sa.Column("key", sa.String(512)),
    sa.Column("value", sa.Text),
    sa.Column("updated_at", UtcDateTime),
)
dynamic_region = sa.Table(
    "dynamic_region",
    metadata,
    sa.Column("id", CompactUUID, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("node_id", sa.String(250)),
    sa.Column("parent_region_id", CompactUUID),
    sa.Column("parent_region_index", sa.Integer),
    sa.Column("created_at", UtcDateTime),
)
task_reschedule = sa.Table(
    "task_reschedule",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("ti_id", sa.Uuid),
    sa.Column("start_date", UtcDateTime),
    sa.Column("end_date", UtcDateTime),
    sa.Column("duration", sa.Integer),
    sa.Column("reschedule_date", UtcDateTime),
)
xcom_v2 = sa.Table(
    "xcom_v2",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("task_instance_id", sa.Uuid),
    sa.Column("key", sa.String(512)),
    sa.Column("value", sa.JSON),
    sa.Column("timestamp", UtcDateTime),
)
legacy_task_data_owner = sa.Table(
    "legacy_task_data_owner",
    metadata,
    sa.Column("dag_id", sa.String(250), primary_key=True),
    sa.Column("task_id", sa.String(250), primary_key=True),
    sa.Column("run_id", sa.String(250), primary_key=True),
    sa.Column("map_index", sa.Integer, primary_key=True),
    sa.Column("task_instance_id", sa.Uuid),
)
RENAMED = (task_instance, task_state_store)
CHILDREN = (task_reschedule, xcom_v2, legacy_task_data_owner)


def _enable_sqlite_foreign_keys(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")


def _delete_rows(connection):
    ti_ids = sa.select(task_instance.c.id).where(task_instance.c.dag_id == DAG_ID)
    connection.execute(task_reschedule.delete().where(task_reschedule.c.ti_id.in_(ti_ids)))
    connection.execute(xcom_v2.delete().where(xcom_v2.c.task_instance_id.in_(ti_ids)))
    connection.execute(legacy_task_data_owner.delete().where(legacy_task_data_owner.c.dag_id == DAG_ID))
    connection.execute(task_state_store.delete().where(task_state_store.c.dag_id == DAG_ID))
    connection.execute(task_instance.delete().where(task_instance.c.dag_id == DAG_ID))
    connection.execute(dynamic_region.delete().where(dynamic_region.c.dag_id == DAG_ID))
    connection.execute(dag_run.delete().where(dag_run.c.dag_id == DAG_ID))


@pytest.fixture(params=[True, False], ids=["sqlite_foreign_keys_on", "sqlite_foreign_keys_off"])
def database_at_revision(request):
    """Move the database to the migration's revision for one test, then back to the current head."""
    if request.param:
        event.listen(sa.engine.Engine, "connect", _enable_sqlite_foreign_keys)
    elif settings.engine.dialect.name != "sqlite":
        pytest.skip("SQLite foreign-key setting")
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=REVISION)
        with settings.engine.begin() as connection:
            _delete_rows(connection)
        yield settings.engine
    finally:
        try:
            upgradedb(to_revision=REVISION)
            with settings.engine.begin() as connection:
                _delete_rows(connection)
            upgradedb()
        finally:
            if request.param:
                event.remove(sa.engine.Engine, "connect", _enable_sqlite_foreign_keys)
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


@pytest.fixture
def stored_rows(database_at_revision):
    """A current, an archived and a regional task instance, a state entry, and a child row of each kind."""
    now = timezone.utcnow()
    current_id, archived_id, regional_id = uuid4(), uuid4(), uuid4()
    with database_at_revision.begin() as connection:
        dag_run_id = connection.execute(
            dag_run.insert().values(dag_id=DAG_ID, run_id=RUN_ID, run_type="manual", run_after=now)
        ).inserted_primary_key[0]
        connection.execute(
            dynamic_region.insert().values(
                id=REGION_ID, dag_id=DAG_ID, run_id=RUN_ID, node_id="task", created_at=now
            )
        )
        connection.execute(
            dynamic_region.insert().values(
                id=CHILD_REGION_ID,
                dag_id=DAG_ID,
                run_id=RUN_ID,
                node_id="task",
                parent_region_id=REGION_ID,
                parent_region_index=0,
                created_at=now,
            )
        )
        common = {
            "dag_id": DAG_ID,
            "task_id": "task",
            "run_id": RUN_ID,
            "pool": "default_pool",
            "pool_slots": 1,
        }
        connection.execute(
            task_instance.insert(),
            [
                {
                    **common,
                    "id": current_id,
                    "region_index": -1,
                    "region_id": UUID(int=0),
                    "working_set": True,
                    "try_number": 0,
                },
                {
                    **common,
                    "id": archived_id,
                    "region_index": -1,
                    "region_id": UUID(int=0),
                    "working_set": None,
                    "try_number": 3,
                },
                {
                    **common,
                    "id": regional_id,
                    "region_index": 4,
                    "region_id": REGION_ID,
                    "working_set": True,
                    "try_number": 0,
                },
            ],
        )
        connection.execute(
            task_reschedule.insert().values(
                ti_id=regional_id, start_date=now, end_date=now, duration=0, reschedule_date=now
            )
        )
        connection.execute(
            legacy_task_data_owner.insert().values(
                dag_id=DAG_ID, task_id="task", run_id=RUN_ID, map_index=-1, task_instance_id=current_id
            )
        )
        connection.execute(
            xcom_v2.insert().values(
                id=uuid4(), task_instance_id=regional_id, key="key", value="value", timestamp=now
            )
        )
        connection.execute(
            task_state_store.insert().values(
                dag_run_id=dag_run_id,
                dag_id=DAG_ID,
                task_id="task",
                run_id=RUN_ID,
                region_index=4,
                region_id=REGION_ID,
                key="state",
                value="value",
                updated_at=now,
            )
        )


def _stored(engine, table_name, index_column):
    """Read a table with the coordinate column named ``region_index``, whatever it is currently called."""
    table = sa.Table(table_name, sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        rows = connection.execute(sa.select(table).where(table.c.dag_id == DAG_ID)).mappings().all()
    renamed = [{("region_index" if k == index_column else k): v for k, v in row.items()} for row in rows]
    return sorted(
        renamed, key=lambda row: (str(row["region_id"]), row["region_index"], str(row.get("try_number")))
    )


def _count_children(engine):
    with engine.connect() as connection:
        return {
            table.name: connection.scalar(sa.select(sa.func.count()).select_from(table)) for table in CHILDREN
        }


def _region_ids(engine):
    with engine.connect() as connection:
        return set(
            connection.scalars(sa.select(dynamic_region.c.id).where(dynamic_region.c.dag_id == DAG_ID))
        )


def _column_names(engine, table):
    return {column["name"] for column in inspect(engine).get_columns(table.name)}


def test_downgrade_then_upgrade_renames_the_column_and_preserves_rows_and_their_children(
    stored_rows, database_at_revision
):
    upgraded = {table.name: _stored(database_at_revision, table.name, "region_index") for table in RENAMED}
    child_counts = _count_children(database_at_revision)
    assert all(child_counts.values())

    downgrade(to_revision=PREVIOUS_REVISION)

    for table in RENAMED:
        columns = _column_names(database_at_revision, table)
        assert "map_index" in columns
        assert "region_index" not in columns
    assert {
        table.name: _stored(database_at_revision, table.name, "map_index") for table in RENAMED
    } == upgraded
    assert _count_children(database_at_revision) == child_counts
    assert _region_ids(database_at_revision) == {REGION_ID, CHILD_REGION_ID}

    upgradedb(to_revision=REVISION)

    for table in RENAMED:
        columns = _column_names(database_at_revision, table)
        assert "region_index" in columns
        assert "map_index" not in columns
    assert {
        table.name: _stored(database_at_revision, table.name, "region_index") for table in RENAMED
    } == upgraded
    assert _count_children(database_at_revision) == child_counts
    assert _region_ids(database_at_revision) == {REGION_ID, CHILD_REGION_ID}
