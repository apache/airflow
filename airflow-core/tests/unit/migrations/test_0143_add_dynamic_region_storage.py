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
Tests for the dynamic region migration, written against the schema as it stood at that revision.

The tables are declared here instead of taken from ``airflow.models``, and alembic moves the database
to the revision, so later migrations and model changes cannot change what these tests exercise.
"""

from __future__ import annotations

from importlib import import_module
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import DBAPIError

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0143_3_4_0_add_dynamic_region_storage")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
DAG_ID = "migration_0143"
RUN_ID = "run"
SENTINEL = UUID(int=0)

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
    sa.Column("map_index", sa.Integer),
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
    sa.Column("map_index", sa.Integer),
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
    sa.Column("created_at", UtcDateTime),
)
TABLES = (task_instance, task_state_store)


def _delete_rows(connection):
    connection.execute(dynamic_region.delete().where(dynamic_region.c.dag_id == DAG_ID))
    connection.execute(task_state_store.delete().where(task_state_store.c.dag_id == DAG_ID))
    connection.execute(task_instance.delete().where(task_instance.c.dag_id == DAG_ID))
    connection.execute(dag_run.delete().where(dag_run.c.dag_id == DAG_ID))


@pytest.fixture
def database_at_revision():
    """Move the database to the migration's revision for one test, then back to the current head."""
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
        upgradedb(to_revision=REVISION)
        with settings.engine.begin() as connection:
            _delete_rows(connection)
        upgradedb()
        settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
        settings.reconfigure_orm()


@pytest.fixture
def stored_rows(database_at_revision):
    """One task instance and one state entry, both in the sentinel region, in a Dag run with no regions."""
    with database_at_revision.begin() as connection:
        dag_run_id = connection.execute(
            dag_run.insert().values(
                dag_id=DAG_ID, run_id=RUN_ID, run_type="manual", run_after=timezone.utcnow()
            )
        ).inserted_primary_key[0]
        connection.execute(
            task_instance.insert().values(
                id=uuid4(),
                dag_id=DAG_ID,
                task_id="task",
                run_id=RUN_ID,
                map_index=-1,
                working_set=True,
                try_number=0,
                pool="default_pool",
                pool_slots=1,
            )
        )
        connection.execute(
            task_state_store.insert().values(
                dag_run_id=dag_run_id,
                dag_id=DAG_ID,
                task_id="task",
                run_id=RUN_ID,
                map_index=-1,
                key="state",
                value="value",
                updated_at=timezone.utcnow(),
            )
        )
    return dag_run_id


def _add_region_row(connection, table, **overrides):
    """Copy the sentinel-region row of ``table`` into a new region, changing the given columns."""
    row = connection.execute(sa.select(table).where(table.c.dag_id == DAG_ID)).mappings().first()
    region_id = uuid4()
    connection.execute(
        dynamic_region.insert().values(
            id=region_id, dag_id=DAG_ID, run_id=RUN_ID, node_id="task", created_at=timezone.utcnow()
        )
    )
    values = {name: value for name, value in row.items() if name != "id" or table is task_instance}
    if table is task_instance:
        values["id"] = uuid4()
    connection.execute(table.insert().values(**{**values, "region_id": region_id, **overrides}))


def _rows(engine, table):
    with engine.connect() as connection:
        return connection.execute(sa.select(table).where(table.c.dag_id == DAG_ID)).mappings().all()


def test_downgrade_then_upgrade_keeps_existing_rows_in_the_sentinel_region(stored_rows, database_at_revision):
    downgrade(to_revision=PREVIOUS_REVISION)
    upgradedb(to_revision=REVISION)

    for table in TABLES:
        (row,) = _rows(database_at_revision, table)
        assert row["region_id"] == SENTINEL
        assert row["map_index"] == -1


@pytest.mark.parametrize("table", TABLES, ids=lambda table: table.name)
def test_downgrade_refuses_each_old_coordinate_collision_before_ddl(stored_rows, database_at_revision, table):
    with database_at_revision.begin() as connection:
        _add_region_row(connection, table)

    with pytest.raises((RuntimeError, DBAPIError), match=table.name):
        downgrade(to_revision=PREVIOUS_REVISION)

    tables = inspect(database_at_revision).get_table_names()
    assert "dynamic_region" in tables
    for retained in TABLES:
        assert "region_id" in {c["name"] for c in inspect(database_at_revision).get_columns(retained.name)}


@pytest.mark.parametrize(
    ("table", "region_rows"),
    [
        # Archived tries leave working_set NULL, which the old current-key does not constrain.
        pytest.param(
            task_instance,
            [{"try_number": 1, "working_set": None}, {"try_number": 2, "working_set": None}],
            id="task_instance",
        ),
        pytest.param(task_state_store, [{"key": "state-1"}, {"key": "state-2"}], id="task_state_store"),
    ],
)
def test_downgrade_keeps_region_rows_that_differ_on_an_old_key_column(
    stored_rows, database_at_revision, table, region_rows
):
    with database_at_revision.begin() as connection:
        for overrides in region_rows:
            _add_region_row(connection, table, **overrides)

    downgrade(to_revision=PREVIOUS_REVISION)
    upgradedb(to_revision=REVISION)

    rows = _rows(database_at_revision, table)
    assert len(rows) == 3
    assert {row["region_id"] for row in rows} == {SENTINEL}
