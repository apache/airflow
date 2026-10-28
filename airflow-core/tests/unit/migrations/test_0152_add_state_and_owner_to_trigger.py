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
Tests for the trigger state and owner migration, written against the schema as it stood at that revision.

The tables are declared here instead of taken from ``airflow.models``, and alembic moves the database
to the revision, so later migrations and model changes cannot change what these tests exercise.
"""

from __future__ import annotations

import json
from importlib import import_module
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0152_3_5_0_add_state_and_owner_to_trigger")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
DAG_ID = "migration_0152"
CLASSPATH = "migration_0152.Trigger"
NEW_COLUMNS = {
    "consecutive_failures",
    "first_failed_at",
    "last_failed_at",
    "last_error",
    "last_event_at",
    "dag_id",
    "owner_ref",
}

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
trigger = sa.Table(
    "trigger",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("classpath", sa.String(1000)),
    sa.Column("kwargs", sa.Text),
    sa.Column("created_date", UtcDateTime),
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
    sa.Column("trigger_id", sa.Integer),
)
callback = sa.Table(
    "callback",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("type", sa.String(20)),
    sa.Column("fetch_method", sa.String(20)),
    # JSON on every database: jsonb on Postgres, so a string would not bind there.
    sa.Column("data", sa.JSON),
    sa.Column("priority_weight", sa.Integer),
    sa.Column("created_at", UtcDateTime),
    sa.Column("trigger_id", sa.Integer),
)


def _delete_rows(connection):
    trigger_ids = sa.select(trigger.c.id).where(trigger.c.classpath == CLASSPATH)
    connection.execute(callback.delete().where(callback.c.trigger_id.in_(trigger_ids)))
    connection.execute(task_instance.delete().where(task_instance.c.dag_id == DAG_ID))
    connection.execute(trigger.delete().where(trigger.c.classpath == CLASSPATH))
    connection.execute(dag_run.delete().where(dag_run.c.dag_id == DAG_ID))


@pytest.fixture
def database_before_revision():
    """Move the database to the revision before the migration for one test, then back to the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        with settings.engine.begin() as connection:
            _delete_rows(connection)
        yield settings.engine
    finally:
        try:
            with settings.engine.begin() as connection:
                _delete_rows(connection)
            upgradedb()
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def _add_trigger(connection) -> int:
    return connection.execute(
        trigger.insert().values(classpath=CLASSPATH, kwargs="{}", created_date=timezone.utcnow())
    ).inserted_primary_key[0]


def _owners(engine) -> dict[int, tuple[str | None, dict | None]]:
    table = sa.Table("trigger", sa.MetaData(), autoload_with=engine)
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(table.c.id, table.c.dag_id, table.c.owner_ref).where(table.c.classpath == CLASSPATH)
        )
        return {
            trigger_id: (dag_id, json.loads(owner_ref) if isinstance(owner_ref, str) else owner_ref)
            for trigger_id, dag_id, owner_ref in rows
        }


def test_upgrade_records_the_owner_of_existing_task_and_callback_triggers(database_before_revision):
    ti_id, callback_id = uuid4(), uuid4()
    with database_before_revision.begin() as connection:
        task_trigger, callback_trigger, watcher_trigger = (_add_trigger(connection) for _ in range(3))
        connection.execute(
            dag_run.insert().values(
                dag_id=DAG_ID, run_id="run", run_type="manual", run_after=timezone.utcnow()
            )
        )
        connection.execute(
            task_instance.insert().values(
                id=ti_id,
                dag_id=DAG_ID,
                task_id="wait",
                run_id="run",
                region_index=-1,
                region_id=UUID(int=0),
                working_set=True,
                try_number=1,
                pool="default_pool",
                pool_slots=1,
                trigger_id=task_trigger,
            )
        )
        # callback.data as ExtendedJSON stores it: every dict wrapped as {"__type": "dict", "__var": ...}.
        context = {"__type": "dict", "__var": {"dag_run": {"__type": "dict", "__var": {"dag_id": DAG_ID}}}}
        connection.execute(
            callback.insert().values(
                id=callback_id,
                type="triggerer",
                fetch_method="import_path",
                data={
                    "__type": "dict",
                    "__var": {"kwargs": {"__type": "dict", "__var": {"context": context}}},
                },
                priority_weight=1,
                created_at=timezone.utcnow(),
                trigger_id=callback_trigger,
            )
        )

    upgradedb(to_revision=REVISION)

    assert _owners(database_before_revision) == {
        task_trigger: (DAG_ID, {"ti_id": str(ti_id)}),
        callback_trigger: (DAG_ID, {"callback_id": str(callback_id)}),
        watcher_trigger: (None, None),
    }


def test_downgrade_drops_the_new_columns(database_before_revision):
    upgradedb(to_revision=REVISION)
    assert {c["name"] for c in inspect(database_before_revision).get_columns("trigger")} >= NEW_COLUMNS

    downgrade(to_revision=PREVIOUS_REVISION)

    assert not NEW_COLUMNS & {c["name"] for c in inspect(database_before_revision).get_columns("trigger")}
