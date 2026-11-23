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
"""Tests for renaming trigger.owner_ref to used_by_ref, against the schema as it stood at that revision."""

from __future__ import annotations

import json
from importlib import import_module

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0157_3_5_0_rename_trigger_owner_ref_to_used_by_ref")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
CLASSPATH = "migration_0157.Trigger"
ROWS = [
    ("task_dag", {"ti_id": "0199f3a8-0000-7000-8000-000000000001"}),
    ("callback_dag", {"callback_id": "0199f3a8-0000-7000-8000-000000000002"}),
    (None, None),
]


def _trigger_table(engine) -> sa.Table:
    return sa.Table("trigger", sa.MetaData(), autoload_with=engine)


def _delete_rows(engine):
    table = _trigger_table(engine)
    with engine.begin() as connection:
        connection.execute(table.delete().where(table.c.classpath == CLASSPATH))


def _columns(engine) -> set[str]:
    return {column["name"] for column in inspect(engine).get_columns("trigger")}


def _rows(engine, column: str) -> list[tuple[str | None, dict | None]]:
    table = _trigger_table(engine)
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(table.c.dag_id, table.c[column])
            .where(table.c.classpath == CLASSPATH)
            .order_by(table.c.id)
        )
        return [(dag_id, json.loads(ref) if isinstance(ref, str) else ref) for dag_id, ref in rows]


@pytest.fixture
def database_before_revision():
    """Move the database to the revision before the migration for one test, then back to the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        _delete_rows(settings.engine)
        yield settings.engine
    finally:
        try:
            _delete_rows(settings.engine)
            upgradedb()
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def test_rename_keeps_what_each_trigger_is_used_by_both_ways(database_before_revision):
    table = _trigger_table(database_before_revision)
    with database_before_revision.begin() as connection:
        for dag_id, owner_ref in ROWS:
            connection.execute(
                table.insert().values(
                    classpath=CLASSPATH,
                    kwargs="{}",
                    created_date=timezone.utcnow(),
                    dag_id=dag_id,
                    owner_ref=owner_ref,
                )
            )

    upgradedb(to_revision=REVISION)

    assert "owner_ref" not in _columns(database_before_revision)
    assert _rows(database_before_revision, "used_by_ref") == ROWS

    downgrade(to_revision=PREVIOUS_REVISION)

    assert "used_by_ref" not in _columns(database_before_revision)
    assert _rows(database_before_revision, "owner_ref") == ROWS
