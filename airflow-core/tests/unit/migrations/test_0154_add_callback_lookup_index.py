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
Tests for the callback lookup index migration, written against the schema as it stood at that revision.

The table is declared here instead of taken from ``airflow.models``, so later model changes cannot change
what these tests exercise.
"""

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa
import uuid6

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.db import downgrade, upgradedb
from airflow.utils.sqlalchemy import UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0154_3_5_0_add_callback_lookup_index")
PREVIOUS_REVISION = _migration.down_revision

metadata = sa.MetaData()
callback = sa.Table(
    "callback",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("type", sa.String(20)),
    sa.Column("fetch_method", sa.String(20)),
    sa.Column("data", sa.JSON),
    sa.Column("state", sa.String(10)),
    sa.Column("output", sa.Text),
    sa.Column("priority_weight", sa.Integer),
    sa.Column("created_at", UtcDateTime),
)


@pytest.fixture
def database_at_revision():
    """Run one test against the migrated database, then move it back to the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        with settings.engine.begin() as connection:
            connection.execute(callback.delete())
        yield settings.engine
    finally:
        try:
            with settings.engine.begin() as connection:
                connection.execute(callback.delete())
            upgradedb()
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def test_downgrade_settles_routed_callbacks(database_at_revision):
    """Earlier versions neither route nor reap callbacks stored for routing."""
    rows = {
        "waiting": ("executor", "dag_attribute", "scheduled"),
        "queued": ("executor", "dag_attribute", "queued"),
        "running": ("executor", "dag_attribute", "running"),
        "finished": ("executor", "dag_attribute", "success"),
        "deadline": ("executor", "import_path", "scheduled"),
        "dag_processor": ("dag_processor", "dag_attribute", None),
    }
    ids = {name: uuid6.uuid7() for name in rows}
    with database_at_revision.begin() as connection:
        for name, (type_, fetch_method, state) in rows.items():
            connection.execute(
                callback.insert().values(
                    id=ids[name],
                    type=type_,
                    fetch_method=fetch_method,
                    data={},
                    state=state,
                    priority_weight=1,
                    created_at=timezone.utcnow(),
                )
            )

    downgrade(to_revision=PREVIOUS_REVISION)

    with database_at_revision.connect() as connection:
        stored = {
            row.id: (row.type, row.state, row.output)
            for row in connection.execute(
                sa.select(callback.c.id, callback.c.type, callback.c.state, callback.c.output)
            )
        }
    downgraded = "Airflow was downgraded before the callback finished"
    assert {name: stored[callback_id] for name, callback_id in ids.items()} == {
        "waiting": ("dag_processor", None, None),
        "queued": ("dag_processor", None, None),
        "running": ("executor", "failed", downgraded),
        "finished": ("executor", "success", None),
        "deadline": ("executor", "scheduled", None),
        "dag_processor": ("dag_processor", None, None),
    }
