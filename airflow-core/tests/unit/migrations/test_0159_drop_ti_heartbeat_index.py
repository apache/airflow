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
"""Tests for the migration dropping the ti_heartbeat index."""

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa
from alembic import command

from airflow import settings
from airflow.utils.db import _get_alembic_config, downgrade, upgradedb

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0159_3_5_0_drop_ti_heartbeat_index")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision


def _get_index_names(engine) -> set[str]:
    return {index["name"] for index in sa.inspect(engine).get_indexes("task_instance")}


def _get_ti_heartbeat_index() -> sa.Index:
    """The index as a deployment manager would drop or create it by hand, outside the TaskInstance model."""
    table = sa.Table("task_instance", sa.MetaData(), sa.Column("last_heartbeat_at", sa.DateTime))
    return sa.Index("ti_heartbeat", table.c.last_heartbeat_at)


def _assert_ti_heartbeat_as_created_by_0045(engine) -> None:
    [index] = [
        index for index in sa.inspect(engine).get_indexes("task_instance") if index["name"] == "ti_heartbeat"
    ]
    assert index["column_names"] == ["last_heartbeat_at"]
    assert not index["unique"]


@pytest.fixture
def database_at_previous_revision():
    """Move the database to the revision before the drop, then restore the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        yield settings.engine
    finally:
        try:
            upgradedb()
            # A test that failed after creating the index by hand must not leave it for later tests.
            if "ti_heartbeat" in _get_index_names(settings.engine):
                _get_ti_heartbeat_index().drop(settings.engine)
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


@pytest.mark.parametrize("changed_by_hand", [False, True])
def test_upgrade_drops_ti_heartbeat_and_downgrade_recreates_it(
    database_at_previous_revision, changed_by_hand
):
    engine = database_at_previous_revision
    _assert_ti_heartbeat_as_created_by_0045(engine)
    if changed_by_hand:
        _get_ti_heartbeat_index().drop(engine)

    upgradedb(to_revision=REVISION)
    assert "ti_heartbeat" not in _get_index_names(engine)

    if changed_by_hand:
        _get_ti_heartbeat_index().create(engine)
    downgrade(to_revision=PREVIOUS_REVISION)
    _assert_ti_heartbeat_as_created_by_0045(engine)


@pytest.mark.parametrize(
    ("url", "drop", "create"),
    [
        pytest.param(
            "postgresql://",
            "DROP INDEX IF EXISTS ti_heartbeat;",
            "CREATE INDEX IF NOT EXISTS ti_heartbeat ON task_instance (last_heartbeat_at);",
            id="postgresql",
        ),
        pytest.param(
            "mysql://",
            "DROP INDEX ti_heartbeat ON task_instance;",
            "CREATE INDEX ti_heartbeat ON task_instance (last_heartbeat_at);",
            id="mysql",
        ),
    ],
)
def test_offline_sql_drops_and_recreates_ti_heartbeat(capsys, monkeypatch, url, drop, create):
    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", url)

    command.upgrade(_get_alembic_config(), f"{PREVIOUS_REVISION}:{REVISION}", sql=True)
    command.downgrade(_get_alembic_config(), f"{REVISION}:{PREVIOUS_REVISION}", sql=True)

    emitted = capsys.readouterr().out
    assert drop in emitted
    assert create in emitted
