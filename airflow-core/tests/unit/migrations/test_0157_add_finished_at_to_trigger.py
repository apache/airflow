# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Tests for the finished_at migration; the database is moved to and from the revision."""

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa

from airflow import settings
from airflow.utils.db import downgrade, upgradedb

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0157_3_5_0_add_finished_at_to_trigger")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision


@pytest.fixture
def database_before_revision():
    """Move the database to the revision before the migration for one test, then back to the current head."""
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
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


def _trigger_columns(engine) -> set[str]:
    return {column["name"] for column in sa.inspect(engine).get_columns("trigger")}


def test_upgrade_adds_finished_at(database_before_revision):
    assert "finished_at" not in _trigger_columns(database_before_revision)

    upgradedb()

    assert "finished_at" in _trigger_columns(database_before_revision)


def test_downgrade_removes_finished_at(database_before_revision):
    upgradedb()
    assert "finished_at" in _trigger_columns(database_before_revision)

    downgrade(to_revision=PREVIOUS_REVISION)

    assert "finished_at" not in _trigger_columns(database_before_revision)
