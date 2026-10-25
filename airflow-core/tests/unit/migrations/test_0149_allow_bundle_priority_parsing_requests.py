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
"""Tests for the bundle priority parsing request migration."""

from __future__ import annotations

from importlib import import_module

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect

from airflow import settings
from airflow.utils.db import downgrade, upgradedb

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0149_3_5_0_allow_bundle_priority_parsing_requests")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
BUNDLE_NAME = "migration_0149"
FILE_REQUEST_ID = "a" * 32
BUNDLE_REQUEST_ID = "b" * 32

metadata = sa.MetaData()
priority_request = sa.Table(
    "dag_priority_parsing_request",
    metadata,
    sa.Column("id", sa.String(32), primary_key=True),
    sa.Column("bundle_name", sa.String(1000), nullable=False),
    sa.Column("relative_fileloc", sa.String(2000)),
)


def _delete_rows(connection):
    connection.execute(priority_request.delete().where(priority_request.c.bundle_name == BUNDLE_NAME))


@pytest.fixture
def database_at_predecessor():
    """Move the database to the migration predecessor for one test, then back to the current head."""
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
        upgradedb(to_revision=REVISION)
        with settings.engine.begin() as connection:
            _delete_rows(connection)
        upgradedb()
        settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
        settings.reconfigure_orm()


def test_upgrade_and_downgrade_handle_bundle_priority_requests(database_at_predecessor):
    with database_at_predecessor.begin() as connection:
        connection.execute(
            priority_request.insert().values(
                id=FILE_REQUEST_ID,
                bundle_name=BUNDLE_NAME,
                relative_fileloc="example.py",
            )
        )

    upgradedb(to_revision=REVISION)

    with database_at_predecessor.begin() as connection:
        connection.execute(
            priority_request.insert().values(
                id=BUNDLE_REQUEST_ID,
                bundle_name=BUNDLE_NAME,
                relative_fileloc=None,
            )
        )

    downgrade(to_revision=PREVIOUS_REVISION)

    with database_at_predecessor.connect() as connection:
        rows = connection.execute(
            sa.select(priority_request.c.id, priority_request.c.relative_fileloc).where(
                priority_request.c.bundle_name == BUNDLE_NAME
            )
        ).all()

    assert rows == [(FILE_REQUEST_ID, "example.py")]
    relative_fileloc = next(
        column
        for column in inspect(database_at_predecessor).get_columns("dag_priority_parsing_request")
        if column["name"] == "relative_fileloc"
    )
    assert relative_fileloc["nullable"] is False
