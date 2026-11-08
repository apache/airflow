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

from importlib import import_module
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.pool import NullPool

from airflow import settings

pytestmark = pytest.mark.db_test

migration = import_module("airflow.migrations.versions.0157_3_5_0_add_asset_partition_dag_run_wake")
TABLE = "asset_partition_dag_run_wake"


def _run_migration(engine, migrate):
    with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
        migrate()


@pytest.fixture
def empty_database(tmp_path):
    source = settings.engine
    name = f"partition_wake_{uuid4().hex}"
    if source.dialect.name == "sqlite":
        url = source.url.set(database=str(tmp_path / "partition_wake.db"))
    else:
        url = source.url.set(database=name)
        with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            admin.exec_driver_sql(f"CREATE DATABASE {name}")
    engine = sa.create_engine(url, poolclass=NullPool)
    try:
        yield engine
    finally:
        engine.dispose()
        if source.dialect.name != "sqlite":
            with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                admin.exec_driver_sql(f"DROP DATABASE {name}")


def test_upgrade_adds_the_wake_table_and_downgrade_drops_it(empty_database):
    _run_migration(empty_database, migration.upgrade)

    inspector = sa.inspect(empty_database)
    assert [column["name"] for column in inspector.get_columns(TABLE)] == [
        "id",
        "apdr_id",
        "target_dag_id",
        "reason",
        "created_at",
    ]
    assert {index["name"]: index["column_names"] for index in inspector.get_indexes(TABLE)} == {
        "idx_apdr_wake_apdr_id": ["apdr_id"],
        "idx_apdr_wake_target_dag_id": ["target_dag_id", "apdr_id"],
    }
    assert inspector.get_foreign_keys(TABLE) == []
    if empty_database.dialect.name == "sqlite":
        # As the model declares it, so that SQLite never gives a deleted row's id to a new one.
        with empty_database.connect() as connection:
            sql = connection.scalar(
                sa.text("SELECT sql FROM sqlite_master WHERE name = :name"), {"name": TABLE}
            )
        assert "AUTOINCREMENT" in sql

    _run_migration(empty_database, migration.downgrade)

    assert not sa.inspect(empty_database).has_table(TABLE)
