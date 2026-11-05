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

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.utils.sqlalchemy import UtcDateTime

pytestmark = pytest.mark.db_test

migration = import_module("airflow.migrations.versions.0154_3_5_0_add_next_evaluation_time_to_partition_")


@pytest.fixture(
    params=[
        pytest.param(("sqlite", True), id="sqlite_foreign_keys_on"),
        pytest.param(("sqlite", False), id="sqlite_foreign_keys_off"),
        pytest.param(("mysql", None), id="mysql", marks=pytest.mark.backend("mysql")),
    ]
)
def migration_connection(request):
    backend, foreign_keys = request.param
    database_name = None
    if backend == "mysql":
        database_name = f"apdr_migration_{uuid4().hex}"
        with settings.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            admin.exec_driver_sql(f"CREATE DATABASE {database_name}")
        engine = sa.create_engine(settings.engine.url.set(database=database_name))
    else:
        engine = sa.create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            if backend == "sqlite":
                connection.exec_driver_sql(f"PRAGMA foreign_keys={int(foreign_keys)}")
            yield connection
    finally:
        engine.dispose()
        if database_name is not None:
            with settings.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                admin.exec_driver_sql(f"DROP DATABASE {database_name}")


def test_partition_evaluation_migration_preserves_rows_and_enqueue_order(migration_connection):
    connection = migration_connection
    metadata = sa.MetaData()
    dag_run = sa.Table("dag_run", metadata, sa.Column("id", sa.Integer, primary_key=True))
    partition_run = sa.Table(
        "asset_partition_dag_run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "created_dag_run_id",
            sa.Integer,
            sa.ForeignKey("dag_run.id", name="apdr_created_dag_run_id_fkey", ondelete="CASCADE"),
        ),
        sa.Column("created_at", UtcDateTime, nullable=False),
    )
    metadata.create_all(connection)
    connection.execute(dag_run.insert().values(id=1))
    connection.execute(
        partition_run.insert(),
        [
            {"id": 1, "created_dag_run_id": None, "created_at": timezone.datetime(2026, 1, 1)},
            {"id": 2, "created_dag_run_id": None, "created_at": timezone.datetime(2026, 1, 2)},
            {"id": 3, "created_dag_run_id": 1, "created_at": timezone.datetime(2026, 1, 3)},
        ],
    )
    connection.commit()
    before = connection.execute(sa.select(partition_run).order_by(partition_run.c.id)).all()
    connection.commit()

    with Operations.context(MigrationContext.configure(connection)):
        migration.upgrade()
    connection.commit()

    inspector = sa.inspect(connection)
    columns = {column["name"]: column for column in inspector.get_columns(partition_run.name)}
    assert columns["next_evaluation_at"]["nullable"] is False
    indexes = {index["name"]: index for index in inspector.get_indexes(partition_run.name)}
    assert indexes["idx_apdr_pending_evaluation"]["column_names"] == [
        "created_dag_run_id",
        "next_evaluation_at",
        "id",
    ]
    rows = connection.execute(
        sa.text("SELECT created_at, next_evaluation_at FROM asset_partition_dag_run ORDER BY id")
    ).all()
    assert len(rows) == len(before)
    assert all(created_at == next_evaluation_at for created_at, next_evaluation_at in rows)
    assert connection.execute(sa.select(partition_run).order_by(partition_run.c.id)).all() == before
    connection.commit()

    with Operations.context(MigrationContext.configure(connection)):
        migration.downgrade()
    connection.commit()

    inspector = sa.inspect(connection)
    assert "next_evaluation_at" not in {
        column["name"] for column in inspector.get_columns(partition_run.name)
    }
    assert "idx_apdr_pending_evaluation" not in {
        index["name"] for index in inspector.get_indexes(partition_run.name)
    }
    assert connection.execute(sa.select(partition_run).order_by(partition_run.c.id)).all() == before
    if connection.dialect.name == "mysql":
        foreign_key = next(
            constraint
            for constraint in inspector.get_foreign_keys(partition_run.name)
            if constraint["name"] == "apdr_created_dag_run_id_fkey"
        )
        assert foreign_key["options"]["ondelete"] == "CASCADE"
        connection.execute(dag_run.delete().where(dag_run.c.id == 1))
        assert connection.execute(
            sa.select(partition_run.c.id).order_by(partition_run.c.id)
        ).scalars().all() == [
            1,
            2,
        ]
