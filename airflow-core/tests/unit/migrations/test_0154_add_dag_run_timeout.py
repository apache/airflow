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

from datetime import UTC, datetime
from importlib import import_module
from uuid import UUID

import pytest
import sqlalchemy as sa
from alembic import command

from airflow.models.base import COLLATION_ARGS
from airflow.utils.db import _get_alembic_config
from airflow.utils.sqlalchemy import CompactUUID, UtcDateTime

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0154_3_5_0_add_dag_run_timeout")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision

metadata = sa.MetaData()
dag_run = sa.Table(
    "dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("run_type", sa.String(50)),
    sa.Column("run_after", UtcDateTime),
    sa.Column("conf", sa.JSON),
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


@pytest.fixture(params=[True, False], ids=["foreign_keys_on", "foreign_keys_off"])
def migration_database(tmp_path, request, monkeypatch):
    # Bootstrap migrations reuse this cached mapping even when the configured backend is MySQL.
    monkeypatch.delitem(COLLATION_ARGS, "collation", raising=False)
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'dag_run_timeout.db'}")
    try:
        with engine.connect() as connection:
            config = _get_alembic_config()
            config.attributes["connection"] = connection
            command.upgrade(config, PREVIOUS_REVISION)
            connection.commit()
            connection.exec_driver_sql(f"PRAGMA foreign_keys={'ON' if request.param else 'OFF'}")
            connection.commit()
            yield connection, config
    finally:
        engine.dispose()


def test_upgrade_and_downgrade_preserve_run_and_linked_task(migration_database):
    connection, config = migration_database
    connection.execute(
        dag_run.insert().values(
            dag_id="migration_0154",
            run_id="run",
            run_type="manual",
            run_after=datetime(2026, 1, 1, tzinfo=UTC),
            conf={"preserved": True},
        )
    )
    connection.execute(
        task_instance.insert().values(
            id=UUID(int=1),
            dag_id="migration_0154",
            task_id="task",
            run_id="run",
            region_index=-1,
            region_id=UUID(int=0),
            working_set=True,
            try_number=0,
            pool="default_pool",
            pool_slots=1,
        )
    )
    original_run = connection.execute(sa.select(dag_run)).mappings().one()
    original_task = connection.execute(sa.select(task_instance)).mappings().one()
    connection.commit()

    command.upgrade(config, REVISION)
    timeout_column = sa.column("dagrun_timeout_seconds", sa.Float)
    assert connection.scalar(sa.select(timeout_column).select_from(dag_run)) is None
    connection.execute(sa.update(sa.table("dag_run", timeout_column)).values(dagrun_timeout_seconds=90.5))
    connection.commit()
    with connection.engine.connect() as reader:
        assert reader.scalar(sa.select(timeout_column).select_from(dag_run)) == 90.5

    command.downgrade(config, PREVIOUS_REVISION)
    assert "dagrun_timeout_seconds" not in {
        column["name"] for column in sa.inspect(connection).get_columns("dag_run")
    }
    assert connection.execute(sa.select(dag_run)).mappings().one() == original_run
    assert connection.execute(sa.select(task_instance)).mappings().one() == original_task
    connection.commit()

    command.upgrade(config, REVISION)
    assert connection.scalar(sa.select(timeout_column).select_from(dag_run)) is None
    assert connection.execute(sa.select(dag_run)).mappings().one() == original_run
    assert connection.execute(sa.select(task_instance)).mappings().one() == original_task
