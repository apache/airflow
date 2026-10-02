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

from contextlib import nullcontext
from importlib import import_module
from uuid import UUID, uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select
from sqlalchemy.exc import DBAPIError

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0143_3_4_0_add_dynamic_region_storage")
MODELS = (TaskInstance, TaskStateStoreModel)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    yield
    clear_db_runs()


@pytest.fixture
def stored_rows(dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun()
    ti = dr.task_instances[0]
    session.add(
        TaskStateStoreModel(
            dag_run_id=dr.id,
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            map_index=-1,
            key="state",
            value="value",
        )
    )
    session.flush()
    return ti


@pytest.fixture
def migration_connection(stored_rows, session):
    session.commit()
    with session.get_bind().connect() as connection:
        try:
            yield connection
        finally:
            if connection.dialect.name == "sqlite":
                connection.rollback()
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")


@pytest.mark.parametrize("sqlite_foreign_keys", [True, False])
def test_downgrade_then_upgrade_keeps_existing_rows_in_the_sentinel_region(
    stored_rows, session, migration_connection, sqlite_foreign_keys
):
    connection = migration_connection
    if not sqlite_foreign_keys and connection.dialect.name != "sqlite":
        pytest.skip("SQLite foreign-key setting")
    context = MigrationContext.configure(connection, opts={"transaction_per_migration": True})
    with Operations.context(context), context.begin_transaction(_per_migration=True):
        if connection.dialect.name == "sqlite":
            connection.exec_driver_sql(f"PRAGMA foreign_keys={int(sqlite_foreign_keys)}")
        _migration.downgrade()
        _migration.upgrade()
    if connection.dialect.name == "sqlite":
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == sqlite_foreign_keys
    session.expire_all()
    for model in MODELS:
        row = session.scalars(select(model)).one()
        assert row.region_id == UUID(int=0)
        assert row.map_index == -1


@pytest.mark.parametrize("model", MODELS)
def test_downgrade_refuses_each_old_coordinate_collision_before_ddl(stored_rows, session, model):
    connection = session.connection()
    table = model.__table__
    duplicate = dict(connection.execute(select(table)).mappings().one())
    duplicate["region_id"] = uuid4()
    if model is TaskInstance:
        duplicate["id"] = uuid4()
    else:
        duplicate.pop("id")
    region = DynamicRegion.__table__
    connection.execute(
        region.insert().values(
            id=duplicate["region_id"],
            dag_id=session.scalar(select(DagRun.dag_id)),
            run_id=session.scalar(select(DagRun.run_id)),
            node_id="task",
            created_at=timezone.utcnow(),
        )
    )
    connection.execute(table.insert().values(**duplicate))
    try:
        with Operations.context(MigrationContext.configure(connection)):
            # SQLite is checked in Python; PostgreSQL and MySQL raise from the SQL guard. PostgreSQL
            # aborts the transaction on error, but MySQL's guard procedure is DDL, which would commit
            # away a savepoint.
            savepoint = (
                connection.begin_nested() if connection.dialect.name == "postgresql" else nullcontext()
            )
            with pytest.raises((RuntimeError, DBAPIError), match=model.__tablename__), savepoint:
                _migration.downgrade()
        assert inspect(connection).has_table("dynamic_region")
        for retained in MODELS:
            assert "region_id" in {c["name"] for c in inspect(connection).get_columns(retained.__tablename__)}
    finally:
        connection.execute(table.delete().where(table.c.region_id == duplicate["region_id"]))
        connection.execute(region.delete().where(region.c.id == duplicate["region_id"]))
