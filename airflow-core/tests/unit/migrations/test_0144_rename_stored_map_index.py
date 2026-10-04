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
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, Table, func, select

from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import XComModelV2
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0144_3_4_0_rename_stored_map_index")
TABLES = ("task_instance", "task_state_store")
CHILDREN = (TaskReschedule, XComModelV2, LegacyTaskDataOwner)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    yield
    clear_db_runs()


def _stored(connection, table_name, index_column):
    table = Table(table_name, MetaData(), autoload_with=connection)
    rows = connection.execute(select(table).order_by(table.c.region_id, table.c[index_column])).mappings()
    return [{("region_index" if k == index_column else k): v for k, v in row.items()} for row in rows]


@pytest.fixture
def stored_rows(dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun()
    ti = dr.task_instances[0]
    now = ti.dag_run.start_date or ti.dag_run.queued_at
    connection = session.connection()
    ti_table = TaskInstance.__table__
    current = dict(connection.execute(select(ti_table)).mappings().one())
    archived = {**current, "id": uuid4(), "working_set": None, "archived_reason": "retry", "try_number": 3}
    regional = {**current, "id": uuid4(), "region_id": uuid4(), "region_index": 4}
    for values in (archived, regional):
        connection.execute(ti_table.insert().values(values))
    session.add(TaskReschedule(ti_id=regional["id"], start_date=now, end_date=now, reschedule_date=now))
    session.add(
        LegacyTaskDataOwner(
            dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, map_index=-1, task_instance_id=ti.id
        )
    )
    session.add(XComModelV2(task_instance_id=regional["id"], key="key", value="value"))
    session.add(
        TaskStateStoreModel(
            dag_run_id=dr.id,
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            region_id=regional["region_id"],
            region_index=4,
            key="state",
            value="value",
        )
    )
    session.flush()
    session.commit()
    return ti


@pytest.fixture
def migration_connection(stored_rows, session):
    with session.get_bind().connect() as connection:
        try:
            yield connection
        finally:
            if connection.dialect.name == "sqlite":
                connection.rollback()
                connection.exec_driver_sql("PRAGMA foreign_keys=ON")


@pytest.mark.parametrize("sqlite_foreign_keys", [True, False])
def test_rename_roundtrip_preserves_rows_and_their_children(
    stored_rows, migration_connection, sqlite_foreign_keys
):
    connection = migration_connection
    if not sqlite_foreign_keys and connection.dialect.name != "sqlite":
        pytest.skip("SQLite foreign-key setting")
    upgraded = {name: _stored(connection, name, "region_index") for name in TABLES}
    child_counts = {model: connection.scalar(select(func.count()).select_from(model)) for model in CHILDREN}
    context = MigrationContext.configure(connection, opts={"transaction_per_migration": True})
    with Operations.context(context), context.begin_transaction(_per_migration=True):
        if connection.dialect.name == "sqlite":
            connection.exec_driver_sql(f"PRAGMA foreign_keys={int(sqlite_foreign_keys)}")
        _migration.downgrade()
        try:
            assert {name: _stored(connection, name, "map_index") for name in TABLES} == upgraded
        finally:
            _migration.upgrade()

    assert {name: _stored(connection, name, "region_index") for name in TABLES} == upgraded
    assert {
        model: connection.scalar(select(func.count()).select_from(model)) for model in CHILDREN
    } == child_counts
