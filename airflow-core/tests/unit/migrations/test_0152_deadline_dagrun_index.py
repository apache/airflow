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
from alembic.migration import MigrationContext
from alembic.operations import Operations

from airflow.utils.sqlalchemy import UtcDateTime

pytestmark = pytest.mark.db_test
MIGRATION = import_module("airflow.migrations.versions.0152_3_5_0_add_index_for_deadline_dag_run_lookups")
INDEX_NAME = "deadline_dagrun_id_idx"
DAGRUN_ID = 8_925_401
CALLBACK_ID = UUID(int=8_925_401)
DEADLINE_ID = UUID(int=8_925_402)
NOW = datetime(2026, 1, 1, tzinfo=UTC)
metadata = sa.MetaData()
dag_run = sa.Table(
    "dag_run",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("run_type", sa.String(50)),
    sa.Column("run_after", UtcDateTime),
)
callback = sa.Table(
    "callback",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("type", sa.String(20)),
    sa.Column("fetch_method", sa.String(20)),
    sa.Column("data", sa.JSON),
    sa.Column("state", sa.String(10)),
    sa.Column("priority_weight", sa.Integer),
    sa.Column("created_at", UtcDateTime),
)
deadline = sa.Table(
    "deadline",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("dagrun_id", sa.Integer),
    sa.Column("callback_id", sa.Uuid),
    sa.Column("deadline_time", UtcDateTime),
    sa.Column("missed", sa.Boolean),
    sa.Column("created_at", UtcDateTime),
    sa.Column("last_updated_at", UtcDateTime),
    sa.Column("deadline_alert_id", sa.Uuid),
)


def test_upgrade_downgrade_preserves_deadlines_and_foreign_keys(session):
    engine = session.get_bind()
    session.execute(
        dag_run.insert().values(
            id=DAGRUN_ID,
            dag_id="deadline_index_migration",
            run_id="deadline_index_migration",
            run_type="manual",
            run_after=NOW,
        )
    )
    session.execute(
        callback.insert().values(
            id=CALLBACK_ID,
            type="executor",
            fetch_method="import_path",
            data={"path": "test.callback"},
            state="success",
            priority_weight=1,
            created_at=NOW,
        )
    )
    session.execute(
        deadline.insert().values(
            id=DEADLINE_ID,
            dagrun_id=DAGRUN_ID,
            callback_id=CALLBACK_ID,
            deadline_time=NOW,
            missed=False,
            created_at=NOW,
            last_updated_at=NOW,
            deadline_alert_id=None,
        )
    )
    session.commit()
    statement = sa.select(deadline).where(deadline.c.id == DEADLINE_ID)
    callback_statement = sa.select(callback).where(callback.c.id == CALLBACK_ID)
    with engine.connect() as connection:
        expected_rows = connection.execute(statement).all()
        expected_callback = connection.execute(callback_statement).one()
    expected_foreign_keys = sa.inspect(engine).get_foreign_keys("deadline")

    try:
        for migrate, expected_index in (
            (MIGRATION.downgrade, False),
            (MIGRATION.upgrade, True),
            (MIGRATION.downgrade, False),
            (MIGRATION.upgrade, True),
        ):
            with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
                migrate()

            inspector = sa.inspect(engine)
            indexes = inspector.get_indexes("deadline")
            assert (INDEX_NAME in {index["name"] for index in indexes}) is expected_index
            run_indexes = [index for index in indexes if index["column_names"] == ["dagrun_id"]]
            if expected_index or engine.dialect.name == "mysql":
                assert len(run_indexes) == 1
                assert not run_indexes[0]["unique"]
                if expected_index and engine.dialect.name == "sqlite":
                    assert str(run_indexes[0]["dialect_options"]["sqlite_where"]) == "missed = 0"
            else:
                assert not run_indexes
            assert inspector.get_foreign_keys("deadline") == expected_foreign_keys
            with engine.begin() as connection:
                assert connection.execute(statement).all() == expected_rows
                assert connection.execute(callback_statement).one() == expected_callback
                connection.execute(dag_run.delete().where(dag_run.c.id == DAGRUN_ID))
                assert connection.execute(statement).all() == []
                connection.rollback()
    finally:
        if INDEX_NAME not in {index["name"] for index in sa.inspect(engine).get_indexes("deadline")}:
            with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
                MIGRATION.upgrade()
        with engine.begin() as connection:
            connection.execute(deadline.delete().where(deadline.c.id == DEADLINE_ID))
            connection.execute(callback.delete().where(callback.c.id == CALLBACK_ID))
            connection.execute(dag_run.delete().where(dag_run.c.id == DAGRUN_ID))
