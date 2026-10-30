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
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import inspect, select

from airflow.models.callback import Callback

pytestmark = pytest.mark.db_test

MIGRATION = import_module("airflow.migrations.versions.0152_3_5_0_add_index_for_executor_callback_polling")
INDEX_NAME = "idx_callback_state_type_priority"
CALLBACK_IDS = (UUID(int=8_925_201), UUID(int=8_925_202), UUID(int=8_925_203))


def test_upgrade_downgrade_preserves_callback_rows_and_foreign_keys(session):
    engine = session.get_bind()
    table = Callback.__table__
    rows = [
        {
            "id": callback_id,
            "type": "executor" if state is not None else "dag_processor",
            "fetch_method": "import_path" if state is not None else "callback_request",
            "data": {"path": "test.callback", "value": priority},
            "state": state,
            "priority_weight": priority,
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        }
        for callback_id, state, priority in zip(CALLBACK_IDS, ("pending", "success", None), (7, 3, 11))
    ]
    session.execute(table.insert(), rows)
    session.commit()
    statement = select(table).where(table.c.id.in_(CALLBACK_IDS)).order_by(table.c.id)
    with engine.connect() as connection:
        expected_rows = connection.execute(statement).all()
    expected_foreign_keys = inspect(engine).get_foreign_keys("callback")

    try:
        for migrate, expected_index in (
            (MIGRATION.downgrade, False),
            (MIGRATION.upgrade, True),
            (MIGRATION.downgrade, False),
            (MIGRATION.upgrade, True),
        ):
            with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
                migrate()

            inspector = inspect(engine)
            indexes = [index for index in inspector.get_indexes("callback") if index["name"] == INDEX_NAME]
            assert bool(indexes) is expected_index
            if expected_index:
                assert indexes[0]["column_names"] == ["state", "type", "priority_weight"]
                assert not indexes[0]["unique"]
            assert inspector.get_foreign_keys("callback") == expected_foreign_keys
            with engine.connect() as connection:
                assert connection.execute(statement).all() == expected_rows
    finally:
        if INDEX_NAME not in {index["name"] for index in inspect(engine).get_indexes("callback")}:
            with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
                MIGRATION.upgrade()
        with engine.begin() as connection:
            connection.execute(table.delete().where(table.c.id.in_(CALLBACK_IDS)))
