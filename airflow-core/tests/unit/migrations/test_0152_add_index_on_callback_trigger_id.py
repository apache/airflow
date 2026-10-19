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
"""Tests for the callback trigger index migration."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from importlib import import_module
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from airflow import settings
from airflow.models.callback import Callback
from airflow.utils.db import downgrade, upgradedb

pytestmark = pytest.mark.db_test

_migration = import_module("airflow.migrations.versions.0152_3_5_0_add_index_on_callback_trigger_id")
REVISION = _migration.revision
PREVIOUS_REVISION = _migration.down_revision
INDEX_NAME = "idx_callback_trigger_id"
CALLBACK_IDS = (UUID(int=9_014_901), UUID(int=9_014_902), UUID(int=9_014_903), UUID(int=9_014_904))
INVALID_CALLBACK_ID = UUID(int=9_014_999)
TEST_TIMESTAMP = datetime(2026, 10, 19, tzinfo=UTC)
TRIGGER_CLASSPATHS = (
    "airflow.triggers.testing.CallbackIndexTrigger1",
    "airflow.triggers.testing.CallbackIndexTrigger2",
)


def _reflect_tables(engine):
    metadata = sa.MetaData()
    return (
        sa.Table("trigger", metadata, autoload_with=engine),
        sa.Table("callback", metadata, sa.Column("id", sa.Uuid(), primary_key=True), autoload_with=engine),
    )


def _delete_test_rows(engine):
    trigger, callback = _reflect_tables(engine)
    with engine.begin() as connection:
        connection.execute(callback.delete().where(callback.c.id.in_((*CALLBACK_IDS, INVALID_CALLBACK_ID))))
        connection.execute(trigger.delete().where(trigger.c.classpath.in_(TRIGGER_CLASSPATHS)))


@pytest.fixture
def database_at_previous_revision():
    """Move the database to the pre-index revision, then restore the current head."""
    original_async_conn = settings.SQL_ALCHEMY_CONN_ASYNC
    settings.SQL_ALCHEMY_CONN_ASYNC = ""
    settings.dispose_orm(do_log=False)
    settings.configure_orm(disable_connection_pool=True)
    try:
        downgrade(to_revision=PREVIOUS_REVISION)
        _delete_test_rows(settings.engine)
        yield settings.engine
    finally:
        try:
            upgradedb()
            _delete_test_rows(settings.engine)
        finally:
            settings.SQL_ALCHEMY_CONN_ASYNC = original_async_conn
            settings.reconfigure_orm()


@pytest.fixture
def stored_rows(database_at_previous_revision):
    trigger, callback = _reflect_tables(database_at_previous_revision)
    with database_at_previous_revision.begin() as connection:
        trigger_ids = tuple(
            connection.execute(
                trigger.insert().values(
                    classpath=classpath,
                    kwargs="{}",
                    created_date=TEST_TIMESTAMP,
                )
            ).inserted_primary_key[0]
            for classpath in TRIGGER_CLASSPATHS
        )
        connection.execute(
            callback.insert(),
            [
                {
                    "id": CALLBACK_IDS[0],
                    "type": "triggerer",
                    "fetch_method": "import_path",
                    "data": {},
                    "state": "scheduled",
                    "priority_weight": 1,
                    "created_at": TEST_TIMESTAMP,
                    "trigger_id": None,
                },
                {
                    "id": CALLBACK_IDS[1],
                    "type": "triggerer",
                    "fetch_method": "import_path",
                    "data": {},
                    "state": "scheduled",
                    "priority_weight": 1,
                    "created_at": TEST_TIMESTAMP,
                    "trigger_id": trigger_ids[0],
                },
                {
                    "id": CALLBACK_IDS[2],
                    "type": "triggerer",
                    "fetch_method": "import_path",
                    "data": {},
                    "state": "scheduled",
                    "priority_weight": 1,
                    "created_at": TEST_TIMESTAMP,
                    "trigger_id": trigger_ids[0],
                },
                {
                    "id": CALLBACK_IDS[3],
                    "type": "triggerer",
                    "fetch_method": "import_path",
                    "data": {},
                    "state": "scheduled",
                    "priority_weight": 1,
                    "created_at": TEST_TIMESTAMP,
                    "trigger_id": trigger_ids[1],
                },
            ],
        )
    return database_at_previous_revision, trigger_ids


def _get_callback_rows(engine):
    _, callback = _reflect_tables(engine)
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(callback.c.id, callback.c.trigger_id)
            .where(callback.c.id.in_(CALLBACK_IDS))
            .order_by(callback.c.id)
        ).all()
    return [(row.id, row.trigger_id) for row in rows]


def _get_callback_trigger_indexes(engine):
    return [
        index
        for index in inspect(engine).get_indexes("callback")
        if index["column_names"] and index["column_names"][0] == "trigger_id"
    ]


def _assert_explicit_index(engine):
    indexes = _get_callback_trigger_indexes(engine)
    assert [index["name"] for index in indexes] == [INDEX_NAME]
    index = indexes[0]
    assert index["column_names"] == ["trigger_id"]
    assert not index["unique"]

    dialect = engine.dialect.name
    if dialect in {"postgresql", "sqlite"}:
        predicate = index["dialect_options"][f"{dialect}_where"]
        normalized = re.sub(r'[\s()"`]+', "", str(predicate).lower())
        assert normalized == "trigger_idisnotnull"


def _assert_callback_foreign_key(engine):
    foreign_key = next(
        constraint
        for constraint in inspect(engine).get_foreign_keys("callback")
        if constraint["name"] == "callback_trigger_id_fkey"
    )
    assert foreign_key["constrained_columns"] == ["trigger_id"]
    assert foreign_key["referred_table"] == "trigger"
    assert foreign_key["referred_columns"] == ["id"]


def _assert_mysql_foreign_key_is_enforced(engine):
    if engine.dialect.name != "mysql":
        return
    _, callback = _reflect_tables(engine)
    with pytest.raises(IntegrityError), engine.begin() as connection:
        connection.execute(
            callback.insert().values(
                id=INVALID_CALLBACK_ID,
                type="triggerer",
                fetch_method="import_path",
                data={},
                state="scheduled",
                priority_weight=1,
                created_at=TEST_TIMESTAMP,
                trigger_id=99_999_999,
            )
        )


def test_callback_model_declares_partial_trigger_id_index():
    indexes = [index for index in Callback.__table__.indexes if index.name == INDEX_NAME]
    assert len(indexes) == 1
    index = indexes[0]
    assert [column.name for column in index.columns] == ["trigger_id"]
    assert not index.unique
    assert str(index.dialect_options["postgresql"]["where"]) == "callback.trigger_id IS NOT NULL"
    assert str(index.dialect_options["sqlite"]["where"]) == "callback.trigger_id IS NOT NULL"


@pytest.mark.backend("sqlite", "postgres", "mysql")
def test_upgrade_downgrade_reupgrade_preserves_rows_and_foreign_key(stored_rows):
    engine, trigger_ids = stored_rows
    expected_rows = [
        (CALLBACK_IDS[0], None),
        (CALLBACK_IDS[1], trigger_ids[0]),
        (CALLBACK_IDS[2], trigger_ids[0]),
        (CALLBACK_IDS[3], trigger_ids[1]),
    ]

    upgradedb(to_revision=REVISION)
    _assert_explicit_index(engine)
    _assert_callback_foreign_key(engine)
    assert _get_callback_rows(engine) == expected_rows

    downgrade(to_revision=PREVIOUS_REVISION)
    trigger_indexes = _get_callback_trigger_indexes(engine)
    if engine.dialect.name == "mysql":
        assert len(trigger_indexes) == 1
        assert trigger_indexes[0]["name"] != INDEX_NAME
    else:
        assert trigger_indexes == []
    _assert_callback_foreign_key(engine)
    _assert_mysql_foreign_key_is_enforced(engine)
    assert _get_callback_rows(engine) == expected_rows

    upgradedb(to_revision=REVISION)
    _assert_explicit_index(engine)
    _assert_callback_foreign_key(engine)
    assert _get_callback_rows(engine) == expected_rows
