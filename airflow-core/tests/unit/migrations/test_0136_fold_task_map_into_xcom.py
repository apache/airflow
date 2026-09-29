#
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

"""
Tests for migration 0136 (3b7a91c5df20), which folds task_map into xcom.mapped_length.

An in-flight DagRun's expansion length either survives the backfill or quietly disappears,
so the real statements run against isolated tables on whichever backend the suite is using.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

from airflow import settings

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

# Migration filenames start with a digit so they cannot be imported via the normal import
# system; load the module by file path instead.
_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0136_3_4_0_fold_task_map_into_xcom_mapped_length.py"
)
_spec = importlib.util.spec_from_file_location("migration_0134", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

# Isolated because the live xcom table has FK and NOT NULL columns, and task_map is dropped.
_XCOM = "_test_xcom_0134"
_TASK_MAP = "_test_task_map_0134"

_RETURN_VALUE = "return_value"

_metadata = sa.MetaData()
_xcom = sa.Table(
    _XCOM,
    _metadata,
    sa.Column("dag_id", sa.String(250)),
    sa.Column("task_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
    sa.Column("map_index", sa.Integer),
    # Declared rather than raw DDL so SQLAlchemy quotes it: reserved on MySQL.
    sa.Column("key", sa.String(512)),
    sa.Column("mapped_length", sa.Integer),
)
_task_map = sa.Table(
    _TASK_MAP,
    _metadata,
    sa.Column("dag_id", sa.String(250), nullable=False),
    sa.Column("task_id", sa.String(250), nullable=False),
    sa.Column("run_id", sa.String(250), nullable=False),
    sa.Column("map_index", sa.Integer, nullable=False),
    sa.Column("length", sa.Integer, nullable=False),
    sa.Column("keys", sa.String(512)),
    # The key-less PK the downgrade recreates, so an unscoped restore collides here.
    sa.PrimaryKeyConstraint("dag_id", "task_id", "run_id", "map_index"),
)

_FEEDS_MAPPED = ("d", "feeds_mapped", "r", -1)
_PLAIN = ("d", "plain", "r", -1)


def _xcom_row(coords, key, mapped_length=None):
    dag_id, task_id, run_id, map_index = coords
    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "map_index": map_index,
        "key": key,
        "mapped_length": mapped_length,
    }


def _task_map_row(coords, length):
    dag_id, task_id, run_id, map_index = coords
    return {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "map_index": map_index,
        "length": length,
        "keys": None,
    }


@pytest.fixture
def conn():
    _metadata.drop_all(settings.engine)
    _metadata.create_all(settings.engine)
    try:
        with settings.engine.begin() as connection:
            yield connection
    finally:
        _metadata.drop_all(settings.engine)


def _lengths(conn) -> dict[tuple[str, str], int | None]:
    rows = conn.execute(sa.select(_xcom.c.task_id, _xcom.c.key, _xcom.c.mapped_length)).all()
    return {(r.task_id, r.key): r.mapped_length for r in rows}


def test_backfill_copies_the_length_onto_the_return_value_row(conn):
    conn.execute(
        _xcom.insert(),
        [
            _xcom_row(_FEEDS_MAPPED, _RETURN_VALUE),
            _xcom_row(_FEEDS_MAPPED, "side_output"),
            _xcom_row(_PLAIN, _RETURN_VALUE),
        ],
    )
    conn.execute(_task_map.insert(), [_task_map_row(_FEEDS_MAPPED, 3)])

    conn.execute(_migration.build_backfill_statement(_XCOM, _TASK_MAP))

    assert _lengths(conn) == {
        ("feeds_mapped", _RETURN_VALUE): 3,
        ("feeds_mapped", "side_output"): None,
        ("plain", _RETURN_VALUE): None,
    }


def test_backfill_is_idempotent(conn):
    conn.execute(_xcom.insert(), [_xcom_row(_FEEDS_MAPPED, _RETURN_VALUE)])
    conn.execute(_task_map.insert(), [_task_map_row(_FEEDS_MAPPED, 3)])

    conn.execute(_migration.build_backfill_statement(_XCOM, _TASK_MAP))
    conn.execute(_migration.build_backfill_statement(_XCOM, _TASK_MAP))

    assert _lengths(conn) == {("feeds_mapped", _RETURN_VALUE): 3}


def test_restore_rebuilds_task_map_from_the_return_value_length(conn):
    conn.execute(
        _xcom.insert(),
        [
            _xcom_row(_FEEDS_MAPPED, _RETURN_VALUE, mapped_length=3),
            _xcom_row(_PLAIN, _RETURN_VALUE),
        ],
    )

    conn.execute(_migration.build_restore_statement(_XCOM, _TASK_MAP))

    # Subscripted because ``.c.keys`` would resolve to ColumnCollection.keys, the method.
    assert conn.execute(sa.select(_task_map.c.task_id, _task_map.c.length, _task_map.c["keys"])).all() == [
        ("feeds_mapped", 3, None)
    ]


def test_restore_ignores_a_length_recorded_under_another_key(conn):
    """Restoring both keys would collide on task_map's key-less primary key."""
    conn.execute(
        _xcom.insert(),
        [
            _xcom_row(_FEEDS_MAPPED, _RETURN_VALUE, mapped_length=3),
            _xcom_row(_FEEDS_MAPPED, "side_output", mapped_length=9),
        ],
    )

    conn.execute(_migration.build_restore_statement(_XCOM, _TASK_MAP))

    assert conn.execute(sa.select(_task_map.c.task_id, _task_map.c.length)).all() == [("feeds_mapped", 3)]
