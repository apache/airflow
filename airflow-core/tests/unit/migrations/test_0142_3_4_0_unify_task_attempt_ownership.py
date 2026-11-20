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
Regression tests for migration 0142 (e7c2a91bd540), which restores archived task attempts.

Before 3.4 every attempt of a task shared one ``task_instance`` row and therefore one ``id``; the
history rows of all those attempts carry that same ``task_instance_id``. Restoring them into
``task_instance`` (where the value becomes the primary key) collides on the primary key, not only
on ``task_instance_try_key``, so a statement scoped to the latter aborts the upgrade on both
PostgreSQL and SQLite for any deployment that ever retried a task.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

_MIGRATION_PATH = (
    Path(AIRFLOW_CORE_SOURCES_PATH)
    / "airflow/migrations/versions/0142_3_4_0_unify_task_attempt_ownership.py"
)
_spec = importlib.util.spec_from_file_location("migration_0142", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]

# The columns the insert has to carry to reproduce the collision. The coordinates are included
# because the SQLite branch names them in its conflict target; the helper forwards the list as-is,
# so a reduced set keeps the fixture small.
_COLUMNS = ["id", "dag_id", "task_id", "run_id", "map_index", "try_number"]
_TI_ROW = {"id": "legacy-shared-id", "dag_id": "d1", "task_id": "t1", "run_id": "r1", "map_index": -1,
           "try_number": 1}
_HISTORY_ROWS = [
    {"task_instance_id": "legacy-shared-id", "dag_id": "d1", "task_id": "t1", "run_id": "r1",
     "map_index": -1, "try_number": 0},
    {"task_instance_id": "legacy-shared-id", "dag_id": "d1", "task_id": "t1", "run_id": "r1",
     "map_index": -1, "try_number": 1},
]


def _make_tables() -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    ti = sa.Table(
        "pr0142_task_instance",
        metadata,
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("dag_id", sa.String(250), nullable=False),
        sa.Column("task_id", sa.String(250), nullable=False),
        sa.Column("run_id", sa.String(250), nullable=False),
        sa.Column("map_index", sa.Integer, nullable=False),
        sa.Column("try_number", sa.Integer, nullable=False),
        sa.UniqueConstraint(
            "dag_id", "task_id", "run_id", "map_index", "try_number",
            name="pr0142_task_instance_try_key",
        ),
    )
    # Mirrors the real table: the id is part of a composite primary key together with the task
    # coordinates and the try number, which is what lets one task keep several archived attempts.
    history = sa.Table(
        "pr0142_task_instance_history",
        metadata,
        sa.Column("task_instance_id", sa.String(36), primary_key=True),
        sa.Column("dag_id", sa.String(250), primary_key=True),
        sa.Column("task_id", sa.String(250), primary_key=True),
        sa.Column("run_id", sa.String(250), primary_key=True),
        sa.Column("map_index", sa.Integer, primary_key=True),
        sa.Column("try_number", sa.Integer, primary_key=True),
    )
    return ti, history


def _history_rows(history: sa.Table):
    return sa.select(
        history.c.task_instance_id,
        history.c.dag_id,
        history.c.task_id,
        history.c.run_id,
        history.c.map_index,
        history.c.try_number,
    )


def test_postgres_statement_has_no_conflict_target():
    """Any unique violation must be skipped: the primary key is one of them."""
    ti, history = _make_tables()

    stmt = _migration._history_insert_statement("postgresql", ti, _history_rows(history), _COLUMNS)
    sql = str(stmt.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT DO NOTHING" in sql
    assert "ON CONSTRAINT" not in sql


@pytest.mark.db_test
def test_restoring_two_attempts_of_one_task_does_not_abort(session):
    """Two archived attempts sharing a legacy id must be skipped, not raise."""
    if session.get_bind().dialect.name == "mysql":
        pytest.skip("the migration pre-filters existing rows for MySQL instead of using ON CONFLICT")

    ti, history = _make_tables()
    bind = session.get_bind()
    ti.create(bind=bind)
    history.create(bind=bind)
    try:
        session.execute(ti.insert(), [_TI_ROW])
        session.execute(history.insert(), _HISTORY_ROWS)
        session.flush()

        stmt = _migration._history_insert_statement(
            bind.dialect.name, ti, _history_rows(history), _COLUMNS
        )
        session.execute(stmt)
        session.flush()

        # The live attempt stays, the archived duplicates are skipped.
        assert session.scalar(sa.select(sa.func.count()).select_from(ti)) == 1
    finally:
        history.drop(bind=bind, checkfirst=True)
        ti.drop(bind=bind, checkfirst=True)
