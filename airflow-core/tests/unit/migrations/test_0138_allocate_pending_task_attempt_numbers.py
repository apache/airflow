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

from __future__ import annotations

from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.migration import MigrationContext
from alembic.operations import Operations

from airflow import settings
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.db import _get_alembic_config
from airflow.utils.state import TaskInstanceState

_migration = import_module("airflow.migrations.versions.0138_3_4_0_allocate_pending_task_attempt_numbers")


@pytest.mark.db_test
@pytest.mark.parametrize("with_history", [False, True])
@pytest.mark.parametrize(
    ("state", "try_number", "allocated_try_number"),
    [
        (None, 0, 0),
        (None, 1, 2),
        (None, 4, 5),
        (TaskInstanceState.UP_FOR_RETRY, 0, 1),
        (TaskInstanceState.UP_FOR_RETRY, 2, 3),
        *[(state, 2, 2) for state in TaskInstanceState if state != TaskInstanceState.UP_FOR_RETRY],
    ],
)
def test_pending_attempt_migration_preserves_other_data(
    dag_maker, session, state, try_number, allocated_try_number, with_history
):
    with dag_maker():
        EmptyOperator(task_id="task", retries=5)
    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance("task", session=session)
    ti.state = state
    ti.try_number = try_number
    ti.end_date = dr.start_date + timedelta(seconds=10)
    ti.retry_delay_override = 123
    ti.retry_reason = "Retry after the service recovers"
    if with_history:
        history = TaskInstanceHistory(ti, state=TaskInstanceState.FAILED)
        history.task_instance_id = uuid4()
        session.add(history)
    session.flush()
    live_query = sa.select(TaskInstance.__table__).where(TaskInstance.id == ti.id)
    history_query = sa.select(TaskInstanceHistory.__table__).where(TaskInstanceHistory.dag_id == ti.dag_id)
    live_before = dict(session.execute(live_query).mappings().one())
    history_before = session.execute(history_query).mappings().all()

    with Operations.context(MigrationContext.configure(session.connection())):
        _migration.upgrade()

    assert dict(session.execute(live_query).mappings().one()) == {
        **live_before,
        "try_number": allocated_try_number,
    }
    assert session.execute(history_query).mappings().all() == history_before

    with Operations.context(MigrationContext.configure(session.connection())):
        _migration.downgrade()

    assert dict(session.execute(live_query).mappings().one()) == live_before
    assert session.execute(history_query).mappings().all() == history_before


@pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "sqlite"])
@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_pending_attempt_migration_emits_sql_offline(capsys, monkeypatch, dialect_name, direction):
    config = _get_alembic_config()
    monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", f"{dialect_name}://")
    revisions = f"{_migration.down_revision}:{_migration.revision}"
    if direction == "downgrade":
        revisions = f"{_migration.revision}:{_migration.down_revision}"
    getattr(command, direction)(config, revisions, sql=True)

    emitted = capsys.readouterr().out
    assert "UPDATE task_instance SET try_number=" in emitted
    assert "up_for_retry" in emitted
    assert "task_instance.state IS NULL" in emitted
    assert "task_instance_history" not in emitted
