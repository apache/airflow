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
from airflow.models.hitl import HITLDetail
from airflow.models.hitl_history import HITLDetailHistory
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.db import _get_alembic_config
from airflow.utils.state import TaskInstanceState

_migration = import_module("airflow.migrations.versions.0138_3_4_0_allocate_pending_task_attempt_numbers")


@pytest.fixture
def run_migration(session):
    def run(direction):
        with Operations.context(MigrationContext.configure(session.connection())):
            direction()

    return run


@pytest.mark.db_test
@pytest.mark.parametrize(
    ("state", "try_number", "allocated_try_number", "with_history"),
    [
        (state, previous, allocated, with_history)
        for state, previous, allocated in [
            (None, 0, 0),
            (None, 1, 2),
            (None, 4, 5),
            (TaskInstanceState.UP_FOR_RETRY, 0, 1),
            (TaskInstanceState.UP_FOR_RETRY, 2, 3),
            *[(state, 2, 2) for state in TaskInstanceState if state != TaskInstanceState.UP_FOR_RETRY],
        ]
        for with_history in (False, True)
        if with_history or state != TaskInstanceState.UP_FOR_RETRY
    ],
)
def test_pending_attempt_migration_preserves_other_data(
    dag_maker, session, run_migration, state, try_number, allocated_try_number, with_history
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

    run_migration(_migration.upgrade)

    assert dict(session.execute(live_query).mappings().one()) == {
        **live_before,
        "try_number": allocated_try_number,
    }
    assert session.execute(history_query).mappings().all() == history_before

    run_migration(_migration.downgrade)

    assert dict(session.execute(live_query).mappings().one()) == live_before
    assert session.execute(history_query).mappings().all() == history_before


@pytest.mark.db_test
@pytest.mark.parametrize("try_number", [0, 2])
@pytest.mark.parametrize("map_index", [-1, 0])
def test_pending_retry_migration_archives_missing_attempt(
    dag_maker, session, run_migration, try_number, map_index
):
    """Retire a legacy retry UUID without losing attempt data or breaking UUID-based dependents.

    A queued/scheduled failure in older releases did not archive or rotate its UUID. The migration
    must do both before advancing its try number. Downgrade keeps that archive and fresh UUID,
    matching the old representation of a pending retry, so upgrading again must not allocate twice.
    """
    with dag_maker():
        EmptyOperator(task_id="task", retries=5)
    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance("task", session=session)
    ti.state = TaskInstanceState.UP_FOR_RETRY
    ti.try_number = try_number
    ti.map_index = map_index
    ti.start_date = dr.start_date
    ti.end_date = dr.start_date + timedelta(seconds=10)
    ti.duration = 10
    ti.retry_delay_override = 123
    ti.retry_reason = "Retry after the service recovers"
    ti.executor_config = {"test": "keep executor configuration"}
    ti.next_kwargs = {"test": "keep continuation arguments"}
    session.flush()
    old_id = ti.id
    session.add(
        HITLDetail(
            ti_id=old_id,
            options=["approve", "reject"],
            subject="Review",
            body="Review the submitted request",
            defaults=["approve"],
            multiple=True,
            params={"reason": {"type": "string"}},
            assignees=[{"id": "reviewer", "name": "Reviewer"}],
            created_at=ti.start_date,
            responded_at=ti.end_date,
            responded_by={"id": "reviewer", "name": "Reviewer"},
            chosen_options=["approve"],
            params_input={"reason": "Approved after review"},
        )
    )
    note = TaskInstanceNote("Retain this note")
    note.ti_id = old_id
    session.add(note)
    session.add(TaskReschedule(old_id, ti.start_date, ti.end_date, ti.end_date))
    session.flush()
    live_query = sa.select(TaskInstance.__table__).where(
        TaskInstance.dag_id == ti.dag_id,
        TaskInstance.task_id == ti.task_id,
        TaskInstance.run_id == ti.run_id,
        TaskInstance.map_index == map_index,
    )
    live_before = dict(session.execute(live_query).mappings().one())
    hitl_before = dict(session.execute(sa.select(HITLDetail.__table__)).mappings().one())

    run_migration(_migration.upgrade)

    live_after = dict(session.execute(live_query).mappings().one())
    new_id = live_after["id"]
    assert new_id != old_id
    assert new_id.version == 7
    assert live_after == {**live_before, "id": new_id, "try_number": try_number + 1}
    history_query = sa.select(TaskInstanceHistory.__table__).where(
        TaskInstanceHistory.task_instance_id == old_id
    )
    history = dict(session.execute(history_query).mappings().one())
    assert history == {
        **{column: live_before[column] for column in history if column != "task_instance_id"},
        "task_instance_id": old_id,
        "state": TaskInstanceState.FAILED,
    }
    assert (
        session.scalar(sa.select(TaskInstanceNote.ti_id).where(TaskInstanceNote.content == note.content))
        == new_id
    )
    assert session.scalar(sa.select(sa.func.count()).select_from(TaskReschedule)) == 0
    assert dict(session.execute(sa.select(HITLDetail.__table__)).mappings().one()) == {
        **hitl_before,
        "ti_id": new_id,
    }
    assert dict(session.execute(sa.select(HITLDetailHistory.__table__)).mappings().one()) == {
        **{column: value for column, value in hitl_before.items() if column != "ti_id"},
        "ti_history_id": old_id,
    }

    run_migration(_migration.downgrade)
    assert dict(session.execute(live_query).mappings().one()) == {**live_before, "id": new_id}
    assert dict(session.execute(history_query).mappings().one()) == history
    run_migration(_migration.upgrade)
    assert dict(session.execute(live_query).mappings().one()) == live_after
    assert dict(session.execute(history_query).mappings().one()) == history


@pytest.mark.db_test
def test_pending_retry_migration_only_retires_unarchived_tries_in_mixed_batch(
    dag_maker, session, run_migration
):
    """Only missing retry histories retire UUIDs; their dependent rows must follow the same selection.

    Older tries and other map indexes must not prevent archival. Already archived retries and
    cleared tasks advance their try numbers without rotating UUIDs or changing dependent rows.
    Unstarted, running, failed, and rescheduled tasks must retain all their data.
    """
    with dag_maker() as dag:
        task = EmptyOperator(task_id="task", retries=5)
    dr = dag_maker.create_dagrun()
    session.execute(
        sa.delete(TaskInstance).where(TaskInstance.dag_id == dag.dag_id, TaskInstance.run_id == dr.run_id)
    )
    states_and_tries = [
        (TaskInstanceState.UP_FOR_RETRY, 2),
        (TaskInstanceState.UP_FOR_RETRY, 2),
        (TaskInstanceState.UP_FOR_RETRY, 2),
        (None, 2),
        (None, 0),
        (TaskInstanceState.RUNNING, 2),
        (TaskInstanceState.FAILED, 2),
        (TaskInstanceState.UP_FOR_RESCHEDULE, 2),
    ]
    tasks = [
        TaskInstance(
            task=task,
            run_id=dr.run_id,
            map_index=index,
            dag_version_id=dr.created_dag_version_id,
            state=state,
        )
        for index, (state, _) in enumerate(states_and_tries)
    ]
    for ti, (_, try_number) in zip(tasks, states_and_tries):
        ti.try_number = try_number
    session.add_all(tasks)
    session.flush()

    dependents = {}
    for index in (0, 1, 2, 3, 5, 6, 7):
        ti = tasks[index]
        hitl = HITLDetail(
            ti_id=ti.id,
            options=["approve"],
            subject=f"Review index {index}",
            params={},
            params_input={},
        )
        dependents[index] = hitl
        note = TaskInstanceNote(f"Note for index {index}")
        note.ti_id = ti.id
        session.add_all(
            [
                hitl,
                note,
                TaskReschedule(ti.id, dr.start_date, dr.start_date, dr.start_date),
                TaskReschedule(ti.id, dr.start_date, dr.start_date, dr.start_date + timedelta(minutes=1)),
            ]
        )
    session.flush()

    for index, try_number in ((0, 2), (1, 1), (3, 2)):
        history = TaskInstanceHistory(tasks[index], state=TaskInstanceState.FAILED)
        history.task_instance_id = uuid4()
        history.try_number = try_number
        session.add(history)
        session.flush()
        hitl_history = HITLDetailHistory(dependents[index])
        hitl_history.ti_history_id = history.task_instance_id
        hitl_history.subject = f"Previous review for index {index}"
        session.add(hitl_history)
    session.flush()

    def rows_by(model, key):
        return {row[key]: dict(row) for row in session.execute(sa.select(model.__table__)).mappings()}

    live_before = rows_by(TaskInstance, "map_index")
    history_before = rows_by(TaskInstanceHistory, "task_instance_id")
    hitl_before = rows_by(HITLDetail, "ti_id")
    hitl_history_before = rows_by(HITLDetailHistory, "ti_history_id")
    notes_before = rows_by(TaskInstanceNote, "ti_id")
    reschedules_before = rows_by(TaskReschedule, "id")
    old_ids = [ti.id for ti in tasks]

    run_migration(_migration.upgrade)

    live_after = rows_by(TaskInstance, "map_index")
    new_ids = [live_after[index]["id"] for index in range(len(tasks))]
    assert len(set(new_ids)) == len(tasks)
    for index in (1, 2):
        assert new_ids[index] not in old_ids
        assert new_ids[index].version == 7
    for index in (0, 3, 4, 5, 6, 7):
        assert new_ids[index] == old_ids[index]
    assert live_after == {
        index: {**live_before[index], "id": new_ids[index], "try_number": try_number}
        for index, try_number in enumerate([3, 3, 3, 3, 0, 2, 2, 2])
    }
    expected_history = {
        **history_before,
        **{
            old_ids[index]: {
                **{
                    column.name: live_before[index][column.name]
                    for column in TaskInstanceHistory.__table__.columns
                    if column.name != "task_instance_id"
                },
                "task_instance_id": old_ids[index],
                "state": TaskInstanceState.FAILED,
            }
            for index in (1, 2)
        },
    }
    expected_hitl = {
        new_ids[index]: {**hitl_before[old_ids[index]], "ti_id": new_ids[index]} for index in dependents
    }
    expected_hitl_history = {
        **hitl_history_before,
        **{
            old_ids[index]: {
                **{key: value for key, value in hitl_before[old_ids[index]].items() if key != "ti_id"},
                "ti_history_id": old_ids[index],
            }
            for index in (1, 2)
        },
    }
    expected_notes = {
        new_ids[index]: {**notes_before[old_ids[index]], "ti_id": new_ids[index]} for index in dependents
    }
    expected_reschedules = {
        key: row for key, row in reschedules_before.items() if row["ti_id"] not in (old_ids[1], old_ids[2])
    }

    assert rows_by(TaskInstanceHistory, "task_instance_id") == expected_history
    assert rows_by(HITLDetail, "ti_id") == expected_hitl
    assert rows_by(HITLDetailHistory, "ti_history_id") == expected_hitl_history
    assert rows_by(TaskInstanceNote, "ti_id") == expected_notes
    assert rows_by(TaskReschedule, "id") == expected_reschedules

    run_migration(_migration.downgrade)
    run_migration(_migration.upgrade)

    assert rows_by(TaskInstance, "map_index") == live_after
    assert rows_by(TaskInstanceHistory, "task_instance_id") == expected_history
    assert rows_by(HITLDetail, "ti_id") == expected_hitl
    assert rows_by(HITLDetailHistory, "ti_history_id") == expected_hitl_history
    assert rows_by(TaskInstanceNote, "ti_id") == expected_notes
    assert rows_by(TaskReschedule, "id") == expected_reschedules


@pytest.mark.db_test
def test_pending_retry_migration_does_not_match_history_from_other_dags_tasks_or_runs(
    dag_maker, session, run_migration
):
    with dag_maker(dag_id="first_dag"):
        EmptyOperator(task_id="task")
        EmptyOperator(task_id="other_task")
    first_run = dag_maker.create_dagrun(run_id="first_run")
    later_run = dag_maker.create_dagrun_after(first_run, run_id="later_run")
    with dag_maker(dag_id="other_dag"):
        EmptyOperator(task_id="task")
    other_dag_run = dag_maker.create_dagrun(run_id="first_run")
    target = first_run.get_task_instance("task", session=session)
    other_tasks = [
        first_run.get_task_instance("other_task", session=session),
        later_run.get_task_instance("task", session=session),
        other_dag_run.get_task_instance("task", session=session),
    ]
    for ti in [target, *other_tasks]:
        ti.state = TaskInstanceState.UP_FOR_RETRY
        ti.try_number = 2
    for ti in other_tasks:
        history = TaskInstanceHistory(ti, state=TaskInstanceState.FAILED)
        history.task_instance_id = uuid4()
        session.add(history)
    session.flush()
    old_id = target.id
    other_ids = [ti.id for ti in other_tasks]
    history_before = {
        row.task_instance_id: dict(row)
        for row in session.execute(sa.select(TaskInstanceHistory.__table__)).mappings()
    }

    run_migration(_migration.upgrade)
    session.expire_all()

    migrated = first_run.get_task_instance("task", session=session)
    assert migrated.id != old_id
    assert migrated.try_number == 3
    assert session.get(TaskInstanceHistory, old_id).try_number == 2
    for ti_id in other_ids:
        assert session.get(TaskInstance, ti_id).try_number == 3
    assert {
        row.task_instance_id: dict(row)
        for row in session.execute(
            sa.select(TaskInstanceHistory.__table__).where(TaskInstanceHistory.task_instance_id != old_id)
        ).mappings()
    } == history_before


@pytest.mark.db_test
def test_pending_try_migration_handles_empty_tables(dag_maker, session, run_migration):
    with dag_maker():
        EmptyOperator(task_id="task")
    dag_maker.create_dagrun()
    session.execute(sa.delete(TaskInstance))

    run_migration(_migration.upgrade)
    run_migration(_migration.downgrade)
    run_migration(_migration.upgrade)

    for model in (
        TaskInstance,
        TaskInstanceHistory,
        HITLDetail,
        HITLDetailHistory,
        TaskInstanceNote,
        TaskReschedule,
    ):
        assert session.scalar(sa.select(sa.func.count()).select_from(model)) == 0


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
    assert "UPDATE task_instance SET" in emitted
    assert "try_number=(task_instance.try_number " in emitted
    assert "up_for_retry" in emitted
    assert "task_instance.state IS NULL" in emitted
    if direction == "upgrade":
        assert "INSERT INTO task_instance_history" in emitted
        assert "INSERT INTO hitl_detail_history" in emitted
        assert "DELETE FROM task_reschedule" in emitted
    else:
        assert "task_instance_history" not in emitted
