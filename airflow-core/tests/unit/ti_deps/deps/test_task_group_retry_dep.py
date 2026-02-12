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

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.taskgroupinstance import TaskGroupInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.task_group_retry_dep import TaskGroupRetryDep
from airflow.utils.state import DagRunState

pytestmark = pytest.mark.db_test


def test_task_group_retry_dep_blocks_until_retry_time(dag_maker, session):
    with dag_maker(dag_id="tg_retry_dep", schedule=None, start_date=timezone.datetime(2024, 1, 1)) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_retry_dep_run", state=DagRunState.RUNNING, session=session)
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    session.add(tgi)
    session.commit()

    assert not TaskGroupRetryDep().is_met(ti=ti, session=session)


def test_task_group_retry_dep_allows_when_ready(dag_maker, session):
    with dag_maker(
        dag_id="tg_retry_dep_ready", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_retry_dep_ready_run", state=DagRunState.RUNNING, session=session)
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() - timedelta(minutes=1),
    )
    session.add(tgi)
    session.commit()

    assert TaskGroupRetryDep().is_met(ti=ti, session=session)


def test_task_group_retry_dep_ignored_via_dep_context(dag_maker, session):
    """T6: The dep should pass when ignore_task_group_retry_delay=True."""
    with dag_maker(
        dag_id="tg_retry_dep_ignore", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_retry_dep_ignore_run", state=DagRunState.RUNNING, session=session)
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    session.add(tgi)
    session.commit()

    # Without the flag — dep should block.
    assert not TaskGroupRetryDep().is_met(ti=ti, session=session)

    # With the flag — dep should pass.
    dep_context = DepContext(ignore_task_group_retry_delay=True)
    assert TaskGroupRetryDep().is_met(ti=ti, session=session, dep_context=dep_context)


def test_task_group_retry_dep_ignored_via_ignore_all_deps(dag_maker, session):
    """ignore_all_deps=True should bypass TaskGroupRetryDep (IGNORABLE=True)."""
    with dag_maker(
        dag_id="tg_retry_dep_ignore_all", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(
        run_id="tg_retry_dep_ignore_all_run", state=DagRunState.RUNNING, session=session
    )
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    session.add(tgi)
    session.commit()

    # Without the flag — dep should block.
    assert not TaskGroupRetryDep().is_met(ti=ti, session=session)

    # With ignore_all_deps — dep should pass.
    dep_context = DepContext(ignore_all_deps=True)
    assert TaskGroupRetryDep().is_met(ti=ti, session=session, dep_context=dep_context)


def test_task_group_retry_dep_uses_prefetched_retries(dag_maker, session):
    """S3: The dep should use pending_task_group_retries from DepContext instead of querying DB."""
    with dag_maker(
        dag_id="tg_retry_dep_prefetch", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(
        run_id="tg_retry_dep_prefetch_run", state=DagRunState.RUNNING, session=session
    )
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    # No DB row, but pre-fetched dict has this group as pending.
    fake_tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    dep_context = DepContext(pending_task_group_retries={"group": fake_tgi})
    assert not TaskGroupRetryDep().is_met(ti=ti, session=session, dep_context=dep_context)

    # Empty pre-fetched dict — should pass (no fallback to DB needed).
    dep_context_empty = DepContext(pending_task_group_retries={})
    assert TaskGroupRetryDep().is_met(ti=ti, session=session, dep_context=dep_context_empty)


def test_task_group_retry_dep_fallback_db_query_after_restart(dag_maker, session):
    """Simulate scheduler restart: dep should block via fallback DB query without prefetched data.

    After a scheduler restart, there is no prefetched pending_task_group_retries in DepContext.
    The dep must fall back to querying the DB directly and still block tasks whose group
    has a future next_retry_at.
    """
    with dag_maker(
        dag_id="tg_retry_dep_restart", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=2):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(
        run_id="tg_retry_dep_restart_run", state=DagRunState.RUNNING, session=session
    )
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    # Simulate a retry that was in progress when the scheduler crashed:
    # TGI row exists with a future next_retry_at, but no prefetched data is available.
    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    session.add(tgi)
    session.commit()

    # No dep_context at all — simulates a fresh scheduler that hasn't pre-fetched yet.
    assert not TaskGroupRetryDep().is_met(ti=ti, session=session)

    # With an empty DepContext (no pending_task_group_retries) — also falls back to DB.
    dep_context = DepContext()
    assert not TaskGroupRetryDep().is_met(ti=ti, session=session, dep_context=dep_context)


@pytest.mark.backend("postgres", "mysql")
def test_task_group_instance_cascade_deleted_with_dagrun(dag_maker, session):
    """T7: When a DagRun is deleted, TaskGroupInstance rows should be cascade-deleted via FK.

    SQLite does not enforce FK CASCADE by default, so this test only runs on Postgres/MySQL.
    """
    with dag_maker(dag_id="tg_cascade_delete", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        with TaskGroup(group_id="group", retries=1):
            EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_cascade_delete_run", state=DagRunState.RUNNING, session=session)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=5),
    )
    session.add(tgi)
    session.flush()

    # Verify the row exists.
    assert session.scalar(
        select(TaskGroupInstance).where(
            TaskGroupInstance.dag_id == dr.dag_id,
            TaskGroupInstance.run_id == dr.run_id,
        )
    )

    # Delete the DagRun — FK CASCADE should remove the TaskGroupInstance row.
    session.delete(dr)
    session.flush()

    assert (
        session.scalar(
            select(TaskGroupInstance).where(
                TaskGroupInstance.dag_id == "tg_cascade_delete",
                TaskGroupInstance.run_id == "tg_cascade_delete_run",
            )
        )
        is None
    )
