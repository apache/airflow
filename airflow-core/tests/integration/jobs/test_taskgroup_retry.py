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
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.models.log import Log
from airflow.models.taskgroupinstance import TaskGroupInstance
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.dag import _run_task
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.utils.state import DagRunState, State


@pytest.mark.integration("scheduler")
@pytest.mark.db_test
@pytest.mark.need_serialized_dag(True)
def test_task_group_retry_increments_try_number(dag_maker, session):
    def always_fail():
        raise AirflowException("fail")

    with dag_maker(
        dag_id="test_task_group_retry_integration",
        schedule=None,
        start_date=timezone.datetime(2024, 1, 1),
    ):
        with TaskGroup(
            group_id="group",
            retries=2,
            retry_delay=timedelta(seconds=0),
            retry_exponential_backoff=0,
        ) as group:
            fail_task = PythonOperator(task_id="fail", python_callable=always_fail)

    dag_maker.dag_model.is_paused = False
    dag_maker.dag_model.is_active = True
    session.merge(dag_maker.dag_model)
    session.commit()

    dag = dag_maker.dag
    serialized_dag = dag_maker.serialized_dag
    dr = dag_maker.create_dagrun(
        run_id="test_tg_retry_integration",
        state=DagRunState.RUNNING,
        session=session,
        data_interval=(timezone.utcnow(), timezone.utcnow()),
    )
    dag_id = dr.dag_id
    run_id = dr.run_id

    fail_task_id = fail_task.task_id
    expected_group_retries = group.retries
    expected_task_tries = group.retries + 1

    max_cycles = 20
    for _ in range(max_cycles):
        dr.dag = serialized_dag
        schedulable_tis, _ = dr.update_state(session=session, execute_callbacks=False)
        session.flush()

        if dr.state in State.finished_dr_states:
            break

        if not schedulable_tis:
            session.expire_all()
            dr = session.get(DagRun, dr.id)
            if dr:
                dr.dag = serialized_dag
            continue

        dr.schedule_tis(schedulable_tis, session=session)
        session.commit()

        for ti in schedulable_tis:
            refreshed_ti = session.get(TaskInstance, ti.id)
            assert refreshed_ti is not None
            refreshed_ti.task = dag.get_task(refreshed_ti.task_id)
            _run_task(ti=refreshed_ti, task=refreshed_ti.task)

        session.commit()
        session.expire_all()
        dr = session.get(DagRun, dr.id)
        if dr:
            dr.dag = serialized_dag

    assert dr.state == DagRunState.FAILED

    tgi = session.scalar(
        select(TaskGroupInstance).where(
            TaskGroupInstance.dag_id == dag_id,
            TaskGroupInstance.run_id == run_id,
            TaskGroupInstance.task_group_id == group.group_id,
        )
    )
    assert tgi is not None
    assert tgi.try_number == expected_group_retries

    fail_ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == fail_task_id,
        )
    )
    assert fail_ti is not None
    assert fail_ti.state == State.FAILED
    assert fail_ti.try_number == expected_task_tries


@pytest.mark.integration("scheduler")
@pytest.mark.db_test
@pytest.mark.need_serialized_dag(True)
def test_task_group_retry_delay_blocks_until_ready(dag_maker, session, time_machine):
    def always_fail():
        raise AirflowException("fail")

    with dag_maker(
        dag_id="test_task_group_retry_delay",
        schedule=None,
        start_date=timezone.datetime(2024, 1, 1),
    ):
        with TaskGroup(
            group_id="group",
            retries=1,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=0,
        ) as group:
            PythonOperator(task_id="fail", python_callable=always_fail)

    dag_maker.dag_model.is_paused = False
    dag_maker.dag_model.is_active = True
    session.merge(dag_maker.dag_model)
    session.commit()

    frozen_now = timezone.datetime(2024, 1, 1, 0, 0, 0)
    time_machine.move_to(frozen_now, tick=False)

    dag = dag_maker.dag
    serialized_dag = dag_maker.serialized_dag
    dr = dag_maker.create_dagrun(
        run_id="test_tg_retry_delay",
        state=DagRunState.RUNNING,
        session=session,
        data_interval=(frozen_now, frozen_now),
    )

    dr.dag = serialized_dag
    schedulable_tis, _ = dr.update_state(session=session, execute_callbacks=False)
    assert schedulable_tis
    dr.schedule_tis(schedulable_tis, session=session)
    session.commit()

    for ti in schedulable_tis:
        refreshed_ti = session.get(TaskInstance, ti.id)
        assert refreshed_ti is not None
        refreshed_ti.task = dag.get_task(refreshed_ti.task_id)
        _run_task(ti=refreshed_ti, task=refreshed_ti.task)

    session.commit()
    session.expire_all()
    dr = session.get(DagRun, dr.id)
    assert dr is not None
    dr.dag = serialized_dag
    dr.update_state(session=session, execute_callbacks=False)

    tgi = session.scalar(
        select(TaskGroupInstance).where(
            TaskGroupInstance.dag_id == dr.dag_id,
            TaskGroupInstance.run_id == dr.run_id,
            TaskGroupInstance.task_group_id == group.group_id,
        )
    )
    assert tgi is not None
    assert tgi.try_number == 1
    assert tgi.next_retry_at == frozen_now + timedelta(minutes=5)

    time_machine.move_to(tgi.next_retry_at - timedelta(seconds=1), tick=False)
    session.expire_all()
    dr = session.get(DagRun, dr.id)
    assert dr is not None
    dr.dag = serialized_dag
    schedulable_tis, _ = dr.update_state(session=session, execute_callbacks=False)
    assert not schedulable_tis

    time_machine.move_to(tgi.next_retry_at + timedelta(seconds=1), tick=False)
    session.expire_all()
    dr = session.get(DagRun, dr.id)
    assert dr is not None
    dr.dag = serialized_dag
    schedulable_tis, _ = dr.update_state(session=session, execute_callbacks=False)
    assert schedulable_tis


@pytest.mark.integration("scheduler")
@pytest.mark.db_test
@pytest.mark.need_serialized_dag(True)
def test_task_group_retry_clear_resets_instance(dag_maker, session):
    def always_fail():
        raise AirflowException("fail")

    with dag_maker(
        dag_id="test_task_group_retry_clear",
        schedule=None,
        start_date=timezone.datetime(2024, 1, 1),
    ):
        with TaskGroup(
            group_id="group",
            retries=2,
            retry_delay=timedelta(seconds=0),
            retry_exponential_backoff=0,
        ) as group:
            fail_task = PythonOperator(task_id="fail", python_callable=always_fail)
            success_task = PythonOperator(task_id="success", python_callable=lambda: None)

    dag_maker.dag_model.is_paused = False
    dag_maker.dag_model.is_active = True
    session.merge(dag_maker.dag_model)
    session.commit()

    dag = dag_maker.dag
    serialized_dag = dag_maker.serialized_dag
    dr = dag_maker.create_dagrun(
        run_id="test_tg_retry_clear",
        state=DagRunState.RUNNING,
        session=session,
        data_interval=(timezone.utcnow(), timezone.utcnow()),
    )

    dr.dag = serialized_dag
    schedulable_tis, _ = dr.update_state(session=session, execute_callbacks=False)
    assert schedulable_tis
    dr.schedule_tis(schedulable_tis, session=session)
    session.commit()

    for ti in schedulable_tis:
        refreshed_ti = session.get(TaskInstance, ti.id)
        assert refreshed_ti is not None
        refreshed_ti.task = dag.get_task(refreshed_ti.task_id)
        _run_task(ti=refreshed_ti, task=refreshed_ti.task)

    session.commit()
    session.expire_all()
    dr = session.get(DagRun, dr.id)
    assert dr is not None
    dr.dag = serialized_dag
    dr.update_state(session=session, execute_callbacks=False)

    tgi = session.scalar(
        select(TaskGroupInstance).where(
            TaskGroupInstance.dag_id == dr.dag_id,
            TaskGroupInstance.run_id == dr.run_id,
            TaskGroupInstance.task_group_id == group.group_id,
        )
    )
    assert tgi is not None
    assert tgi.try_number == 1

    serialized_dag.clear(run_id=dr.run_id, dag_run_state=DagRunState.QUEUED, session=session)

    cleared_tgi = session.scalar(
        select(TaskGroupInstance).where(
            TaskGroupInstance.dag_id == dr.dag_id,
            TaskGroupInstance.run_id == dr.run_id,
            TaskGroupInstance.task_group_id == group.group_id,
        )
    )
    assert cleared_tgi is None

    cleared_fail_ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.run_id == dr.run_id,
            TaskInstance.task_id == fail_task.task_id,
        )
    )
    cleared_success_ti = session.scalar(
        select(TaskInstance).where(
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.run_id == dr.run_id,
            TaskInstance.task_id == success_task.task_id,
        )
    )
    assert cleared_fail_ti is not None
    assert cleared_success_ti is not None
    assert cleared_fail_ti.state is None
    assert cleared_success_ti.state is None


@pytest.mark.integration("scheduler")
@pytest.mark.db_test
@pytest.mark.need_serialized_dag(True)
def test_task_group_retry_fast_fail_stops_sibling_tasks(dag_maker, session):
    with dag_maker(
        dag_id="test_task_group_retry_fast_fail",
        schedule=None,
        start_date=timezone.datetime(2024, 1, 1),
    ) as dag:
        with TaskGroup(group_id="group", retries=1, retry_fast_fail=True):
            fail_task = EmptyOperator(task_id="fail")
            running_task = EmptyOperator(task_id="running")
            queued_task = EmptyOperator(task_id="queued")
        outside_task = EmptyOperator(task_id="outside")

    dr = dag_maker.create_dagrun(
        run_id="test_tg_retry_fast_fail",
        state=DagRunState.RUNNING,
        session=session,
        data_interval=(timezone.utcnow(), timezone.utcnow()),
    )

    for ti in dr.task_instances:
        ti.task = dag.get_task(ti.task_id)

    tis = {ti.task_id: ti for ti in dr.task_instances}
    tis[fail_task.task_id].state = State.RUNNING
    tis[running_task.task_id].state = State.RUNNING
    tis[queued_task.task_id].state = State.QUEUED
    tis[outside_task.task_id].state = State.SCHEDULED
    session.flush()

    tis[fail_task.task_id].handle_failure("test task group retry fast fail", session=session)
    session.expire_all()

    refreshed = {ti.task_id: ti.state for ti in dr.get_task_instances(session=session)}
    assert refreshed[fail_task.task_id] == State.FAILED
    assert refreshed[running_task.task_id] == State.FAILED
    assert refreshed[queued_task.task_id] == State.SKIPPED
    assert refreshed[outside_task.task_id] == State.SCHEDULED

    fail_log = session.scalar(
        select(Log).where(
            Log.dag_id == dr.dag_id,
            Log.run_id == dr.run_id,
            Log.task_id == running_task.task_id,
            Log.event == "fail task",
        )
    )
    skip_log = session.scalar(
        select(Log).where(
            Log.dag_id == dr.dag_id,
            Log.run_id == dr.run_id,
            Log.task_id == queued_task.task_id,
            Log.event == "skip task",
        )
    )
    assert fail_log is not None
    assert skip_log is not None
    assert fail_log.extra == "Forcing task to fail due to TaskGroup retry_fast_fail."
    assert skip_log.extra == "Skipping task due to TaskGroup retry_fast_fail."


@pytest.mark.integration("scheduler")
@pytest.mark.db_test
@pytest.mark.need_serialized_dag(True)
def test_task_group_retry_fast_fail_respects_retry_condition(dag_maker, session):
    with dag_maker(
        dag_id="test_task_group_retry_fast_fail_condition",
        schedule=None,
        start_date=timezone.datetime(2024, 1, 1),
    ) as dag:
        with TaskGroup(
            group_id="group",
            retries=1,
            retry_fast_fail=True,
            retry_condition="all_failed",
        ):
            fail_task = EmptyOperator(task_id="fail")
            running_task = EmptyOperator(task_id="running")
            queued_task = EmptyOperator(task_id="queued")
        outside_task = EmptyOperator(task_id="outside")

    dr = dag_maker.create_dagrun(
        run_id="test_tg_retry_fast_fail_all_failed",
        state=DagRunState.RUNNING,
        session=session,
        data_interval=(timezone.utcnow(), timezone.utcnow()),
    )

    for ti in dr.task_instances:
        ti.task = dag.get_task(ti.task_id)

    tis = {ti.task_id: ti for ti in dr.task_instances}
    tis[fail_task.task_id].state = State.RUNNING
    tis[running_task.task_id].state = State.RUNNING
    tis[queued_task.task_id].state = State.QUEUED
    tis[outside_task.task_id].state = State.SCHEDULED
    session.flush()

    tis[fail_task.task_id].handle_failure("test task group retry fast fail condition", session=session)
    session.expire_all()

    refreshed = {ti.task_id: ti.state for ti in dr.get_task_instances(session=session)}
    assert refreshed[fail_task.task_id] == State.FAILED
    assert refreshed[running_task.task_id] == State.RUNNING
    assert refreshed[queued_task.task_id] == State.QUEUED
    assert refreshed[outside_task.task_id] == State.SCHEDULED
