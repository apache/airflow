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

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api.common.mark_tasks import (
    find_task_relatives,
    set_dag_run_state_to_failed,
    set_dag_run_state_to_success,
)
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, State, TaskInstanceState

from tests_common.test_utils.mock_operators import MockOperator

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.definitions.dag import SerializedDAG

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def test_set_dag_run_state_to_failed(dag_maker: DagMaker[SerializedDAG]):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="running")
            EmptyOperator(task_id="pending")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
    dag_maker.session.flush()

    result: tuple[list[TaskInstance], list[TaskInstance]] = set_dag_run_state_to_failed(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    updated_tis, _ = result
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.FAILED
    assert task_dict["pending"].state == TaskInstanceState.SKIPPED
    assert "teardown" not in task_dict


def test_set_dag_run_state_to_failed_skips_pending_tis_in_bulk(dag_maker: DagMaker[SerializedDAG]):
    """Pending TIs that never started are skipped by one UPDATE; one that did keeps its start_date."""
    with dag_maker("TEST_DAG_1") as dag:
        EmptyOperator(task_id="never_started")
        EmptyOperator(task_id="never_started_2")
        EmptyOperator(task_id="up_for_retry")
    dr = dag_maker.create_dagrun()
    session = dag_maker.session
    started_at = timezone.datetime(2024, 1, 1, 12, 0, 0)
    for ti in dr.get_task_instances(session=session):
        if ti.task_id == "up_for_retry":
            ti.state = TaskInstanceState.UP_FOR_RETRY
            ti.start_date = started_at
    session.flush()

    updated_tis, killed_tis = set_dag_run_state_to_failed(
        dag=dag, run_id=dr.run_id, commit=True, session=session
    )
    session.flush()

    assert killed_tis == []
    assert {ti.task_id for ti in updated_tis} == {"never_started", "never_started_2", "up_for_retry"}
    # The returned objects reflect the new state without a reload...
    assert all(ti.state == TaskInstanceState.SKIPPED for ti in updated_tis)
    # ...and so does the database.
    session.expire_all()
    task_dict = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    for task_id in ("never_started", "never_started_2"):
        ti = task_dict[task_id]
        assert ti.state == TaskInstanceState.SKIPPED
        assert ti.start_date is not None
        assert ti.end_date == ti.start_date
        assert ti.duration == 0
    ti = task_dict["up_for_retry"]
    assert ti.state == TaskInstanceState.SKIPPED
    assert ti.start_date == started_at
    assert ti.end_date is not None
    assert ti.duration == pytest.approx((ti.end_date - started_at).total_seconds())


def test_set_dag_run_state_to_failed_mapped_task_only_fails_running_map_indexes(
    dag_maker: DagMaker[SerializedDAG],
):
    """Only the running map indexes fail; the pending siblings of the same mapped task are skipped."""
    with dag_maker("TEST_DAG_1") as dag:
        MockOperator.partial(task_id="mapped").expand(arg2=[1, 2, 3])
    dr = dag_maker.create_dagrun()
    session = dag_maker.session
    for ti in dr.get_task_instances(session=session):
        if ti.map_index == 1:
            ti.set_state(TaskInstanceState.RUNNING, session=session)
    session.flush()

    updated_tis, killed_tis = set_dag_run_state_to_failed(
        dag=dag, run_id=dr.run_id, commit=True, session=session
    )
    session.flush()

    assert [ti.map_index for ti in killed_tis] == [1]
    assert len(updated_tis) == 3
    session.expire_all()
    states = {ti.map_index: ti.state for ti in dr.get_task_instances(session=session)}
    assert states == {
        0: TaskInstanceState.SKIPPED,
        1: TaskInstanceState.FAILED,
        2: TaskInstanceState.SKIPPED,
    }


@pytest.mark.parametrize(
    "unfinished_state", sorted([state for state in State.unfinished if state is not None])
)
def test_set_dag_run_state_to_success_unfinished_teardown(
    dag_maker: DagMaker[SerializedDAG],
    unfinished_state,
):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="running")
            EmptyOperator(task_id="pending")

    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
        if ti.task_id == "teardown":
            ti.set_state(unfinished_state)

    dag_maker.session.flush()
    assert dr.state == DagRunState.RUNNING

    result: tuple[list[TaskInstance], list[TaskInstance]] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    updated_tis, _ = result
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state != DagRunState.SUCCESS
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SKIPPED
    assert "teardown" not in task_dict


@pytest.mark.parametrize("finished_state", sorted(list(State.finished)))
def test_set_dag_run_state_to_success_keeps_finished_task_states(
    dag_maker: DagMaker[SerializedDAG], finished_state
):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="finished")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "finished":
            ti.set_state(finished_state)
        if ti.task_id == "teardown":
            ti.set_state(TaskInstanceState.SUCCESS)
    dag_maker.session.flush()
    dr.set_state(DagRunState.FAILED)

    result: tuple[list[TaskInstance], list[TaskInstance]] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    updated_tis, _ = result
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state == DagRunState.SUCCESS
    assert updated_tis == []
    states = {ti.task_id: ti.state for ti in dr.get_task_instances(session=dag_maker.session)}
    assert states == {"finished": finished_state, "teardown": TaskInstanceState.SUCCESS}


def test_find_task_relatives_downstream_skips_teardowns(dag_maker: DagMaker[SerializedDAG]):
    with dag_maker("test_find_task_relatives_downstream_skips_teardowns") as dag:
        setup_t = EmptyOperator(task_id="setup_t").as_setup()
        normal_t = EmptyOperator(task_id="normal_t")
        teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
        setup_t >> normal_t >> teardown_t
    dag_maker.create_dagrun()
    normal_task = dag.get_task("normal_t")

    relatives = list(find_task_relatives([normal_task], downstream=True, upstream=False))

    assert "normal_t" in relatives
    assert "teardown_t" not in relatives


def test_find_task_relatives_upstream_still_includes_setups(dag_maker: DagMaker[SerializedDAG]):
    with dag_maker("test_find_task_relatives_upstream_still_includes_setups") as dag:
        setup_t = EmptyOperator(task_id="setup_t").as_setup()
        normal_t = EmptyOperator(task_id="normal_t")
        teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
        setup_t >> normal_t >> teardown_t
    dag_maker.create_dagrun()
    normal_task = dag.get_task("normal_t")

    relatives = list(find_task_relatives([normal_task], downstream=False, upstream=True))

    assert "normal_t" in relatives
    assert "setup_t" in relatives
