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

from airflow.api.common.mark_tasks import (
    find_task_relatives,
    set_dag_run_state_to_failed,
    set_dag_run_state_to_success,
)
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, State, TaskInstanceState

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

    updated_tis: list[TaskInstance] = set_dag_run_state_to_failed(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.FAILED
    assert task_dict["pending"].state == TaskInstanceState.SKIPPED
    assert "teardown" not in task_dict


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

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state != DagRunState.SUCCESS
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SUCCESS
    assert "teardown" not in task_dict


@pytest.mark.parametrize("finished_state", sorted(list(State.finished)))
def test_set_dag_run_state_to_success_finished_teardown(dag_maker: DagMaker[SerializedDAG], finished_state):
    with dag_maker("TEST_DAG_1") as dag:
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="failed")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "failed":
            ti.set_state(TaskInstanceState.FAILED)
        if ti.task_id == "teardown":
            ti.set_state(finished_state)
    dag_maker.session.flush()
    dr.set_state(DagRunState.FAILED)

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run is not None
    assert run.state == DagRunState.SUCCESS
    if finished_state == TaskInstanceState.SUCCESS:
        assert len(updated_tis) == 1
    else:
        assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["failed"].state == TaskInstanceState.SUCCESS
    if finished_state != TaskInstanceState.SUCCESS:
        assert task_dict["teardown"].state == TaskInstanceState.SUCCESS


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
