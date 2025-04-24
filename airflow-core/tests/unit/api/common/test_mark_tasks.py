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

from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_dag_run_state_to_success
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


def test_set_dag_run_state_to_failed(dag_maker: DagMaker):
    with dag_maker("TEST_DAG_1"):
        with EmptyOperator(task_id="teardown").as_teardown():
            EmptyOperator(task_id="running")
            EmptyOperator(task_id="pending")
    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
    dag_maker.session.flush()
    assert dr.dag

    updated_tis: list[TaskInstance] = set_dag_run_state_to_failed(
        dag=dr.dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.FAILED
    assert task_dict["pending"].state == TaskInstanceState.SKIPPED
    assert "teardown" not in task_dict


def test_set_dag_run_state_to_success_running_teardown(dag_maker: DagMaker):
    with dag_maker("TEST_DAG_1"):
        (
            [EmptyOperator(task_id="running"), EmptyOperator(task_id="pending")]
            >> EmptyOperator(task_id="teardown_running").as_teardown()
            >> EmptyOperator(task_id="teardown_deferred").as_teardown()
            >> EmptyOperator(task_id="teardown_reschedule").as_teardown()
        )

    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running" or ti.task_id == "teardown_running":
            ti.set_state(TaskInstanceState.RUNNING)
        if ti.task_id == "teardown_deferred":
            ti.set_state(TaskInstanceState.DEFERRED)
        if ti.task_id == "teardown_reschedule":
            ti.set_state(TaskInstanceState.UP_FOR_RESCHEDULE)
    dag_maker.session.flush()
    assert dr.dag

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dr.dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run.state != DagRunState.SUCCESS
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SUCCESS
    assert "teardown_running" not in task_dict
    assert "teardown_deferred" not in task_dict
    assert "teardown_reschedule" not in task_dict


def test_set_dag_run_state_to_success_failed_teardown(dag_maker: DagMaker):
    with dag_maker("TEST_DAG_1"):
        (
            [EmptyOperator(task_id="running"), EmptyOperator(task_id="pending")]
            >> EmptyOperator(task_id="teardown_failed").as_teardown()
            >> EmptyOperator(task_id="teardown_upstream_failed").as_teardown()
        )

    dr = dag_maker.create_dagrun()
    for ti in dr.get_task_instances():
        if ti.task_id == "running":
            ti.set_state(TaskInstanceState.RUNNING)
        if ti.task_id == "teardown_failed":
            ti.set_state(TaskInstanceState.FAILED)
        if ti.task_id == "teardown_upstream_failed":
            ti.set_state(TaskInstanceState.UPSTREAM_FAILED)
    dag_maker.session.flush()
    assert dr.dag

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dr.dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    run = dag_maker.session.scalar(select(DagRun).filter_by(dag_id=dr.dag_id, run_id=dr.run_id))
    assert run.state == DagRunState.SUCCESS
    assert len(updated_tis) == 4
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SUCCESS
    assert task_dict["teardown_failed"].state == TaskInstanceState.SUCCESS
    assert task_dict["teardown_upstream_failed"].state == TaskInstanceState.SUCCESS
