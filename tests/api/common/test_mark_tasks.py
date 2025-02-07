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

from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_dag_run_state_to_success
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

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


def test_set_dag_run_state_to_success(dag_maker: DagMaker):
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

    updated_tis: list[TaskInstance] = set_dag_run_state_to_success(
        dag=dr.dag, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    assert len(updated_tis) == 2
    task_dict = {ti.task_id: ti for ti in updated_tis}
    assert task_dict["running"].state == TaskInstanceState.SUCCESS
    assert task_dict["pending"].state == TaskInstanceState.SUCCESS
    assert "teardown" not in task_dict
