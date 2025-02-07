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

from unittest import mock

import pendulum
import pytest

from airflow.decorators import task
from airflow.models import DagRun, TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk.execution_time.comms import XComCountResponse
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.models.test_mappedoperator import TestMappedSetupTeardown

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db(session):
    yield
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()


def test_no_parent(session, dag_maker):
    """
    A simple DAG with a single task. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_test_no_parent_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = EmptyOperator(task_id="op1")

    (ti1,) = dag_maker.create_dagrun(logical_date=start_date).task_instances
    ti1.refresh_from_task(op1)

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti1, session, DepContext()))) == 0
    assert dep.is_met(ti1, session)
    assert ti1.state != State.SKIPPED


def test_no_skipmixin_parent(session, dag_maker):
    """
    A simple DAG with no branching. Both op1 and op2 are EmptyOperator. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_no_skipmixin_parent_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = EmptyOperator(task_id="op1")
        op2 = EmptyOperator(task_id="op2")
        op1 >> op2

    _, ti2 = dag_maker.create_dagrun().task_instances
    ti2.refresh_from_task(op2)

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
    assert dep.is_met(ti2, session)
    assert ti2.state != State.SKIPPED


def test_parent_follow_branch(session, dag_maker):
    """
    A simple DAG with a BranchPythonOperator that follows op2. NotPreviouslySkippedDep is met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_parent_follow_branch_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op2")
        op2 = EmptyOperator(task_id="op2")
        op1 >> op2

    dagrun = dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING)
    ti, ti2 = dagrun.task_instances
    ti.run()

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
    assert dep.is_met(ti2, session)
    assert ti2.state != State.SKIPPED


def test_parent_skip_branch(session, dag_maker):
    """
    A simple DAG with a BranchPythonOperator that does not follow op2. NotPreviouslySkippedDep is not met.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_parent_skip_branch_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op3")
        op2 = EmptyOperator(task_id="op2")
        op3 = EmptyOperator(task_id="op3")
        op1 >> [op2, op3]

    tis = {
        ti.task_id: ti
        for ti in dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING).task_instances
    }
    tis["op1"].run()

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(tis["op2"], session, DepContext()))) == 1
    assert not dep.is_met(tis["op2"], session)
    assert tis["op2"].state == State.SKIPPED


def test_parent_not_executed(session, dag_maker):
    """
    A simple DAG with a BranchPythonOperator that does not follow op2. Parent task is not yet
    executed (no xcom data). NotPreviouslySkippedDep is met (no decision).
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_parent_not_executed_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op3")
        op2 = EmptyOperator(task_id="op2")
        op3 = EmptyOperator(task_id="op3")
        op1 >> [op2, op3]

    _, ti2, _ = dag_maker.create_dagrun().task_instances
    ti2.refresh_from_task(op2)

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, session, DepContext()))) == 0
    assert dep.is_met(ti2, session)
    assert ti2.state == State.NONE


@pytest.mark.parametrize("condition, final_state", [(True, State.SUCCESS), (False, State.SKIPPED)])
def test_parent_is_mapped_short_circuit(session, dag_maker, condition, final_state):
        with dag_maker(session=session) as dag:

            @task
            def op1():
                return [1]

            @task.short_circuit
            def op2(i: int):
                return condition

            @task
            def op3(res: bool):
                pass

            op3.expand(res=op2.expand(i=op1()))

        # TODO: TaskSDK: same hack as in tests/models/test_mappedoperator.py::TestMappedSetupTeardown::test_one_to_many_with_teardown_and_fail_fast_more_tasks_mapped_setup
        with mock.patch(
            "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
        ) as supervisor_comms:
            supervisor_comms.get_message.return_value = XComCountResponse(len=1)
            dr = dag.test()
        states = TestMappedSetupTeardown.get_states(dr)
        raise Exception(states)

        # def _one_scheduling_decision_iteration() -> dict[tuple[str, int], TaskInstance]:
        #     decision = dr.task_instance_scheduling_decisions(session=session)
        #     return {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

        # tis = _one_scheduling_decision_iteration()
        #
        # tis["op1", -1].run()
        # assert tis["op1", -1].state == State.SUCCESS
        #
        # tis = _one_scheduling_decision_iteration()
        # tis["op2", 0].run()

        # assert tis["op2", 0].state == State.SUCCESS
        # tis = _one_scheduling_decision_iteration()

        # if condition:
        #     tis["op3", 0].run()
        # else:
        # ti3 = dr.get_task_instance("op3", map_index=0, session=session)
        #
        # assert ti3.state == final_state
