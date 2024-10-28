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

import pendulum
import pytest

from airflow.models import DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.utils.state import State
from airflow.utils.types import DagRunType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


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

    (ti1,) = dag_maker.create_dagrun(execution_date=start_date).task_instances
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
        for ti in dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL, state=State.RUNNING
        ).task_instances
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
