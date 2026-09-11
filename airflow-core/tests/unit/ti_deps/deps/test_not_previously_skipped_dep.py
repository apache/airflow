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
from sqlalchemy import delete

from airflow.models import DagRun, TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import task_group
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.not_previously_skipped_dep import (
    XCOM_SKIPMIXIN_FOLLOWED,
    XCOM_SKIPMIXIN_KEY,
    NotPreviouslySkippedDep,
)
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.taskinstance import run_task_instance

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db(session):
    yield
    session.execute(delete(DagRun))
    session.execute(delete(TaskInstance))


def _ti(dr, task_id: str, map_index: int = -1):
    """Return the DagRun task instance for ``task_id`` / ``map_index``."""
    matches = [ti for ti in dr.task_instances if ti.task_id == task_id and ti.map_index == map_index]
    if matches:
        return matches[0]
    # Mapped expansion may not have materialized yet; fall back and set map_index.
    matches = [ti for ti in dr.task_instances if ti.task_id == task_id]
    assert matches, f"No task instance for {task_id}"
    ti = matches[0]
    ti.map_index = map_index
    return ti


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
        EmptyOperator(task_id="op1")

    (ti1,) = dag_maker.create_dagrun(logical_date=start_date).task_instances

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti1, DepContext(), session=session))) == 0
    assert dep.is_met(ti1, session=session)
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

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, DepContext(), session=session))) == 0
    assert dep.is_met(ti2, session=session)
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
    run_task_instance(ti, op1)

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, DepContext(), session=session))) == 0
    assert dep.is_met(ti2, session=session)
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
    run_task_instance(tis["op1"], op1)

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(tis["op2"], DepContext(), session=session))) == 1
    assert not dep.is_met(tis["op2"], session=session)
    assert tis["op2"].state == State.SKIPPED


@pytest.mark.parametrize("expand_kwargs", [False, True])
def test_branch_in_mapped_task_group_skips_same_map_index_sibling(session, dag_maker, expand_kwargs):
    """
    Branch operators inside mapped TaskGroups write SkipMixin XComs with their
    runtime map_index even though the operator itself is not a MappedOperator.

    Regression for https://github.com/apache/airflow/issues/67265 — without
    looking up XCom at the child's map_index, non-selected siblings run instead
    of being skipped.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        f"test_mapped_task_group_branch_skip_dag_{expand_kwargs}",
        schedule=None,
        start_date=start_date,
        session=session,
    ):

        @task_group(group_id="group")
        def mapped_group(value):
            _ = value
            branch = BranchPythonOperator(task_id="branch", python_callable=lambda: "group.followed")
            skipped = EmptyOperator(task_id="skipped")
            followed = EmptyOperator(task_id="followed")
            branch >> [skipped, followed]

        if expand_kwargs:
            mapped_group.expand_kwargs([{"value": 1}, {"value": 2}])
        else:
            mapped_group.expand(value=[1, 2])

    dr = dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING)

    branch_ti = _ti(dr, "group.branch", map_index=1)
    skipped_ti = _ti(dr, "group.skipped", map_index=1)
    followed_ti = _ti(dr, "group.followed", map_index=1)

    # Branch is not a MappedOperator, but lives in a mapped TaskGroup.
    assert branch_ti.task.is_mapped is False
    assert branch_ti.task.get_closest_mapped_task_group() is not None

    branch_ti.state = State.SUCCESS
    session.merge(branch_ti)
    session.merge(skipped_ti)
    session.merge(followed_ti)
    XComModel.set(
        key=XCOM_SKIPMIXIN_KEY,
        value={XCOM_SKIPMIXIN_FOLLOWED: ["group.followed"]},
        dag_id=dr.dag_id,
        task_id="group.branch",
        run_id=dr.run_id,
        map_index=1,
        session=session,
    )
    session.flush()

    dep = NotPreviouslySkippedDep()

    # Non-selected sibling at the same map_index must be skipped.
    assert len(list(dep.get_dep_statuses(skipped_ti, DepContext(), session=session))) == 1
    assert not dep.is_met(skipped_ti, session=session)
    assert skipped_ti.state == State.SKIPPED

    # Selected sibling at the same map_index must still be runnable.
    assert len(list(dep.get_dep_statuses(followed_ti, DepContext(), session=session))) == 0
    assert dep.is_met(followed_ti, session=session)
    assert followed_ti.state != State.SKIPPED


@pytest.mark.parametrize("expand_kwargs", [False, True])
def test_branch_in_mapped_task_group_does_not_cross_map_index(session, dag_maker, expand_kwargs):
    """
    SkipMixin XCom for map_index=0 must not skip the sibling at map_index=1.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        f"test_mapped_task_group_branch_no_cross_dag_{expand_kwargs}",
        schedule=None,
        start_date=start_date,
        session=session,
    ):

        @task_group(group_id="group")
        def mapped_group(value):
            _ = value
            branch = BranchPythonOperator(task_id="branch", python_callable=lambda: "group.followed")
            skipped = EmptyOperator(task_id="skipped")
            followed = EmptyOperator(task_id="followed")
            branch >> [skipped, followed]

        if expand_kwargs:
            mapped_group.expand_kwargs([{"value": "a"}, {"value": "b"}])
        else:
            mapped_group.expand(value=["a", "b"])

    dr = dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING)

    branch_0 = _ti(dr, "group.branch", map_index=0)
    skipped_1 = _ti(dr, "group.skipped", map_index=1)

    branch_0.state = State.SUCCESS
    session.merge(branch_0)
    session.merge(skipped_1)
    # Only map_index=0 decided to skip "group.skipped".
    XComModel.set(
        key=XCOM_SKIPMIXIN_KEY,
        value={XCOM_SKIPMIXIN_FOLLOWED: ["group.followed"]},
        dag_id=dr.dag_id,
        task_id="group.branch",
        run_id=dr.run_id,
        map_index=0,
        session=session,
    )
    session.flush()

    dep = NotPreviouslySkippedDep()
    # map_index=1 has no SkipMixin XCom yet → dep is met (no decision).
    assert len(list(dep.get_dep_statuses(skipped_1, DepContext(), session=session))) == 0
    assert dep.is_met(skipped_1, session=session)
    assert skipped_1.state != State.SKIPPED


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

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, DepContext(), session=session))) == 0
    assert dep.is_met(ti2, session=session)
    assert ti2.state == State.NONE


def test_unmapped_parent_skip_mapped_downstream(session, dag_maker):
    """
    When an unmapped SkipMixin parent writes XCom with map_index=-1,
    mapped downstream TIs (map_index >= 0) should still be skipped
    by NotPreviouslySkippedDep.

    Regression test for https://github.com/apache/airflow/issues/62118
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_unmapped_skip_mapped_dag",
        schedule=None,
        start_date=start_date,
        session=session,
    ):
        op1 = BranchPythonOperator(task_id="op1", python_callable=lambda: "op3")
        op2 = EmptyOperator(task_id="op2")
        op3 = EmptyOperator(task_id="op3")
        op1 >> [op2, op3]

    dr = dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING)
    tis = {ti.task_id: ti for ti in dr.task_instances}

    # Simulate the unmapped branch operator having run: set it to SUCCESS
    # and store XCom with map_index=-1 (as SkipMixin does for unmapped tasks).
    tis["op1"].state = State.SUCCESS
    session.merge(tis["op1"])
    XComModel.set(
        key=XCOM_SKIPMIXIN_KEY,
        value={XCOM_SKIPMIXIN_FOLLOWED: ["op3"]},
        dag_id=dr.dag_id,
        task_id="op1",
        run_id=dr.run_id,
        map_index=-1,
        session=session,
    )

    # Simulate a mapped downstream TI by changing map_index to 0.
    tis["op2"].map_index = 0
    session.merge(tis["op2"])
    session.flush()

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(tis["op2"], DepContext(), session=session))) == 1
    assert not dep.is_met(tis["op2"], session=session)
    assert tis["op2"].state == State.SKIPPED
