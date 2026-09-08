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


def test_hostile_skipmixin_xcom_is_not_deserialized(session, dag_maker):
    """
    A serde envelope stored under the skipmixin key must not be instantiated.

    XCom bytes are written by task code, and the Execution API stores the caller's
    value verbatim. The scheduler evaluates this dep in-process, so a value reaching
    it must never be able to import a class or construct an object -- the security
    model reserves the scheduler for code the Deployment Manager installed.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_hostile_skipmixin_xcom_dag",
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

    # Overwrite the parent's skipmixin XCom with a serialization envelope, the shape
    # `XComDecoder.object_hook` would hand to `deserialize(..., full=True)`.
    session.execute(delete(XComModel).where(XComModel.key == XCOM_SKIPMIXIN_KEY))
    # `serialize=False` with a pre-serialized string is exactly what the Execution API
    # does with a caller-supplied value -- it stores the task's bytes verbatim.
    XComModel.set(
        key=XCOM_SKIPMIXIN_KEY,
        value='{"__classname__": "builtins.dict", "__version__": 1, "__data__": {}}',
        serialize=False,
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        session=session,
    )
    session.commit()

    dep = NotPreviouslySkippedDep()

    # The envelope is read as an inert mapping. It carries neither "followed" nor
    # "skipped", so it yields no skip decision and the dep passes.
    assert len(list(dep.get_dep_statuses(ti2, DepContext(), session=session))) == 0
    assert dep.is_met(ti2, session=session)
    assert ti2.state != State.SKIPPED


def test_non_mapping_skipmixin_xcom_is_ignored(session, dag_maker):
    """
    A skipmixin XCom that is not a mapping is ignored rather than crashing the scheduler.

    Without the shape check the membership test runs against whatever the task wrote, so
    a scalar raises TypeError inside dependency evaluation.
    """
    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_non_mapping_skipmixin_xcom_dag",
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

    session.execute(delete(XComModel).where(XComModel.key == XCOM_SKIPMIXIN_KEY))
    XComModel.set(
        key=XCOM_SKIPMIXIN_KEY,
        value="5",  # a bare scalar: `"followed" in 5` raises TypeError
        serialize=False,
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        session=session,
    )
    session.commit()

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(ti2, DepContext(), session=session))) == 0
    assert ti2.state != State.SKIPPED
