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
from sqlalchemy import delete, select

from airflow.models import DagRun, TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.sdk import task, task_group
from airflow.sdk.bases.xcom import BaseXCom
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.not_previously_skipped_dep import (
    XCOM_SKIPMIXIN_FOLLOWED,
    XCOM_SKIPMIXIN_KEY,
    XCOM_SKIPMIXIN_SKIPPED,
    NotPreviouslySkippedDep,
)
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import capture_orm_selects
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


def _create_run(dag_maker):
    dr = dag_maker.create_dagrun(run_type=DagRunType.MANUAL, state=State.RUNNING)
    return dr, {(ti.task_id, ti.map_index): ti for ti in dr.task_instances}


def _finish_with_skip_decisions(dr, tis, task_id, decisions, *, session, state=State.SUCCESS):
    """Set every map index of ``task_id`` to ``state`` and record its SkipMixin decision per map index."""
    for (ti_task_id, _), ti in tis.items():
        if ti_task_id == task_id:
            ti.state = state
            session.merge(ti)
    for map_index, decision in decisions.items():
        XComModel.set(
            key=XCOM_SKIPMIXIN_KEY,
            value=decision,
            dag_id=dr.dag_id,
            task_id=task_id,
            run_id=dr.run_id,
            map_index=map_index,
            session=session,
        )
    session.flush()


def _short_circuit_chain_in_mapped_group(dag_maker, session, dag_id, map_count=2):
    with dag_maker(dag_id, schedule=None, session=session):

        @task.short_circuit(task_id="gate")
        def gate(value):
            return value

        @task_group
        def group(value):
            (
                gate(value)
                >> EmptyOperator(task_id="a")
                >> EmptyOperator(task_id="b", trigger_rule="all_done")
                >> EmptyOperator(task_id="c", trigger_rule="all_done")
            )

        group.expand(value=[map_index % 2 == 0 for map_index in range(map_count)])

    return _create_run(dag_maker)


def test_parent_in_mapped_task_group_skips_same_map_index(session, dag_maker):
    """
    A SkipMixin parent inside a mapped task group writes XCom per map index, so
    each child TI in the group must read the decision for its own map index.
    """
    with dag_maker("test_mapped_group_skip_dag", schedule=None, session=session):

        @task.short_circuit(task_id="gate")
        def gate(value):
            return value

        @task_group
        def group(value):
            gate(value) >> EmptyOperator(task_id="child")

        group.expand(value=[True, False])

    dr, tis = _create_run(dag_maker)
    # Only the map index 1 gate short-circuited, as SkipMixin.skip records it.
    _finish_with_skip_decisions(
        dr, tis, "group.gate", {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.child"]}}, session=session
    )

    dep = NotPreviouslySkippedDep()

    assert not dep.is_met(tis[("group.child", 1)], session=session)
    assert tis[("group.child", 1)].state == State.SKIPPED
    assert dep.is_met(tis[("group.child", 0)], session=session)
    assert tis[("group.child", 0)].state != State.SKIPPED


def test_short_circuit_in_mapped_task_group_skips_transitive_downstream(session, dag_maker):
    """
    ShortCircuitOperator with ignore_downstream_trigger_rules=True lists every downstream task,
    so a task further down the same mapped task group is skipped for that map index even
    though its trigger rule would let it run after a skipped upstream.
    """
    dr, tis = _short_circuit_chain_in_mapped_group(
        dag_maker, session, "test_mapped_group_transitive_skip_dag"
    )
    _finish_with_skip_decisions(
        dr,
        tis,
        "group.gate",
        {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a", "group.b", "group.c"]}},
        session=session,
    )

    dep = NotPreviouslySkippedDep()

    for task_id in ("group.b", "group.c"):
        assert not dep.is_met(tis[(task_id, 1)], session=session)
        assert tis[(task_id, 1)].state == State.SKIPPED
        assert dep.is_met(tis[(task_id, 0)], session=session)
        assert tis[(task_id, 0)].state != State.SKIPPED


@pytest.mark.parametrize("gate_state", [State.SKIPPED, State.UPSTREAM_FAILED, State.FAILED, State.REMOVED])
def test_mapped_task_group_later_tasks_ignore_decision_of_gate_that_did_not_succeed(
    session, dag_maker, gate_state
):
    """
    Clearing a task instance keeps its XComs until it runs again, so a gate that was cleared and
    then finished without running leaves its earlier decision behind. Its direct downstream keeps
    honouring it, as outside a mapped task group, but tasks further down must not.
    """
    dr, tis = _short_circuit_chain_in_mapped_group(dag_maker, session, "test_mapped_group_stale_decision_dag")
    _finish_with_skip_decisions(
        dr,
        tis,
        "group.gate",
        {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a", "group.b", "group.c"]}},
        session=session,
        state=gate_state,
    )

    dep = NotPreviouslySkippedDep()

    assert not dep.is_met(tis[("group.a", 1)], session=session)
    for task_id in ("group.b", "group.c"):
        assert dep.is_met(tis[(task_id, 1)], session=session)
        assert tis[(task_id, 1)].state != State.SKIPPED


@pytest.mark.parametrize("gate_state", [State.RUNNING, None])
def test_mapped_task_group_ignores_decision_of_unfinished_gate(session, dag_maker, gate_state):
    """A decision only counts once the gate that wrote it has finished."""
    dr, tis = _short_circuit_chain_in_mapped_group(
        dag_maker, session, "test_mapped_group_unfinished_gate_dag"
    )
    _finish_with_skip_decisions(
        dr,
        tis,
        "group.gate",
        {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a", "group.b", "group.c"]}},
        session=session,
        state=gate_state,
    )

    dep = NotPreviouslySkippedDep()

    for task_id in ("group.a", "group.b", "group.c"):
        assert dep.is_met(tis[(task_id, 1)], session=session)
        assert tis[(task_id, 1)].state != State.SKIPPED


def test_unmapped_short_circuit_skips_first_task_of_mapped_task_group(session, dag_maker):
    """
    A SkipMixin parent outside the mapped task group writes one decision at map index -1,
    which still skips every map index of its direct downstream inside the group.
    """
    with dag_maker("test_unmapped_gate_mapped_group_dag", schedule=None, session=session):

        @task.short_circuit(task_id="gate")
        def gate():
            return False

        @task_group
        def group(value):
            EmptyOperator(task_id="a")

        gate() >> group.expand(value=[1, 2])

    dr, tis = _create_run(dag_maker)
    _finish_with_skip_decisions(dr, tis, "gate", {-1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a"]}}, session=session)

    dep = NotPreviouslySkippedDep()

    for map_index in (0, 1):
        assert not dep.is_met(tis[("group.a", map_index)], session=session)
        assert tis[("group.a", map_index)].state == State.SKIPPED


def test_short_circuit_does_not_skip_other_mapped_task_group(session, dag_maker):
    """
    Two mapped task groups expand independently, so map index 1 of one group is unrelated to
    map index 1 of the other, and a decision of one group must not skip tasks of the other.
    """
    with dag_maker("test_mapped_group_other_group_dag", schedule=None, session=session):

        @task.short_circuit(task_id="gate")
        def gate(value):
            return value

        @task_group
        def first(value):
            gate(value) >> EmptyOperator(task_id="a")

        @task_group
        def second(value):
            EmptyOperator(task_id="b", trigger_rule="all_done")

        first.expand(value=[True, False]) >> second.expand(value=[1, 2])

    dr, tis = _create_run(dag_maker)
    _finish_with_skip_decisions(
        dr, tis, "first.gate", {1: {XCOM_SKIPMIXIN_SKIPPED: ["first.a", "second.b"]}}, session=session
    )

    # Both groups are evaluated in one pass, as the scheduler does, so the memo is shared.
    dep_context = DepContext()
    dep = NotPreviouslySkippedDep()

    assert not dep.is_met(tis[("first.a", 1)], dep_context, session=session)
    assert dep.is_met(tis[("second.b", 1)], dep_context, session=session)
    assert tis[("second.b", 1)].state != State.SKIPPED


def test_mapped_task_group_does_not_skip_task_missing_from_decision(session, dag_maker):
    """
    A decision that lists only the direct downstream, as ignore_downstream_trigger_rules=False
    writes it, leaves a task further down the mapped task group to its trigger rule.
    """
    dr, tis = _short_circuit_chain_in_mapped_group(dag_maker, session, "test_mapped_group_partial_decision")
    _finish_with_skip_decisions(
        dr, tis, "group.gate", {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a"]}}, session=session
    )

    dep = NotPreviouslySkippedDep()

    assert not dep.is_met(tis[("group.a", 1)], session=session)
    assert dep.is_met(tis[("group.b", 1)], session=session)
    assert tis[("group.b", 1)].state != State.SKIPPED


def test_branch_in_mapped_task_group_does_not_skip_join(session, dag_maker):
    """
    A branch decision only names the branch operator's direct downstream tasks, so it must not
    skip a join further down the mapped task group.
    """
    with dag_maker("test_mapped_group_branch_join_dag", schedule=None, session=session):

        @task.branch(task_id="branch")
        def branch(value):
            return value

        @task_group
        def group(value):
            join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
            branch(value) >> [EmptyOperator(task_id="t1"), EmptyOperator(task_id="t2")] >> join

        group.expand(value=["group.t1", "group.t2"])

    dr, tis = _create_run(dag_maker)
    _finish_with_skip_decisions(
        dr,
        tis,
        "group.branch",
        {0: {XCOM_SKIPMIXIN_FOLLOWED: ["group.t1"]}, 1: {XCOM_SKIPMIXIN_FOLLOWED: ["group.t2"]}},
        session=session,
    )

    dep = NotPreviouslySkippedDep()

    assert not dep.is_met(tis[("group.t2", 0)], session=session)
    assert not dep.is_met(tis[("group.t1", 1)], session=session)
    for map_index in (0, 1):
        assert dep.is_met(tis[("group.join", map_index)], session=session)
        assert tis[("group.join", map_index)].state != State.SKIPPED


def test_short_circuit_in_mapped_task_group_does_not_skip_task_after_group(session, dag_maker):
    """
    A task after the mapped task group depends on every map index, so one map index's
    short-circuit must not skip it, even though the decision lists it.
    """
    with dag_maker("test_mapped_group_after_group_dag", schedule=None, session=session):

        @task.short_circuit(task_id="gate")
        def gate(value):
            return value

        @task_group
        def group(value):
            gate(value) >> EmptyOperator(task_id="a")

        group.expand(value=[True, False]) >> EmptyOperator(task_id="after", trigger_rule="all_done")

    dr, tis = _create_run(dag_maker)
    _finish_with_skip_decisions(
        dr, tis, "group.gate", {1: {XCOM_SKIPMIXIN_SKIPPED: ["group.a", "after"]}}, session=session
    )

    dep = NotPreviouslySkippedDep()

    assert dep.is_met(tis[("after", -1)], session=session)
    assert tis[("after", -1)].state != State.SKIPPED


def test_mapped_task_group_skip_decisions_read_once_per_pass(session, dag_maker):
    """
    The scheduler evaluates every map index of every task in a pass with one DepContext, so the
    group's skip decisions must be read with a single XCom query, not one per task instance.
    """
    map_count = 20
    dr, tis = _short_circuit_chain_in_mapped_group(
        dag_maker, session, "test_mapped_group_skip_decisions_once_dag", map_count=map_count
    )
    downstream = ["group.a", "group.b", "group.c"]
    short_circuited = range(1, map_count, 2)
    _finish_with_skip_decisions(
        dr,
        tis,
        "group.gate",
        {map_index: {XCOM_SKIPMIXIN_SKIPPED: downstream} for map_index in short_circuited},
        session=session,
    )
    dep_context = DepContext(finished_tis=dr.get_task_instances(state=State.finished, session=session))

    dep = NotPreviouslySkippedDep()
    with capture_orm_selects("xcom_v2") as statements:
        met = {
            (task_id, map_index): dep.is_met(tis[(task_id, map_index)], dep_context, session=session)
            for task_id in downstream
            for map_index in range(map_count)
        }

    assert len(statements) == 1
    assert {key for key, is_met in met.items() if not is_met} == {
        (task_id, map_index) for task_id in downstream for map_index in short_circuited
    }


def test_mapped_task_group_without_skipmixin_reads_no_xcom(session, dag_maker):
    """A mapped task group without SkipMixin tasks must not pay for an XCom query."""
    with dag_maker("test_mapped_group_no_skipmixin_dag", schedule=None, session=session):

        @task_group
        def group(value):
            EmptyOperator(task_id="a") >> EmptyOperator(task_id="b", trigger_rule="all_done")

        group.expand(value=[1, 2, 3])

    dr, tis = _create_run(dag_maker)
    _finish_with_skip_decisions(dr, tis, "group.a", {}, session=session)
    dep_context = DepContext(finished_tis=dr.get_task_instances(state=State.finished, session=session))

    dep = NotPreviouslySkippedDep()
    with capture_orm_selects("xcom_v2") as statements:
        assert all(dep.is_met(tis[("group.b", i)], dep_context, session=session) for i in range(3))

    assert statements == []


def test_branch_skip_decision_bypasses_custom_xcom_backend(session, dag_maker):
    """
    A value-externalizing custom XCom backend must not break branch-skip of
    mapped/cleared downstream tasks.

    The branch decision is written through the real worker push path with such a
    backend configured. It must be stored readably (not as the backend's opaque
    pointer) so that NotPreviouslySkippedDep can skip a not-yet-expanded mapped
    downstream task, which the worker does not skip directly.

    Regression test for https://github.com/apache/airflow/issues/50491.
    """

    class _PointerXComBackend(BaseXCom):
        @staticmethod
        def serialize_value(value, **kwargs):
            return "xcom_s3://pointer"

        @staticmethod
        def deserialize_value(result):
            return "xcom_s3://pointer"

    start_date = pendulum.datetime(2020, 1, 1)
    with dag_maker(
        "test_skip_bypass_backend_dag",
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

    with mock.patch("airflow.sdk.execution_time.task_runner.XCom", _PointerXComBackend):
        run_task_instance(tis["op1"], op1)

    stored = session.scalar(
        select(XComModel.value).where(
            XComModel.dag_id == dr.dag_id,
            XComModel.task_id == "op1",
            XComModel.run_id == dr.run_id,
            XComModel.key == XCOM_SKIPMIXIN_KEY,
            XComModel.map_index == -1,
        )
    )

    assert stored is not None
    assert "xcom_s3://pointer" not in str(stored)

    tis["op2"].map_index = 0
    session.merge(tis["op2"])
    session.flush()

    dep = NotPreviouslySkippedDep()
    assert len(list(dep.get_dep_statuses(tis["op2"], DepContext(), session=session))) == 1
    assert tis["op2"].state == State.SKIPPED
