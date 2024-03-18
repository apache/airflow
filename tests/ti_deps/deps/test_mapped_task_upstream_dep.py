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

from typing import TYPE_CHECKING

import pytest

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.base_ti_dep import TIDepStatus
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

FAILED = TaskInstanceState.FAILED
REMOVED = TaskInstanceState.REMOVED
SKIPPED = TaskInstanceState.SKIPPED
SUCCESS = TaskInstanceState.SUCCESS
UPSTREAM_FAILED = TaskInstanceState.UPSTREAM_FAILED


@pytest.mark.parametrize(
    ["task_state", "upstream_states", "expected_state", "expect_failed_dep"],
    [
        # finished mapped dependencies with state != success result in failed dep and a modified state
        (None, [None, None], None, False),
        (None, [SUCCESS, None], None, False),
        (None, [SKIPPED, None], SKIPPED, True),
        (None, [FAILED, None], UPSTREAM_FAILED, True),
        (None, [UPSTREAM_FAILED, None], UPSTREAM_FAILED, True),
        (None, [REMOVED, None], None, True),
        # success does not cancel out failed finished mapped dependencies
        (None, [SKIPPED, SUCCESS], SKIPPED, True),
        (None, [FAILED, SUCCESS], UPSTREAM_FAILED, True),
        (None, [UPSTREAM_FAILED, SUCCESS], UPSTREAM_FAILED, True),
        (None, [REMOVED, SUCCESS], None, True),
        # skipped and failed/upstream_failed result in upstream_failed
        (None, [SKIPPED, FAILED], UPSTREAM_FAILED, True),
        (None, [SKIPPED, UPSTREAM_FAILED], UPSTREAM_FAILED, True),
        (None, [SKIPPED, REMOVED], SKIPPED, True),
        # if state of the mapped task is already set (e.g., by another ti dep), then failed and
        # upstream_failed are not overwritten but failed deps are still reported
        (SKIPPED, [None, None], SKIPPED, False),
        (SKIPPED, [SUCCESS, None], SKIPPED, False),
        (SKIPPED, [SKIPPED, None], SKIPPED, True),
        (SKIPPED, [FAILED, None], UPSTREAM_FAILED, True),
        (SKIPPED, [UPSTREAM_FAILED, None], UPSTREAM_FAILED, True),
        (SKIPPED, [REMOVED, None], SKIPPED, True),
        (FAILED, [None, None], FAILED, False),
        (FAILED, [SUCCESS, None], FAILED, False),
        (FAILED, [SKIPPED, None], FAILED, True),
        (FAILED, [FAILED, None], FAILED, True),
        (FAILED, [UPSTREAM_FAILED, None], FAILED, True),
        (FAILED, [REMOVED, None], FAILED, True),
        (UPSTREAM_FAILED, [None, None], UPSTREAM_FAILED, False),
        (UPSTREAM_FAILED, [SUCCESS, None], UPSTREAM_FAILED, False),
        (UPSTREAM_FAILED, [SKIPPED, None], UPSTREAM_FAILED, True),
        (UPSTREAM_FAILED, [FAILED, None], UPSTREAM_FAILED, True),
        (UPSTREAM_FAILED, [UPSTREAM_FAILED, None], UPSTREAM_FAILED, True),
        (UPSTREAM_FAILED, [REMOVED, None], UPSTREAM_FAILED, True),
        (REMOVED, [None, None], REMOVED, False),
        (REMOVED, [SUCCESS, None], REMOVED, False),
        (REMOVED, [SKIPPED, None], SKIPPED, True),
        (REMOVED, [FAILED, None], UPSTREAM_FAILED, True),
        (REMOVED, [UPSTREAM_FAILED, None], UPSTREAM_FAILED, True),
        (REMOVED, [REMOVED, None], REMOVED, True),
    ],
)
@pytest.mark.parametrize("testcase", ["task", "group"])
def test_mapped_task_upstream_dep(
    dag_maker,
    session: Session,
    task_state: TaskInstanceState | None,
    upstream_states: list[TaskInstanceState | None],
    expected_state: TaskInstanceState | None,
    expect_failed_dep: bool,
    testcase: str,
):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t():
            return [1, 2]

        @task
        def m(x, y):
            return x + y

        @task_group
        def g1(x, y):
            @task_group
            def g2():
                return m(x, y)

            return g2()

        if testcase == "task":
            m.expand(x=t.override(task_id="t1")(), y=t.override(task_id="t2")())
        else:
            g1.expand(x=t.override(task_id="t1")(), y=t.override(task_id="t2")())

    mapped_task = "m" if testcase == "task" else "g1.g2.m"

    dr: DagRun = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    if task_state is not None:
        tis[mapped_task].set_state(task_state, session=session)
    if upstream_states[0] is not None:
        tis["t1"].set_state(upstream_states[0], session=session)
    if upstream_states[1] is not None:
        tis["t2"].set_state(upstream_states[1], session=session)

    expected_statuses = (
        []
        if not expect_failed_dep
        else [
            TIDepStatus(
                dep_name="Mapped dependencies have succeeded",
                passed=False,
                reason="At least one of task's mapped dependencies has not succeeded!",
            )
        ]
    )
    assert get_dep_statuses(dr, mapped_task, session) == expected_statuses
    ti = dr.get_task_instance(session=session, task_id=mapped_task)
    assert ti is not None and ti.state == expected_state


@pytest.mark.parametrize("failure_mode", [None, FAILED, UPSTREAM_FAILED])
@pytest.mark.parametrize("skip_upstream", [True, False])
@pytest.mark.parametrize("testcase", ["task", "group"])
def test_step_by_step(
    dag_maker, session: Session, failure_mode: TaskInstanceState | None, skip_upstream: bool, testcase: str
):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t1():
            return [0]

        @task
        def t2_a():
            if failure_mode == UPSTREAM_FAILED:
                raise AirflowFailException()
            return [1, 2]

        @task
        def t2_b(x):
            if failure_mode == FAILED:
                raise AirflowFailException()
            return x

        @task
        def t3():
            if skip_upstream:
                raise AirflowSkipException()
            return [3, 4]

        @task
        def t4():
            return 17

        @task(trigger_rule="all_done")
        def m1(a, x, y, z):
            return a + x + y + z

        @task(trigger_rule="all_done")
        def m2(x, y):
            return x + y

        @task_group
        def tg(a, x, y, z):
            return m2(a, m1(a, x, y, z))

        vals = t1()
        if testcase == "task":
            m2.expand(x=vals, y=m1.partial(a=t4()).expand(x=vals, y=t2_b(t2_a()), z=t3()))
        else:
            tg.partial(a=t4()).expand(x=vals, y=t2_b(t2_a()), z=t3())

    dr: DagRun = dag_maker.create_dagrun()

    mapped_task_1 = "m1" if testcase == "task" else "tg.m1"
    mapped_task_2 = "m2" if testcase == "task" else "tg.m2"

    # Initial decision, t1, t2 and t3 can be scheduled
    schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
    assert sorted(schedulable_tis) == ["t1", "t2_a", "t3", "t4"]
    assert not finished_tis_states

    # Run first schedulable task - expect no dep statuses for m1 as only one of its 3 mapped dependencies is
    # finished
    schedulable_tis["t1"].run()
    _one_scheduling_decision_iteration(dr, session)
    assert not get_dep_statuses(dr, mapped_task_1, session)

    # Run remaining schedulable tasks
    if failure_mode == UPSTREAM_FAILED:
        with pytest.raises(AirflowFailException):
            schedulable_tis["t2_a"].run()
    else:
        schedulable_tis["t2_a"].run()
        schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
        if not failure_mode:
            schedulable_tis["t2_b"].run()
        else:
            with pytest.raises(AirflowFailException):
                schedulable_tis["t2_b"].run()
    schedulable_tis["t3"].run()
    schedulable_tis["t4"].run()

    # Decision after running all tasks
    _one_scheduling_decision_iteration(dr, session)

    # Standalone test of the mapped task upstream dependency status
    expect_passed = not failure_mode and not skip_upstream
    expected_statuses = (
        []
        if expect_passed
        else [
            TIDepStatus(
                dep_name="Mapped dependencies have succeeded",
                passed=expect_passed,
                reason=(
                    "The task's mapped dependencies have all succeeded!"
                    if expect_passed
                    else "At least one of task's mapped dependencies has not succeeded!"
                ),
            )
        ]
    )
    assert get_dep_statuses(dr, mapped_task_1, session) == expected_statuses
    if not expect_passed:
        assert get_dep_statuses(dr, mapped_task_2, session) == expected_statuses

    # Full test of the mapped task upstream dependency status
    schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
    expected_finished_tis_states = {
        "t1": SUCCESS,
        "t2_a": FAILED if failure_mode == UPSTREAM_FAILED else SUCCESS,
        "t2_b": failure_mode if failure_mode else SUCCESS,
        "t3": SKIPPED if skip_upstream else SUCCESS,
        "t4": SUCCESS,
    }
    if not expect_passed:
        expected_finished_tis_states[mapped_task_1] = UPSTREAM_FAILED if failure_mode else SKIPPED
        expected_finished_tis_states[mapped_task_2] = UPSTREAM_FAILED if failure_mode else SKIPPED
    assert finished_tis_states == expected_finished_tis_states

    if expect_passed:
        # Run the m1 tasks
        for i in range(4):
            schedulable_tis[f"{mapped_task_1}_{i}"].run()
            expected_finished_tis_states[f"{mapped_task_1}_{i}"] = SUCCESS
        schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
        # Since m1 was expanded successfully, the upstream dep check does not do anything
        assert not get_dep_statuses(dr, mapped_task_2, session)
        # Run the m2 tasks
        for i in range(4):
            schedulable_tis[f"{mapped_task_2}_{i}"].run()
            expected_finished_tis_states[f"{mapped_task_2}_{i}"] = SUCCESS
        schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
        assert finished_tis_states == expected_finished_tis_states


def test_nested_mapped_task_groups(dag_maker, session: Session):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t():
            return [[1, 2], [3, 4]]

        @task
        def m(x):
            return x

        @task_group
        def g1(x):
            @task_group
            def g2(y):
                return m(y)

            return g2.expand(y=x)

        g1.expand(x=t())

    # Add a test once nested mapped task groups become supported
    with pytest.raises(NotImplementedError) as ctx:
        dag_maker.create_dagrun()
    assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"


def test_mapped_in_mapped_task_group(dag_maker, session: Session):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def t():
            return [[1, 2], [3, 4]]

        @task
        def m(x):
            return x

        @task_group
        def g(x):
            return m.expand(x=x)

        # Add a test once mapped tasks within mapped task groups become supported
        with pytest.raises(NotImplementedError) as ctx:
            g.expand(x=t())
        assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"


@pytest.mark.parametrize("testcase", ["task", "group"])
def test_no_mapped_dependencies(dag_maker, session: Session, testcase: str):
    from airflow.decorators import task, task_group

    with dag_maker(session=session):

        @task
        def m(x):
            return x

        @task_group
        def tg(x):
            return m(x)

        if testcase == "task":
            m.expand(x=[1, 2, 3])
        else:
            tg.expand(x=[1, 2, 3])

    dr: DagRun = dag_maker.create_dagrun()

    mapped_task = "m" if testcase == "task" else "tg.m"

    # Initial decision, t can be scheduled
    schedulable_tis, finished_tis_states = _one_scheduling_decision_iteration(dr, session)
    assert sorted(schedulable_tis) == [f"{mapped_task}_{i}" for i in range(3)]
    assert not finished_tis_states

    # Expect passed dep status for t as it does not have any mapped dependencies
    expected_statuses = TIDepStatus(
        dep_name="Mapped dependencies have succeeded",
        passed=True,
        reason="There are no mapped dependencies!",
    )
    assert get_dep_statuses(dr, mapped_task, session) == [expected_statuses]


def test_non_mapped_operator(dag_maker, session: Session):
    with dag_maker(session=session):
        op = EmptyOperator(task_id="op")
        op

    dr: DagRun = dag_maker.create_dagrun()

    assert not get_dep_statuses(dr, "op", session)


def test_non_mapped_task_group(dag_maker, session: Session):
    from airflow.decorators import task_group

    with dag_maker(session=session):

        @task_group
        def tg():
            op1 = EmptyOperator(task_id="op1")
            op2 = EmptyOperator(task_id="op2")
            op1 >> op2

        tg()

    dr: DagRun = dag_maker.create_dagrun()

    assert not get_dep_statuses(dr, "tg.op1", session)


def _one_scheduling_decision_iteration(
    dr: DagRun, session: Session
) -> tuple[dict[str, TaskInstance], dict[str, str]]:
    def _key(ti) -> str:
        return ti.task_id if ti.map_index == -1 else f"{ti.task_id}_{ti.map_index}"

    decision = dr.task_instance_scheduling_decisions(session=session)
    return (
        {_key(ti): ti for ti in decision.schedulable_tis},
        {_key(ti): ti.state for ti in decision.finished_tis},
    )


def get_dep_statuses(dr: DagRun, task_id: str, session: Session) -> list[TIDepStatus]:
    return list(
        MappedTaskUpstreamDep()._get_dep_statuses(
            ti=_get_ti(dr, task_id),
            dep_context=DepContext(),
            session=session,
        )
    )


def _get_ti(dr: DagRun, task_id: str) -> TaskInstance:
    return next(ti for ti in dr.task_instances if ti.task_id == task_id)
