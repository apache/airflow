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
from sqlalchemy import select, update

from airflow.api.common.mark_tasks import (
    find_task_relatives,
    set_dag_run_state_to_failed,
    set_dag_run_state_to_success,
    set_state,
)
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import task as task_decorator, task_group
from airflow.utils.state import DagRunState, State, TaskInstanceState

from tests_common.test_utils.mapping import expand_mapped_task

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def test_set_dag_run_state_to_failed_ignores_removed_task_tis(dag_maker: DagMaker[SerializedDAG]):
    with dag_maker("TEST_DAG_REMOVED_TASK"):
        EmptyOperator(task_id="pending")
        EmptyOperator(task_id="removed_task")
    dr = dag_maker.create_dagrun()
    dag_maker.session.flush()

    with dag_maker("TEST_DAG_REMOVED_TASK") as dag_without_removed:
        EmptyOperator(task_id="pending")

    result: tuple[list[TaskInstance], list[TaskInstance]] = set_dag_run_state_to_failed(
        dag=dag_without_removed, run_id=dr.run_id, commit=True, session=dag_maker.session
    )
    updated_tis, _ = result
    assert len(updated_tis) == 1
    assert updated_tis[0].task_id == "pending"
    assert updated_tis[0].state == TaskInstanceState.SKIPPED


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


def _create_mapped_group_dagrun(dag_maker: DagMaker[SerializedDAG], name: str):
    """Build a mapped task group ``tg`` of two instances, each holding ``a >> b``, all failed."""
    with dag_maker(name) as dag:

        @task_decorator
        def make_arg_lists():
            return [[1], [2]]

        @task_group
        def tg(value):
            EmptyOperator(task_id="a") >> EmptyOperator(task_id="b")

        tg.expand(value=make_arg_lists())

    dr = dag_maker.create_dagrun(state=DagRunState.FAILED)
    session = dag_maker.session
    for task_id in ("tg.a", "tg.b"):
        expand_mapped_task(dag.get_task(task_id), dr.run_id, "make_arg_lists", length=2, session=session)
    session.execute(
        update(TaskInstance).where(TaskInstance.dag_id == dag.dag_id).values(state=TaskInstanceState.FAILED)
    )
    session.flush()
    return dag, dr


def _read_states(dag_maker: DagMaker[SerializedDAG], dag: SerializedDAG):
    rows = dag_maker.session.execute(
        select(TaskInstance.task_id, TaskInstance.map_index, TaskInstance.state).where(
            TaskInstance.dag_id == dag.dag_id
        )
    ).all()
    return {(task_id, map_index): state for task_id, map_index, state in rows}


@pytest.mark.parametrize(
    ("selected", "direction", "expected"),
    [
        pytest.param(
            "tg.a",
            "downstream",
            {
                ("make_arg_lists", -1): TaskInstanceState.FAILED,
                ("tg.a", 0): TaskInstanceState.FAILED,
                ("tg.a", 1): TaskInstanceState.SUCCESS,
                ("tg.b", 0): TaskInstanceState.FAILED,
                ("tg.b", 1): TaskInstanceState.SUCCESS,
            },
            id="downstream",
        ),
        pytest.param(
            "tg.b",
            "upstream",
            {
                ("make_arg_lists", -1): TaskInstanceState.SUCCESS,
                ("tg.a", 0): TaskInstanceState.FAILED,
                ("tg.a", 1): TaskInstanceState.SUCCESS,
                ("tg.b", 0): TaskInstanceState.FAILED,
                ("tg.b", 1): TaskInstanceState.SUCCESS,
            },
            id="upstream",
        ),
    ],
)
def test_set_state_marks_only_the_relatives_of_the_selected_map_index(
    dag_maker: DagMaker[SerializedDAG], selected: str, direction: str, expected: dict
):
    dag, dr = _create_mapped_group_dagrun(dag_maker, "TEST_DAG_MAPPED_GROUP")

    set_state(
        tasks=[(dag.get_task(selected), 1)],
        run_id=dr.run_id,
        upstream=direction == "upstream",
        downstream=direction == "downstream",
        state=TaskInstanceState.SUCCESS,
        commit=True,
        session=dag_maker.session,
    )

    assert _read_states(dag_maker, dag) == expected


def test_set_state_keeps_every_map_index_when_several_runs_are_in_scope(
    dag_maker: DagMaker[SerializedDAG],
):
    """``past`` and ``future`` span runs whose expansions differ, so map indexes are not narrowed."""
    dag, dr = _create_mapped_group_dagrun(dag_maker, "TEST_DAG_MAPPED_GROUP_FUTURE")

    set_state(
        tasks=[(dag.get_task("tg.a"), 1)],
        run_id=dr.run_id,
        downstream=True,
        future=True,
        state=TaskInstanceState.SUCCESS,
        commit=True,
        session=dag_maker.session,
    )

    assert _read_states(dag_maker, dag) == {
        ("make_arg_lists", -1): TaskInstanceState.FAILED,
        ("tg.a", 0): TaskInstanceState.FAILED,
        ("tg.a", 1): TaskInstanceState.SUCCESS,
        ("tg.b", 0): TaskInstanceState.SUCCESS,
        ("tg.b", 1): TaskInstanceState.SUCCESS,
    }


def _create_nested_mapped_group_dagrun(dag_maker: DagMaker[SerializedDAG], name: str):
    """Build a mapped group ``tg`` holding ``a`` and a mapped group ``inner`` holding ``b``, all failed."""
    with dag_maker(name) as dag:

        @task_decorator
        def make_arg_lists():
            return [[1], [2]]

        @task_decorator
        def consume(value):
            pass

        @task_group
        def inner(value):
            consume.override(task_id="b")(value=value)

        @task_group
        def tg(value):
            consume.override(task_id="a")(value=value) >> inner.expand(value=[1, 2])

        tg.expand(value=make_arg_lists())

    dr = dag_maker.create_dagrun(state=DagRunState.FAILED)
    session = dag_maker.session
    for task_id in ("tg.a", "tg.inner.b"):
        expand_mapped_task(dag.get_task(task_id), dr.run_id, "make_arg_lists", length=2, session=session)
    session.execute(
        update(TaskInstance).where(TaskInstance.dag_id == dag.dag_id).values(state=TaskInstanceState.FAILED)
    )
    session.flush()
    return dag, dr


def test_set_state_narrows_a_relative_expanded_further_inside_the_group(
    dag_maker: DagMaker[SerializedDAG],
):
    """
    A relative expanded again inside the common group is narrowed by the shared helper.

    The helper derives the width of that further expansion from the selected task's own
    expansion, so ``tg.inner.b`` map index 1, which also belongs to this group instance, is
    not reached. What this pins is that map indexes 2 and 3, which belong to the other
    instance of ``tg``, are left alone.
    """
    dag, dr = _create_nested_mapped_group_dagrun(dag_maker, "TEST_DAG_NESTED_MAPPED_GROUP")

    set_state(
        tasks=[(dag.get_task("tg.a"), 0)],
        run_id=dr.run_id,
        downstream=True,
        state=TaskInstanceState.SUCCESS,
        commit=True,
        session=dag_maker.session,
    )

    assert _read_states(dag_maker, dag) == {
        ("make_arg_lists", -1): TaskInstanceState.FAILED,
        ("tg.a", 0): TaskInstanceState.SUCCESS,
        ("tg.a", 1): TaskInstanceState.FAILED,
        ("tg.inner.b", 0): TaskInstanceState.SUCCESS,
        ("tg.inner.b", 1): TaskInstanceState.FAILED,
        ("tg.inner.b", 2): TaskInstanceState.FAILED,
        ("tg.inner.b", 3): TaskInstanceState.FAILED,
    }
