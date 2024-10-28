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

import pytest

from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap, TaskMapVariant
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

pytestmark = pytest.mark.db_test


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map(dag_maker, session):
    results = set()
    with dag_maker(session=session) as dag:

        @dag.task
        def push():
            return ["a", "b", "c"]

        @dag.task
        def pull(value):
            results.add(value)

        pull.expand_kwargs(push().map(lambda v: {"value": v * 2}))

    # The function passed to "map" is *NOT* a task.
    assert set(dag.task_dict) == {"push", "pull"}

    dr = dag_maker.create_dagrun(session=session)

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    session.commit()

    # Run "pull".
    decision = dr.task_instance_scheduling_decisions(session=session)
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}
    assert sorted(tis) == [("pull", 0), ("pull", 1), ("pull", 2)]
    for ti in tis.values():
        ti.run(session=session)

    assert results == {"aa", "bb", "cc"}


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map_transform_to_none(dag_maker, session):
    results = set()

    with dag_maker(session=session) as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return v

        pull.expand(value=push().map(c_to_none))

    dr = dag_maker.create_dagrun(session=session)

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Run "pull". This should automatically convert "c" to None.
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    assert results == {"a", "b", None}


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_convert_to_kwargs_fails_task(dag_maker, session):
    results = set()

    with dag_maker(session=session) as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return {"value": v}

        pull.expand_kwargs(push().map(c_to_none))

    dr = dag_maker.create_dagrun(session=session)

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Prepare to run "pull"...
    decision = dr.task_instance_scheduling_decisions(session=session)
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

    # The first two "pull" tis should also succeed.
    tis[("pull", 0)].run(session=session)
    tis[("pull", 1)].run(session=session)

    # But the third one fails because the map() result cannot be used as kwargs.
    with pytest.raises(ValueError) as ctx:
        tis[("pull", 2)].run(session=session)
    assert str(ctx.value) == "expand_kwargs() expects a list[dict], not list[None]"

    assert [tis[("pull", i)].state for i in range(3)] == [
        TaskInstanceState.SUCCESS,
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
    ]


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map_error_fails_task(dag_maker, session):
    with dag_maker(session=session) as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            print(value)

        def does_not_work_with_c(v):
            if v == "c":
                raise ValueError("nope")
            return {"value": v * 2}

        pull.expand_kwargs(push().map(does_not_work_with_c))

    dr = dag_maker.create_dagrun(session=session)

    # The "push" task should not fail.
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    assert [ti.state for ti in decision.schedulable_tis] == [TaskInstanceState.SUCCESS]

    # Prepare to run "pull"...
    decision = dr.task_instance_scheduling_decisions(session=session)
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}

    # The first two "pull" tis should also succeed.
    tis[("pull", 0)].run(session=session)
    tis[("pull", 1)].run(session=session)

    # But the third one (for "c") will fail.
    with pytest.raises(ValueError) as ctx:
        tis[("pull", 2)].run(session=session)
    assert str(ctx.value) == "nope"

    assert [tis[("pull", i)].state for i in range(3)] == [
        TaskInstanceState.SUCCESS,
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
    ]


def test_task_map_from_task_instance_xcom():
    task = EmptyOperator(task_id="test_task")
    ti = TaskInstance(task=task, run_id="test_run", map_index=0)
    ti.dag_id = "test_dag"
    value = {"key1": "value1", "key2": "value2"}

    # Test case where run_id is not None
    task_map = TaskMap.from_task_instance_xcom(ti, value)
    assert task_map.dag_id == ti.dag_id
    assert task_map.task_id == ti.task_id
    assert task_map.run_id == ti.run_id
    assert task_map.map_index == ti.map_index
    assert task_map.length == len(value)
    assert task_map.keys == list(value)

    # Test case where run_id is None
    ti.run_id = None
    with pytest.raises(
        ValueError, match="cannot record task map for unrun task instance"
    ):
        TaskMap.from_task_instance_xcom(ti, value)


def test_task_map_with_invalid_task_instance():
    task = EmptyOperator(task_id="test_task")
    ti = TaskInstance(task=task, run_id=None, map_index=0)
    ti.dag_id = "test_dag"

    # Define some arbitrary XCom-like value data
    value = {"example_key": "example_value"}

    with pytest.raises(
        ValueError, match="cannot record task map for unrun task instance"
    ):
        TaskMap.from_task_instance_xcom(ti, value)


def test_task_map_variant():
    # Test case where keys is None
    task_map = TaskMap(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=0,
        length=3,
        keys=None,
    )
    assert task_map.variant == TaskMapVariant.LIST

    # Test case where keys is not None
    task_map.keys = ["key1", "key2"]
    assert task_map.variant == TaskMapVariant.DICT


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map_raise_to_skip(dag_maker, session):
    result = None

    with dag_maker(session=session) as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def forward(value):
            return value

        @dag.task(trigger_rule=TriggerRule.ALL_DONE)
        def collect(value):
            nonlocal result
            result = list(value)

        def skip_c(v):
            if v == "c":
                raise AirflowSkipException
            return {"value": v}

        collect(value=forward.expand_kwargs(push().map(skip_c)))

    dr = dag_maker.create_dagrun(session=session)

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Run "forward". This should automatically skip "c".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Now "collect" should only get "a" and "b".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    assert result == ["a", "b"]


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map_nest(dag_maker, session):
    results = set()

    with dag_maker(session=session) as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        converted = push().map(lambda v: v * 2).map(lambda v: {"value": v})
        pull.expand_kwargs(converted)

    dr = dag_maker.create_dagrun(session=session)

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    session.flush()
    session.commit()

    # Now "pull" should apply the mapping functions in order.
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    assert results == {"aa", "bb", "cc"}


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_map_zip_nest(dag_maker, session):
    results = set()

    with dag_maker(session=session) as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c", "d"]

        @dag.task
        def push_numbers():
            return [1, 2, 3, 4]

        @dag.task
        def pull(value):
            results.add(value)

        doubled = push_numbers().map(lambda v: v * 2)
        combined = doubled.zip(push_letters())

        def convert_zipped(zipped):
            letter, number = zipped
            return letter * number

        pull.expand(value=combined.map(convert_zipped))

    dr = dag_maker.create_dagrun(session=session)

    # Run "push_letters" and "push_numbers".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert decision.schedulable_tis
    assert all(ti.task_id.startswith("push_") for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    session.commit()

    # Run "pull".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert decision.schedulable_tis
    assert all(ti.task_id == "pull" for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    assert results == {"aa", "bbbb", "cccccc", "dddddddd"}


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_xcom_concat(dag_maker, session):
    from airflow.models.xcom_arg import _ConcatResult

    agg_results = set()
    all_results = None

    with dag_maker(session=session) as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c"]

        @dag.task
        def push_numbers():
            return [1, 2]

        @dag.task
        def pull_one(value):
            agg_results.add(value)

        @dag.task
        def pull_all(value):
            assert isinstance(value, _ConcatResult)
            assert value[0] == "a"
            assert value[1] == "b"
            assert value[2] == "c"
            assert value[3] == 1
            assert value[4] == 2
            with pytest.raises(IndexError):
                value[5]
            assert value[-5] == "a"
            assert value[-4] == "b"
            assert value[-3] == "c"
            assert value[-2] == 1
            assert value[-1] == 2
            with pytest.raises(IndexError):
                value[-6]
            nonlocal all_results
            all_results = list(value)

        pushed_values = push_letters().concat(push_numbers())

        pull_one.expand(value=pushed_values)
        pull_all(pushed_values)

    dr = dag_maker.create_dagrun(session=session)

    # Run "push_letters" and "push_numbers".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2
    assert all(ti.task_id.startswith("push_") for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    session.commit()

    # Run "pull_one" and "pull_all".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 6
    assert all(ti.task_id.startswith("pull_") for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    assert agg_results == {"a", "b", "c", 1, 2}
    assert all_results == ["a", "b", "c", 1, 2]

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert not decision.schedulable_tis
