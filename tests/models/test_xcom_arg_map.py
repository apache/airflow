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
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

pytestmark = pytest.mark.db_test


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
    assert decision.schedulable_tis and all(ti.task_id.startswith("push_") for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)
    session.commit()

    # Run "pull".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert decision.schedulable_tis and all(ti.task_id == "pull" for ti in decision.schedulable_tis)
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    assert results == {"aa", "bbbb", "cccccc", "dddddddd"}
