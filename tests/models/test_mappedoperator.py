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

import logging
from collections import defaultdict
from datetime import timedelta
from unittest.mock import patch

import pendulum
import pytest

from airflow.decorators import setup, task, task_group, teardown
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.models.mappedoperator import MappedOperator
from airflow.models.param import ParamsDict
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.models.xcom_arg import XComArg
from airflow.utils.context import Context
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.xcom import XCOM_RETURN_KEY
from tests.models import DEFAULT_DATE
from tests.test_utils.mapping import expand_mapped_task
from tests.test_utils.mock_operators import MockOperator, MockOperatorWithNestedFields, NestedFields


def test_task_mapping_with_dag():
    with DAG("test-dag", start_date=DEFAULT_DATE) as dag:
        task1 = BaseOperator(task_id="op1")
        literal = ["a", "b", "c"]
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)
        finish = MockOperator(task_id="finish")

        task1 >> mapped >> finish

    assert task1.downstream_list == [mapped]
    assert mapped in dag.tasks
    assert mapped.task_group == dag.task_group
    # At parse time there should only be three tasks!
    assert len(dag.tasks) == 3

    assert finish.upstream_list == [mapped]
    assert mapped.downstream_list == [finish]


@patch("airflow.models.abstractoperator.AbstractOperator.render_template")
def test_task_mapping_with_dag_and_list_of_pandas_dataframe(mock_render_template, caplog):
    caplog.set_level(logging.INFO)

    class UnrenderableClass:
        def __bool__(self):
            raise ValueError("Similar to Pandas DataFrames, this class raises an exception.")

    class CustomOperator(BaseOperator):
        template_fields = ("arg",)

        def __init__(self, arg, **kwargs):
            super().__init__(**kwargs)
            self.arg = arg

        def execute(self, context: Context):
            pass

    with DAG("test-dag", start_date=DEFAULT_DATE) as dag:
        task1 = CustomOperator(task_id="op1", arg=None)
        unrenderable_values = [UnrenderableClass(), UnrenderableClass()]
        mapped = CustomOperator.partial(task_id="task_2").expand(arg=unrenderable_values)
        task1 >> mapped
    dag.test()
    assert caplog.text.count("task_2 ran successfully") == 2
    assert (
        "Unable to check if the value of type 'UnrenderableClass' is False for task 'task_2', field 'arg'"
        in caplog.text
    )
    mock_render_template.assert_called()


def test_task_mapping_without_dag_context():
    with DAG("test-dag", start_date=DEFAULT_DATE) as dag:
        task1 = BaseOperator(task_id="op1")
    literal = ["a", "b", "c"]
    mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)

    task1 >> mapped

    assert isinstance(mapped, MappedOperator)
    assert mapped in dag.tasks
    assert task1.downstream_list == [mapped]
    assert mapped in dag.tasks
    # At parse time there should only be two tasks!
    assert len(dag.tasks) == 2


def test_task_mapping_default_args():
    default_args = {"start_date": DEFAULT_DATE.now(), "owner": "test"}
    with DAG("test-dag", start_date=DEFAULT_DATE, default_args=default_args):
        task1 = BaseOperator(task_id="op1")
        literal = ["a", "b", "c"]
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)

        task1 >> mapped

    assert mapped.partial_kwargs["owner"] == "test"
    assert mapped.start_date == pendulum.instance(default_args["start_date"])


def test_task_mapping_override_default_args():
    default_args = {"retries": 2, "start_date": DEFAULT_DATE.now()}
    with DAG("test-dag", start_date=DEFAULT_DATE, default_args=default_args):
        literal = ["a", "b", "c"]
        mapped = MockOperator.partial(task_id="task", retries=1).expand(arg2=literal)

    # retries should be 1 because it is provided as a partial arg
    assert mapped.partial_kwargs["retries"] == 1
    # start_date should be equal to default_args["start_date"] because it is not provided as partial arg
    assert mapped.start_date == pendulum.instance(default_args["start_date"])
    # owner should be equal to Airflow default owner (airflow) because it is not provided at all
    assert mapped.owner == "airflow"


def test_map_unknown_arg_raises():
    with pytest.raises(TypeError, match=r"argument 'file'"):
        BaseOperator.partial(task_id="a").expand(file=[1, 2, {"a": "b"}])


def test_map_xcom_arg():
    """Test that dependencies are correct when mapping with an XComArg"""
    with DAG("test-dag", start_date=DEFAULT_DATE):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)
        finish = MockOperator(task_id="finish")

        mapped >> finish

    assert task1.downstream_list == [mapped]


def test_map_xcom_arg_multiple_upstream_xcoms(dag_maker, session):
    """Test that the correct number of downstream tasks are generated when mapping with an XComArg"""

    class PushExtraXComOperator(BaseOperator):
        """Push an extra XCom value along with the default return value."""

        def __init__(self, return_value, **kwargs):
            super().__init__(**kwargs)
            self.return_value = return_value

        def execute(self, context):
            context["task_instance"].xcom_push(key="extra_key", value="extra_value")
            return self.return_value

    with dag_maker("test-dag", session=session, start_date=DEFAULT_DATE) as dag:
        upstream_return = [1, 2, 3]
        task1 = PushExtraXComOperator(return_value=upstream_return, task_id="task_1")
        task2 = PushExtraXComOperator.partial(task_id="task_2").expand(return_value=task1.output)
        task3 = PushExtraXComOperator.partial(task_id="task_3").expand(return_value=task2.output)

    dr = dag_maker.create_dagrun()
    ti_1 = dr.get_task_instance("task_1", session)
    ti_1.run()

    ti_2s, _ = task2.expand_mapped_task(dr.run_id, session=session)
    for ti in ti_2s:
        ti.refresh_from_task(dag.get_task("task_2"))
        ti.run()

    ti_3s, _ = task3.expand_mapped_task(dr.run_id, session=session)
    for ti in ti_3s:
        ti.refresh_from_task(dag.get_task("task_3"))
        ti.run()

    assert len(ti_3s) == len(ti_2s) == len(upstream_return)


def test_partial_on_instance() -> None:
    """`.partial` on an instance should fail -- it's only designed to be called on classes"""
    with pytest.raises(TypeError):
        MockOperator(task_id="a").partial()


def test_partial_on_class() -> None:
    # Test that we accept args for superclasses too
    op = MockOperator.partial(task_id="a", arg1="a", trigger_rule=TriggerRule.ONE_FAILED)
    assert op.kwargs["arg1"] == "a"
    assert op.kwargs["trigger_rule"] == TriggerRule.ONE_FAILED


def test_partial_on_class_invalid_ctor_args() -> None:
    """Test that when we pass invalid args to partial().

    I.e. if an arg is not known on the class or any of its parent classes we error at parse time
    """
    with pytest.raises(TypeError, match=r"arguments 'foo', 'bar'"):
        MockOperator.partial(task_id="a", foo="bar", bar=2)


@pytest.mark.parametrize(
    ["num_existing_tis", "expected"],
    (
        pytest.param(0, [(0, None), (1, None), (2, None)], id="only-unmapped-ti-exists"),
        pytest.param(
            3,
            [(0, "success"), (1, "success"), (2, "success")],
            id="all-tis-exist",
        ),
        pytest.param(
            5,
            [
                (0, "success"),
                (1, "success"),
                (2, "success"),
                (3, TaskInstanceState.REMOVED),
                (4, TaskInstanceState.REMOVED),
            ],
            id="tis-to-be-removed",
        ),
    ),
)
def test_expand_mapped_task_instance(dag_maker, session, num_existing_tis, expected):
    literal = [1, 2, {"a": "b"}]
    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )

    if num_existing_tis:
        # Remove the map_index=-1 TI when we're creating other TIs
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.run_id == dr.run_id,
        ).delete()

    for index in range(num_existing_tis):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(mapped, run_id=dr.run_id, map_index=index, state=TaskInstanceState.SUCCESS)
        session.add(ti)
    session.flush()

    mapped.expand_mapped_task(dr.run_id, session=session)

    indices = (
        session.query(TaskInstance.map_index, TaskInstance.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TaskInstance.map_index)
        .all()
    )

    assert indices == expected


def test_expand_mapped_task_failed_state_in_db(dag_maker, session):
    """
    This test tries to recreate a faulty state in the database and checks if we can recover from it.
    The state that happens is that there exists mapped task instances and the unmapped task instance.
    So we have instances with map_index [-1, 0, 1]. The -1 task instances should be removed in this case.
    """
    literal = [1, 2]
    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )

    for index in range(2):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(mapped, run_id=dr.run_id, map_index=index, state=TaskInstanceState.SUCCESS)
        session.add(ti)
    session.flush()

    indices = (
        session.query(TaskInstance.map_index, TaskInstance.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TaskInstance.map_index)
        .all()
    )
    # Make sure we have the faulty state in the database
    assert indices == [(-1, None), (0, "success"), (1, "success")]

    mapped.expand_mapped_task(dr.run_id, session=session)

    indices = (
        session.query(TaskInstance.map_index, TaskInstance.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TaskInstance.map_index)
        .all()
    )
    # The -1 index should be cleaned up
    assert indices == [(0, "success"), (1, "success")]


def test_expand_mapped_task_instance_skipped_on_zero(dag_maker, session):
    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)

    dr = dag_maker.create_dagrun()

    expand_mapped_task(mapped, dr.run_id, task1.task_id, length=0, session=session)

    indices = (
        session.query(TaskInstance.map_index, TaskInstance.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TaskInstance.map_index)
        .all()
    )

    assert indices == [(-1, TaskInstanceState.SKIPPED)]


def test_mapped_task_applies_default_args_classic(dag_maker):
    with dag_maker(default_args={"execution_timeout": timedelta(minutes=30)}) as dag:
        MockOperator(task_id="simple", arg1=None, arg2=0)
        MockOperator.partial(task_id="mapped").expand(arg1=[1], arg2=[2, 3])

    assert dag.get_task("simple").execution_timeout == timedelta(minutes=30)
    assert dag.get_task("mapped").execution_timeout == timedelta(minutes=30)


def test_mapped_task_applies_default_args_taskflow(dag_maker):
    with dag_maker(default_args={"execution_timeout": timedelta(minutes=30)}) as dag:

        @dag.task
        def simple(arg):
            pass

        @dag.task
        def mapped(arg):
            pass

        simple(arg=0)
        mapped.expand(arg=[1, 2])

    assert dag.get_task("simple").execution_timeout == timedelta(minutes=30)
    assert dag.get_task("mapped").execution_timeout == timedelta(minutes=30)


@pytest.mark.parametrize(
    "dag_params, task_params, expected_partial_params",
    [
        pytest.param(None, None, ParamsDict(), id="none"),
        pytest.param({"a": -1}, None, ParamsDict({"a": -1}), id="dag"),
        pytest.param(None, {"b": -2}, ParamsDict({"b": -2}), id="task"),
        pytest.param({"a": -1}, {"b": -2}, ParamsDict({"a": -1, "b": -2}), id="merge"),
    ],
)
def test_mapped_expand_against_params(dag_maker, dag_params, task_params, expected_partial_params):
    with dag_maker(params=dag_params) as dag:
        MockOperator.partial(task_id="t", params=task_params).expand(params=[{"c": "x"}, {"d": 1}])

    t = dag.get_task("t")
    assert isinstance(t, MappedOperator)
    assert t.params == expected_partial_params
    assert t.expand_input.value == {"params": [{"c": "x"}, {"d": 1}]}


def test_mapped_render_template_fields_validating_operator(dag_maker, session):
    class MyOperator(MockOperator):
        def __init__(self, value, arg1, **kwargs):
            assert isinstance(value, str), "value should have been resolved before unmapping"
            assert isinstance(arg1, str), "value should have been resolved before unmapping"
            super().__init__(arg1=arg1, **kwargs)
            self.value = value

    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        output1 = task1.output
        mapped = MyOperator.partial(task_id="a", arg2="{{ ti.task_id }}").expand(value=output1, arg1=output1)

    dr = dag_maker.create_dagrun()
    ti: TaskInstance = dr.get_task_instance(task1.task_id, session=session)

    ti.xcom_push(key=XCOM_RETURN_KEY, value=["{{ ds }}"], session=session)

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=1,
            keys=None,
        )
    )
    session.flush()

    mapped_ti: TaskInstance = dr.get_task_instance(mapped.task_id, session=session)
    mapped_ti.map_index = 0

    assert isinstance(mapped_ti.task, MappedOperator)
    mapped.render_template_fields(context=mapped_ti.get_template_context(session=session))
    assert isinstance(mapped_ti.task, MyOperator)

    assert mapped_ti.task.value == "{{ ds }}", "Should not be templated!"
    assert mapped_ti.task.arg1 == "{{ ds }}", "Should not be templated!"
    assert mapped_ti.task.arg2 == "a"


def test_mapped_render_nested_template_fields(dag_maker, session):
    with dag_maker(session=session):
        MockOperatorWithNestedFields.partial(
            task_id="t", arg2=NestedFields(field_1="{{ ti.task_id }}", field_2="value_2")
        ).expand(arg1=["{{ ti.task_id }}", ["s", "{{ ti.task_id }}"]])

    dr = dag_maker.create_dagrun()
    decision = dr.task_instance_scheduling_decisions()
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}
    assert len(tis) == 2

    ti = tis[("t", 0)]
    ti.run(session=session)
    assert ti.task.arg1 == "t"
    assert ti.task.arg2.field_1 == "t"
    assert ti.task.arg2.field_2 == "value_2"

    ti = tis[("t", 1)]
    ti.run(session=session)
    assert ti.task.arg1 == ["s", "t"]
    assert ti.task.arg2.field_1 == "t"
    assert ti.task.arg2.field_2 == "value_2"


@pytest.mark.parametrize(
    ["num_existing_tis", "expected"],
    (
        pytest.param(0, [(0, None), (1, None), (2, None)], id="only-unmapped-ti-exists"),
        pytest.param(
            3,
            [(0, "success"), (1, "success"), (2, "success")],
            id="all-tis-exist",
        ),
        pytest.param(
            5,
            [
                (0, "success"),
                (1, "success"),
                (2, "success"),
                (3, TaskInstanceState.REMOVED),
                (4, TaskInstanceState.REMOVED),
            ],
            id="tis-to-be-removed",
        ),
    ),
)
def test_expand_kwargs_mapped_task_instance(dag_maker, session, num_existing_tis, expected):
    literal = [{"arg1": "a"}, {"arg1": "b"}, {"arg1": "c"}]
    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand_kwargs(task1.output)

    dr = dag_maker.create_dagrun()

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=len(literal),
            keys=None,
        )
    )

    if num_existing_tis:
        # Remove the map_index=-1 TI when we're creating other TIs
        session.query(TaskInstance).filter(
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.run_id == dr.run_id,
        ).delete()

    for index in range(num_existing_tis):
        # Give the existing TIs a state to make sure we don't change them
        ti = TaskInstance(mapped, run_id=dr.run_id, map_index=index, state=TaskInstanceState.SUCCESS)
        session.add(ti)
    session.flush()

    mapped.expand_mapped_task(dr.run_id, session=session)

    indices = (
        session.query(TaskInstance.map_index, TaskInstance.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TaskInstance.map_index)
        .all()
    )

    assert indices == expected


@pytest.mark.parametrize(
    "map_index, expected",
    [
        pytest.param(0, "{{ ds }}", id="0"),
        pytest.param(1, 2, id="1"),
    ],
)
def test_expand_kwargs_render_template_fields_validating_operator(dag_maker, session, map_index, expected):
    with dag_maker(session=session):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="a", arg2="{{ ti.task_id }}").expand_kwargs(task1.output)

    dr = dag_maker.create_dagrun()
    ti: TaskInstance = dr.get_task_instance(task1.task_id, session=session)

    ti.xcom_push(key=XCOM_RETURN_KEY, value=[{"arg1": "{{ ds }}"}, {"arg1": 2}], session=session)

    session.add(
        TaskMap(
            dag_id=dr.dag_id,
            task_id=task1.task_id,
            run_id=dr.run_id,
            map_index=-1,
            length=2,
            keys=None,
        )
    )
    session.flush()

    ti: TaskInstance = dr.get_task_instance(mapped.task_id, session=session)
    ti.refresh_from_task(mapped)
    ti.map_index = map_index
    assert isinstance(ti.task, MappedOperator)
    mapped.render_template_fields(context=ti.get_template_context(session=session))
    assert isinstance(ti.task, MockOperator)
    assert ti.task.arg1 == expected
    assert ti.task.arg2 == "a"


def test_xcomarg_property_of_mapped_operator(dag_maker):
    with dag_maker("test_xcomarg_property_of_mapped_operator"):
        op_a = MockOperator.partial(task_id="a").expand(arg1=["x", "y", "z"])
    dag_maker.create_dagrun()

    assert op_a.output == XComArg(op_a)


def test_set_xcomarg_dependencies_with_mapped_operator(dag_maker):
    with dag_maker("test_set_xcomargs_dependencies_with_mapped_operator"):
        op1 = MockOperator.partial(task_id="op1").expand(arg1=[1, 2, 3])
        op2 = MockOperator.partial(task_id="op2").expand(arg2=["a", "b", "c"])
        op3 = MockOperator(task_id="op3", arg1=op1.output)
        op4 = MockOperator(task_id="op4", arg1=[op1.output, op2.output])
        op5 = MockOperator(task_id="op5", arg1={"op1": op1.output, "op2": op2.output})

    assert op1 in op3.upstream_list
    assert op1 in op4.upstream_list
    assert op2 in op4.upstream_list
    assert op1 in op5.upstream_list
    assert op2 in op5.upstream_list


def test_all_xcomargs_from_mapped_tasks_are_consumable(dag_maker, session):
    class PushXcomOperator(MockOperator):
        def __init__(self, arg1, **kwargs):
            super().__init__(arg1=arg1, **kwargs)

        def execute(self, context):
            return self.arg1

    class ConsumeXcomOperator(PushXcomOperator):
        def execute(self, context):
            assert {i for i in self.arg1} == {1, 2, 3}

    with dag_maker("test_all_xcomargs_from_mapped_tasks_are_consumable"):
        op1 = PushXcomOperator.partial(task_id="op1").expand(arg1=[1, 2, 3])
        ConsumeXcomOperator(task_id="op2", arg1=op1.output)

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances(session=session)
    for ti in tis:
        ti.run()


def test_task_mapping_with_task_group_context():
    with DAG("test-dag", start_date=DEFAULT_DATE) as dag:
        task1 = BaseOperator(task_id="op1")
        finish = MockOperator(task_id="finish")

        with TaskGroup("test-group") as group:
            literal = ["a", "b", "c"]
            mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)

            task1 >> group >> finish

    assert task1.downstream_list == [mapped]
    assert mapped.upstream_list == [task1]

    assert mapped in dag.tasks
    assert mapped.task_group == group

    assert finish.upstream_list == [mapped]
    assert mapped.downstream_list == [finish]


def test_task_mapping_with_explicit_task_group():
    with DAG("test-dag", start_date=DEFAULT_DATE) as dag:
        task1 = BaseOperator(task_id="op1")
        finish = MockOperator(task_id="finish")

        group = TaskGroup("test-group")
        literal = ["a", "b", "c"]
        mapped = MockOperator.partial(task_id="task_2", task_group=group).expand(arg2=literal)

        task1 >> group >> finish

    assert task1.downstream_list == [mapped]
    assert mapped.upstream_list == [task1]

    assert mapped in dag.tasks
    assert mapped.task_group == group

    assert finish.upstream_list == [mapped]
    assert mapped.downstream_list == [finish]


class TestMappedSetupTeardown:
    @staticmethod
    def get_states(dr):
        ti_dict = defaultdict(dict)
        for ti in dr.get_task_instances():
            if ti.map_index == -1:
                ti_dict[ti.task_id] = ti.state
            else:
                ti_dict[ti.task_id][ti.map_index] = ti.state
        return ti_dict

    def test_one_to_many_work_failed(self, session, dag_maker):
        """
        Work task failed.  Setup maps to teardown.  Should have 3 teardowns all successful even
        though the work task has failed.
        """
        with dag_maker(dag_id="one_to_many") as dag:

            @setup
            def my_setup():
                print("setting up multiple things")
                return [1, 2, 3]

            @task
            def my_work(val):
                print(f"doing work with multiple things: {val}")
                raise ValueError("fail!")
                return val

            @teardown
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup()
            t = my_teardown.expand(val=s)
            with t:
                my_work(s)

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_work": "failed",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
        }
        assert states == expected

    def test_many_one_explicit_odd_setup_mapped_setups_fail(self, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        mapped setups fail
        teardowns should still run
        """
        with dag_maker(
            dag_id="many_one_explicit_odd_setup_mapped_setups_fail",
        ) as dag:

            @task
            def other_setup():
                print("other setup")
                return "other setup"

            @task
            def other_work():
                print("other work")
                return "other work"

            @task
            def other_teardown():
                print("other teardown")
                return "other teardown"

            @task
            def my_setup(val):
                print(f"setup: {val}")
                raise ValueError("fail")
                return val

            @task
            def my_work(val):
                print(f"work: {val}")

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
            o_setup = other_setup()
            o_teardown = other_teardown()
            with o_teardown.as_teardown(setups=o_setup):
                other_work()
            t = my_teardown(s).as_teardown(setups=s)
            with t:
                my_work(s)
            o_setup >> t
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "failed", 1: "failed", 2: "failed"},
            "other_setup": "success",
            "other_teardown": "success",
            "other_work": "success",
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected

    def test_many_one_explicit_odd_setup_all_setups_fail(self, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        all setups fail
        teardowns should not run
        """
        with dag_maker(
            dag_id="many_one_explicit_odd_setup_all_setups_fail",
        ) as dag:

            @task
            def other_setup():
                print("other setup")
                raise ValueError("fail")
                return "other setup"

            @task
            def other_work():
                print("other work")
                return "other work"

            @task
            def other_teardown():
                print("other teardown")
                return "other teardown"

            @task
            def my_setup(val):
                print(f"setup: {val}")
                raise ValueError("fail")
                return val

            @task
            def my_work(val):
                print(f"work: {val}")

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
            o_setup = other_setup()
            o_teardown = other_teardown()
            with o_teardown.as_teardown(setups=o_setup):
                other_work()
            t = my_teardown(s).as_teardown(setups=s)
            with t:
                my_work(s)
            o_setup >> t

        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_teardown": "upstream_failed",
            "other_setup": "failed",
            "other_work": "upstream_failed",
            "other_teardown": "upstream_failed",
            "my_setup": {0: "failed", 1: "failed", 2: "failed"},
            "my_work": "upstream_failed",
        }
        assert states == expected

    def test_many_one_explicit_odd_setup_one_mapped_fails(self, dag_maker):
        """
        one unmapped setup goes to two different teardowns
        one mapped setup goes to same teardown
        one of the mapped setup instances fails
        teardowns should all run
        """
        with dag_maker(dag_id="many_one_explicit_odd_setup_one_mapped_fails") as dag:

            @task
            def other_setup():
                print("other setup")
                return "other setup"

            @task
            def other_work():
                print("other work")
                return "other work"

            @task
            def other_teardown():
                print("other teardown")
                return "other teardown"

            @task
            def my_setup(val):
                if val == "data2.json":
                    raise ValueError("fail!")
                elif val == "data3.json":
                    raise AirflowSkipException("skip!")
                print(f"setup: {val}")
                return val

            @task
            def my_work(val):
                print(f"work: {val}")

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
            o_setup = other_setup()
            o_teardown = other_teardown()
            with o_teardown.as_teardown(setups=o_setup):
                other_work()
            t = my_teardown(s).as_teardown(setups=s)
            with t:
                my_work(s)
            o_setup >> t
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "other_setup": "success",
            "other_teardown": "success",
            "other_work": "success",
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected

    def test_one_to_many_as_teardown(self, dag_maker, session):
        """
        1 setup mapping to 3 teardowns
        1 work task
        work fails
        teardowns succeed
        dagrun should be failure
        """
        with dag_maker(dag_id="one_to_many_as_teardown") as dag:

            @task
            def my_setup():
                print("setting up multiple things")
                return [1, 2, 3]

            @task
            def my_work(val):
                print(f"doing work with multiple things: {val}")
                raise ValueError("this fails")
                return val

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup()
            t = my_teardown.expand(val=s).as_teardown(setups=s)
            with t:
                my_work(s)
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "success", 2: "success"},
            "my_work": "failed",
        }
        assert states == expected

    def test_one_to_many_as_teardown_offd(self, dag_maker, session):
        """
        1 setup mapping to 3 teardowns
        1 work task
        work succeeds
        all but one teardown succeed
        offd=True
        dagrun should be success
        """
        with dag_maker(dag_id="one_to_many_as_teardown_offd") as dag:

            @task
            def my_setup():
                print("setting up multiple things")
                return [1, 2, 3]

            @task
            def my_work(val):
                print(f"doing work with multiple things: {val}")
                return val

            @task
            def my_teardown(val):
                print(f"teardown: {val}")
                if val == 2:
                    raise ValueError("failure")

            s = my_setup()
            t = my_teardown.expand(val=s).as_teardown(setups=s, on_failure_fail_dagrun=True)
            with t:
                my_work(s)
            # todo: if on_failure_fail_dagrun=True, should we still regard the WORK task as a leaf?
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": "success",
            "my_teardown": {0: "success", 1: "failed", 2: "success"},
            "my_work": "success",
        }
        assert states == expected

    def test_mapped_task_group_simple(self, dag_maker, session):
        """
        Mapped task group wherein there's a simple s >> w >> t pipeline.
        When s is skipped, all should be skipped
        When s is failed, all should be upstream failed
        """
        with dag_maker(dag_id="mapped_task_group_simple") as dag:

            @setup
            def my_setup(val):
                if val == "data2.json":
                    raise ValueError("fail!")
                elif val == "data3.json":
                    raise AirflowSkipException("skip!")
                print(f"setup: {val}")

            @task
            def my_work(val):
                print(f"work: {val}")

            @teardown
            def my_teardown(val):
                print(f"teardown: {val}")

            @task_group
            def file_transforms(filename):
                s = my_setup(filename)
                t = my_teardown(filename).as_teardown(setups=s)
                with t:
                    my_work(filename)

            file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "file_transforms.my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "file_transforms.my_work": {0: "success", 1: "upstream_failed", 2: "skipped"},
            "file_transforms.my_teardown": {0: "success", 1: "upstream_failed", 2: "skipped"},
        }

        assert states == expected

    def test_mapped_task_group_work_fail_or_skip(self, dag_maker, session):
        """
        Mapped task group wherein there's a simple s >> w >> t pipeline.
        When w is skipped, teardown should still run
        When w is failed, teardown should still run
        """
        with dag_maker(dag_id="mapped_task_group_work_fail_or_skip") as dag:

            @setup
            def my_setup(val):
                print(f"setup: {val}")

            @task
            def my_work(val):
                if val == "data2.json":
                    raise ValueError("fail!")
                elif val == "data3.json":
                    raise AirflowSkipException("skip!")
                print(f"work: {val}")

            @teardown
            def my_teardown(val):
                print(f"teardown: {val}")

            @task_group
            def file_transforms(filename):
                s = my_setup(filename)
                t = my_teardown(filename).as_teardown(setups=s)
                with t:
                    my_work(filename)

            file_transforms.expand(filename=["data1.json", "data2.json", "data3.json"])
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "file_transforms.my_setup": {0: "success", 1: "success", 2: "success"},
            "file_transforms.my_teardown": {0: "success", 1: "success", 2: "success"},
            "file_transforms.my_work": {0: "success", 1: "failed", 2: "skipped"},
        }
        assert states == expected

    def test_teardown_many_one_explicit(self, dag_maker, session):
        """-- passing
        one mapped setup going to one unmapped work
        3 diff states for setup: success / failed / skipped
        teardown still runs, and receives the xcom from the single successful setup
        """
        with dag_maker(dag_id="teardown_many_one_explicit") as dag:

            @task
            def my_setup(val):
                if val == "data2.json":
                    raise ValueError("fail!")
                elif val == "data3.json":
                    raise AirflowSkipException("skip!")
                print(f"setup: {val}")
                return val

            @task
            def my_work(val):
                print(f"work: {val}")

            @task
            def my_teardown(val):
                print(f"teardown: {val}")

            s = my_setup.expand(val=["data1.json", "data2.json", "data3.json"])
            with my_teardown(s).as_teardown(setups=s):
                my_work(s)
        dr = dag.test()
        states = self.get_states(dr)
        expected = {
            "my_setup": {0: "success", 1: "failed", 2: "skipped"},
            "my_teardown": "success",
            "my_work": "upstream_failed",
        }
        assert states == expected
