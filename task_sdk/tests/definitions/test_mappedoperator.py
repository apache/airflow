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

from datetime import datetime, timedelta

import pendulum
import pytest

from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.definitions.xcom_arg import XComArg
from airflow.utils.trigger_rule import TriggerRule

from tests_common.test_utils.mapping import expand_mapped_task  # noqa: F401
from tests_common.test_utils.mock_operators import (
    MockOperator,
)

DEFAULT_DATE = datetime(2016, 1, 1)


def test_task_mapping_with_dag():
    with DAG("test-dag") as dag:
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


# TODO:
# test_task_mapping_with_dag_and_list_of_pandas_dataframe


def test_task_mapping_without_dag_context():
    with DAG("test-dag") as dag:
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
    with DAG("test-dag", schedule=None, start_date=DEFAULT_DATE, default_args=default_args):
        task1 = BaseOperator(task_id="op1")
        literal = ["a", "b", "c"]
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)

        task1 >> mapped

    assert mapped.partial_kwargs["owner"] == "test"
    assert mapped.start_date == pendulum.instance(default_args["start_date"])


def test_task_mapping_override_default_args():
    default_args = {"retries": 2, "start_date": DEFAULT_DATE.now()}
    with DAG("test-dag", schedule=None, start_date=DEFAULT_DATE, default_args=default_args):
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
    with DAG("test-dag"):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task1.output)
        finish = MockOperator(task_id="finish")

        mapped >> finish

    assert task1.downstream_list == [mapped]


# def test_map_xcom_arg_multiple_upstream_xcoms(dag_maker, session):


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


def test_partial_on_invalid_pool_slots_raises() -> None:
    """Test that when we pass an invalid value to pool_slots in partial(),

    i.e. if the value is not an integer, an error is raised at import time."""

    with pytest.raises(TypeError, match="'<' not supported between instances of 'str' and 'int'"):
        MockOperator.partial(task_id="pool_slots_test", pool="test", pool_slots="a").expand(arg1=[1, 2, 3])


# def test_expand_mapped_task_instance(dag_maker, session, num_existing_tis, expected):


# def test_expand_mapped_task_failed_state_in_db(dag_maker, session):


# def test_expand_mapped_task_instance_skipped_on_zero(dag_maker, session):


def test_mapped_task_applies_default_args_classic():
    with DAG("test", default_args={"execution_timeout": timedelta(minutes=30)}) as dag:
        MockOperator(task_id="simple", arg1=None, arg2=0)
        MockOperator.partial(task_id="mapped").expand(arg1=[1], arg2=[2, 3])

    assert dag.get_task("simple").execution_timeout == timedelta(minutes=30)
    assert dag.get_task("mapped").execution_timeout == timedelta(minutes=30)


def test_mapped_task_applies_default_args_taskflow():
    with DAG("test", default_args={"execution_timeout": timedelta(minutes=30)}) as dag:

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
def test_mapped_expand_against_params(dag_params, task_params, expected_partial_params):
    with DAG("test", params=dag_params) as dag:
        MockOperator.partial(task_id="t", params=task_params).expand(params=[{"c": "x"}, {"d": 1}])

    t = dag.get_task("t")
    assert isinstance(t, MappedOperator)
    assert t.params == expected_partial_params
    assert t.expand_input.value == {"params": [{"c": "x"}, {"d": 1}]}


# def test_mapped_render_template_fields_validating_operator(dag_maker, session, tmp_path):


# def test_mapped_expand_kwargs_render_template_fields_validating_operator(dag_maker, session, tmp_path):


# def test_mapped_render_nested_template_fields(dag_maker, session):


# def test_expand_kwargs_mapped_task_instance(dag_maker, session, num_existing_tis, expected):


# def test_expand_mapped_task_instance_with_named_index(


# def test_expand_mapped_task_task_instance_mutation_hook(dag_maker, session, create_mapped_task) -> None:


# def test_expand_kwargs_render_template_fields_validating_operator(dag_maker, session, map_index, expected):


def test_xcomarg_property_of_mapped_operator():
    with DAG("test_xcomarg_property_of_mapped_operator"):
        op_a = MockOperator.partial(task_id="a").expand(arg1=["x", "y", "z"])

    assert op_a.output == XComArg(op_a)


def test_set_xcomarg_dependencies_with_mapped_operator():
    with DAG("test_set_xcomargs_dependencies_with_mapped_operator"):
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


# def test_all_xcomargs_from_mapped_tasks_are_consumable(dag_maker, session):


def test_task_mapping_with_task_group_context():
    from airflow.sdk.definitions.taskgroup import TaskGroup

    with DAG("test-dag") as dag:
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
    from airflow.sdk.definitions.taskgroup import TaskGroup

    with DAG("test-dag") as dag:
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
