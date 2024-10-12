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

import warnings
from datetime import datetime, timedelta, timezone

import pytest

from airflow.sdk.definitions.baseoperator import BaseOperator, BaseOperatorMeta
from airflow.sdk.definitions.dag import DAG
from airflow.task.priority_strategy import _DownstreamPriorityWeightStrategy, _UpstreamPriorityWeightStrategy

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


# Essentially similar to airflow.models.baseoperator.BaseOperator
class FakeOperator(metaclass=BaseOperatorMeta):
    def __init__(self, test_param, params=None, default_args=None):
        self.test_param = test_param

    def _set_xcomargs_dependencies(self): ...


class FakeSubClass(FakeOperator):
    def __init__(self, test_sub_param, test_param, **kwargs):
        super().__init__(test_param=test_param, **kwargs)
        self.test_sub_param = test_sub_param


class DeprecatedOperator(BaseOperator):
    def __init__(self, **kwargs):
        warnings.warn("This operator is deprecated.", DeprecationWarning, stacklevel=2)
        super().__init__(**kwargs)


class MockOperator(BaseOperator):
    """Operator for testing purposes."""

    template_fields = ("arg1", "arg2")

    def __init__(self, arg1: str = "", arg2: str = "", **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2


class TestBaseOperator:
    # Since we have a custom metaclass, lets double check the behaviour of passing args in the wrong way (args
    # etc)
    def test_kwargs_only(self):
        with pytest.raises(TypeError, match="keyword arguments"):
            BaseOperator("task_id")

    def test_missing_kwarg(self):
        with pytest.raises(TypeError, match="missing keyword argument"):
            FakeOperator(task_id="task_id")

    def test_missing_kwargs(self):
        with pytest.raises(TypeError, match="missing keyword arguments"):
            FakeSubClass(task_id="task_id")

    def test_hash(self):
        """Two operators created equally should hash equaylly"""
        # Include a "non-hashable" type too
        assert hash(MockOperator(task_id="one", retries=1024 * 1024, arg1="abcef", params={"a": 1})) == hash(
            MockOperator(task_id="one", retries=1024 * 1024, arg1="abcef", params={"a": 2})
        )

    def test_expand(self):
        op = FakeOperator(test_param=True)
        assert op.test_param

        with pytest.raises(TypeError, match="missing keyword argument 'test_param'"):
            FakeSubClass(test_sub_param=True)

    def test_default_args(self):
        default_args = {"test_param": True}
        op = FakeOperator(default_args=default_args)
        assert op.test_param

        default_args = {"test_param": True, "test_sub_param": True}
        op = FakeSubClass(default_args=default_args)
        assert op.test_param
        assert op.test_sub_param

        default_args = {"test_param": True}
        op = FakeSubClass(default_args=default_args, test_sub_param=True)
        assert op.test_param
        assert op.test_sub_param

        with pytest.raises(TypeError, match="missing keyword argument 'test_sub_param'"):
            FakeSubClass(default_args=default_args)

    def test_execution_timeout_type(self):
        with pytest.raises(
            ValueError, match="execution_timeout must be timedelta object but passed as type: <class 'str'>"
        ):
            BaseOperator(task_id="test", execution_timeout="1")

        with pytest.raises(
            ValueError, match="execution_timeout must be timedelta object but passed as type: <class 'int'>"
        ):
            BaseOperator(task_id="test", execution_timeout=1)

    def test_default_resources(self):
        task = BaseOperator(task_id="default-resources")
        assert task.resources is None

    def test_custom_resources(self):
        task = BaseOperator(task_id="custom-resources", resources={"cpus": 1, "ram": 1024})
        assert task.resources.cpus.qty == 1
        assert task.resources.ram.qty == 1024

    def test_default_email_on_actions(self):
        test_task = BaseOperator(task_id="test_default_email_on_actions")
        assert test_task.email_on_retry is True
        assert test_task.email_on_failure is True

    def test_email_on_actions(self):
        test_task = BaseOperator(
            task_id="test_default_email_on_actions", email_on_retry=False, email_on_failure=True
        )
        assert test_task.email_on_retry is False
        assert test_task.email_on_failure is True

    def test_incorrect_default_args(self):
        default_args = {"test_param": True, "extra_param": True}
        op = FakeOperator(default_args=default_args)
        assert op.test_param

        default_args = {"random_params": True}
        with pytest.raises(TypeError, match="missing keyword argument 'test_param'"):
            FakeOperator(default_args=default_args)

    def test_incorrect_priority_weight(self):
        error_msg = "'priority_weight' for task 'test_op' expects <class 'int'>, got <class 'str'>"
        with pytest.raises(TypeError, match=error_msg):
            BaseOperator(task_id="test_op", priority_weight="2")

    def test_illegal_args_forbidden(self):
        """
        Tests that operators raise exceptions on illegal arguments when
        illegal arguments are not allowed.
        """
        msg = r"Invalid arguments were passed to BaseOperator \(task_id: test_illegal_args\)"
        with pytest.raises(TypeError, match=msg):
            BaseOperator(
                task_id="test_illegal_args",
                illegal_argument_1234="hello?",
            )

    def test_invalid_type_for_default_arg(self):
        error_msg = "'max_active_tis_per_dag' for task 'test' expects <class 'int'>, got <class 'str'> with value 'not_an_int'"
        with pytest.raises(TypeError, match=error_msg):
            BaseOperator(task_id="test", default_args={"max_active_tis_per_dag": "not_an_int"})

    def test_invalid_type_for_operator_arg(self):
        error_msg = "'max_active_tis_per_dag' for task 'test' expects <class 'int'>, got <class 'str'> with value 'not_an_int'"
        with pytest.raises(TypeError, match=error_msg):
            BaseOperator(task_id="test", max_active_tis_per_dag="not_an_int")

    def test_weight_rule_default(self):
        op = BaseOperator(task_id="test_task")
        assert _DownstreamPriorityWeightStrategy() == op.weight_rule

    def test_weight_rule_override(self):
        op = BaseOperator(task_id="test_task", weight_rule="upstream")
        assert _UpstreamPriorityWeightStrategy() == op.weight_rule

    def test_dag_task_invalid_weight_rule(self):
        # Test if we enter an invalid weight rule
        with pytest.raises(ValueError):
            BaseOperator(task_id="should_fail", weight_rule="no rule")

    def test_dag_task_not_registered_weight_strategy(self):
        from airflow.task.priority_strategy import PriorityWeightStrategy

        class NotRegisteredPriorityWeightStrategy(PriorityWeightStrategy):
            def get_weight(self, ti):
                return 99

        with pytest.raises(ValueError, match="Unknown priority strategy"):
            BaseOperator(
                task_id="empty_task",
                weight_rule=NotRegisteredPriorityWeightStrategy(),
            )

    def test_warnings_are_properly_propagated(self):
        with pytest.warns(DeprecationWarning) as warnings:
            DeprecatedOperator(task_id="test")
            assert len(warnings) == 1
            warning = warnings[0]
            # Here we check that the trace points to the place
            # where the deprecated class was used
            assert warning.filename == __file__

    def test_setattr_performs_no_custom_action_at_execute_time(self, spy_agency):
        from airflow.models.xcom_arg import XComArg

        op = MockOperator(task_id="test_task")

        op._lock_for_execution = True
        # TODO: Task-SDK
        # op_copy = op.prepare_for_execution()
        op_copy = op

        spy_agency.spy_on(XComArg.apply_upstream_relationship, call_original=False)
        op_copy.arg1 = "b"
        assert XComArg.apply_upstream_relationship.called == False

    def test_upstream_is_set_when_template_field_is_xcomarg(self):
        with DAG("xcomargs_test", schedule=None):
            op1 = BaseOperator(task_id="op1")
            op2 = MockOperator(task_id="op2", arg1=op1.output)

        assert op1.task_id in op2.upstream_task_ids
        assert op2.task_id in op1.downstream_task_ids

    def test_set_xcomargs_dependencies_works_recursively(self):
        with DAG("xcomargs_test", schedule=None):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op3 = MockOperator(task_id="op3", arg1=[op1.output, op2.output])
            op4 = MockOperator(task_id="op4", arg1={"op1": op1.output, "op2": op2.output})

        assert op1.task_id in op3.upstream_task_ids
        assert op2.task_id in op3.upstream_task_ids
        assert op1.task_id in op4.upstream_task_ids
        assert op2.task_id in op4.upstream_task_ids

    def test_set_xcomargs_dependencies_works_when_set_after_init(self):
        with DAG(dag_id="xcomargs_test", schedule=None):
            op1 = BaseOperator(task_id="op1")
            op2 = MockOperator(task_id="op2")
            op2.arg1 = op1.output  # value is set after init

        assert op1.task_id in op2.upstream_task_ids

    def test_set_xcomargs_dependencies_error_when_outside_dag(self):
        op1 = BaseOperator(task_id="op1")
        with pytest.raises(ValueError):
            MockOperator(task_id="op2", arg1=op1.output)

    def test_cannot_change_dag(self):
        with DAG(dag_id="dag1", schedule=None):
            op1 = BaseOperator(task_id="op1")
        with pytest.raises(ValueError, match="can not be changed"):
            op1.dag = DAG(dag_id="dag2")

    def test_invalid_trigger_rule(self):
        with pytest.raises(
            ValueError,
            match=(r"The trigger_rule must be one of .*,'\.op1'; received 'some_rule'\."),
        ):
            BaseOperator(task_id="op1", trigger_rule="some_rule")


def test_init_subclass_args():
    class InitSubclassOp(BaseOperator):
        class_arg: Any

        def __init_subclass__(cls, class_arg=None, **kwargs) -> None:
            cls.class_arg = class_arg
            super().__init_subclass__()

    class_arg = "foo"

    class ConcreteSubclassOp(InitSubclassOp, class_arg=class_arg):
        pass

    task = ConcreteSubclassOp(task_id="op1")

    assert task.class_arg == class_arg


class CustomInt(int):
    def __int__(self):
        raise ValueError("Cannot cast to int")


@pytest.mark.parametrize(
    ("retries", "expected"),
    [
        pytest.param("foo", "'retries' type must be int, not str", id="string"),
        pytest.param(CustomInt(10), "'retries' type must be int, not CustomInt", id="custom int"),
    ],
)
def test_operator_retries_invalid(dag_maker, retries, expected):
    with pytest.raises(TypeError) as ctx:
        BaseOperator(task_id="test_illegal_args", retries=retries)
    assert str(ctx.value) == expected


@pytest.mark.parametrize(
    ("retries", "expected"),
    [
        pytest.param(None, 0, id="None"),
        pytest.param("5", 5, id="str"),
        pytest.param(1, 1, id="int"),
    ],
)
def test_operator_retries_conversion(retries, expected):
    op = BaseOperator(
        task_id="test_illegal_args",
        retries=retries,
    )
    assert op.retries == expected


def test_default_retry_delay():
    task1 = BaseOperator(task_id="test_no_explicit_retry_delay")

    assert task1.retry_delay == timedelta(seconds=300)


def test_dag_level_retry_delay():
    with DAG(dag_id="test_dag_level_retry_delay", default_args={"retry_delay": timedelta(seconds=100)}):
        task1 = BaseOperator(task_id="test_no_explicit_retry_delay")

        assert task1.retry_delay == timedelta(seconds=100)


def test_task_level_retry_delay():
    with DAG(dag_id="test_task_level_retry_delay", default_args={"retry_delay": timedelta(seconds=100)}):
        task1 = BaseOperator(task_id="test_no_explicit_retry_delay", retry_delay=200)

        assert task1.retry_delay == timedelta(seconds=200)
