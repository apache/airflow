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

import copy
import logging
import uuid
import warnings
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple
from unittest import mock

import jinja2
import pytest
import structlog

from airflow.sdk import task as task_decorator
from airflow.sdk._shared.secrets_masker import _secrets_masker, mask_secret
from airflow.sdk.bases.operator import (
    BaseOperator,
    BaseOperatorMeta,
    ExecutorSafeguard,
    chain,
    chain_linear,
    cross_downstream,
)
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.edges import Label
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.sdk.definitions.template import literal
from airflow.task.priority_strategy import _DownstreamPriorityWeightStrategy, _UpstreamPriorityWeightStrategy

DEFAULT_DATE = datetime(2016, 1, 1, tzinfo=timezone.utc)


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return f"{ClassWithCustomAttributes.__name__}({str(self.__dict__)})"

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__dict__)

    def __ne__(self, other):
        return not self.__eq__(other)


# Objects with circular references (for testing purpose)
object1 = ClassWithCustomAttributes(attr="{{ foo }}_1", template_fields=["ref"])
object2 = ClassWithCustomAttributes(attr="{{ foo }}_2", ref=object1, template_fields=["ref"])
setattr(object1, "ref", object2)


class MockNamedTuple(NamedTuple):
    var1: str
    var2: str


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
    # Since we have a custom metaclass, lets double check the behaviour of
    # passing args in the wrong way (args etc)
    def test_kwargs_only(self):
        with pytest.raises(TypeError, match="keyword arguments"):
            BaseOperator("task_id")

    def test_missing_kwarg(self):
        with pytest.raises(TypeError, match="missing keyword argument"):
            FakeOperator(task_id="task_id")

    def test_missing_kwargs(self):
        with pytest.raises(TypeError, match="missing keyword arguments"):
            FakeSubClass(task_id="task_id")

    def test_baseoperator_raises_exception_when_task_id_plus_taskgroup_id_exceeds_250_chars(self):
        with DAG(dag_id="foo"), TaskGroup("A"):
            with pytest.raises(ValueError, match="The key has to be less than 250 characters"):
                BaseOperator(task_id="1" * 249)

    def test_baseoperator_with_task_id_and_taskgroup_id_less_than_250_chars(self):
        with DAG(dag_id="foo", schedule=None), TaskGroup("A" * 10):
            BaseOperator(task_id="1" * 239)

    def test_baseoperator_with_task_id_less_than_250_chars(self):
        """Test exception is not raised when operator task id  < 250 chars."""
        with DAG(dag_id="foo"):
            op = BaseOperator(task_id="1" * 249)
        assert op.task_id == "1" * 249

    def test_task_naive_datetime(self):
        naive_datetime = DEFAULT_DATE.replace(tzinfo=None)
        op_no_dag = BaseOperator(
            task_id="test_task_naive_datetime", start_date=naive_datetime, end_date=naive_datetime
        )
        assert op_no_dag.start_date.tzinfo
        assert op_no_dag.end_date.tzinfo

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

    @mock.patch("airflow.sdk.bases.operator.redact")
    def test_illegal_args_with_secrets(self, mock_redact):
        """
        Tests that operators on illegal arguments with secrets are correctly masked.
        """
        secret = "secretP4ssw0rd!"
        mock_redact.side_effect = ["***"]

        msg = r"Invalid arguments were passed to BaseOperator"
        with pytest.raises(TypeError, match=msg) as exc_info:
            BaseOperator(
                task_id="test_illegal_args",
                secret_argument=secret,
            )
        assert "***" in str(exc_info.value)
        assert secret not in str(exc_info.value)

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

    def test_db_safe_priority(self):
        """Test the db_safe_priority function."""
        from airflow.sdk.bases.operator import DB_SAFE_MAXIMUM, DB_SAFE_MINIMUM, db_safe_priority

        assert db_safe_priority(1) == 1
        assert db_safe_priority(-1) == -1
        assert db_safe_priority(9999999999) == DB_SAFE_MAXIMUM
        assert db_safe_priority(-9999999999) == DB_SAFE_MINIMUM

    def test_db_safe_constants(self):
        """Test the database safe constants."""
        from airflow.sdk.bases.operator import DB_SAFE_MAXIMUM, DB_SAFE_MINIMUM

        assert DB_SAFE_MINIMUM == -2147483648
        assert DB_SAFE_MAXIMUM == 2147483647

    def test_warnings_are_properly_propagated(self):
        with pytest.warns(DeprecationWarning, match="deprecated") as warnings:
            DeprecatedOperator(task_id="test")
        assert len(warnings) == 1
        warning = warnings[0]
        # Here we check that the trace points to the place
        # where the deprecated class was used
        assert warning.filename == __file__

    def test_setattr_performs_no_custom_action_at_execute_time(self, spy_agency):
        op = MockOperator(task_id="test_task")

        op_copy = op.prepare_for_execution()
        spy_agency.spy_on(op._set_xcomargs_dependency, call_original=False)
        op_copy.arg1 = "b"
        assert op._set_xcomargs_dependency.called is False

    def test_upstream_is_set_when_template_field_is_xcomarg(self):
        with DAG("xcomargs_test", schedule=None):
            op1 = BaseOperator(task_id="op1")
            op2 = MockOperator(task_id="op2", arg1=op1.output)

        assert op1.task_id in op2.upstream_task_ids
        assert op2.task_id in op1.downstream_task_ids

    def test_cross_downstream(self):
        """Test if all dependencies between tasks are all set correctly."""
        dag = DAG(dag_id="test_dag", schedule=None, start_date=datetime.now())
        start_tasks = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 4)]
        end_tasks = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(4, 7)]
        cross_downstream(from_tasks=start_tasks, to_tasks=end_tasks)

        for start_task in start_tasks:
            assert set(start_task.get_direct_relatives(upstream=False)) == set(end_tasks)

        # Begin test for `XComArgs`
        xstart_tasks = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 4)
        ]
        xend_tasks = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(4, 7)
        ]
        cross_downstream(from_tasks=xstart_tasks, to_tasks=xend_tasks)

        for xstart_task in xstart_tasks:
            assert set(xstart_task.operator.get_direct_relatives(upstream=False)) == {
                xend_task.operator for xend_task in xend_tasks
            }

    def test_chain(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())

        # Begin test for classic operators with `EdgeModifiers`
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5, op6] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 7)]
        chain(op1, [label1, label2], [op2, op3], [op4, op5], op6)

        assert {op2, op3} == set(op1.get_direct_relatives(upstream=False))
        assert [op4] == op2.get_direct_relatives(upstream=False)
        assert [op5] == op3.get_direct_relatives(upstream=False)
        assert {op4, op5} == set(op6.get_direct_relatives(upstream=True))

        assert dag.get_edge_info(upstream_task_id=op1.task_id, downstream_task_id=op2.task_id) == {
            "label": "label1"
        }
        assert dag.get_edge_info(upstream_task_id=op1.task_id, downstream_task_id=op3.task_id) == {
            "label": "label2"
        }

        # Begin test for `XComArgs` with `EdgeModifiers`
        [xlabel1, xlabel2] = [Label(label=f"xcomarg_label{i}") for i in range(1, 3)]
        [xop1, xop2, xop3, xop4, xop5, xop6] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 7)
        ]
        chain(xop1, [xlabel1, xlabel2], [xop2, xop3], [xop4, xop5], xop6)

        assert {xop2.operator, xop3.operator} == set(xop1.operator.get_direct_relatives(upstream=False))
        assert [xop4.operator] == xop2.operator.get_direct_relatives(upstream=False)
        assert [xop5.operator] == xop3.operator.get_direct_relatives(upstream=False)
        assert {xop4.operator, xop5.operator} == set(xop6.operator.get_direct_relatives(upstream=True))

        assert dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop2.operator.task_id
        ) == {"label": "xcomarg_label1"}
        assert dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop3.operator.task_id
        ) == {"label": "xcomarg_label2"}

        # Begin test for `TaskGroups`
        [tg1, tg2] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3)]
        [op1, op2] = [BaseOperator(task_id=f"task{i}", dag=dag) for i in range(1, 3)]
        [tgop1, tgop2] = [
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg1, dag=dag) for i in range(1, 3)
        ]
        [tgop3, tgop4] = [
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg2, dag=dag) for i in range(1, 3)
        ]
        chain(op1, tg1, tg2, op2)

        assert {tgop1, tgop2} == set(op1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop2.get_direct_relatives(upstream=False))
        assert [op2] == tgop3.get_direct_relatives(upstream=False)
        assert [op2] == tgop4.get_direct_relatives(upstream=False)

    def test_chain_linear(self):
        dag = DAG(dag_id="test_chain_linear", schedule=None, start_date=datetime.now())

        t1, t2, t3, t4, t5, t6, t7 = (BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 8))
        chain_linear(t1, [t2, t3, t4], [t5, t6], t7)

        assert set(t1.get_direct_relatives(upstream=False)) == {t2, t3, t4}
        assert set(t2.get_direct_relatives(upstream=False)) == {t5, t6}
        assert set(t3.get_direct_relatives(upstream=False)) == {t5, t6}
        assert set(t7.get_direct_relatives(upstream=True)) == {t5, t6}

        t1, t2, t3, t4, t5, t6 = (
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 7)
        )
        chain_linear(t1, [t2, t3], [t4, t5], t6)

        assert set(t1.operator.get_direct_relatives(upstream=False)) == {t2.operator, t3.operator}
        assert set(t2.operator.get_direct_relatives(upstream=False)) == {t4.operator, t5.operator}
        assert set(t3.operator.get_direct_relatives(upstream=False)) == {t4.operator, t5.operator}
        assert set(t6.operator.get_direct_relatives(upstream=True)) == {t4.operator, t5.operator}

        # Begin test for `TaskGroups`
        tg1, tg2 = (TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3))
        op1, op2 = (BaseOperator(task_id=f"task{i}", dag=dag) for i in range(1, 3))
        tgop1, tgop2 = (
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg1, dag=dag) for i in range(1, 3)
        )
        tgop3, tgop4 = (
            BaseOperator(task_id=f"task_group_task{i}", task_group=tg2, dag=dag) for i in range(1, 3)
        )
        chain_linear(op1, tg1, tg2, op2)

        assert set(op1.get_direct_relatives(upstream=False)) == {tgop1, tgop2}
        assert set(tgop1.get_direct_relatives(upstream=False)) == {tgop3, tgop4}
        assert set(tgop2.get_direct_relatives(upstream=False)) == {tgop3, tgop4}
        assert set(tgop3.get_direct_relatives(upstream=False)) == {op2}
        assert set(tgop4.get_direct_relatives(upstream=False)) == {op2}

        t1, t2 = (BaseOperator(task_id=f"t-{i}", dag=dag) for i in range(1, 3))
        with pytest.raises(ValueError, match="Labels are not supported"):
            chain_linear(t1, Label("hi"), t2)

        with pytest.raises(ValueError, match="nothing to do"):
            chain_linear()

        with pytest.raises(ValueError, match="Did you forget to expand"):
            chain_linear(t1)

    def test_chain_not_support_type(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())
        [op1, op2] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 3)]
        with pytest.raises(TypeError):
            chain([op1, op2], 1)

        # Begin test for `XComArgs`
        [xop1, xop2] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 3)
        ]

        with pytest.raises(TypeError):
            chain([xop1, xop2], 1)

        # Begin test for `EdgeModifiers`
        with pytest.raises(TypeError):
            chain([Label("labe1"), Label("label2")], 1)

        # Begin test for `TaskGroups`
        [tg1, tg2] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3)]

        with pytest.raises(TypeError):
            chain([tg1, tg2], 1)

    def test_chain_different_length_iterable(self):
        dag = DAG(dag_id="test_chain", schedule=None, start_date=datetime.now())
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5] = [BaseOperator(task_id=f"t{i}", dag=dag) for i in range(1, 6)]

        with pytest.raises(ValueError):
            chain([op1, op2], [op3, op4, op5])

        with pytest.raises(ValueError):
            chain([op1, op2, op3], [label1, label2])

        # Begin test for `XComArgs` with `EdgeModifiers`
        [label3, label4] = [Label(label=f"xcomarg_label{i}") for i in range(1, 3)]
        [xop1, xop2, xop3, xop4, xop5] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 6)
        ]

        with pytest.raises(ValueError):
            chain([xop1, xop2], [xop3, xop4, xop5])

        with pytest.raises(ValueError):
            chain([xop1, xop2, xop3], [label1, label2])

        # Begin test for `TaskGroups`
        [tg1, tg2, tg3, tg4, tg5] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 6)]

        with pytest.raises(ValueError):
            chain([tg1, tg2], [tg3, tg4, tg5])

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

    def test_trigger_rule_validation(self):
        from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_TRIGGER_RULE

        # An operator with default trigger rule and a fail-stop dag should be allowed.
        with DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=True,
        ):
            BaseOperator(task_id="test_valid_trigger_rule", trigger_rule=DEFAULT_TRIGGER_RULE)

        # An operator with non default trigger rule and a non fail-stop dag should be allowed.
        with DAG(
            dag_id="test_dag_trigger_rule_validation",
            schedule=None,
            start_date=DEFAULT_DATE,
            fail_fast=False,
        ):
            BaseOperator(task_id="test_valid_trigger_rule", trigger_rule="always")

    @pytest.mark.parametrize(
        ("content", "context", "expected_output"),
        [
            ("{{ foo }}", {"foo": "bar"}, "bar"),
            (["{{ foo }}_1", "{{ foo }}_2"], {"foo": "bar"}, ["bar_1", "bar_2"]),
            (("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, ("bar_1", "bar_2")),
            (
                {"key1": "{{ foo }}_1", "key2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key1": "bar_1", "key2": "bar_2"},
            ),
            (
                {"key_{{ foo }}_1": 1, "key_2": "{{ foo }}_2"},
                {"foo": "bar"},
                {"key_{{ foo }}_1": 1, "key_2": "bar_2"},
            ),
            (date(2018, 12, 6), {"foo": "bar"}, date(2018, 12, 6)),
            (datetime(2018, 12, 6, 10, 55), {"foo": "bar"}, datetime(2018, 12, 6, 10, 55)),
            (MockNamedTuple("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, MockNamedTuple("bar_1", "bar_2")),
            ({"{{ foo }}_1", "{{ foo }}_2"}, {"foo": "bar"}, {"bar_1", "bar_2"}),
            (None, {}, None),
            ([], {}, []),
            ({}, {}, {}),
            (
                # check nested fields can be templated
                ClassWithCustomAttributes(att1="{{ foo }}_1", att2="{{ foo }}_2", template_fields=["att1"]),
                {"foo": "bar"},
                ClassWithCustomAttributes(att1="bar_1", att2="{{ foo }}_2", template_fields=["att1"]),
            ),
            (
                # check deep nested fields can be templated
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="{{ foo }}_1", att2="{{ foo }}_2", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ foo }}_3", att4="{{ foo }}_4", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
                {"foo": "bar"},
                ClassWithCustomAttributes(
                    nested1=ClassWithCustomAttributes(
                        att1="bar_1", att2="{{ foo }}_2", template_fields=["att1"]
                    ),
                    nested2=ClassWithCustomAttributes(
                        att3="{{ foo }}_3", att4="{{ foo }}_4", template_fields=["att3"]
                    ),
                    template_fields=["nested1"],
                ),
            ),
            (
                # check null value on nested template field
                ClassWithCustomAttributes(att1=None, template_fields=["att1"]),
                {},
                ClassWithCustomAttributes(att1=None, template_fields=["att1"]),
            ),
            (
                # check there is no RecursionError on circular references
                object1,
                {"foo": "bar"},
                object1,
            ),
            # By default, Jinja2 drops one (single) trailing newline
            ("{{ foo }}\n\n", {"foo": "bar"}, "bar\n"),
            (literal("{{ foo }}"), {"foo": "bar"}, "{{ foo }}"),
            (literal(["{{ foo }}_1", "{{ foo }}_2"]), {"foo": "bar"}, ["{{ foo }}_1", "{{ foo }}_2"]),
            (literal(("{{ foo }}_1", "{{ foo }}_2")), {"foo": "bar"}, ("{{ foo }}_1", "{{ foo }}_2")),
        ],
    )
    def test_render_template(self, content, context, expected_output):
        """Test render_template given various input types."""
        task = BaseOperator(task_id="op1")

        result = task.render_template(content, context)
        assert result == expected_output

    @pytest.mark.parametrize(
        ("content", "context", "expected_output"),
        [
            ("{{ foo }}", {"foo": "bar"}, "bar"),
            ("{{ foo }}", {"foo": ["bar1", "bar2"]}, ["bar1", "bar2"]),
            (["{{ foo }}", "{{ foo | length}}"], {"foo": ["bar1", "bar2"]}, [["bar1", "bar2"], 2]),
            (("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, ("bar_1", "bar_2")),
            ("{{ ds }}", {"ds": date(2018, 12, 6)}, date(2018, 12, 6)),
            (datetime(2018, 12, 6, 10, 55), {"foo": "bar"}, datetime(2018, 12, 6, 10, 55)),
            ("{{ ds }}", {"ds": datetime(2018, 12, 6, 10, 55)}, datetime(2018, 12, 6, 10, 55)),
            (MockNamedTuple("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, MockNamedTuple("bar_1", "bar_2")),
            (
                ("{{ foo }}", "{{ foo.isoformat() }}"),
                {"foo": datetime(2018, 12, 6, 10, 55)},
                (datetime(2018, 12, 6, 10, 55), "2018-12-06T10:55:00"),
            ),
            (None, {}, None),
            ([], {}, []),
            ({}, {}, {}),
        ],
    )
    def test_render_template_with_native_envs(self, content, context, expected_output):
        """Test render_template given various input types with Native Python types"""
        with DAG("test-dag", schedule=None, start_date=DEFAULT_DATE, render_template_as_native_obj=True):
            task = BaseOperator(task_id="op1")

        result = task.render_template(content, context)
        assert result == expected_output

    def test_render_template_fields(self):
        """Verify if operator attributes are correctly templated."""
        task = MockOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        # Assert nothing is templated yet
        assert task.arg1 == "{{ foo }}"
        assert task.arg2 == "{{ bar }}"

        # Trigger templating and verify if attributes are templated correctly
        task.render_template_fields(context={"foo": "footemplated", "bar": "bartemplated"})
        assert task.arg1 == "footemplated"
        assert task.arg2 == "bartemplated"

    def test_render_template_fields_func_using_context(self):
        """Verify if operator attributes are correctly templated."""

        def fn_to_template(context, jinja_env):
            tmp = context["task"].render_template("{{ bar }}", context, jinja_env)
            return "foo_" + tmp

        task = MockOperator(task_id="op1", arg2=fn_to_template)

        # Trigger templating and verify if attributes are templated correctly
        task.render_template_fields(context={"bar": "bartemplated", "task": task})
        assert task.arg2 == "foo_bartemplated"

    def test_render_template_fields_simple_func(self):
        """Verify if operator attributes are correctly templated."""

        def fn_to_template(**kwargs):
            a = "foo_" + ("bar" * 3)
            return a

        task = MockOperator(task_id="op1", arg2=fn_to_template)
        task.render_template_fields({})
        assert task.arg2 == "foo_barbarbar"

    @pytest.mark.parametrize(("content",), [(object(),), (uuid.uuid4(),)])
    def test_render_template_fields_no_change(self, content):
        """Tests if non-templatable types remain unchanged."""
        task = BaseOperator(task_id="op1")

        result = task.render_template(content, {"foo": "bar"})
        assert content is result

    def test_nested_template_fields_declared_must_exist(self):
        """Test render_template when a nested template field is missing."""
        task = BaseOperator(task_id="op1")

        error_message = (
            "'missing_field' is configured as a template field but ClassWithCustomAttributes does not have "
            "this attribute."
        )
        with pytest.raises(AttributeError, match=error_message):
            task.render_template(
                ClassWithCustomAttributes(
                    template_fields=["missing_field"], task_type="ClassWithCustomAttributes"
                ),
                {},
            )

    def test_string_template_field_attr_is_converted_to_list(self):
        """Verify template_fields attribute is converted to a list if declared as a string."""

        class StringTemplateFieldsOperator(BaseOperator):
            template_fields = "a_string"

        warning_message = (
            "The `template_fields` value for StringTemplateFieldsOperator is a string but should be a "
            "list or tuple of string. Wrapping it in a list for execution. Please update "
            "StringTemplateFieldsOperator accordingly."
        )
        with pytest.warns(UserWarning, match=warning_message) as warnings:
            task = StringTemplateFieldsOperator(task_id="op1")

        assert len(warnings) == 1
        assert isinstance(task.template_fields, list)

    def test_jinja_invalid_expression_is_just_propagated(self):
        """Test render_template propagates Jinja invalid expression errors."""
        task = BaseOperator(task_id="op1")

        with pytest.raises(jinja2.exceptions.TemplateSyntaxError):
            task.render_template("{{ invalid expression }}", {})

    @mock.patch("airflow.sdk.definitions._internal.templater.SandboxedEnvironment", autospec=True)
    def test_jinja_env_creation(self, mock_jinja_env):
        """Verify if a Jinja environment is created only once when templating."""
        task = MockOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        task.render_template_fields(context={"foo": "whatever", "bar": "whatever"})
        assert mock_jinja_env.call_count == 1

    def test_deepcopy(self):
        # Test bug when copying an operator attached to a Dag
        with DAG("dag0", schedule=None, start_date=DEFAULT_DATE) as dag:

            @dag.task
            def task0():
                pass

            MockOperator(task_id="task1", arg1=task0())

        copy.deepcopy(dag)

    def test_mro(self):
        from airflow.providers.common.sql.operators import sql

        class Mixin(sql.BaseSQLOperator):
            pass

        class Branch(Mixin, sql.BranchSQLOperator):
            pass

        # The following throws an exception if metaclass breaks MRO:
        #   airflow.exceptions.AirflowException: Invalid arguments were passed to Branch (task_id: test). Invalid arguments were:
        #   **kwargs: {'sql': 'sql', 'follow_task_ids_if_true': ['x'], 'follow_task_ids_if_false': ['y']}
        op = Branch(
            task_id="test",
            conn_id="abc",
            sql="sql",
            follow_task_ids_if_true=["x"],
            follow_task_ids_if_false=["y"],
        )
        assert isinstance(op, Branch)


def test_init_subclass_args():
    class InitSubclassOp(BaseOperator):
        class_arg = None

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


@pytest.mark.parametrize(
    ("task", "context", "expected_exception", "expected_rendering", "expected_log", "not_expected_log"),
    [
        # Simple success case.
        (
            MockOperator(task_id="op1", arg1="{{ foo }}"),
            dict(foo="footemplated"),
            None,
            dict(arg1="footemplated"),
            None,
            "Exception rendering Jinja template",
        ),
        # Jinja syntax error.
        (
            MockOperator(task_id="op1", arg1="{{ foo"),
            dict(),
            jinja2.TemplateSyntaxError,
            None,
            "Exception rendering Jinja template for task 'op1', field 'arg1'. Template: '{{ foo'",
            None,
        ),
        # Type error
        (
            MockOperator(task_id="op1", arg1="{{ foo + 1 }}"),
            dict(foo="footemplated"),
            TypeError,
            None,
            "Exception rendering Jinja template for task 'op1', field 'arg1'. Template: '{{ foo + 1 }}'",
            None,
        ),
    ],
)
def test_render_template_fields_logging(
    caplog, task, context, expected_exception, expected_rendering, expected_log, not_expected_log
):
    """Verify if operator attributes are correctly templated."""

    # Trigger templating and verify results
    def _do_render():
        task.render_template_fields(context=context)

    if expected_exception:
        with (
            pytest.raises(expected_exception),
            caplog.at_level(logging.ERROR, logger="airflow.sdk.definitions.templater"),
        ):
            _do_render()
    else:
        _do_render()
        for k, v in expected_rendering.items():
            assert getattr(task, k) == v
    if expected_log:
        assert expected_log in caplog.text
    if not_expected_log:
        assert not_expected_log not in caplog.text


@pytest.mark.enable_redact
def test_render_template_fields_secret_masking(caplog):
    """Test that sensitive values are masked in Jinja template rendering exceptions."""
    masker = _secrets_masker()
    masker.reset_masker()

    masker.sensitive_variables_fields = ["password", "secret", "token"]

    mask_secret("mysecretpassword", "password")

    task = MockOperator(task_id="op1", arg1="{{ password + 1 }}")
    context = {"password": "mysecretpassword"}

    with (
        pytest.raises(TypeError),
        caplog.at_level(logging.ERROR, logger="airflow.sdk.definitions.templater"),
    ):
        task.render_template_fields(context=context)

    assert "mysecretpassword" not in caplog.text
    assert "Template: '{{ password + 1 }}'" in caplog.text
    assert "Exception rendering Jinja template for task 'op1', field 'arg1'" in caplog.text


class HelloWorldOperator(BaseOperator):
    log = structlog.get_logger(__name__)

    def execute(self, context):
        return f"Hello {self.owner}!"


class ExtendedHelloWorldOperator(HelloWorldOperator):
    def execute(self, context):
        return super().execute(context)


class TestExecutorSafeguard:
    @pytest.fixture(autouse=True)
    def _disable_test_mode(self, monkeypatch):
        monkeypatch.setattr(ExecutorSafeguard, "test_mode", False)

    def test_execute_not_allow_nested_ops(self):
        with DAG("d1"):
            op = ExtendedHelloWorldOperator(task_id="hello_operator", allow_nested_operators=False)

        with pytest.raises(RuntimeError):
            op.execute(context={})

    def test_execute_subclassed_op_warns_once(self, captured_logs):
        with DAG("d1"):
            op = ExtendedHelloWorldOperator(task_id="hello_operator")

        op.execute(context={})
        assert captured_logs == [
            {
                "event": "ExtendedHelloWorldOperator.execute cannot be called outside of the Task Runner!",
                "level": "warning",
                "timestamp": mock.ANY,
                "logger": "tests.task_sdk.bases.test_operator",
                "loc": mock.ANY,
            },
        ]

    def test_decorated_operators(self, caplog):
        with DAG("d1") as dag:

            @dag.task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            op = say_hello()

        op.operator.execute(context={})
        assert {
            "event": "HelloWorldOperator.execute cannot be called outside of the Task Runner!",
            "log_level": "warning",
        } in caplog

    @pytest.mark.log_level(logging.WARNING)
    def test_python_op(self, caplog):
        from airflow.providers.standard.operators.python import PythonOperator

        with DAG("d1"):

            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            op = PythonOperator(
                task_id="say_hello",
                python_callable=say_hello,
            )
        op.execute(context={}, PythonOperator__sentinel=ExecutorSafeguard.sentinel_value)
        assert {
            "event": "HelloWorldOperator.execute cannot be called outside of the Task Runner!",
            "log_level": "warning",
        } in caplog


def test_partial_default_args():
    class MockOperator(BaseOperator):
        def __init__(self, arg1, arg2, arg3, **kwargs):
            self.arg1 = arg1
            self.arg2 = arg2
            self.arg3 = arg3
            self.kwargs = kwargs
            super().__init__(**kwargs)

    with DAG(
        dag_id="test_partial_default_args",
        default_args={"queue": "THIS", "arg1": 1, "arg2": 2, "arg3": 3, "arg4": 4},
    ):
        t1 = BaseOperator(task_id="t1")
        t2 = MockOperator.partial(task_id="t2", arg2="b").expand(arg1=t1.output)

    # Only default_args recognized by BaseOperator are applied.
    assert t2.partial_kwargs["queue"] == "THIS"
    assert "arg1" not in t2.partial_kwargs
    assert t2.partial_kwargs["arg2"] == "b"
    assert "arg3" not in t2.partial_kwargs
    assert "arg4" not in t2.partial_kwargs

    # Simulate resolving mapped operator. This should apply all default_args.
    op = t2.unmap({"arg1": "a"})
    assert isinstance(op, MockOperator)
    assert "arg4" not in op.kwargs  # Not recognized by any class; never passed.
    assert op.arg1 == "a"
    assert op.arg2 == "b"
    assert op.arg3 == 3
    assert op.queue == "THIS"
