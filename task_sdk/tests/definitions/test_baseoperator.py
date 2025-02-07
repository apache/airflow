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
import uuid
import warnings
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple
from unittest import mock

import jinja2
import pytest

from airflow.sdk.definitions.baseoperator import BaseOperator, BaseOperatorMeta
from airflow.sdk.definitions.dag import DAG
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
    caplog, monkeypatch, task, context, expected_exception, expected_rendering, expected_log, not_expected_log
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
