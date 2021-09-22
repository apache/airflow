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
import uuid
from datetime import date, datetime
from unittest import mock

import jinja2
import pytest

from airflow.decorators import task as task_decorator
from airflow.exceptions import AirflowException
from airflow.lineage.entities import File
from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator, BaseOperatorMeta, chain, cross_downstream
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from tests.models import DEFAULT_DATE
from tests.test_utils.mock_operators import DeprecatedOperator, MockNamedTuple, MockOperator


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
setattr(object1, 'ref', object2)


# Essentially similar to airflow.models.baseoperator.BaseOperator
class DummyClass(metaclass=BaseOperatorMeta):
    def __init__(self, test_param, params=None, default_args=None):
        self.test_param = test_param

    def set_xcomargs_dependencies(self):
        ...


class DummySubClass(DummyClass):
    def __init__(self, test_sub_param, **kwargs):
        super().__init__(**kwargs)
        self.test_sub_param = test_sub_param


class TestBaseOperator:
    def test_apply(self):
        dummy = DummyClass(test_param=True)
        assert dummy.test_param

        with pytest.raises(AirflowException, match='Argument.*test_param.*required'):
            DummySubClass(test_sub_param=True)

    def test_default_args(self):
        default_args = {'test_param': True}
        dummy_class = DummyClass(default_args=default_args)
        assert dummy_class.test_param

        default_args = {'test_param': True, 'test_sub_param': True}
        dummy_subclass = DummySubClass(default_args=default_args)
        assert dummy_class.test_param
        assert dummy_subclass.test_sub_param

        default_args = {'test_param': True}
        dummy_subclass = DummySubClass(default_args=default_args, test_sub_param=True)
        assert dummy_class.test_param
        assert dummy_subclass.test_sub_param

        with pytest.raises(AirflowException, match='Argument.*test_sub_param.*required'):
            DummySubClass(default_args=default_args)

    def test_incorrect_default_args(self):
        default_args = {'test_param': True, 'extra_param': True}
        dummy_class = DummyClass(default_args=default_args)
        assert dummy_class.test_param

        default_args = {'random_params': True}
        with pytest.raises(AirflowException, match='Argument.*test_param.*required'):
            DummyClass(default_args=default_args)

    def test_incorrect_priority_weight(self):
        error_msg = "`priority_weight` for task 'test_op' only accepts integers, received '<class 'str'>'."
        with pytest.raises(AirflowException, match=error_msg):
            BaseOperator(task_id="test_op", priority_weight="2")

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
            (["{{ foo }}", "{{ foo | length}}"], {"foo": ["bar1", "bar2"]}, [['bar1', 'bar2'], 2]),
            (("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, ("bar_1", "bar_2")),
            ("{{ ds }}", {"ds": date(2018, 12, 6)}, date(2018, 12, 6)),
            (datetime(2018, 12, 6, 10, 55), {"foo": "bar"}, datetime(2018, 12, 6, 10, 55)),
            ("{{ ds }}", {"ds": datetime(2018, 12, 6, 10, 55)}, datetime(2018, 12, 6, 10, 55)),
            (MockNamedTuple("{{ foo }}_1", "{{ foo }}_2"), {"foo": "bar"}, MockNamedTuple("bar_1", "bar_2")),
            (
                ("{{ foo }}", "{{ foo.isoformat() }}"),
                {"foo": datetime(2018, 12, 6, 10, 55)},
                (datetime(2018, 12, 6, 10, 55), '2018-12-06T10:55:00'),
            ),
            (None, {}, None),
            ([], {}, []),
            ({}, {}, {}),
        ],
    )
    def test_render_template_with_native_envs(self, content, context, expected_output):
        """Test render_template given various input types with Native Python types"""
        with DAG("test-dag", start_date=DEFAULT_DATE, render_template_as_native_obj=True):
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

    @pytest.mark.parametrize(("content",), [(object(),), (uuid.uuid4(),)])
    def test_render_template_fields_no_change(self, content):
        """Tests if non-templatable types remain unchanged."""
        task = BaseOperator(task_id="op1")

        result = task.render_template(content, {"foo": "bar"})
        assert content is result

    def test_nested_template_fields_declared_must_exist(self):
        """Test render_template when a nested template field is missing."""
        task = BaseOperator(task_id="op1")

        with pytest.raises(AttributeError) as ctx:
            task.render_template(ClassWithCustomAttributes(template_fields=["missing_field"]), {})

        assert "'ClassWithCustomAttributes' object has no attribute 'missing_field'" == str(ctx.value)

    def test_jinja_invalid_expression_is_just_propagated(self):
        """Test render_template propagates Jinja invalid expression errors."""
        task = BaseOperator(task_id="op1")

        with pytest.raises(jinja2.exceptions.TemplateSyntaxError):
            task.render_template("{{ invalid expression }}", {})

    @mock.patch("airflow.templates.SandboxedEnvironment", autospec=True)
    def test_jinja_env_creation(self, mock_jinja_env):
        """Verify if a Jinja environment is created only once when templating."""
        task = MockOperator(task_id="op1", arg1="{{ foo }}", arg2="{{ bar }}")

        task.render_template_fields(context={"foo": "whatever", "bar": "whatever"})
        assert mock_jinja_env.call_count == 1

    def test_default_resources(self):
        task = BaseOperator(task_id="default-resources")
        assert task.resources is None

    def test_custom_resources(self):
        task = BaseOperator(task_id="custom-resources", resources={"cpus": 1, "ram": 1024})
        assert task.resources.cpus.qty == 1
        assert task.resources.ram.qty == 1024

    def test_default_email_on_actions(self):
        test_task = BaseOperator(task_id='test_default_email_on_actions')
        assert test_task.email_on_retry is True
        assert test_task.email_on_failure is True

    def test_email_on_actions(self):
        test_task = BaseOperator(
            task_id='test_default_email_on_actions', email_on_retry=False, email_on_failure=True
        )
        assert test_task.email_on_retry is False
        assert test_task.email_on_failure is True

    def test_cross_downstream(self):
        """Test if all dependencies between tasks are all set correctly."""
        dag = DAG(dag_id="test_dag", start_date=datetime.now())
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
        dag = DAG(dag_id='test_chain', start_date=datetime.now())

        # Begin test for classic operators with `EdgeModifiers`
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5, op6] = [BaseOperator(task_id=f't{i}', dag=dag) for i in range(1, 7)]
        chain(op1, [label1, label2], [op2, op3], [op4, op5], op6)

        assert {op2, op3} == set(op1.get_direct_relatives(upstream=False))
        assert [op4] == op2.get_direct_relatives(upstream=False)
        assert [op5] == op3.get_direct_relatives(upstream=False)
        assert {op4, op5} == set(op6.get_direct_relatives(upstream=True))

        assert {"label": "label1"} == dag.get_edge_info(
            upstream_task_id=op1.task_id, downstream_task_id=op2.task_id
        )
        assert {"label": "label2"} == dag.get_edge_info(
            upstream_task_id=op1.task_id, downstream_task_id=op3.task_id
        )

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

        assert {"label": "xcomarg_label1"} == dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop2.operator.task_id
        )
        assert {"label": "xcomarg_label2"} == dag.get_edge_info(
            upstream_task_id=xop1.operator.task_id, downstream_task_id=xop3.operator.task_id
        )

        # Begin test for `TaskGroups`
        [tg1, tg2] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 3)]
        [op1, op2] = [BaseOperator(task_id=f'task{i}', dag=dag) for i in range(1, 3)]
        [tgop1, tgop2] = [
            BaseOperator(task_id=f'task_group_task{i}', task_group=tg1, dag=dag) for i in range(1, 3)
        ]
        [tgop3, tgop4] = [
            BaseOperator(task_id=f'task_group_task{i}', task_group=tg2, dag=dag) for i in range(1, 3)
        ]
        chain(op1, tg1, tg2, op2)

        assert {tgop1, tgop2} == set(op1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop1.get_direct_relatives(upstream=False))
        assert {tgop3, tgop4} == set(tgop2.get_direct_relatives(upstream=False))
        assert [op2] == tgop3.get_direct_relatives(upstream=False)
        assert [op2] == tgop4.get_direct_relatives(upstream=False)

    def test_chain_not_support_type(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [op1, op2] = [BaseOperator(task_id=f't{i}', dag=dag) for i in range(1, 3)]
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
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [label1, label2] = [Label(label=f"label{i}") for i in range(1, 3)]
        [op1, op2, op3, op4, op5] = [BaseOperator(task_id=f't{i}', dag=dag) for i in range(1, 6)]

        with pytest.raises(AirflowException):
            chain([op1, op2], [op3, op4, op5])

        with pytest.raises(AirflowException):
            chain([op1, op2, op3], [label1, label2])

        # Begin test for `XComArgs` with `EdgeModifiers`
        [label3, label4] = [Label(label=f"xcomarg_label{i}") for i in range(1, 3)]
        [xop1, xop2, xop3, xop4, xop5] = [
            task_decorator(task_id=f"xcomarg_task{i}", python_callable=lambda: None, dag=dag)()
            for i in range(1, 6)
        ]

        with pytest.raises(AirflowException):
            chain([xop1, xop2], [xop3, xop4, xop5])

        with pytest.raises(AirflowException):
            chain([xop1, xop2, xop3], [label1, label2])

        # Begin test for `TaskGroups`
        [tg1, tg2, tg3, tg4, tg5] = [TaskGroup(group_id=f"tg{i}", dag=dag) for i in range(1, 6)]

        with pytest.raises(AirflowException):
            chain([tg1, tg2], [tg3, tg4, tg5])

    def test_lineage_composition(self):
        """
        Test composition with lineage
        """
        inlet = File(url="in")
        outlet = File(url="out")
        dag = DAG("test-dag", start_date=DEFAULT_DATE)
        task1 = BaseOperator(task_id="op1", dag=dag)
        task2 = BaseOperator(task_id="op2", dag=dag)

        # mock
        task1.supports_lineage = True

        # note: operator precedence still applies
        inlet > task1 | (task2 > outlet)

        assert task1.get_inlet_defs() == [inlet]
        assert task2.get_inlet_defs() == [task1.task_id]
        assert task2.get_outlet_defs() == [outlet]

        fail = ClassWithCustomAttributes()
        with pytest.raises(TypeError):
            fail > task1
        with pytest.raises(TypeError):
            task1 > fail
        with pytest.raises(TypeError):
            fail | task1
        with pytest.raises(TypeError):
            task1 | fail

        task3 = BaseOperator(task_id="op3", dag=dag)
        extra = File(url="extra")
        [inlet, extra] > task3

        assert task3.get_inlet_defs() == [inlet, extra]

        task1.supports_lineage = False
        with pytest.raises(ValueError):
            task1 | task3

        assert task2.supports_lineage is False
        task2 | task3
        assert len(task3.get_inlet_defs()) == 3

        task4 = BaseOperator(task_id="op4", dag=dag)
        task4 > [inlet, outlet, extra]
        assert task4.get_outlet_defs() == [inlet, outlet, extra]

    def test_warnings_are_properly_propagated(self):
        with pytest.warns(DeprecationWarning) as warnings:
            DeprecatedOperator(task_id="test")
            assert len(warnings) == 1
            warning = warnings[0]
            # Here we check that the trace points to the place
            # where the deprecated class was used
            assert warning.filename == __file__

    def test_pre_execute_hook(self):
        hook = mock.MagicMock()

        op = BaseOperator(task_id="test_task", pre_execute=hook)
        op_copy = op.prepare_for_execution()
        op_copy.pre_execute({})
        assert hook.called

    def test_post_execute_hook(self):
        hook = mock.MagicMock()

        op = BaseOperator(task_id="test_task", post_execute=hook)
        op_copy = op.prepare_for_execution()
        op_copy.post_execute({})
        assert hook.called

    def test_task_naive_datetime(self):
        naive_datetime = DEFAULT_DATE.replace(tzinfo=None)

        op_no_dag = BaseOperator(
            task_id='test_task_naive_datetime', start_date=naive_datetime, end_date=naive_datetime
        )

        assert op_no_dag.start_date.tzinfo
        assert op_no_dag.end_date.tzinfo

    def test_setattr_performs_no_custom_action_at_execute_time(self):
        op = MockOperator(task_id="test_task")
        op_copy = op.prepare_for_execution()

        with mock.patch("airflow.models.baseoperator.BaseOperator.set_xcomargs_dependencies") as method_mock:
            op_copy.execute({})
        assert method_mock.call_count == 0

    def test_upstream_is_set_when_template_field_is_xcomarg(self):
        with DAG("xcomargs_test", default_args={"start_date": datetime.today()}):
            op1 = BaseOperator(task_id="op1")
            op2 = MockOperator(task_id="op2", arg1=op1.output)

        assert op1 in op2.upstream_list
        assert op2 in op1.downstream_list

    def test_set_xcomargs_dependencies_works_recursively(self):
        with DAG("xcomargs_test", default_args={"start_date": datetime.today()}):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op3 = MockOperator(task_id="op3", arg1=[op1.output, op2.output])
            op4 = MockOperator(task_id="op4", arg1={"op1": op1.output, "op2": op2.output})

        assert op1 in op3.upstream_list
        assert op2 in op3.upstream_list
        assert op1 in op4.upstream_list
        assert op2 in op4.upstream_list

    def test_set_xcomargs_dependencies_works_when_set_after_init(self):
        with DAG(dag_id='xcomargs_test', default_args={"start_date": datetime.today()}):
            op1 = BaseOperator(task_id="op1")
            op2 = MockOperator(task_id="op2")
            op2.arg1 = op1.output  # value is set after init

        assert op1 in op2.upstream_list

    def test_set_xcomargs_dependencies_error_when_outside_dag(self):
        with pytest.raises(AirflowException):
            op1 = BaseOperator(task_id="op1")
            MockOperator(task_id="op2", arg1=op1.output)

    def test_invalid_trigger_rule(self):
        with pytest.raises(
            AirflowException,
            match=(
                f"The trigger_rule must be one of {TriggerRule.all_triggers()},"
                "'.op1'; received 'some_rule'."
            ),
        ):
            BaseOperator(task_id="op1", trigger_rule="some_rule")

    @pytest.mark.parametrize(("rule"), [("dummy"), (TriggerRule.DUMMY)])
    def test_replace_dummy_trigger_rule(self, rule):
        with pytest.warns(
            DeprecationWarning, match="dummy Trigger Rule is deprecated. Please use `TriggerRule.ALWAYS`."
        ):
            op1 = BaseOperator(task_id="op1", trigger_rule=rule)

            assert op1.trigger_rule == TriggerRule.ALWAYS


def test_init_subclass_args():
    class InitSubclassOp(BaseOperator):
        def __init_subclass__(cls, class_arg=None, **kwargs) -> None:
            cls._class_arg = class_arg
            super().__init_subclass__(**kwargs)

        def execute(self, context):
            self.context_arg = context

    class_arg = "foo"
    context = {"key": "value"}

    class ConcreteSubclassOp(InitSubclassOp, class_arg=class_arg):
        pass

    task = ConcreteSubclassOp(task_id="op1")
    task_copy = task.prepare_for_execution()

    task_copy.execute(context)

    assert task_copy._class_arg == class_arg
    assert task_copy.context_arg == context
