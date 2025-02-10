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

import json
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable
from unittest import mock

import pendulum
import pytest

from airflow.sdk.api.datamodels._generated import TerminalTIState
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.xcom_arg import XComArg
from airflow.sdk.execution_time.comms import GetXCom, SetXCom, XComResult
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
    ("callable", "expected"),
    [
        pytest.param(
            lambda partial, output1: partial.expand(
                map_template=output1, map_static=output1, file_template=["/path/to/file.ext"]
            ),
            # Note to the next person to come across this. In #32272 we changed expand_kwargs so that it
            # resolves the mapped template when it's in `expand_kwargs()`, but we _didn't_ do the same for
            # things in `expand()`. This feels like a bug to me (ashb) but I am not changing that now, I have
            # just moved and parametrized this test.
            "{{ ds }}",
            id="expand",
        ),
        pytest.param(
            lambda partial, output1: partial.expand_kwargs(
                [{"map_template": "{{ ds }}", "map_static": "{{ ds }}", "file_template": "/path/to/file.ext"}]
            ),
            "2024-12-01",
            id="expand_kwargs",
        ),
    ],
)
def test_mapped_render_template_fields_validating_operator(
    tmp_path, create_runtime_ti, mock_supervisor_comms, callable, expected: bool
):
    file_template_dir = tmp_path / "path" / "to"
    file_template_dir.mkdir(parents=True, exist_ok=True)
    file_template = file_template_dir / "file.ext"
    file_template.write_text("loaded data")

    class MyOperator(BaseOperator):
        template_fields = ("partial_template", "map_template", "file_template")
        template_ext = (".ext",)

        def __init__(
            self, partial_template, partial_static, map_template, map_static, file_template, **kwargs
        ):
            for value in [partial_template, partial_static, map_template, map_static, file_template]:
                assert isinstance(value, str), "value should have been resolved before unmapping"
                super().__init__(**kwargs)
                self.partial_template = partial_template
            self.partial_static = partial_static
            self.map_template = map_template
            self.map_static = map_static
            self.file_template = file_template

        def execute(self, context):
            pass

    with DAG("test_dag", template_searchpath=tmp_path.__fspath__()):
        task1 = BaseOperator(task_id="op1")
        mapped = MyOperator.partial(
            task_id="a", partial_template="{{ ti.task_id }}", partial_static="{{ ti.task_id }}"
        )
        mapped = callable(mapped, task1.output)

    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["{{ ds }}"]')

    mapped_ti = create_runtime_ti(task=mapped, map_index=0, upstream_map_indexes={task1.task_id: 1})

    assert isinstance(mapped_ti.task, MappedOperator)
    mapped_ti.task.render_template_fields(context=mapped_ti.get_template_context())
    assert isinstance(mapped_ti.task, MyOperator)

    assert mapped_ti.task.partial_template == "a", "Should be templated!"
    assert mapped_ti.task.partial_static == "{{ ti.task_id }}", "Should not be templated!"
    assert mapped_ti.task.map_template == expected
    assert mapped_ti.task.map_static == "{{ ds }}", "Should not be templated!"
    assert mapped_ti.task.file_template == "loaded data", "Should be templated!"


def test_mapped_render_nested_template_fields(create_runtime_ti, mock_supervisor_comms):
    with DAG("test_dag"):
        mapped = MockOperatorWithNestedFields.partial(
            task_id="t", arg2=NestedFields(field_1="{{ ti.task_id }}", field_2="value_2")
        ).expand(arg1=["{{ ti.task_id }}", ["s", "{{ ti.task_id }}"]])

    ti = create_runtime_ti(task=mapped, map_index=0, upstream_map_indexes={})
    ti.task.render_template_fields(context=ti.get_template_context())
    assert ti.task.arg1 == "t"
    assert ti.task.arg2.field_1 == "t"
    assert ti.task.arg2.field_2 == "value_2"

    ti = create_runtime_ti(task=mapped, map_index=1, upstream_map_indexes={})
    ti.task.render_template_fields(context=ti.get_template_context())
    assert ti.task.arg1 == ["s", "t"]
    assert ti.task.arg2.field_1 == "t"
    assert ti.task.arg2.field_2 == "value_2"


@pytest.mark.parametrize(
    ("map_index", "expected"),
    [
        pytest.param(0, "2024-12-01", id="0"),
        pytest.param(1, 2, id="1"),
    ],
)
def test_expand_kwargs_render_template_fields_validating_operator(
    map_index, expected, create_runtime_ti, mock_supervisor_comms
):
    with DAG("test_dag"):
        task1 = BaseOperator(task_id="op1")
        mapped = MockOperator.partial(task_id="a", arg2="{{ ti.task_id }}").expand_kwargs(task1.output)

    mock_supervisor_comms.get_message.return_value = XComResult(
        key="return_value", value=json.dumps([{"arg1": "{{ ds }}"}, {"arg1": 2}])
    )

    ti = create_runtime_ti(task=mapped, map_index=map_index, upstream_map_indexes={})
    assert isinstance(ti.task, MappedOperator)
    ti.task.render_template_fields(context=ti.get_template_context())
    assert isinstance(ti.task, MockOperator)
    assert ti.task.arg1 == expected
    assert ti.task.arg2 == "a"


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


def test_nested_mapped_task_groups():
    from airflow.decorators import task, task_group

    with DAG("test"):

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

        # Add a test once nested mapped task groups become supported
        with pytest.raises(ValueError, match="Nested Mapped TaskGroups are not yet supported"):
            g1.expand(x=t())


RunTI = Callable[[DAG, str, int], TerminalTIState]


def test_map_cross_product(run_ti: RunTI, mock_supervisor_comms):
    outputs = []

    with DAG(dag_id="cross_product") as dag:

        @dag.task
        def emit_numbers():
            return [1, 2]

        @dag.task
        def emit_letters():
            return {"a": "x", "b": "y", "c": "z"}

        @dag.task
        def show(number, letter):
            outputs.append((number, letter))

        show.expand(number=emit_numbers(), letter=emit_letters())

    def xcom_get():
        # TODO: Tidy this after #45927 is reopened and fixed properly
        last_request = mock_supervisor_comms.send_request.mock_calls[-1].kwargs["msg"]
        if not isinstance(last_request, GetXCom):
            return mock.DEFAULT
        task = dag.get_task(last_request.task_id)
        value = json.dumps(task.python_callable())
        return XComResult(key="return_value", value=value)

    mock_supervisor_comms.get_message.side_effect = xcom_get

    states = [run_ti(dag, "show", map_index) for map_index in range(6)]
    assert states == [TerminalTIState.SUCCESS] * 6
    assert outputs == [
        (1, ("a", "x")),
        (1, ("b", "y")),
        (1, ("c", "z")),
        (2, ("a", "x")),
        (2, ("b", "y")),
        (2, ("c", "z")),
    ]


def test_map_product_same(run_ti: RunTI, mock_supervisor_comms):
    """Test a mapped task can refer to the same source multiple times."""
    outputs = []

    with DAG(dag_id="product_same") as dag:

        @dag.task
        def emit_numbers():
            return [1, 2]

        @dag.task
        def show(a, b):
            outputs.append((a, b))

        emit_task = emit_numbers()
        show.expand(a=emit_task, b=emit_task)

    def xcom_get():
        # TODO: Tidy this after #45927 is reopened and fixed properly
        last_request = mock_supervisor_comms.send_request.mock_calls[-1].kwargs["msg"]
        if not isinstance(last_request, GetXCom):
            return mock.DEFAULT
        task = dag.get_task(last_request.task_id)
        value = json.dumps(task.python_callable())
        return XComResult(key="return_value", value=value)

    mock_supervisor_comms.get_message.side_effect = xcom_get

    states = [run_ti(dag, "show", map_index) for map_index in range(4)]
    assert states == [TerminalTIState.SUCCESS] * 4
    assert outputs == [(1, 1), (1, 2), (2, 1), (2, 2)]


class NestedFields:
    """Nested fields for testing purposes."""

    def __init__(self, field_1, field_2):
        self.field_1 = field_1
        self.field_2 = field_2


class MockOperatorWithNestedFields(BaseOperator):
    """Operator with nested fields for testing purposes."""

    template_fields = ("arg1", "arg2")

    def __init__(self, arg1: str = "", arg2: NestedFields | None = None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2

    def _render_nested_template_fields(self, content, context, jinja_env, seen_oids) -> None:
        if id(content) not in seen_oids:
            template_fields: tuple | None = None

            if isinstance(content, NestedFields):
                template_fields = ("field_1", "field_2")

            if template_fields:
                seen_oids.add(id(content))
                self._do_render_template_fields(content, template_fields, context, jinja_env, seen_oids)
                return

        super()._render_nested_template_fields(content, context, jinja_env, seen_oids)


def test_find_mapped_dependants_in_another_group():
    from airflow.decorators import task as task_decorator
    from airflow.sdk import TaskGroup

    @task_decorator
    def gen(x):
        return list(range(x))

    @task_decorator
    def add(x, y):
        return x + y

    with DAG(dag_id="test"):
        with TaskGroup(group_id="g1"):
            gen_result = gen(3)
        with TaskGroup(group_id="g2"):
            add_result = add.partial(y=1).expand(x=gen_result)

    dependants = list(gen_result.operator.iter_mapped_dependants())
    assert dependants == [add_result.operator]


@pytest.mark.parametrize(
    "partial_params, mapped_params, expected",
    [
        pytest.param(None, [{"a": 1}], [{"a": 1}], id="simple"),
        pytest.param({"b": 2}, [{"a": 1}], [{"a": 1, "b": 2}], id="merge"),
        pytest.param({"b": 2}, [{"a": 1, "b": 3}], [{"a": 1, "b": 3}], id="override"),
        pytest.param({"b": 2}, [{"a": 1, "b": 3}, {"b": 1}], [{"a": 1, "b": 3}, {"b": 1}], id="multiple"),
    ],
)
def test_mapped_expand_against_params(create_runtime_ti, partial_params, mapped_params, expected):
    with DAG("test"):
        task = BaseOperator.partial(task_id="t", params=partial_params).expand(params=mapped_params)

    for map_index, expansion in enumerate(expected):
        mapped_ti = create_runtime_ti(task=task, map_index=map_index)
        mapped_ti.task.render_template_fields(context=mapped_ti.get_template_context())
        assert mapped_ti.task.params == expansion


def test_operator_mapped_task_group_receives_value(create_runtime_ti, mock_supervisor_comms):
    # Test the runtime expansion behaviour of mapped task groups + mapped operators
    results = {}

    from airflow.decorators import task_group

    with DAG("test") as dag:

        @dag.task
        def t(value, *, ti=None):
            results[(ti.task_id, ti.map_index)] = value
            return value

        @task_group
        def tg(va):
            # Each expanded group has one t1 and t2 each.
            t1 = t.override(task_id="t1")(va)
            t2 = t.override(task_id="t2")(t1)

            with pytest.raises(NotImplementedError) as ctx:
                t.override(task_id="t4").expand(value=va)
            assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"

            return t2

        # The group is mapped by 3.
        t2 = tg.expand(va=[["a", "b"], [4], ["z"]])

        # Aggregates results from task group.
        t.override(task_id="t3")(t2)

    def xcom_get():
        # TODO: Tidy this after #45927 is reopened and fixed properly
        last_request = mock_supervisor_comms.send_request.mock_calls[-1].kwargs["msg"]
        if not isinstance(last_request, GetXCom):
            return mock.DEFAULT
        key = (last_request.task_id, last_request.map_index)
        if key in expected_values:
            value = expected_values[key]
            return XComResult(key="return_value", value=json.dumps(value))
        elif last_request.map_index is None:
            # Get all mapped XComValues for this ti
            value = [v for k, v in expected_values.items() if k[0] == last_request.task_id]
            return XComResult(key="return_value", value=json.dumps(value))
        return mock.DEFAULT

    mock_supervisor_comms.get_message.side_effect = xcom_get

    expected_values = {
        ("tg.t1", 0): ["a", "b"],
        ("tg.t1", 1): [4],
        ("tg.t1", 2): ["z"],
        ("tg.t2", 0): ["a", "b"],
        ("tg.t2", 1): [4],
        ("tg.t2", 2): ["z"],
        ("t3", None): [["a", "b"], [4], ["z"]],
    }

    # We hard-code the number of expansions here as the server is in charge of that.
    expansion_per_task_id = {
        "tg.t1": range(3),
        "tg.t2": range(3),
        "t3": [None],
    }
    for task in dag.tasks:
        for map_index in expansion_per_task_id[task.task_id]:
            mapped_ti = create_runtime_ti(task=task.prepare_for_execution(), map_index=map_index)
            context = mapped_ti.get_template_context()
            mapped_ti.task.render_template_fields(context)
            mapped_ti.task.execute(context)
    assert results == expected_values


@pytest.mark.xfail(reason="SkipMixin hasn't been ported over to use the Task Execution API yet")
def test_mapped_xcom_push_skipped_tasks(create_runtime_ti, mock_supervisor_comms):
    from airflow.decorators import task_group
    from airflow.operators.empty import EmptyOperator

    if TYPE_CHECKING:
        from airflow.providers.standard.operators.python import ShortCircuitOperator
    else:
        ShortCircuitOperator = pytest.importorskip(
            "airflow.providers.standard.operators.python"
        ).ShortCircuitOperator

    with DAG("test") as dag:

        @task_group
        def group(x):
            short_op_push_xcom = ShortCircuitOperator(
                task_id="push_xcom_from_shortcircuit",
                python_callable=lambda arg: arg % 2 == 0,
                op_kwargs={"arg": x},
            )
            empty_task = EmptyOperator(task_id="empty_task")
            short_op_push_xcom >> empty_task

        group.expand(x=[0, 1])

    for task in dag.tasks:
        for map_index in range(2):
            ti = create_runtime_ti(task=task.prepare_for_execution(), map_index=map_index)
            context = ti.get_template_context()
            ti.task.render_template_fields(context)
            ti.task.execute(context)

    assert ti
    # TODO: these tests might not be right
    mock_supervisor_comms.send_request.assert_has_calls(
        [
            SetXCom(
                key="skipmixin_key",
                value=None,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id="group.push_xcom_from_shortcircuit",
                map_index=0,
            ),
            SetXCom(
                key="return_value",
                value=True,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id="group.push_xcom_from_shortcircuit",
                map_index=0,
            ),
            SetXCom(
                key="skipmixin_key",
                value={"skipped": ["group.empty_task"]},
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id="group.push_xcom_from_shortcircuit",
                map_index=1,
            ),
        ]
    )
    #
    # assert (
    #     tis[0].xcom_pull(task_ids="group.push_xcom_from_shortcircuit", key="return_value", map_indexes=0)
    #     is True
    # )
    # assert (
    #     tis[0].xcom_pull(task_ids="group.push_xcom_from_shortcircuit", key="skipmixin_key", map_indexes=0)
    #     is None
    # )
    # assert tis[0].xcom_pull(
    #     task_ids="group.push_xcom_from_shortcircuit", key="skipmixin_key", map_indexes=1
    # ) == {"skipped": ["group.empty_task"]}
