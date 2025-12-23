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

from datetime import timedelta

import pendulum
import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, task_group
from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
    MappedArgument,
)
from airflow.sdk.definitions.taskgroup import MappedTaskGroup


def test_task_group_with_overridden_kwargs():
    @task_group(
        default_args={
            "params": {
                "x": 5,
                "y": 5,
            },
        },
        add_suffix_on_collision=True,
    )
    def simple_tg(): ...

    tg_with_overridden_kwargs = simple_tg.override(
        group_id="custom_group_id",
        default_args={
            "params": {
                "x": 10,
            },
        },
    )

    assert tg_with_overridden_kwargs.tg_kwargs == {
        "group_id": "custom_group_id",
        "default_args": {
            "params": {
                "x": 10,
            },
        },
        "add_suffix_on_collision": True,
    }


def test_tooltip_derived_from_function_docstring():
    """Test that the tooltip for TaskGroup is the decorated-function's docstring."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg():
            """Function docstring."""

        tg()

    _ = pipeline()

    assert _.task_group_dict["tg"].tooltip == "Function docstring."


def test_tooltip_not_overridden_by_function_docstring():
    """
    Test that the tooltip for TaskGroup is the explicitly set value even if the decorated function has a
    docstring.
    """

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group(tooltip="tooltip for the TaskGroup")
        def tg():
            """Function docstring."""

        tg()

    _ = pipeline()

    assert _.task_group_dict["tg"].tooltip == "tooltip for the TaskGroup"


def test_partial_evolves_factory():
    tgp = None

    @task_group()
    def tg(a, b):
        pass

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        nonlocal tgp
        tgp = tg.partial(a=1)

    d = pipeline()

    assert d.task_group_dict == {}  # Calling partial() without expanding does not create a task group.

    assert type(tgp) is type(tg)
    assert tgp.partial_kwargs == {"a": 1}  # Partial kwargs are saved.

    # Warn if the partial object goes out of scope without being mapped.
    with pytest.warns(UserWarning, match=r"Partial task group 'tg' was never mapped!"):
        del tgp


def test_expand_fail_empty():
    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg():
            pass

        tg.expand()

    with pytest.raises(TypeError) as ctx:
        pipeline()
    assert str(ctx.value) == "no arguments to expand against"


def test_expand_create_mapped():
    saved = {}

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg(a, b):
            saved["a"] = a
            saved["b"] = b

        tg.partial(a=1).expand(b=["x", "y"])

    d = pipeline()

    tg = d.task_group_dict["tg"]
    assert isinstance(tg, MappedTaskGroup)
    assert tg._expand_input == DictOfListsExpandInput({"b": ["x", "y"]})

    assert saved == {"a": 1, "b": MappedArgument(input=tg._expand_input, key="b")}


def test_expand_invalid_xcomarg_return_value():
    saved = {}

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def t():
            return {"values": ["value_1", "value_2"]}

        @task_group()
        def tg(a, b):
            saved["a"] = a
            saved["b"] = b

        tg.partial(a=1).expand(b=t()["values"])

    with pytest.raises(
        ValueError,
        match=r"cannot map over XCom with custom key 'values' from <Task\(_PythonDecoratedOperator\): t>",
    ):
        pipeline()


def test_expand_kwargs_no_wildcard():
    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg(**kwargs):
            pass

        tg.expand_kwargs([])

    with pytest.raises(TypeError) as ctx:
        pipeline()

    assert str(ctx.value) == "calling expand_kwargs() on task group function with * or ** is not supported"


def test_expand_kwargs_create_mapped():
    saved = {}

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group()
        def tg(a, b):
            saved["a"] = a
            saved["b"] = b

        tg.partial(a=1).expand_kwargs([{"b": "x"}, {"b": None}])

    d = pipeline()

    tg = d.task_group_dict["tg"]
    assert isinstance(tg, MappedTaskGroup)
    assert tg._expand_input == ListOfDictsExpandInput([{"b": "x"}, {"b": None}])

    assert saved == {"a": 1, "b": MappedArgument(input=tg._expand_input, key="b")}


def test_expand_kwargs_invalid_xcomarg_return_value():
    saved = {}

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def t():
            return {"values": [{"b": 2}, {"b": 3}]}

        @task_group()
        def tg(a, b):
            saved["a"] = a
            saved["b"] = b

        tg.partial(a=1).expand_kwargs(t()["values"])

    with pytest.raises(
        ValueError,
        match=r"cannot map over XCom with custom key 'values' from <Task\(_PythonDecoratedOperator\): t>",
    ):
        pipeline()


def test_override_dag_default_args():
    @dag(
        dag_id="test_dag",
        schedule=None,
        start_date=pendulum.parse("20200101"),
        default_args={
            "retries": 1,
            "owner": "x",
        },
    )
    def pipeline():
        @task_group(
            group_id="task_group",
            default_args={
                "owner": "y",
                "execution_timeout": timedelta(seconds=10),
            },
        )
        def tg():
            EmptyOperator(task_id="task")

        tg()

    test_dag = pipeline()
    test_task = test_dag.task_group_dict["task_group"].children["task_group.task"]
    assert test_task.retries == 1
    assert test_task.owner == "y"
    assert test_task.execution_timeout == timedelta(seconds=10)


def test_override_dag_default_args_nested_tg():
    @dag(
        dag_id="test_dag",
        schedule=None,
        start_date=pendulum.parse("20200101"),
        default_args={
            "retries": 1,
            "owner": "x",
        },
    )
    def pipeline():
        @task_group(
            group_id="task_group",
            default_args={
                "owner": "y",
                "execution_timeout": timedelta(seconds=10),
            },
        )
        def tg():
            @task_group(group_id="nested_task_group")
            def nested_tg():
                @task_group(group_id="another_task_group")
                def another_tg():
                    EmptyOperator(task_id="task")

                another_tg()

            nested_tg()

        tg()

    test_dag = pipeline()
    test_task = (
        test_dag.task_group_dict["task_group"]
        .children["task_group.nested_task_group"]
        .children["task_group.nested_task_group.another_task_group"]
        .children["task_group.nested_task_group.another_task_group.task"]
    )
    assert test_task.retries == 1
    assert test_task.owner == "y"
    assert test_task.execution_timeout == timedelta(seconds=10)


def test_task_group_display_name_used_as_label():
    """Test that the group_display_name for TaskGroup is used as the label for display on the UI."""

    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task_group(group_display_name="my_custom_name")
        def tg():
            pass

        tg()

    p = pipeline()

    assert p.task_group_dict["tg"].label == "my_custom_name"
