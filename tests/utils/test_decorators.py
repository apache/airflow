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

from typing import TYPE_CHECKING

import pytest

from airflow.decorators import task

if TYPE_CHECKING:
    from airflow.decorators.base import Task, TaskDecorator

_CONDITION_DECORATORS = frozenset({"skip_if", "run_if"})
_NO_SOURCE_DECORATORS = frozenset({"sensor"})
DECORATORS = sorted(
    set(x for x in dir(task) if not x.startswith("_"))
    - _CONDITION_DECORATORS
    - _NO_SOURCE_DECORATORS
)
DECORATORS_USING_SOURCE = (
    "external_python",
    "virtualenv",
    "branch_virtualenv",
    "branch_external_python",
)


@pytest.fixture
def decorator(request: pytest.FixtureRequest) -> TaskDecorator:
    decorator_factory = getattr(task, request.param)

    kwargs = {}
    if "external" in request.param:
        kwargs["python"] = "python3"
    return decorator_factory(**kwargs)


@pytest.mark.parametrize("decorator", DECORATORS_USING_SOURCE, indirect=["decorator"])
def test_task_decorator_using_source(decorator: TaskDecorator):
    @decorator
    def f():
        return ["some_task"]

    assert parse_python_source(f, "decorator") == 'def f():\n    return ["some_task"]\n'


@pytest.mark.parametrize("decorator", DECORATORS, indirect=["decorator"])
def test_skip_if(decorator: TaskDecorator):
    @task.skip_if(lambda context: True)
    @decorator
    def f():
        return "hello world"

    assert parse_python_source(f, "decorator") == 'def f():\n    return "hello world"\n'


@pytest.mark.parametrize("decorator", DECORATORS, indirect=["decorator"])
def test_run_if(decorator: TaskDecorator):
    @task.run_if(lambda context: True)
    @decorator
    def f():
        return "hello world"

    assert parse_python_source(f, "decorator") == 'def f():\n    return "hello world"\n'


def test_skip_if_and_run_if():
    @task.skip_if(lambda context: True)
    @task.run_if(lambda context: True)
    @task.virtualenv()
    def f():
        return "hello world"

    assert parse_python_source(f) == 'def f():\n    return "hello world"\n'


def test_run_if_and_skip_if():
    @task.run_if(lambda context: True)
    @task.skip_if(lambda context: True)
    @task.virtualenv()
    def f():
        return "hello world"

    assert parse_python_source(f) == 'def f():\n    return "hello world"\n'


def test_skip_if_allow_decorator():
    def non_task_decorator(func):
        return func

    @task.skip_if(lambda context: True)
    @task.virtualenv()
    @non_task_decorator
    def f():
        return "hello world"

    assert (
        parse_python_source(f)
        == '@non_task_decorator\ndef f():\n    return "hello world"\n'
    )


def test_run_if_allow_decorator():
    def non_task_decorator(func):
        return func

    @task.run_if(lambda context: True)
    @task.virtualenv()
    @non_task_decorator
    def f():
        return "hello world"

    assert (
        parse_python_source(f)
        == '@non_task_decorator\ndef f():\n    return "hello world"\n'
    )


def parse_python_source(task: Task, custom_operator_name: str | None = None) -> str:
    operator = task().operator
    if custom_operator_name:
        custom_operator_name = (
            custom_operator_name
            if custom_operator_name.startswith("@")
            else f"@{custom_operator_name}"
        )
        operator.__dict__["custom_operator_name"] = custom_operator_name
    return operator.get_python_source()
