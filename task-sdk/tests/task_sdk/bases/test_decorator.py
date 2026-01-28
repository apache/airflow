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

import ast
import functools
import importlib.util
import textwrap
from pathlib import Path

import pytest

from airflow.sdk import task
from airflow.sdk.bases.decorator import DecoratedOperator, is_async_callable

RAW_CODE = """
from airflow.sdk import task

@task.kubernetes(
    namespace="airflow",
    image="python:3.12",
)
def a_task():
##################
    return "success A"
"""


class DummyK8sDecoratedOperator(DecoratedOperator):
    custom_operator_name = "@task.kubernetes"


class TestBaseDecorator:
    def test_get_python_source_strips_decorator_and_comment(self, tmp_path: Path):
        module_path = tmp_path / "tmp_mod.py"
        module_path.write_text(textwrap.dedent(RAW_CODE))

        spec = importlib.util.spec_from_file_location("tmp_mod", module_path)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        a_task_callable = module.a_task

        op = DummyK8sDecoratedOperator(task_id="t", python_callable=a_task_callable)
        cleaned = op.get_python_source()

        # Decorator & comment should be gone
        assert "@task.kubernetes" not in cleaned
        assert "##################" not in cleaned

        # Returned source must be valid Python
        ast.parse(cleaned)
        assert cleaned.lstrip().splitlines()[0].startswith("def a_task")


def simple_decorator(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


def decorator_without_wraps(fn):
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


async def async_fn():
    return 42


def sync_fn():
    return 42


@simple_decorator
async def wrapped_async_fn():
    return 42


@simple_decorator
def wrapped_sync_fn():
    return 42


@decorator_without_wraps
async def wrapped_async_fn_no_wraps():
    return 42


@simple_decorator
@simple_decorator
async def multi_wrapped_async_fn():
    return 42


async def async_with_args(x, y):
    return x + y


def sync_with_args(x, y):
    return x + y


class AsyncCallable:
    async def __call__(self):
        return 42


class SyncCallable:
    def __call__(self):
        return 42


class WrappedAsyncCallable:
    @simple_decorator
    async def __call__(self):
        return 42


class TestAsyncCallable:
    def test_plain_async_function(self):
        assert is_async_callable(async_fn)

    def test_plain_sync_function(self):
        assert not is_async_callable(sync_fn)

    def test_wrapped_async_function_with_wraps(self):
        assert is_async_callable(wrapped_async_fn)

    def test_wrapped_sync_function_with_wraps(self):
        assert not is_async_callable(wrapped_sync_fn)

    def test_wrapped_async_function_without_wraps(self):
        """
        Without functools.wraps, inspect.unwrap cannot recover the coroutine.
        This documents expected behavior.
        """
        assert not is_async_callable(wrapped_async_fn_no_wraps)

    def test_multi_wrapped_async_function(self):
        assert is_async_callable(multi_wrapped_async_fn)

    def test_partial_async_function(self):
        fn = functools.partial(async_with_args, 1)
        assert is_async_callable(fn)

    def test_partial_sync_function(self):
        fn = functools.partial(sync_with_args, 1)
        assert not is_async_callable(fn)

    def test_nested_partial_async_function(self):
        fn = functools.partial(
            functools.partial(async_with_args, 1),
            2,
        )
        assert is_async_callable(fn)

    def test_async_callable_class(self):
        assert is_async_callable(AsyncCallable())

    def test_sync_callable_class(self):
        assert not is_async_callable(SyncCallable())

    def test_wrapped_async_callable_class(self):
        assert is_async_callable(WrappedAsyncCallable())

    def test_partial_callable_class(self):
        fn = functools.partial(AsyncCallable())
        assert is_async_callable(fn)

    @pytest.mark.parametrize("value", [None, 42, "string", object()])
    def test_non_callable(self, value):
        assert not is_async_callable(value)

    def test_task_decorator_async_function(self):
        @task
        async def async_task_fn():
            return 42

        assert is_async_callable(async_task_fn)

    def test_task_decorator_sync_function(self):
        @task
        def sync_task_fn():
            return 42

        assert not is_async_callable(sync_task_fn)
