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
from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS, DecoratedOperator, is_async_callable

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


class DummyDecoratedOperator(DecoratedOperator):
    custom_operator_name = "@task.dummy"

    def execute(self, context):
        return self.python_callable(*self.op_args, **self.op_kwargs)


class TestDefaultFillingLogic:
    @pytest.mark.parametrize(
        ("func", "kwargs", "args"),
        [
            pytest.param(
                lambda: 42,
                {},
                [],
                id="no_params",
            ),
            pytest.param(
                lambda x, y=99: (x, y),
                {"x": 1},
                [],
                id="param_after_first_default_is_given_none",
            ),
            pytest.param(
                lambda a, b=5, c=None: (a, b, c),
                {"a": 1},
                [],
                id="all_params_after_first_default_already_have_defaults",
            ),
            pytest.param(
                lambda a, b, c=99: (a, b, c),
                {},
                [1, 2],
                id="single_trailing_optional",
            ),
        ],
    )
    def test_construction_succeeds(self, func, kwargs, args):
        op = make_op(func, op_kwargs=kwargs, op_args=args)
        assert op is not None

    def test_construction_succeeds_with_context_key_params(self):
        def foo(ds, my_data):
            return my_data

        assert make_op(foo, op_args=[None, None]) is not None

    def test_context_key_default_none_does_not_raise(self):
        ctx_key = next(iter(sorted(KNOWN_CONTEXT_KEYS)))
        f = _make_func(f"def dummy_task(x, {ctx_key}=None): return x")
        assert make_op(f, op_kwargs={"x": 1}) is not None

    def test_context_key_with_non_none_default_raises(self):
        ctx_key = next(iter(sorted(KNOWN_CONTEXT_KEYS)))
        f = _make_func(f"def dummy_task(x, {ctx_key}='bad_default'): return x")
        with pytest.raises(ValueError, match="can't have a default other than None"):
            make_op(f, op_kwargs={"x": 1})

    @pytest.mark.parametrize(
        ("func_src", "op_kwargs"),
        [
            pytest.param(
                "def dummy_task({ctx0}, x, y=10): return (x, y)",
                {"x": 1},
                id="context_key_before_first_default_shifts_boundary",
            ),
            pytest.param(
                "def dummy_task(x, y=5, {ctx0}=None): return (x, y)",
                {"x": 1},
                id="context_key_after_regular_default",
            ),
            pytest.param(
                "def dummy_task(a, {ctx0}=None, b=7, {ctx1}=None): return (a, b)",
                {"a": 1},
                id="multiple_context_keys_mixed_with_regular_defaults",
            ),
            pytest.param(
                "def dummy_task({ctx0}, x, y=10): return (x, y)",
                {"x": 42},
                id="required_param_between_context_key_and_regular_default_gets_none",
            ),
            pytest.param(
                "def dummy_task({ctx0}=None, {ctx1}=None, {ctx2}=None): return True",
                {},
                id="context_key_only_signature",
            ),
        ],
    )
    def test_context_key_construction_succeeds(self, func_src, op_kwargs):
        """All context-key signature shapes must construct without raising."""

        ctx_keys = sorted(KNOWN_CONTEXT_KEYS)
        src = func_src.format(
            ctx0=ctx_keys[0],
            ctx1=ctx_keys[1] if len(ctx_keys) > 1 else ctx_keys[0],
            ctx2=ctx_keys[2] if len(ctx_keys) > 2 else ctx_keys[0],
        )
        op = make_op(_make_func(src), op_kwargs=op_kwargs)
        assert op is not None

    def test_non_context_param_after_context_key_gets_none_injected(self):
        ctx_key = next(iter(sorted(KNOWN_CONTEXT_KEYS)))
        f = _make_func(f"def dummy_task({ctx_key}, a): ...")
        assert make_op(f, op_kwargs={"a": "2024-01-01"}) is not None

    def test_non_context_param_after_context_key_without_value_raises(self):
        ctx_key = next(iter(sorted(KNOWN_CONTEXT_KEYS)))
        f = _make_func(f"def dummy_task({ctx_key}, a): ...")
        with pytest.raises(TypeError, match="missing required argument"):
            make_op(f)

    def test_bind_validation_fails_for_missing_required_args(self):
        """Truly required args with no supplied value must still cause a bind failure."""

        def dummy_task(required_arg):
            return required_arg

        with pytest.raises(TypeError):
            make_op(dummy_task)

    def test_variadic_and_keyword_only_params_are_not_assigned_defaults(self):
        """Construction succeeds when variadic and keyword-only params are present."""

        def dummy_task(a, b=1, *args, kw_required, **kwargs):
            return a, b, args, kw_required, kwargs

        assert make_op(dummy_task, op_kwargs={"a": 1, "kw_required": "x"}) is not None

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            pytest.param(
                {"task_id": "fetch_{}".format},
                "Expected 'task_id' to be .*, got builtin_function_or_method",
                id="task_id_bound_method",
            ),
            pytest.param(
                {"retries": "three"},
                "Expected 'retries' to be .*, got str",
                id="retries_string",
            ),
            pytest.param(
                {"queue": 42},
                "Expected 'queue' to be .*, got int",
                id="queue_int",
            ),
            pytest.param(
                {"priority_weight": 1.5},
                "Expected 'priority_weight' to be .*, got float",
                id="priority_weight_float",
            ),
        ],
    )
    def test_wrong_arg_type_raises_type_error_at_decoration_time(self, kwargs, match):
        """Non-matching types for operator kwargs raise TypeError at decoration time."""
        with pytest.raises(TypeError, match=match):

            @task(**kwargs)
            def my_task():
                return 1


def make_op(func, op_args=None, op_kwargs=None, **kwargs):
    return DummyDecoratedOperator(
        task_id="test_task",
        python_callable=func,
        op_args=op_args or [],
        op_kwargs=op_kwargs or {},
        **kwargs,
    )


def _make_func(src: str):
    ns: dict = {}
    exec(src, ns)
    return next(v for v in ns.values() if callable(v))


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
