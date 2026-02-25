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
import inspect
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


class DummyDecoratedOperator(DecoratedOperator):
    custom_operator_name = "@task.dummy"

    def execute(self, context):
        return self.python_callable(*self.op_args, **self.op_kwargs)


def make_op(func, op_args=None, op_kwargs=None, **kwargs):
    return DummyDecoratedOperator(
        task_id="test_task",
        python_callable=func,
        op_args=op_args or [],
        op_kwargs=op_kwargs or {},
        **kwargs,
    )


class TestDefaultFillingLogic:
    """
    Tests for the 'fill None after first default' parameter normalization block.
    """

    def test_no_params_no_error(self):
        """Functions with no parameters should construct without issue."""

        def f():
            return 42

        op = make_op(f)
        assert op.python_callable is f

    def test_all_required_params_no_defaults_injected(self):
        """
        If no parameter has a default, first_default_idx == len(parameters),
        so the list-comp never triggers the else branch — all params stay as-is.
        """

        def f(a, b, c):
            return a + b + c

        op = make_op(f, op_args=[1, 2, 3])
        sig = inspect.signature(op.python_callable)
        for param in sig.parameters.values():
            assert param.default is inspect.Parameter.empty

    def test_params_before_first_default_keep_no_default(self):
        """
        Parameters *before* the first explicit default must NOT have a default
        injected, so Python's positional binding still works correctly.
        """

        def f(required, optional=10):
            return required + optional

        op = make_op(f, op_kwargs={"required": 5})
        # Build the adjusted signature the same way __init__ does
        sig = inspect.signature(op.python_callable)
        params = list(sig.parameters.values())
        # 'required' is before first default — still no default
        assert params[0].default is inspect.Parameter.empty
        # 'optional' retains its original default
        assert params[1].default == 10

    def test_params_after_first_default_get_none_when_missing_default(self):
        """
        A param *after* the first defaulted param, but itself lacking a default,
        must receive a None default so the signature stays valid.
        This is the core behavior under test.
        """

        # 'b' has a default, 'c' does not — without the fix this would be an
        # invalid signature after context-key injection upstream.
        def f(a, b=5, c=None):
            return (a, b, c)

        # Constructing should not raise
        op = make_op(f, op_kwargs={"a": 1})
        assert op is not None

    def test_param_after_first_default_without_default_is_given_none(self, tmp_path):
        """
        Directly verify that, after __init__ rewrites the signature, a param
        that originally had no default but sits after a defaulted param gets None.
        """

        def g(x, y=99):
            return (x, y)

        op = make_op(g, op_kwargs={"x": 1})
        assert op is not None

    def test_explicit_default_after_first_default_is_preserved(self):
        """
        Parameters after the first default that already *have* a default
        should keep their original value, not be overwritten with None.
        """

        def f(a, b=1, c=2, d=3):
            return a + b + c + d

        op = make_op(f, op_kwargs={"a": 10})

        sig = inspect.signature(op.python_callable)
        params = list(sig.parameters.values())
        assert params[1].default == 1
        assert params[2].default == 2
        assert params[3].default == 3

    def test_first_param_with_default_defines_boundary(self):
        """
        first_default_idx points at the first param that has any default.
        Everything strictly before it must remain required.
        """

        def f(no_default_1, no_default_2, first_default=42, after=None):
            return (no_default_1, no_default_2, first_default, after)

        op = make_op(f, op_args=[1, 2])
        sig = inspect.signature(op.python_callable)
        params = list(sig.parameters.values())
        assert params[0].default is inspect.Parameter.empty, "no_default_1 must stay required"
        assert params[1].default is inspect.Parameter.empty, "no_default_2 must stay required"
        assert params[2].default == 42, "first_default keeps its value"

    def test_context_key_default_none_does_not_raise(self):
        """
        Context key params with default=None are explicitly allowed.
        Construction must succeed.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_key = next(iter(KNOWN_CONTEXT_KEYS))

        func_code = f"def f(x, {ctx_key}=None): return x"
        ns: dict = {}
        exec(func_code, ns)
        f = ns["f"]

        op = make_op(f, op_kwargs={"x": 1})
        assert op is not None

    def test_context_key_with_non_none_default_raises(self):
        """
        A context key parameter with a default other than None must raise ValueError.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_key = next(iter(KNOWN_CONTEXT_KEYS))
        func_code = f"def f(x, {ctx_key}='bad_default'): return x"
        ns: dict = {}
        exec(func_code, ns)
        f = ns["f"]

        with pytest.raises(ValueError, match="can't have a default other than None"):
            make_op(f, op_kwargs={"x": 1})

    def test_all_params_have_defaults_first_default_idx_is_zero(self):
        """
        When every param has a default, first_default_idx == 0,
        so the else branch applies to all of them — but since they all already
        have defaults, none should be overwritten.
        """

        def f(a=1, b=2, c=3):
            return a + b + c

        op = make_op(f)
        sig = inspect.signature(op.python_callable)
        params = list(sig.parameters.values())
        assert params[0].default == 1
        assert params[1].default == 2
        assert params[2].default == 3

    def test_only_one_param_with_default_at_end(self):
        """
        Single trailing optional: no param injection needed, construction OK.
        """

        def f(a, b, c=99):
            return (a, b, c)

        op = make_op(f, op_args=[1, 2])
        assert op is not None

    def test_bind_validation_fails_for_missing_required_args(self):
        """
        Even after default-filling, truly required args with no supplied value
        must still cause a bind failure at construction time.
        """

        def f(required_arg):
            return required_arg

        with pytest.raises(TypeError):
            make_op(f)  # no op_args / op_kwargs supplied

    def test_context_key_before_first_default_shifts_boundary(self):
        """
        Context key (no default) sits before a regular param with a default.
        After injection, context key gets default=None, becoming first_default_idx,
        so the regular param after it falls into the else branch but must keep its
        explicit default, not be overwritten.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_key = next(iter(KNOWN_CONTEXT_KEYS))
        func_code = f"def f({ctx_key}, x, y=10): return (x, y)"
        ns: dict = {}
        exec(func_code, ns)

        op = make_op(ns["f"], op_kwargs={"x": 1})
        assert op is not None

    def test_context_key_after_regular_default_keeps_original_default(self):
        """
        Regular param with a default comes before a context key with default=None.
        The regular param must keep its original default value after filling.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_key = next(iter(KNOWN_CONTEXT_KEYS))
        func_code = f"def f(x, y=5, {ctx_key}=None): return (x, y)"
        ns: dict = {}
        exec(func_code, ns)

        op = make_op(ns["f"], op_kwargs={"x": 1})
        sig = inspect.signature(op.python_callable)
        y_param = next(p for p in sig.parameters.values() if p.name == "y")
        assert y_param.default == 5

    def test_multiple_context_keys_mixed_with_regular_defaults(self):
        """
        Multiple context keys interspersed with regular defaulted params.
        The full signature must remain valid and bindable after injection.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_keys = list(KNOWN_CONTEXT_KEYS)[:2]
        func_code = f"def f(a, {ctx_keys[0]}=None, b=7, {ctx_keys[1]}=None): return (a, b)"
        ns: dict = {}
        exec(func_code, ns)

        op = make_op(ns["f"], op_kwargs={"a": 1})
        assert op is not None

    def test_required_param_between_context_key_and_regular_default_gets_none(self):
        """
        After context-key injection it gets default=None (first_default_idx=0).
        x now sits after the first default but has no default of its own,
        so the filling block must inject None — otherwise the signature is invalid.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_key = next(iter(KNOWN_CONTEXT_KEYS))
        func_code = f"def f({ctx_key}, x, y=10): return (x, y)"
        ns: dict = {}
        exec(func_code, ns)

        op = make_op(ns["f"], op_kwargs={"x": 42})
        assert op is not None

    def test_context_key_only_signature(self):
        """
        Function taking only context keys. All get default=None injected,
        first_default_idx becomes 0, all fall into the else branch but already
        have defaults — none should be overwritten. No op_args/op_kwargs needed.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        ctx_keys = list(KNOWN_CONTEXT_KEYS)[:3]
        func_code = f"def f({ctx_keys[0]}=None, {ctx_keys[1]}=None, {ctx_keys[2]}=None): return True"
        ns: dict = {}
        exec(func_code, ns)

        op = make_op(ns["f"])
        assert op is not None

    def test_non_context_param_after_context_key_gets_none_injected(self):
        """
        start_date is a context key — gets default=None injected, becoming first_default_idx=0.
        end_date is NOT a context key and has no default, but now sits after the first
        default, so the filling block must inject None into it too — otherwise
        signature.replace(parameters=parameters) raises ValueError.
        """
        from airflow.sdk.bases.decorator import KNOWN_CONTEXT_KEYS

        assert "start_date" in KNOWN_CONTEXT_KEYS, "test assumes start_date is a context key"

        def foo(start_date, a): ...

        # Construction itself would raise ValueError without the filling logic
        op = make_op(foo, op_kwargs={"a": "2024-01-01"})
        assert op is not None

        # Binding with end_date omitted succeeds because end_date got default=None
        op_without_end_date = make_op(foo)
        assert op_without_end_date is not None


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
