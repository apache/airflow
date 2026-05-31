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

import functools
from typing import Annotated, Any

import pytest

from airflow.providers.common.ai.hooks.base import ToolSpec
from airflow.providers.common.ai.utils.function_schema import (
    _EMPTY_OBJECT_SCHEMA,
    build_function_json_schema,
    callable_to_tool_spec,
    extract_function_description,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _plain(x: int, y: str = "hi") -> str:
    """Do something useful.

    Args:
        x: a number.
        y: a string.

    Returns:
        A string result.
    """


def _no_doc(x: int) -> str:
    pass


def _no_params() -> None:
    """No parameters at all."""


def _annotated(q: Annotated[str, "The search query"], limit: Annotated[int, "Max results"] = 10) -> list:
    """Search."""


class _CallableObj:
    """Callable object used to test non-function callables."""

    def __call__(self, value: str) -> str:
        """Process value."""


# ---------------------------------------------------------------------------
# extract_function_description
# ---------------------------------------------------------------------------


class TestExtractFunctionDescription:
    def test_returns_first_paragraph(self):
        assert extract_function_description(_plain) == "Do something useful."

    def test_no_docstring_falls_back_to_name(self):
        assert extract_function_description(_no_doc) == "_no_doc"

    def test_empty_docstring_falls_back_to_name(self):
        def fn():
            """"""

        assert extract_function_description(fn) == "fn"

    def test_lambda_falls_back_to_lambda_name(self):
        f = lambda x: x
        assert extract_function_description(f) == "<lambda>"

    def test_callable_object_uses_call_docstring(self):
        obj = _CallableObj()
        # prefers __call__ docstring over class docstring over class name
        assert extract_function_description(obj) == "Process value."

    def test_callable_object_falls_back_to_class_docstring(self):
        class _NoCallDoc:
            """Describes the class."""

            def __call__(self, x: int) -> int: ...

        assert extract_function_description(_NoCallDoc()) == "Describes the class."

    def test_callable_object_falls_back_to_class_name(self):
        class _NoDocs:
            def __call__(self, x: int) -> int: ...

        assert extract_function_description(_NoDocs()) == "_NoDocs"

    @pytest.mark.parametrize(
        "header",
        [
            "Args:",
            "Arguments:",
            "Parameters:",
            "Params:",
            "Returns:",
            "Return:",
            "Yields:",
            "Yield:",
            "Raises:",
            "Raise:",
            "Except:",
            "Exceptions:",
            "Example:",
            "Examples:",
            "Note:",
            "Notes:",
            "See also:",
            "References:",
        ],
    )
    def test_stops_before_section_headers(self, header):
        def fn():
            pass

        fn.__doc__ = f"First paragraph.\n\n{header}\n    detail"
        assert extract_function_description(fn) == "First paragraph."

    def test_section_header_case_insensitive(self):
        def fn():
            """Summary line.

            ARGS:
                x: something.
            """

        assert extract_function_description(fn) == "Summary line."

    def test_multiline_first_paragraph_preserved(self):
        def fn():
            """Line one.
            Line two.

            Args:
                x: something.
            """

        desc = extract_function_description(fn)
        assert "Line one." in desc
        assert "Line two." in desc
        assert "Args" not in desc

    def test_partial_uses_underlying_function_docstring(self):
        p = functools.partial(_plain, 1)
        assert extract_function_description(p) == "Do something useful."


# ---------------------------------------------------------------------------
# build_function_json_schema
# ---------------------------------------------------------------------------


class TestBuildFunctionJsonSchema:
    def test_no_params_returns_empty_schema(self):
        assert build_function_json_schema(_no_params) == _EMPTY_OBJECT_SCHEMA

    def test_required_and_optional_params(self):
        schema = build_function_json_schema(_plain)
        props = schema["properties"]
        assert "x" in props
        assert "y" in props
        assert schema["required"] == ["x"]
        assert props["y"]["default"] == "hi"

    def test_int_type(self):
        def fn(n: int): ...

        schema = build_function_json_schema(fn)
        assert schema["properties"]["n"]["type"] == "integer"

    def test_str_type(self):
        def fn(s: str): ...

        schema = build_function_json_schema(fn)
        assert schema["properties"]["s"]["type"] == "string"

    def test_float_type(self):
        def fn(f: float): ...

        schema = build_function_json_schema(fn)
        assert schema["properties"]["f"]["type"] == "number"

    def test_bool_type(self):
        def fn(flag: bool): ...

        schema = build_function_json_schema(fn)
        assert schema["properties"]["flag"]["type"] == "boolean"

    def test_list_type(self):
        def fn(items: list[str]): ...

        schema = build_function_json_schema(fn)
        assert schema["properties"]["items"]["type"] == "array"

    def test_annotated_description_used(self):
        schema = build_function_json_schema(_annotated)
        assert schema["properties"]["q"]["description"] == "The search query"
        assert schema["properties"]["limit"]["description"] == "Max results"

    def test_annotated_default_preserved(self):
        schema = build_function_json_schema(_annotated)
        assert schema["properties"]["limit"]["default"] == 10
        assert "limit" not in schema.get("required", [])

    def test_self_excluded(self):
        class MyClass:
            def method(self, x: int): ...

        schema = build_function_json_schema(MyClass.method)
        assert "self" not in schema.get("properties", {})
        assert "x" in schema["properties"]

    def test_cls_excluded(self):
        class MyClass:
            @classmethod
            def create(cls, x: int): ...

        schema = build_function_json_schema(MyClass.create)
        assert "cls" not in schema.get("properties", {})

    def test_var_positional_excluded(self):
        def fn(*args: int): ...

        schema = build_function_json_schema(fn)
        assert "args" not in schema.get("properties", {})

    def test_var_keyword_excluded(self):
        def fn(**kwargs: str): ...

        schema = build_function_json_schema(fn)
        assert "kwargs" not in schema.get("properties", {})

    def test_unannotated_param_included_as_any(self):
        def fn(x): ...

        schema = build_function_json_schema(fn)
        assert "x" in schema["properties"]

    def test_title_stripped_from_schema(self):
        schema = build_function_json_schema(_plain)
        assert "title" not in schema

    def test_title_stripped_from_properties(self):
        schema = build_function_json_schema(_plain)
        for prop in schema["properties"].values():
            assert "title" not in prop

    def test_additional_properties_stripped(self):
        schema = build_function_json_schema(_plain)
        assert "additionalProperties" not in schema

    def test_partial_positional_bind_removes_param(self):
        def add(a: int, b: int) -> int: ...

        p = functools.partial(add, 1)
        schema = build_function_json_schema(p)
        assert "a" not in schema.get("properties", {})
        assert "b" in schema["properties"]

    def test_partial_keyword_bind_keeps_param_as_optional(self):
        def add(a: int, b: int) -> int: ...

        p = functools.partial(add, a=1)
        schema = build_function_json_schema(p)
        assert "a" in schema["properties"]
        assert schema["properties"]["a"].get("default") == 1
        assert "b" in schema["required"]

    def test_nested_partial_unwraps_hint_source(self):
        def fn(x: int, y: str) -> str: ...

        p = functools.partial(functools.partial(fn, 1), "hello")
        schema = build_function_json_schema(p)
        # both args bound positionally — empty schema
        assert schema == _EMPTY_OBJECT_SCHEMA

    def test_optional_type(self):
        def fn(x: int | None = None): ...

        schema = build_function_json_schema(fn)
        assert "x" in schema["properties"]
        assert "x" not in schema.get("required", [])

    def test_introspection_failure_returns_empty_schema(self):
        # Built-in functions have no inspectable signature.
        schema = build_function_json_schema(len)
        assert schema == _EMPTY_OBJECT_SCHEMA

    def test_callable_object_schema_from_call(self):
        obj = _CallableObj()
        schema = build_function_json_schema(obj)
        assert "value" in schema["properties"]
        assert "self" not in schema.get("properties", {})

    @pytest.mark.parametrize("default", [0, "", False, 0.0, [], {}])
    def test_falsy_defaults_preserved(self, default):
        def fn(x: Any = None): ...

        fn.__defaults__ = (default,)
        import inspect

        fn.__signature__ = inspect.signature(fn)
        schema = build_function_json_schema(fn)
        assert schema["properties"]["x"].get("default") == default


# ---------------------------------------------------------------------------
# callable_to_tool_spec
# ---------------------------------------------------------------------------


class TestCallableToToolSpec:
    def test_returns_tool_spec_instance(self):
        spec = callable_to_tool_spec(_plain)
        assert isinstance(spec, ToolSpec)

    def test_name_from_function_name(self):
        spec = callable_to_tool_spec(_plain)
        assert spec.name == "_plain"

    def test_description_from_docstring(self):
        spec = callable_to_tool_spec(_plain)
        assert spec.description == "Do something useful."

    def test_parameters_schema_populated(self):
        spec = callable_to_tool_spec(_plain)
        assert "x" in spec.parameters["properties"]
        assert "y" in spec.parameters["properties"]

    def test_fn_is_original_callable(self):
        spec = callable_to_tool_spec(_plain)
        assert spec.fn is _plain

    def test_sequential_defaults_false(self):
        spec = callable_to_tool_spec(_plain)
        assert spec.sequential is False

    def test_no_docstring_uses_function_name_as_description(self):
        spec = callable_to_tool_spec(_no_doc)
        assert spec.description == "_no_doc"

    def test_partial_name_from_underlying_function(self):
        p = functools.partial(_plain, 1)
        spec = callable_to_tool_spec(p)
        assert spec.name == "_plain"

    def test_partial_fn_is_the_partial_not_inner(self):
        p = functools.partial(_plain, 1)
        spec = callable_to_tool_spec(p)
        assert spec.fn is p

    def test_partial_schema_reflects_remaining_params(self):
        p = functools.partial(_plain, 1)
        spec = callable_to_tool_spec(p)
        assert "x" not in spec.parameters.get("properties", {})
        assert "y" in spec.parameters["properties"]

    def test_callable_object_name_from_class(self):
        obj = _CallableObj()
        spec = callable_to_tool_spec(obj)
        assert spec.name == "_CallableObj"

    def test_callable_object_fn_is_the_object(self):
        obj = _CallableObj()
        spec = callable_to_tool_spec(obj)
        assert spec.fn is obj
