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
"""Helpers for extracting JSON Schema and tool metadata from plain Python callables."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

from pydantic import Field, create_model
from typing_extensions import get_type_hints

if TYPE_CHECKING:
    from airflow.providers.common.ai.hooks.base import ToolSpec

_EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}
_SKIP_PARAMS = frozenset({"self", "cls"})
_DOCSTRING_SECTION_PREFIXES = (
    "args:",
    "arguments:",
    "parameters:",
    "params:",
    "returns:",
    "return:",
    "yields:",
    "yield:",
    "raises:",
    "raise:",
    "except:",
    "exceptions:",
    "example:",
    "examples:",
    "note:",
    "notes:",
    "see also:",
    "references:",
)


def extract_function_description(fn: Callable[..., Any]) -> str:
    """Return the first paragraph of *fn*'s docstring, stopping before Args/Returns sections."""
    doc = inspect.getdoc(fn)
    if not doc:
        return getattr(fn, "__name__", type(fn).__name__)
    result: list[str] = []
    for line in doc.split("\n"):
        if line.strip().lower().startswith(_DOCSTRING_SECTION_PREFIXES):
            break
        result.append(line)
    return "\n".join(result).strip() or getattr(fn, "__name__", type(fn).__name__)


def build_function_json_schema(fn: Callable[..., Any]) -> dict[str, Any]:
    """
    Build a JSON Schema ``object`` for the parameters of *fn*.

    Reads type hints (including ``Annotated[T, "description"]``) and default
    values to produce a schema suitable for LLM tool binding.
    Falls back to an empty object schema on any introspection failure.

    ``self``, ``cls``, ``*args``, and ``**kwargs`` are excluded.
    For ``functools.partial``, only the remaining free parameters appear.
    """
    # Partials: sig from partial (bound args already removed), hints from inner fn.
    hint_source: Callable[..., Any] = fn
    if isinstance(fn, functools.partial):
        hint_source = fn.func
        while isinstance(hint_source, functools.partial):
            hint_source = hint_source.func

    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return _EMPTY_OBJECT_SCHEMA

    try:
        hints = get_type_hints(hint_source, include_extras=True)
    except Exception:
        hints = {}

    field_defs: dict[str, Any] = {}
    for param_name, param in sig.parameters.items():
        if param_name in _SKIP_PARAMS:
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        annotation = hints.get(param_name, param.annotation)
        if annotation is inspect.Parameter.empty:
            annotation = Any
        default = ... if param.default is inspect.Parameter.empty else param.default

        if get_origin(annotation) is Annotated:
            type_args = get_args(annotation)
            actual_type = type_args[0]
            desc: str | None = next((a for a in type_args[1:] if isinstance(a, str)), None)
        else:
            actual_type = annotation
            desc = None

        field_defs[param_name] = (
            actual_type,
            Field(default=default, description=desc) if desc else Field(default=default),
        )

    if not field_defs:
        return _EMPTY_OBJECT_SCHEMA

    try:
        schema = create_model(f"_{getattr(fn, '__name__', 'tool')}", **field_defs).model_json_schema()
    except Exception:
        return _EMPTY_OBJECT_SCHEMA

    schema.pop("title", None)
    schema.pop("additionalProperties", None)
    for prop in schema.get("properties", {}).values():
        prop.pop("title", None)
    return schema


def callable_to_tool_spec(fn: Callable[..., Any]) -> ToolSpec:
    """
    Build a :class:`~airflow.providers.common.ai.hooks.base.ToolSpec` from a plain callable.

    Combines :func:`extract_function_description` and :func:`build_function_json_schema`
    so callers get name, description, and a full parameter schema in one call.
    """
    # Lazy import avoids a circular dependency: base imports this module,
    # this module imports ToolSpec from base.
    from airflow.providers.common.ai.hooks.base import ToolSpec

    if isinstance(fn, functools.partial):
        inner: Callable[..., Any] = fn.func
        while isinstance(inner, functools.partial):
            inner = inner.func
        name = getattr(inner, "__name__", type(inner).__name__)
        source = inner
    else:
        name = getattr(fn, "__name__", type(fn).__name__)
        source = fn

    return ToolSpec(
        name=name,
        description=extract_function_description(source),
        parameters=build_function_json_schema(fn),  # fn not source: sig already reflects bound args
        fn=fn,
    )
