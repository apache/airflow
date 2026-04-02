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
"""Generic adapter that exposes Airflow Hook methods as pydantic-ai tools."""

from __future__ import annotations

import inspect
import json
import re
import types
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin, get_type_hints

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.compat.sdk import BaseHook

# Single shared validator — accepts any JSON-decoded dict from the LLM.
_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

# Maps Python types to JSON Schema fragments.
_TYPE_MAP: dict[type, dict[str, Any]] = {
    str: {"type": "string"},
    int: {"type": "integer"},
    float: {"type": "number"},
    bool: {"type": "boolean"},
    list: {"type": "array"},
    dict: {"type": "object"},
    bytes: {"type": "string"},
}


class HookToolset(AbstractToolset[Any]):
    """
    Expose selected methods of an Airflow Hook as pydantic-ai tools.

    This adapter introspects the method signatures and docstrings of the given
    hook to build :class:`~pydantic_ai.tools.ToolDefinition` objects that an LLM
    agent can call.

    :param hook: An instantiated Airflow Hook.
    :param allowed_methods: Method names to expose as tools. Required —
        auto-discovery is intentionally not supported for safety.
    :param tool_name_prefix: Optional prefix prepended to each tool name
        (e.g. ``"s3_"`` → ``"s3_list_keys"``).
    """

    def __init__(
        self,
        hook: BaseHook,
        *,
        allowed_methods: list[str],
        tool_name_prefix: str = "",
    ) -> None:
        if not allowed_methods:
            raise ValueError("allowed_methods must be a non-empty list.")

        hook_cls_name = type(hook).__name__
        for method_name in allowed_methods:
            if not hasattr(hook, method_name):
                raise ValueError(
                    f"Hook {hook_cls_name!r} has no method {method_name!r}. Check your allowed_methods list."
                )
            if not callable(getattr(hook, method_name)):
                raise ValueError(f"{hook_cls_name}.{method_name} is not callable.")

        self._hook = hook
        self._allowed_methods = allowed_methods
        self._tool_name_prefix = tool_name_prefix
        self._id = f"hook-{type(hook).__name__}"

    @property
    def id(self) -> str:
        return self._id

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for method_name in self._allowed_methods:
            method = getattr(self._hook, method_name)
            tool_name = f"{self._tool_name_prefix}{method_name}" if self._tool_name_prefix else method_name

            json_schema = _build_json_schema_from_signature(method)
            description = _extract_description(method)
            param_docs = _parse_param_docs(method.__doc__ or "")

            # Enrich parameter descriptions from docstring.
            for param_name, param_desc in param_docs.items():
                if param_name in json_schema.get("properties", {}):
                    json_schema["properties"][param_name]["description"] = param_desc

            # sequential=True because hook methods perform synchronous I/O
            # (network calls, DB queries) and should not run concurrently.
            tool_def = ToolDefinition(
                name=tool_name,
                description=description,
                parameters_json_schema=json_schema,
                sequential=True,
            )
            tools[tool_name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        method_name = name.removeprefix(self._tool_name_prefix) if self._tool_name_prefix else name
        method: Callable[..., Any] = getattr(self._hook, method_name)
        result = method(**tool_args)
        return _serialize_for_llm(result)


# ---------------------------------------------------------------------------
# Private introspection helpers
# ---------------------------------------------------------------------------


def _python_type_to_json_schema(annotation: Any) -> dict[str, Any]:
    """Convert a Python type annotation to a JSON Schema fragment."""
    if annotation is inspect.Parameter.empty or annotation is Any:
        return {"type": "string"}

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Optional[X] is Union[X, None] — handle both types.UnionType (3.10+) and typing.Union
    if origin is types.UnionType or origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _python_type_to_json_schema(non_none[0])
        return {"type": "string"}

    # list[X]
    if origin is list:
        items = _python_type_to_json_schema(args[0]) if args else {"type": "string"}
        return {"type": "array", "items": items}

    # dict[K, V]
    if origin is dict:
        return {"type": "object"}

    # Always return a fresh copy — callers may mutate the dict (e.g. adding "description").
    schema = _TYPE_MAP.get(annotation)
    return dict(schema) if schema else {"type": "string"}


def _build_json_schema_from_signature(method: Callable[..., Any]) -> dict[str, Any]:
    """Build a JSON Schema ``object`` from a method's signature and type hints."""
    sig = inspect.signature(method)

    try:
        hints = get_type_hints(method)
    except Exception:
        hints = {}

    properties: dict[str, Any] = {}
    required: list[str] = []

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        # Skip **kwargs and *args
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue

        annotation = hints.get(name, param.annotation)
        prop = _python_type_to_json_schema(annotation)
        properties[name] = prop

        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema


def _extract_description(method: Callable[..., Any]) -> str:
    """Return the first paragraph of a method's docstring."""
    doc = inspect.getdoc(method)
    if not doc:
        return method.__name__.replace("_", " ").capitalize()

    # First paragraph = everything up to the first blank line.
    lines: list[str] = []
    for line in doc.splitlines():
        if not line.strip():
            if lines:
                break
            continue
        lines.append(line.strip())
    return " ".join(lines) if lines else method.__name__.replace("_", " ").capitalize()


# Matches Sphinx-style `:param name:` and Google-style `name:` under an ``Args:`` block.
_SPHINX_PARAM_RE = re.compile(r":param\s+(\w+):\s*(.+?)(?=\n\s*:|$)", re.DOTALL)
_GOOGLE_ARGS_RE = re.compile(r"^\s{2,}(\w+)\s*(?:\(.+?\))?:\s*(.+)", re.MULTILINE)


def _parse_param_docs(docstring: str) -> dict[str, str]:
    """Parse parameter descriptions from Sphinx or Google-style docstrings."""
    params: dict[str, str] = {}

    # Try Sphinx style first.
    for match in _SPHINX_PARAM_RE.finditer(docstring):
        name = match.group(1)
        desc = " ".join(match.group(2).split())
        params[name] = desc

    if params:
        return params

    # Fall back to Google style (``Args:`` section).
    in_args = False
    for line in docstring.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("args:"):
            in_args = True
            continue
        if in_args:
            if stripped and not stripped[0].isspace() and ":" not in stripped:
                break
            m = _GOOGLE_ARGS_RE.match(line)
            if m:
                params[m.group(1)] = " ".join(m.group(2).split())

    return params


def _serialize_for_llm(value: Any) -> str:
    """Convert a Python return value to a string suitable for an LLM."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return str(value)
