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

import base64
import copy
import inspect
import re
import types
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin, get_type_hints

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.ai.tools import AirflowTool
    from airflow.providers.common.compat.sdk import BaseHook

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

_BASE64_PARAM_NOTE = "Provide this value base64-encoded."

_MAX_UNRESOLVABLE_ANNOTATION_NAMES = 20


class HookToolset(AbstractToolset[Any]):
    """
    Expose selected methods of an Airflow Hook as pydantic-ai tools.

    This adapter introspects the method signatures and docstrings of the given
    hook to build :class:`~pydantic_ai.tools.ToolDefinition` objects that an LLM
    agent can call.

    :param hook: An instantiated Airflow Hook. Its connection ID -- the attribute
        the hook's ``conn_name_attr`` names, such as ``postgres_conn_id`` -- is
        templated when the toolset is passed to ``AgentOperator`` / ``@task.agent``,
        so ``HookToolset(PostgresHook(postgres_conn_id="tenant_{{ ... }}"), ...)``
        reaches a different database per task instance. The hook in the Dag file
        is not modified; each task instance gets a copy.
    :param allowed_methods: Method names to expose as tools. Required —
        auto-discovery is intentionally not supported for safety.
    :param tool_name_prefix: Optional prefix prepended to each tool name
        (e.g. ``"s3_"`` → ``"s3_list_keys"``).
    """

    # Rendered, on a copy, by AgentOperator. Deliberately not ``template_fields``, which
    # Airflow's templater would render in place wherever the toolset is nested.
    agent_template_fields: Sequence[str] = ("conn_id",)

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
        # The attribute holding the hook's connection ID, e.g. ``postgres_conn_id``. Some hooks
        # name one attribute in conn_name_attr but keep the ID in ``conn_id`` (WasbHook,
        # KubernetesHook), so fall back to that.
        conn_attr: str | None = getattr(hook, "conn_name_attr", None)
        if conn_attr is None or not hasattr(hook, conn_attr):
            conn_attr = "conn_id" if hasattr(hook, "conn_id") else None
        self._conn_attr = conn_attr
        self._bytes_params: dict[str, frozenset[str]] = {}

    @property
    def conn_id(self) -> str | None:
        """The hook's connection ID, or ``None`` when the hook keeps it under neither attribute."""
        return getattr(self._hook, self._conn_attr, None) if self._conn_attr else None

    @conn_id.setter
    def conn_id(self, value: str) -> None:
        if self._conn_attr is None:
            raise AttributeError(f"{type(self._hook).__name__} keeps no connection ID to set.")
        # Set on a copy: the hook in the Dag file backs every task instance that shares this
        # toolset, so writing the rendered ID onto it would carry one instance's connection
        # into the next.
        hook = copy.copy(self._hook)
        setattr(hook, self._conn_attr, value)
        self._hook = hook

    @property
    def id(self) -> str:
        name = type(self._hook).__name__
        return f"hook-{name}-{self.conn_id}" if self.conn_id else f"hook-{name}"

    def airflow_tools(self) -> list[AirflowTool]:
        """
        Return this toolset's tools as framework-neutral tools.

        Each is an :class:`~airflow.providers.common.ai.tools.AirflowTool`. Use
        them to give the tools to an agent framework other than pydantic-ai,
        for example through
        :func:`~airflow.providers.common.ai.tools.strands.as_strands_tools`. The
        tools behave as they do in ``AgentOperator``, and every result and error
        passes through Airflow's secret masker before the model sees it.

        .. warning:: Experimental; see :mod:`airflow.providers.common.ai.tools`.
        """
        return airflow_tools_from_toolset(self)

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for method_name in self._allowed_methods:
            method = getattr(self._hook, method_name)
            tool_name = f"{self._tool_name_prefix}{method_name}" if self._tool_name_prefix else method_name

            json_schema, bytes_params = _introspect_signature(method)
            self._bytes_params[tool_name] = bytes_params
            description = _extract_description(method)
            param_docs = _parse_param_docs(method.__doc__ or "")
            properties = json_schema.get("properties", {})

            # Enrich parameter descriptions from docstring.
            for param_name, param_desc in param_docs.items():
                if param_name in properties:
                    properties[param_name]["description"] = param_desc

            # After the loop above, which would otherwise overwrite the note.
            for param_name in bytes_params & properties.keys():
                existing = properties[param_name].get("description")
                properties[param_name]["description"] = (
                    f"{existing} {_BASE64_PARAM_NOTE}" if existing else _BASE64_PARAM_NOTE
                )

            # sequential=True because hook methods perform synchronous I/O
            # (network calls, DB queries) and should not run concurrently.
            # return_schema is "string": call_tool serializes every result with
            # serialize_for_llm, so the tool always returns a (JSON-encoded)
            # string regardless of the method's own return annotation. This lets
            # code mode render `-> str` instead of `-> Any`.
            tool_def = ToolDefinition(
                name=tool_name,
                description=description,
                parameters_json_schema=json_schema,
                sequential=True,
                **return_schema_kwargs({"type": "string"}),
            )
            tools[tool_name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=build_args_validator(json_schema),
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
        bytes_params = self._bytes_params.get(name)
        if bytes_params is None:
            bytes_params = _introspect_signature(method)[1]
        # Decoding belongs here rather than in the args validator: validated args
        # travel the whole toolset chain, and CachingToolset fingerprints them with
        # a plain json.dumps, so bytes upstream of this point would make every
        # binary call unverifiable on durable replay.
        result = method(**_decode_bytes_args(tool_args, bytes_params))
        return serialize_for_llm(result)


# ---------------------------------------------------------------------------
# Private introspection helpers
# ---------------------------------------------------------------------------


def _python_type_to_json_schema(annotation: Any) -> dict[str, Any]:
    """Convert a Python type annotation to a JSON Schema fragment."""
    if annotation is inspect.Parameter.empty or annotation is Any:
        return {}

    if annotation is type(None):
        return {"type": "null"}

    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is types.UnionType or origin is Union:
        return {"anyOf": [_python_type_to_json_schema(arg) for arg in args]}

    # list[X]
    if origin is list:
        items = _python_type_to_json_schema(args[0]) if args else {"type": "string"}
        return {"type": "array", "items": items}

    # dict[K, V]
    if origin is dict:
        return {"type": "object"}

    # Always return a fresh copy — callers may mutate the dict (e.g. adding "description").
    schema = _TYPE_MAP.get(annotation)
    return dict(schema) if schema else {}


def _resolves_to_bytes(annotation: Any) -> bool:
    """Whether ``annotation`` is ``bytes`` or ``Optional[bytes]``/``bytes | None``."""
    if annotation is bytes:
        return True
    origin = get_origin(annotation)
    if origin is types.UnionType or origin is Union:
        non_none = [a for a in get_args(annotation) if a is not type(None)]
        if len(non_none) == 1:
            return _resolves_to_bytes(non_none[0])
    return False


def _decode_bytes_args(tool_args: dict[str, Any], bytes_params: frozenset[str]) -> dict[str, Any]:
    """Decode the base64 strings the model supplied for ``bytes``-typed parameters."""
    if not bytes_params:
        return tool_args

    decoded: dict[str, Any] = {}
    for key, value in tool_args.items():
        if key not in bytes_params or not isinstance(value, str):
            decoded[key] = value
            continue
        try:
            # ``validate=True``: the default discards invalid characters instead of
            # erroring, which is the silent corruption this decoding exists to avoid.
            decoded[key] = base64.b64decode(value, validate=True)
        except ValueError as e:
            # ModelRetry is the only exception pydantic-ai feeds back to the model;
            # anything else fails the run without giving it a chance to correct.
            raise ModelRetry(f"Parameter {key!r} must be base64-encoded binary data.") from e
    return decoded


def _resolve_annotations(method: Callable[..., Any]) -> dict[str, Any]:
    """
    Resolve ``method``'s annotations, tolerating names that are not importable at runtime.

    ``get_type_hints`` is all-or-nothing: a single unresolvable annotation discards
    the hints for every parameter, which would silently stop base64 decoding for the
    rest of the signature. ``CloudKMSHook.encrypt`` hits this today — its ``bytes``
    parameters sit next to ``retry: Retry | _MethodDefault``, where ``Retry`` is
    imported under ``TYPE_CHECKING``. Unresolvable names are substituted with ``Any``
    so the parameters that *can* be resolved still are.
    """
    localns: dict[str, Any] = {}
    for _ in range(_MAX_UNRESOLVABLE_ANNOTATION_NAMES):
        try:
            return get_type_hints(method, localns=localns)
        except NameError as e:
            if not e.name or e.name in localns:
                break
            localns[e.name] = Any
        except TypeError:
            break
    return {}


def _introspect_signature(method: Callable[..., Any]) -> tuple[dict[str, Any], frozenset[str]]:
    """
    Build ``method``'s JSON Schema and the names of its ``bytes``-typed parameters.

    Both come from one pass so the schema advertised to the model and the arguments
    decoded on the way back can never disagree about which parameters are binary.
    """
    sig = inspect.signature(method)
    hints = _resolve_annotations(method)

    properties: dict[str, Any] = {}
    required: list[str] = []
    bytes_params: set[str] = set()
    allows_additional_properties = False

    for name, param in sig.parameters.items():
        if name in ("self", "cls"):
            continue
        if param.kind is param.VAR_POSITIONAL:
            continue
        if param.kind is param.VAR_KEYWORD:
            allows_additional_properties = True
            continue

        annotation = hints.get(name, param.annotation)
        prop = _python_type_to_json_schema(annotation)
        properties[name] = prop
        # One condition drives both, so a parameter can never be advertised as
        # base64 without also being decoded on the way back.
        if _resolves_to_bytes(annotation):
            bytes_params.add(name)
            prop["contentEncoding"] = "base64"

        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    if allows_additional_properties:
        schema["additionalProperties"] = True
    return schema, frozenset(bytes_params)


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
