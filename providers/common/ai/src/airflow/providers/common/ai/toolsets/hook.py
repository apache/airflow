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
import functools
import inspect
import re
import types
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin, get_type_hints

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import ToolsetTool

from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    return_schema_kwargs,
    serialize_for_llm,
)
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset, validate_max_retries

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from pydantic_ai._run_context import RunContext

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
_DATETIME_PARAM_NOTE = "An ISO 8601 date-time with a UTC offset, such as 2026-09-01T00:00:00Z."


def _decode_base64(value: str) -> bytes:
    # Whitespace never carries data, so line-wrapped base64 is accepted. validate=True
    # because the default discards any other character outside the alphabet instead of
    # failing, which would store the wrong bytes without a word.
    return base64.b64decode("".join(value.split()), validate=True)


def _parse_aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        # A naive value is ambiguous: S3Hook.list_keys and GCSHook.list_by_timespan raise
        # TypeError comparing it with an aware time, other hooks assume UTC for it.
        raise ValueError(f"{value!r} has no UTC offset")
    return parsed


@dataclass(frozen=True)
class _StringEncoding:
    """How a parameter type JSON cannot carry is sent as a string, and turned back."""

    schema: dict[str, Any]
    """The JSON Schema of the string."""
    note: str
    """Added to the parameter's description."""
    expected: str
    """What the value must be, for the retry prompt."""
    decode: Callable[[str], Any]
    """Raises ``ValueError`` for a value it cannot decode."""


_STRING_ENCODINGS: dict[type, _StringEncoding] = {
    bytes: _StringEncoding(
        {"type": "string"}, _BASE64_PARAM_NOTE, "base64-encoded binary data", _decode_base64
    ),
    datetime: _StringEncoding(
        {"type": "string", "format": "date-time"},
        _DATETIME_PARAM_NOTE,
        "an ISO 8601 date-time with a UTC offset",
        _parse_aware_datetime,
    ),
}


class HookToolset(AirflowToolset):
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
    :param tool_prefix: Optional prefix joined to each tool name with an underscore
        (e.g. ``"s3"`` → ``"s3_list_keys"``). Must be a valid Python identifier.
    :param pinned_arguments: Arguments the Dag author fixes, such as the bucket a
        storage hook may use: ``{"bucket_name": "reports"}``. Each is left out of the
        arguments the model sees, refused if the model supplies it anyway, and passed to
        every allowed method as it is written here, not rendered as a template. Every allowed
        method must take each pinned argument as a named parameter: one that does not, such
        as a method taking ``bucket`` or only ``**kwargs``, raises ``ValueError``, because
        the model could still choose the value through it. Expose such a method from a
        second ``HookToolset``.
    :param max_retries: How many times the model may correct a call with invalid arguments,
        or one that supplies a pinned argument, before the run fails. An exception from the
        hook itself fails the run straight away. ``None`` (the default) uses the agent's
        tool retry budget, its ``retries``, as pydantic-ai's own toolsets do.
    """

    # Rendered, on a copy, by AgentOperator. Deliberately not ``template_fields``, which
    # Airflow's templater would render in place wherever the toolset is nested.
    agent_template_fields: Sequence[str] = ("conn_id",)

    def __init__(
        self,
        hook: BaseHook,
        *,
        allowed_methods: list[str],
        tool_prefix: str = "",
        pinned_arguments: dict[str, Any] | None = None,
        max_retries: int | None = None,
    ) -> None:
        if tool_prefix and not tool_prefix.isidentifier():
            # The prefixed names are rendered as Python function signatures under
            # code mode, so a name that is not an identifier breaks there.
            raise ValueError(f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}.")
        name_prefix = f"{tool_prefix}_" if tool_prefix else ""
        self._max_retries = validate_max_retries(max_retries)
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

        # Every allowed method has to name each pin as a parameter it can be passed by name. A
        # method that takes the value under another name, inside a dict, or through **kwargs
        # would let the model choose it after all, so it is refused rather than left unpinned.
        pinned_arguments = pinned_arguments or {}
        unpinned: dict[str, list[str]] = {}
        for method_name in allowed_methods if pinned_arguments else ():
            parameters = inspect.signature(getattr(hook, method_name)).parameters.values()
            named = {p.name for p in parameters if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)}
            if missing := sorted(set(pinned_arguments) - named):
                unpinned[method_name] = missing
        if unpinned:
            details = "; ".join(
                f"{method}() does not take {', '.join(args)}" for method, args in unpinned.items()
            )
            raise ValueError(
                f"Every allowed method of {hook_cls_name!r} has to take each pinned argument by name, or "
                f"the model could still choose it through that method: {details}. Expose such a method "
                "from a second HookToolset."
            )
        self._pinned: dict[str, Any] = dict(pinned_arguments)

        self._hook = hook
        self._allowed_methods = allowed_methods
        self._name_prefix = name_prefix
        # The attribute holding the hook's connection ID, e.g. ``postgres_conn_id``. Some hooks
        # name one attribute in conn_name_attr but keep the ID in ``conn_id`` (WasbHook,
        # KubernetesHook), so fall back to that.
        conn_attr: str | None = getattr(hook, "conn_name_attr", None)
        if conn_attr is None or not hasattr(hook, conn_attr):
            conn_attr = "conn_id" if hasattr(hook, "conn_id") else None
        self._conn_attr = conn_attr
        self._encoded_params = {
            method_name: _introspect_signature(getattr(hook, method_name))[1]
            for method_name in allowed_methods
        }

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

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        max_retries = self._get_tool_max_retries(ctx)
        tools: dict[str, ToolsetTool[Any]] = {}
        for method_name in self._allowed_methods:
            method = getattr(self._hook, method_name)
            tool_name = f"{self._name_prefix}{method_name}"

            json_schema, encoded_params = _introspect_signature(method)
            description = _extract_description(method)
            param_docs = _parse_param_docs(method.__doc__ or "")
            properties = json_schema["properties"]

            # Enrich parameter descriptions from docstring.
            for param_name, param_desc in param_docs.items():
                if param_name in properties:
                    properties[param_name]["description"] = param_desc
            # After the loop above, which would otherwise overwrite the note. The encoding is
            # stated in the description rather than as ``contentEncoding``: Gemini drops that
            # keyword, and pydantic-ai passes it through to OpenAI strict mode.
            for param_name, encoding in encoded_params.items():
                existing = properties[param_name].get("description")
                properties[param_name]["description"] = (
                    f"{existing} {encoding.note}" if existing else encoding.note
                )
            _drop_properties(json_schema, self._pinned)
            # The validator accepts the pinned names, with any value, so a model that sends one
            # anyway is told it is fixed rather than given a generic extra-input or type error.
            args_schema = {
                **json_schema,
                "properties": json_schema["properties"] | {name: {} for name in self._pinned},
            }

            # sequential=True keeps pydantic-ai from running these calls concurrently
            # within a turn; run_blocking's process-wide lock serializes them with the
            # blocking calls of the other toolsets that use it.
            # return_schema is "string": execute_tool serializes every result with
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
                max_retries=max_retries,
                args_validator=build_args_validator(args_schema),
                # Refused during validation, so an approval gate never asks about such a call.
                args_validator_func=(
                    functools.partial(self._validate_args, encoded_params)
                    if self._pinned or encoded_params
                    else None
                ),
            )
        return tools

    def _validate_args(
        self, encoded_params: dict[str, _StringEncoding], ctx: RunContext[Any], /, **tool_args: Any
    ) -> None:
        self._refuse_pinned(ctx, **tool_args)
        # Only checked here: the decoded values are not kept, so what the toolset chain passes
        # on, and the durable journal fingerprints, stays JSON.
        _decode_string_args(tool_args, encoded_params)

    def _refuse_pinned(self, ctx: RunContext[Any], /, **tool_args: Any) -> None:
        if supplied := sorted(self._pinned.keys() & tool_args.keys()):
            one = len(supplied) == 1
            raise ModelRetry(
                f"{', '.join(supplied)} {'is' if one else 'are'} fixed for this tool: call it again "
                f"without {'it' if one else 'them'}."
            )

    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        method_name = name.removeprefix(self._name_prefix)
        method: Callable[..., Any] = getattr(self._hook, method_name)
        # The framework bridges validate arguments without args_validator_func, so check again here.
        self._refuse_pinned(ctx, **tool_args)
        # Decoded here rather than in the args validator: validated arguments travel the whole
        # toolset chain, and the durable journal fingerprints them with a plain json.dumps, so bytes
        # or datetimes upstream of this point would make such calls unverifiable on durable replay.
        tool_args = _decode_string_args(tool_args, self._encoded_params[method_name])
        # A copy per call, so a method that modifies an argument it is given cannot change the pin.
        result = await self.run_blocking(method, **tool_args, **copy.deepcopy(self._pinned))
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


def _string_encoding(annotation: Any) -> _StringEncoding | None:
    """Return the encoding of ``annotation`` when it is ``T`` or ``T | None`` for a ``T`` in ``_STRING_ENCODINGS``."""
    if get_origin(annotation) in (types.UnionType, Union):
        non_none = [arg for arg in get_args(annotation) if arg is not type(None)]
        return _string_encoding(non_none[0]) if len(non_none) == 1 else None
    return _STRING_ENCODINGS.get(annotation)


def _string_encoded_schema(annotation: Any, encoding: _StringEncoding) -> dict[str, Any]:
    """Return ``encoding.schema`` for ``T``, and it or null for ``T | None``."""
    if get_origin(annotation) in (types.UnionType, Union):
        return {
            "anyOf": [
                {"type": "null"} if arg is type(None) else _string_encoded_schema(arg, encoding)
                for arg in get_args(annotation)
            ]
        }
    return dict(encoding.schema)


def _decode_string_args(
    tool_args: dict[str, Any], encoded_params: dict[str, _StringEncoding]
) -> dict[str, Any]:
    """Decode the strings the model supplied for parameters of a type JSON cannot carry."""
    decoded = dict(tool_args)
    for name in encoded_params.keys() & tool_args.keys():
        value = tool_args[name]
        if not isinstance(value, str):
            continue
        encoding = encoded_params[name]
        try:
            decoded[name] = encoding.decode(value)
        except ValueError as e:
            # ModelRetry asks the model to correct the call; a ValueError would fail the run.
            raise ModelRetry(f"Parameter {name!r} must be {encoding.expected}: {e}.") from e
    return decoded


def _resolve_annotations(method: Callable[..., Any]) -> dict[str, Any]:
    """
    Resolve each of ``method``'s annotations on its own.

    ``get_type_hints`` is all-or-nothing: one annotation that does not resolve discards the
    hints for every parameter. ``CloudKMSHook.encrypt`` hits this, its ``bytes`` parameters
    sitting next to ``retry: Retry | _MethodDefault`` with ``Retry`` imported under
    ``TYPE_CHECKING``. An annotation that does not resolve is left out, so only its own
    parameter is advertised untyped.
    """
    globalns = getattr(inspect.unwrap(method), "__globals__", {})
    hints: dict[str, Any] = {}
    for name, annotation in getattr(method, "__annotations__", {}).items():
        holder = types.SimpleNamespace(__annotations__={name: annotation})
        try:
            hints |= get_type_hints(holder, globalns=globalns)
        except Exception:
            # Evaluating an annotation runs an arbitrary expression: a TYPE_CHECKING-only name
            # raises NameError, a name a module no longer has AttributeError.
            continue
    return hints


def _introspect_signature(
    method: Callable[..., Any],
) -> tuple[dict[str, Any], dict[str, _StringEncoding]]:
    """
    Build ``method``'s JSON Schema and the string encoding of each parameter that needs one.

    Both come from one pass, so the parameters described to the model as an encoded
    string are exactly the ones decoded on the way back.
    """
    sig = inspect.signature(method)
    hints = _resolve_annotations(method)

    properties: dict[str, Any] = {}
    required: list[str] = []
    encoded_params: dict[str, _StringEncoding] = {}
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
        if encoding := _string_encoding(annotation):
            # Only here, not in _TYPE_MAP: a datetime nested in a list is not decoded, so it is
            # not described as an encoded string either.
            properties[name] = _string_encoded_schema(annotation, encoding)
            encoded_params[name] = encoding
        else:
            properties[name] = _python_type_to_json_schema(annotation)

        if param.default is inspect.Parameter.empty:
            required.append(name)

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    if allows_additional_properties:
        schema["additionalProperties"] = True
    return schema, encoded_params


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


def _drop_properties(schema: dict[str, Any], names: Iterable[str]) -> None:
    for name in names:
        schema["properties"].pop(name, None)
        if name in schema.get("required", ()):
            schema["required"].remove(name)
    if "required" in schema and not schema["required"]:
        del schema["required"]
