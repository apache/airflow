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
"""
Introspect a Python handler for mixed-language validation.

This is conceptually the inverse of
:func:`airflow.serialization.stub_arg_bindings.build_arg_bindings`. That
function builds the stub side from the caller; this builds the handler side.
"""

from __future__ import annotations

import types
import typing
from collections.abc import Callable
from dataclasses import dataclass, field
from inspect import Parameter, signature

from pydantic import TypeAdapter
from pydantic.json_schema import GenerateJsonSchema

from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS

__all__ = ["TaskHandlerParam", "TaskHandlerDeclaration", "declare_handler", "infer_value_schema"]


@dataclass(frozen=True)
class TaskHandlerParam:
    """One declared handler parameter (ADR-0012). Bindings are positional; name is carried too."""

    name: str
    value_schema: dict[str, typing.Any] | None = None
    required: bool = True  # the handler declares no default


@dataclass
class TaskHandlerDeclaration:
    """What a handler declares for one task_id, to compare against the stub's arg_bindings."""

    task_id: str
    params: list[TaskHandlerParam] = field(default_factory=list)
    handler: Callable[..., typing.Any] | None = field(default=None, compare=False, repr=False)


def declare_handler(
    task_id: str,
    fn: Callable,
    *,
    schema_fn: Callable[[typing.Any], dict[str, typing.Any] | None] | None = None,
) -> TaskHandlerDeclaration:
    """Introspect ``fn`` into a ``TaskHandlerDeclaration`` (ordered params + optional schemas)."""
    schema_fn = schema_fn or infer_value_schema
    sig = signature(fn)

    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        # Unresolvable annotations (forward refs to unavailable names) -> treat as untyped.
        hints = {}

    params: list[TaskHandlerParam] = []
    for name, p in sig.parameters.items():
        if p.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            raise ValueError(f"task handler {task_id!r} may not use varadic *args or **kwargs")
        if name in KNOWN_CONTEXT_KEYS:
            raise ValueError(f"task handler {task_id!r} argument {name!r} is a reserved context key")
        params.append(
            TaskHandlerParam(
                name=name,
                value_schema=schema_fn(hints.get(name, p.annotation)),
                required=p.default is Parameter.empty,
            )
        )
    return TaskHandlerDeclaration(task_id=task_id, params=params, handler=fn)


class _ValueSchemaGenerator(GenerateJsonSchema):
    # OpenAPI fixed-width numeric formats, matching core so a foreign runtime decodes
    # into the right machine types.
    def int_schema(self, schema):
        return {**super().int_schema(schema), "format": "int64"}

    def float_schema(self, schema):
        return {**super().float_schema(schema), "format": "double"}


def infer_value_schema(annotation: typing.Any) -> dict[str, typing.Any] | None:
    """
    Default ``value_schema`` generator: pydantic-based, mirroring core's ``_infer_value_schema``.

    Returns ``None`` for unconstrained annotations, and also when pydantic is unavailable, so
    the pure-stdlib parts of declaration stay usable in minimal environments. For validation
    parity, prefer injecting core's ``_infer_value_schema`` at the integration point.
    """
    if annotation in (Parameter.empty, None, typing.Any, types.NoneType):
        return None

    try:
        schema = TypeAdapter(annotation).json_schema(schema_generator=_ValueSchemaGenerator)
    except Exception:
        return None
    return schema or None
