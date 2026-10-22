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

import dataclasses
from inspect import Parameter, signature
from typing import TYPE_CHECKING

from airflow.sdk.definitions.context import KNOWN_CONTEXT_KEYS

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["TaskHandlerParam", "TaskHandlerDeclaration"]


@dataclasses.dataclass(frozen=True, kw_only=True)
class TaskHandlerParam:
    """
    One declared handler parameter (ADR-0012).

    Arguments are bound to the handler by name.
    """

    name: str
    required: bool

    @classmethod
    def from_parameter(cls, parameter: Parameter) -> TaskHandlerParam:
        if parameter.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            raise ValueError("may not use variadic *args or **kwargs")
        if parameter.kind is Parameter.POSITIONAL_ONLY:
            raise ValueError(f"may not use positional-only parameter {parameter.name!r}")
        if (name := parameter.name) in KNOWN_CONTEXT_KEYS:
            raise ValueError(f"argument {name!r} is a reserved context key")
        return cls(name=name, required=parameter.default is Parameter.empty)


@dataclasses.dataclass
class TaskHandlerDeclaration:
    """
    The signature a handler declares.

    This is the parse-time argument specification compared against a stub's
    captured arguments. It carries only the parameters: the caller holds the
    dag_id/task_id it queried, and the callable to run is resolved separately.
    """

    params: list[TaskHandlerParam]

    @classmethod
    def from_handler(cls, fn: Callable) -> TaskHandlerDeclaration:
        return cls([TaskHandlerParam.from_parameter(p) for p in signature(fn).parameters.values()])
