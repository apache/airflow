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
Positional-argument binding spec for stub (foreign-runtime) tasks.

Captured at parse time from a stub task's TaskFlow call (``@task.stub``), stored in the
serialized Dag, and delivered to the lang-SDK runtime through ``TIRunContext.arg_bindings``
so it can bind the values onto the native task function's parameters.
"""

from __future__ import annotations

from enum import Enum
from functools import cache
from typing import Annotated, Literal

from pydantic import Field, JsonValue, TypeAdapter
from typing_extensions import TypeAliasType

from airflow.api_fastapi.core_api.base import BaseModel


class ArgBindingDataType(str, Enum):
    """Language-neutral value type a stub-task argument binds to in the foreign runtime."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    ANY = "any"


class XComArgBinding(BaseModel):
    """One positional stub-task argument pulled from an upstream task's XCom."""

    kind: Literal["xcom"]

    name: str
    """The stub function's parameter name this binding fills, in declaration order."""

    data_type: ArgBindingDataType = ArgBindingDataType.ANY
    """Declared type from the stub function's annotation; runtimes type-check against it."""

    task_id: str
    """Upstream task id to pull the XCom from; the ``return_value`` XCom is always the one pulled."""


class LiteralArgBinding(BaseModel):
    """One positional stub-task argument carrying an inline literal from the Dag file."""

    kind: Literal["literal"]

    name: str
    """The stub function's parameter name this binding fills, in declaration order."""

    data_type: ArgBindingDataType = ArgBindingDataType.ANY
    """Declared type from the stub function's annotation; runtimes type-check against it."""

    value: JsonValue | None = None
    """The literal value from the Dag file."""


# A named alias (TypeAliasType, not a bare Annotated) so the union lands in every
# schema as its own named definition instead of an anonymous field-title-derived one.
# The explicit title lets the supervisor-schema dump merge this def with the
# task-sdk-generated twin (its core/SDK dedup keys on titles).
TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[XComArgBinding | LiteralArgBinding, Field(discriminator="kind", title="TaskArgBinding")],
)
"""One positional argument of a stub (foreign-runtime) task, in declaration order."""


@cache
def get_arg_bindings_adapter() -> TypeAdapter[list[TaskArgBinding]]:
    """
    Validate serialized arg-binding dicts into the kind-discriminated ``TaskArgBinding`` union.

    Constructed lazily on first use (then cached): only the stub-task path in the
    execution API ever needs the adapter, and most workloads are not stub operators,
    so regular task runs never pay for building it.
    """
    return TypeAdapter(list[TaskArgBinding])
