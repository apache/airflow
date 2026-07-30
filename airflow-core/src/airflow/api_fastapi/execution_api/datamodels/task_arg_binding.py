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

Captured at parse time from the ``@task.stub`` TaskFlow call, stored in the serialized
Dag, and delivered to the lang-SDK runtime via ``TIRunContext.arg_bindings``.
"""

from __future__ import annotations

from functools import cache
from typing import Annotated, Literal

from pydantic import Field, JsonValue, TypeAdapter
from typing_extensions import TypeAliasType

from airflow.api_fastapi.core_api.base import BaseModel

# A named, titled alias (like TaskArgBinding below) kept as free-form JSON rather than a
# typed model, so unknown JSON-schema keywords survive re-serialization along the way.
ArgValueSchema = TypeAliasType(
    "ArgValueSchema", Annotated[dict[str, JsonValue], Field(title="ArgValueSchema")]
)
"""JSON-schema fragment constraining the value a stub-task argument binds to; generated
by pydantic from the stub annotation, carried verbatim, unknown keywords ignored."""


class XComArgBinding(BaseModel):
    """One positional stub-task argument pulled from an upstream task's XCom."""

    # No default: it would drop ``kind`` from ``required``, and the generated task-sdk
    # client then types it ``Literal | None``, invalid as a tagged-union discriminator.
    kind: Literal["xcom"]

    name: str
    """The stub function's parameter name this binding fills, in declaration order."""

    value_schema: ArgValueSchema | None = None
    """Schema fragment from the stub function's annotation; omitted when unconstrained."""

    task_id: str
    """Upstream task id whose ``return_value`` XCom is pulled."""


class LiteralArgBinding(BaseModel):
    """One positional stub-task argument carrying an inline literal from the Dag file."""

    kind: Literal["literal"]
    """No default, for the same generated-client reason as ``XComArgBinding.kind``."""

    name: str
    """The stub function's parameter name this binding fills, in declaration order."""

    value_schema: ArgValueSchema | None = None
    """Schema fragment from the stub function's annotation; omitted when unconstrained."""

    value: JsonValue | None = None
    """The literal value from the Dag file."""

    from_default: bool = False
    """True when the value was filled from the stub signature's default rather than passed in the call."""


# A named alias with an explicit title so the union lands in every schema as its own
# named definition, which the supervisor-schema dump dedups with its task-sdk twin by title.
TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[XComArgBinding | LiteralArgBinding, Field(discriminator="kind", title="TaskArgBinding")],
)
"""One positional argument of a stub (foreign-runtime) task, in declaration order."""


@cache
def get_arg_bindings_adapter() -> TypeAdapter[list[TaskArgBinding]]:
    """
    Build (lazily, then cache) the adapter validating serialized dicts into ``TaskArgBinding``.

    Only the stub-task path in the execution API needs it, so regular runs never pay for it.
    """
    return TypeAdapter(list[TaskArgBinding])
