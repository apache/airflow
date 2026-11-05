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

Captured at parse time from the ``@task.stub`` TaskFlow call, or derived per map index at
``ti_run`` for a mapped stub, and delivered to the lang-SDK runtime via
``TIRunContext.arg_bindings``.
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


class _ArgBindingBase(BaseModel):
    """Fields every :class:`TaskArgBinding` variant carries, regardless of ``kind``."""

    name: str
    """The stub function's parameter name this binding fills, in declaration order."""

    value_schema: ArgValueSchema | None = None
    """Schema fragment from the stub function's annotation; omitted when unconstrained."""


class XComArgBinding(_ArgBindingBase):
    """One positional stub-task argument pulled from an upstream task's XCom."""

    # No default: it would drop ``kind`` from ``required``, and the generated task-sdk
    # client then types it ``Literal | None``, invalid as a tagged-union discriminator.
    kind: Literal["xcom"]

    task_id: str
    """Upstream task id whose ``return_value`` XCom is pulled."""

    map_index: int = -1
    """Map index of the upstream XCom row to pull; -1 is the unmapped row."""

    element_index: int | None = None
    """When set, the pulled value is a sequence and this binding takes its element at this index."""

    mapping_key: str | None = None
    """When set, the selected value is a mapping and this binding takes its entry at this key.

    Applied after ``element_index``. Sent for ``expand_kwargs()`` over an upstream output, whose
    keys are unknown until it runs, so a missing key fails the task rather than falling back to
    the parameter's default."""


class LiteralArgBinding(_ArgBindingBase):
    """One positional stub-task argument carrying an inline literal from the Dag file."""

    kind: Literal["literal"]
    """No default, for the same generated-client reason as ``XComArgBinding.kind``."""

    value: JsonValue | None = None
    """The literal value from the Dag file."""

    from_default: bool = False
    """True when the value was filled from the stub signature's default rather than passed in the call."""


class XComSequenceArgBinding(_ArgBindingBase):
    """One stub-task argument reading a mapped task's whole output as a lazy sequence."""

    kind: Literal["sequence"]

    task_id: str
    """Task whose ``return_value`` rows make up the sequence; the runtime fetches them on demand."""


TupleArgBindingPart = TypeAliasType(
    "TupleArgBindingPart",
    Annotated[XComArgBinding | LiteralArgBinding, Field(discriminator="kind", title="TupleArgBindingPart")],
)


class TupleArgBinding(_ArgBindingBase):
    """One ``zip()`` element: one part per zipped argument, gathered into a single argument."""

    kind: Literal["tuple"]

    parts: list[TupleArgBindingPart]
    """One part per zipped argument, in zip order; a ``fillvalue`` arrives as a literal part."""


class UnresolvedArgBinding(_ArgBindingBase):
    """
    An argument the server could not finish.

    Its ``map()`` callables are Python, or its nesting fits no single binding. The coordinator's
    Python helper resolves it against the Dag file and replaces it with a ``LiteralArgBinding``,
    so no SDK runtime ever receives this kind.
    """

    kind: Literal["unresolved"]


# A named alias with an explicit title so the union lands in every schema as its own
# named definition, which the supervisor-schema dump dedups with its task-sdk twin by title.
TaskArgBinding = TypeAliasType(
    "TaskArgBinding",
    Annotated[
        XComArgBinding | LiteralArgBinding | XComSequenceArgBinding | TupleArgBinding | UnresolvedArgBinding,
        Field(discriminator="kind", title="TaskArgBinding"),
    ],
)
"""One positional argument of a stub (foreign-runtime) task, in declaration order."""


@cache
def get_arg_bindings_adapter() -> TypeAdapter[list[TaskArgBinding]]:
    """
    Build (lazily, then cache) the adapter validating serialized dicts into ``TaskArgBinding``.

    Only the stub-task path in the execution API needs it, so regular runs never pay for it.
    """
    return TypeAdapter(list[TaskArgBinding])
