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
"""Helpers for handling pydantic-ai ``output_type`` shapes."""

from __future__ import annotations

import json
import logging
from typing import Any, get_origin

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_ai import NativeOutput, PromptedOutput, ToolOutput

log = logging.getLogger(__name__)

_OUTPUT_MARKERS = (ToolOutput, NativeOutput, PromptedOutput)


def dump_output_to_json(output: Any) -> str:
    """
    Serialize an LLM output into the string carried through human review.

    The inverse of :func:`rehydrate_pydantic_output`: both sides of the review
    round-trip live here so they cannot drift apart.
    """
    if isinstance(output, BaseModel):
        return output.model_dump_json()
    if isinstance(output, str):
        return output
    try:
        return TypeAdapter(type(output)).dump_json(output).decode()
    except Exception:
        log.warning(
            "Could not build a pydantic schema for %s; showing its repr to the reviewer instead.",
            type(output),
        )
        return str(output)


def rehydrate_pydantic_output(
    output_type: Any,
    raw: str,
    *,
    serialize_output: bool,
) -> Any:
    """
    Turn a JSON string back into a value of ``output_type``.

    Used by the HITL/approval paths in ``LLMOperator`` and ``AgentOperator``
    that round-trip the output through a string when deferring to a human
    reviewer. ``str`` outputs pass through unchanged; any other ``output_type``
    (``BaseModel`` subclass, ``int``, ``list[str]``, ``list[A]`` for a
    ``BaseModel`` subclass ``A``, ...) is validated with a pydantic
    ``TypeAdapter``. When validation fails (reviewer edited the string into
    something the type rejects), returns ``raw`` unchanged. A
    ``ToolOutput``/``NativeOutput``/``PromptedOutput`` marker wrapping a single
    type is unwrapped first. An ``output_type`` that is a ``[A, B]`` union
    list, a marker wrapping one, or a pydantic-ai *output function* falls back
    to plain JSON instead -- none of those are a plain class or a generic
    alias ``TypeAdapter`` can build a schema for from the type alone, and an
    output function in particular must never reach ``TypeAdapter``: it builds
    an *arguments* schema for a callable rather than raising, so validating
    against it would call the function a second time with reviewer-controlled
    input.

    When ``serialize_output`` is ``True``, returns the value dumped to plain
    Python containers (``dict``/``list``/scalars, with any nested
    ``BaseModel`` dumped too) -- matches the operator's
    ``serialize_output=True`` opt-in for consumers that want the dict shape.
    """
    if output_type is str:
        return raw

    unwrapped = output_type
    if isinstance(output_type, _OUTPUT_MARKERS):
        unwrapped = output_type.output if isinstance(output_type, ToolOutput) else output_type.outputs

    if not isinstance(unwrapped, type) and get_origin(unwrapped) is None:
        # Not a plain class or generic alias: an output function, a ``[A, B]``
        # union list, or a marker wrapping one. These reach us because the
        # operator passes output_type straight to Agent(...). Fall back to
        # plain JSON rather than risk building a schema that re-invokes an
        # output function.
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return raw

    adapter: TypeAdapter[Any] = TypeAdapter(unwrapped)
    try:
        rehydrated = adapter.validate_json(raw)
    except (ValidationError, ValueError, TypeError):
        return raw
    if serialize_output:
        return adapter.dump_python(rehydrated, mode="python")
    return rehydrated
