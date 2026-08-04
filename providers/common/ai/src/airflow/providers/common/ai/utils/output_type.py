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
from typing import Any

from pydantic import BaseModel, TypeAdapter, ValidationError


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
    (``BaseModel`` subclass, ``int``, ``list[str]``, ...) is validated with a
    pydantic ``TypeAdapter``. When validation fails (reviewer edited the string
    into something the type rejects), returns ``raw`` unchanged. An
    ``output_type`` pydantic cannot build a schema for falls back to plain JSON.

    When ``serialize_output`` is ``True``, returns the model dumped to a
    ``dict`` -- matches the operator's ``serialize_output=True`` opt-in for
    consumers that want the dict shape.
    """
    if output_type is str:
        return raw
    try:
        adapter = TypeAdapter(output_type)
    except Exception:
        # No pydantic schema for this output_type: a ToolOutput/NativeOutput/
        # PromptedOutput marker, an output function, or a ``[A, B]`` union list.
        # These reach us because the operator passes output_type straight to
        # Agent(...). Schema-build failures are not ValidationError subclasses,
        # so raising here would lose an already-approved output.
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return raw
    try:
        rehydrated = adapter.validate_json(raw)
    except (ValidationError, ValueError, TypeError):
        return raw
    if serialize_output and isinstance(rehydrated, BaseModel):
        return rehydrated.model_dump()
    return rehydrated
