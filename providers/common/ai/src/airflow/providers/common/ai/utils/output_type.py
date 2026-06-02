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

from typing import Any

from pydantic import BaseModel, ValidationError


def rehydrate_pydantic_output(
    output_type: Any,
    raw: str,
    *,
    serialize_output: bool,
) -> Any:
    """
    Turn a JSON string back into the ``output_type`` Pydantic model.

    Used by the HITL/approval paths in ``LLMOperator`` and ``AgentOperator``
    that round-trip the model through a string when deferring to a human
    reviewer. When ``output_type`` is not a ``BaseModel`` subclass, returns
    ``raw`` unchanged so the caller can apply its own fallback (e.g.
    ``json.loads``). When validation fails (reviewer edited the string into
    something the schema rejects), also returns ``raw`` unchanged.

    When ``serialize_output`` is ``True``, returns the model dumped to a
    ``dict`` -- matches the operator's ``serialize_output=True`` opt-in for
    consumers that want the dict shape.
    """
    if not (isinstance(output_type, type) and issubclass(output_type, BaseModel)):
        return raw
    try:
        rehydrated = output_type.model_validate_json(raw)
    except (ValidationError, ValueError, TypeError):
        return raw
    if serialize_output:
        return rehydrated.model_dump()
    return rehydrated
