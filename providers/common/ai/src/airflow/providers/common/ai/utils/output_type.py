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

from typing import Any, get_origin

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic.errors import PydanticSchemaGenerationError

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError

_MAX_SHOWN_CHARS = 200


def rehydrate_pydantic_output(
    output_type: Any,
    raw: str,
    *,
    serialize_output: bool,
) -> Any:
    """
    Turn the reviewed string (JSON, or bare text for str-valued types) back into ``output_type``.

    Used by the HITL/approval paths in ``LLMOperator`` and ``AgentOperator``
    that round-trip the output through a string when deferring to a human
    reviewer. ``str`` outputs pass through unchanged; any other ``output_type``
    (``BaseModel`` subclass, ``int``, ``list[str]``, ...) is validated with a
    pydantic ``TypeAdapter``, first as JSON and then as bare text.
    An ``output_type`` pydantic has no schema for, or an output function, returns ``raw`` unchanged.

    When ``serialize_output`` is ``True``, returns the model dumped to a
    ``dict`` -- matches the operator's ``serialize_output=True`` opt-in for
    consumers that want the dict shape.

    :raises ReviewedOutputValidationError: If ``output_type`` is not ``str`` and ``raw``
        (typically a reviewer's edit) is neither valid JSON for it nor valid as plain text.
    """
    if output_type is str:
        return raw
    try:
        adapter: TypeAdapter[Any] = TypeAdapter(output_type)
    except PydanticSchemaGenerationError:
        # Nothing to validate against, so the reviewed text is all there is.
        return raw
    if adapter.core_schema["type"] == "call":
        # An output function: validating would treat the text as its arguments and call it again.
        return raw
    try:
        rehydrated = adapter.validate_json(raw)
    except (ValidationError, ValueError, TypeError) as json_error:
        # Bare text is how a str-valued Literal, Enum or ``str | None`` output is carried through review.
        try:
            rehydrated = adapter.validate_python(raw)
        except (ValidationError, ValueError, TypeError) as text_error:
            is_not_json = isinstance(json_error, ValidationError) and all(
                error["type"] == "json_invalid" for error in json_error.errors()
            )
            type_name = (
                output_type if get_origin(output_type) else getattr(output_type, "__name__", output_type)
            )
            shown = raw if len(raw) <= _MAX_SHOWN_CHARS else f"{raw[:_MAX_SHOWN_CHARS]}..."
            raise ReviewedOutputValidationError(
                f"The reviewed output could not be converted to the output_type {type_name}. "
                f"Received {shown!r}. {text_error if is_not_json else json_error}"
            ) from json_error
    if serialize_output and isinstance(rehydrated, BaseModel):
        return rehydrated.model_dump()
    return rehydrated
