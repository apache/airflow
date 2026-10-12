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

import inspect
import logging
from typing import Any, get_origin

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic.errors import PydanticUserError
from pydantic_core import PydanticSerializationError

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError

log = logging.getLogger(__name__)

_MAX_SHOWN_CHARS = 200


def dump_output_to_json(output: Any) -> str:
    """Serialize an output for review as JSON that validates back into its type, or ``str()`` if pydantic cannot."""
    if isinstance(output, str):
        return output
    # ``round_trip`` drops computed fields, and ``by_alias=False`` keeps field names even when a model
    # sets ``serialize_by_alias``, so a field name or validation alias reads back.
    try:
        if isinstance(output, BaseModel):
            return output.model_dump_json(round_trip=True, by_alias=False)
        return TypeAdapter(type(output)).dump_json(output, round_trip=True, by_alias=False).decode()
    except (PydanticUserError, PydanticSerializationError):
        log.warning("Cannot serialize a %s to JSON, so the reviewer sees its str().", type(output).__name__)
        return str(output)


def rehydrate_pydantic_output(
    output_type: Any,
    raw: str,
    *,
    serialize_output: bool,
    is_edited: bool = True,
) -> Any:
    """
    Turn the reviewed string back into ``output_type``.

    ``raw`` is JSON, or bare text for types pydantic builds from a string. ``str`` outputs, output
    functions and types pydantic has no schema for return ``raw`` unchanged, and so does an unedited
    output that does not validate back (with a warning).

    :param is_edited: ``False`` when ``raw`` is the unmodified generated output.

    :raises ReviewedOutputValidationError: If a reviewer's edit does not validate against ``output_type``.
    """
    if output_type is str:
        return raw
    try:
        adapter: TypeAdapter[Any] = TypeAdapter(output_type)
        is_output_function = _is_output_function(adapter.core_schema)
    except PydanticUserError:
        return raw
    if is_output_function:
        return raw
    # Output is dumped by field name, so accept a field name or a validation alias.
    try:
        rehydrated = adapter.validate_json(raw, by_alias=True, by_name=True)
    except (ValidationError, ValueError, TypeError) as json_error:
        # Bare text is how a str-valued Literal, Enum or ``str | None`` output is carried through review.
        try:
            rehydrated = _validate_bare_text(adapter, raw)
        except (ValidationError, ValueError, TypeError) as text_error:
            message = _build_failure_message(output_type, raw, json_error, text_error)
            if not is_edited:
                log.warning("%s Returning the output unchanged.", message)
                return raw
            raise ReviewedOutputValidationError(message) from json_error
    if serialize_output and isinstance(rehydrated, BaseModel):
        return rehydrated.model_dump()
    return rehydrated


def _validate_bare_text(adapter: TypeAdapter[Any], raw: str) -> Any:
    """Validate ``raw`` as is, then without the surrounding whitespace a reviewer's textarea tends to add."""
    try:
        return adapter.validate_python(raw)
    except ValidationError:
        if raw.strip() == raw:
            raise
        return adapter.validate_python(raw.strip())


def _build_failure_message(output_type: Any, raw: str, json_error: Exception, text_error: Exception) -> str:
    """Describe why ``raw`` does not match ``output_type``, reporting the text error when it is not JSON."""
    is_not_json = isinstance(json_error, ValidationError) and all(
        error["type"] == "json_invalid" for error in json_error.errors()
    )
    type_name = output_type if get_origin(output_type) else getattr(output_type, "__name__", output_type)
    shown = raw if len(raw) <= _MAX_SHOWN_CHARS else f"{raw[:_MAX_SHOWN_CHARS]}..."
    return (
        f"The reviewed output could not be converted to the output_type {type_name}. "
        f"Received {shown!r}. {text_error if is_not_json else json_error}"
    )


def _is_output_function(schema: Any) -> bool:
    """
    Tell an output function from a type, both of which can have a ``call`` core schema.

    A NamedTuple, wrapped or not, calls a class. A reused parameter model wraps the schema in ``definitions``.
    """
    if schema["type"] == "definitions":
        schema = schema["schema"]
    return schema["type"] == "call" and not inspect.isclass(schema["function"])
