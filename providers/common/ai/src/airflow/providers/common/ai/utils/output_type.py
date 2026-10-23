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
import operator
from collections.abc import Iterator
from functools import reduce
from typing import Any, get_origin

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic.errors import PydanticUserError
from pydantic_core import CoreSchema, PydanticSerializationError, to_jsonable_python

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError

try:
    from airflow.sdk.serde import iter_pydantic_models
except ImportError:  # pragma: no cover - missing ``apache-airflow-task-sdk`` walker
    iter_pydantic_models = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

_MAX_SHOWN_CHARS = 200
# What a value of the wrong shape raises on its way through ``output_type``. Pydantic wraps a validator's
# ``ValueError``, but not the ``AttributeError``/``KeyError``/``IndexError`` of a ``mode="before"`` validator
# that indexes its input (``data.get``) when the review text is not the shape it expects. Anything else,
# such as a validator's network or timeout error, propagates instead of reading as a failed conversion.
_CONVERSION_ERRORS = (ValidationError, ValueError, TypeError, AttributeError, KeyError, IndexError)


def is_model_dump_required(output: Any, output_type: Any, *, serialize_output: bool) -> bool:
    """
    Tell whether an output holding ``BaseModel`` instances must be pushed to XCom dumped.

    The instances are looked up in the output itself, including the elements of a list, tuple, set or
    frozenset and the values of a dict. A downstream task can load a model from XCom only if the worker
    registered the model's class, and the worker registers the classes ``iter_pydantic_models`` finds in
    ``output_type``. It finds none in a list of output types, an output marker, an output function, a
    ``TypeAliasType`` or a ``NewType``, so an instance of a class it did not find is dumped.
    Without ``iter_pydantic_models`` there is no evidence either way, so the caller's ``serialize_output`` decides.
    """
    if not (found := [model for model, _ in _iter_models_in_output(output)]):
        return False
    if serialize_output:
        return True
    if iter_pydantic_models is None:
        return False
    registered = set(iter_pydantic_models(output_type))
    return any(type(model) not in registered for model in found)


def dump_output_for_xcom(output: Any, output_type: Any, *, serialize_output: bool) -> Any:
    """
    Return the output as it is pushed to XCom: dumped when ``is_model_dump_required``, else unchanged.

    With ``serialize_output`` the dump holds Python values, as the ``model_dump()`` of 1.0.0 does, so a
    ``date`` or ``Decimal`` field keeps its type; an output with a set, frozenset or dict key holding a model
    is dumped to JSON values instead. A model the worker registers no class for, without ``serialize_output``,
    is dumped to JSON values as well, field names rather than aliases, so a ``HttpUrl``, ``Path`` or ``Enum``
    field is a string that XCom accepts.
    """
    if not is_model_dump_required(output, output_type, serialize_output=serialize_output):
        return output
    # Python mode is what 1.0.0 pushed, a minor release must not change it. A model dumped to a dict cannot
    # be a set member, so python mode raises on a set holding models.
    if serialize_output and not any(in_set for _, in_set in _iter_models_in_output(output)):
        if isinstance(output, BaseModel):
            return output.model_dump()
        return TypeAdapter(Any).dump_python(output, mode="python")
    return to_jsonable_python(output, by_alias=False)


def _iter_models_in_output(output: Any) -> Iterator[tuple[BaseModel, bool]]:
    """
    Yield the ``BaseModel`` instances in an output, each with whether it must be hashable.

    Looks inside list, tuple, set, frozenset and dict. A set member or a dict key is hashable, so it is
    flagged, and so is whatever is nested in one. Dataclass and NamedTuple contents and model fields
    are not walked.
    """
    stack = [(output, False)]
    while stack:
        item, in_set = stack.pop()
        if isinstance(item, BaseModel):
            yield item, in_set
        elif isinstance(item, (set, frozenset)):
            stack.extend((element, True) for element in item)
        elif isinstance(item, (list, tuple)):
            stack.extend((element, in_set) for element in item)
        elif isinstance(item, dict):
            stack.extend((key, True) for key in item)
            stack.extend((value, in_set) for value in item.values())


def dump_output_to_json(output: Any) -> str:
    """Serialize an output as JSON that ``rehydrate_pydantic_output`` validates back, or ``str()`` if pydantic cannot."""
    # ``round_trip`` drops computed fields, and ``by_alias=False`` keeps field names even when a model
    # sets ``serialize_by_alias``, so a field name or validation alias reads back.
    return _dump_json(output, round_trip=True, by_alias=False)


def dump_output_for_display(output: Any) -> str:
    """Serialize an output as JSON for a reviewer to read, or ``str()`` if pydantic cannot."""
    return _dump_json(output)


def _dump_json(output: Any, *, round_trip: bool = False, by_alias: bool | None = None) -> str:
    if isinstance(output, str):
        return output
    try:
        if isinstance(output, BaseModel):
            return output.model_dump_json(round_trip=round_trip, by_alias=by_alias)
        return TypeAdapter(type(output)).dump_json(output, round_trip=round_trip, by_alias=by_alias).decode()
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

    ``raw`` is JSON, or bare text for types pydantic builds from a string. A list of candidate types is validated as their union. ``str`` outputs, output
    functions, pydantic-ai output markers such as ``ToolOutput`` and types pydantic has no schema for
    return ``raw`` unchanged, and so does an unedited output that does not validate back (with a warning).

    :param is_edited: ``False`` when ``raw`` is the unmodified generated output.

    :raises ReviewedOutputValidationError: If a reviewer's edit does not validate against ``output_type``.
    """
    if output_type is str:
        return raw
    declared_type = output_type
    try:
        if isinstance(output_type, (list, tuple)):
            # pydantic-ai treats a list of output types as "any of these".
            output_type = reduce(operator.or_, output_type)
        adapter: TypeAdapter[Any] = TypeAdapter(output_type)
        is_output_function = _is_output_function(adapter.core_schema)
    # A pydantic-ai output marker (``ToolOutput``, ``NativeOutput``, ...) is not a type, and
    # ``TypeAdapter`` fails on it with an ``AttributeError``. Unioning an empty list or markers is a ``TypeError``.
    except (PydanticUserError, AttributeError, TypeError):
        return raw
    if is_output_function:
        return raw
    # Output is dumped by field name, so also accept a field name. An alias stays accepted when the model
    # validates by alias, its default; forcing it would override a model that turned it off.
    try:
        rehydrated = adapter.validate_json(raw, by_name=True)
    except _CONVERSION_ERRORS as json_error:
        # Bare text is how a str-valued Literal, Enum or ``str | None`` output is carried through review.
        try:
            rehydrated = _validate_bare_text(adapter, raw)
        except _CONVERSION_ERRORS as text_error:
            message = _build_failure_message(output_type, raw, json_error, text_error)
            if not is_edited:
                log.warning("%s Returning the output unchanged.", message)
                return raw
            raise ReviewedOutputValidationError(message) from json_error
    return dump_output_for_xcom(rehydrated, declared_type, serialize_output=serialize_output)


def _validate_bare_text(adapter: TypeAdapter[Any], raw: str) -> Any:
    """Validate ``raw`` as is, then without the surrounding whitespace a reviewer's textarea tends to add."""
    try:
        return adapter.validate_python(raw)
    except ValidationError:
        if raw.strip() == raw:
            raise
        return adapter.validate_python(raw.strip())


def _build_failure_message(output_type: Any, raw: str, json_error: Exception, text_error: Exception) -> str:
    """Describe why ``raw`` does not match ``output_type``, reporting the text error for bare-text types."""
    type_name = output_type if get_origin(output_type) else getattr(output_type, "__name__", output_type)
    shown = raw if len(raw) <= _MAX_SHOWN_CHARS else f"{raw[:_MAX_SHOWN_CHARS]}..."
    reported = text_error if _is_bare_text_failure(raw, json_error, text_error) else json_error
    return (
        f"The reviewed output could not be converted to the output_type {type_name}. "
        f"Received {shown!r}. {reported}"
    )


def _is_bare_text_failure(raw: str, json_error: Exception, text_error: Exception) -> bool:
    """Tell a text that was never JSON from a JSON edit with a typo, which the JSON error explains best."""
    # An object or array the reviewer typed is JSON with a typo, whatever the type also accepts as text.
    if not isinstance(text_error, ValidationError) or raw.lstrip().startswith(("{", "[")):
        return False
    is_not_json = isinstance(json_error, ValidationError) and all(
        error["type"] == "json_invalid" for error in json_error.errors()
    )
    # A union adds its member to ``loc``, so match on the error type alone.
    is_container_mismatch = all(_is_container_mismatch(error["type"]) for error in text_error.errors())
    return is_not_json and not is_container_mismatch


def _is_container_mismatch(error_type: str) -> bool:
    """
    Tell whether pydantic refused the whole text for a structured type, where the JSON error says more.

    Those errors end in ``_type`` (``model_type``, ``list_type``, ``named_tuple_type``, ...), while a type
    that accepts text fails with ``*_parsing``, ``*_error`` or ``enum``. Matching the suffix rather than a
    list keeps up with pydantic renaming them, as 2.14 did for a NamedTuple and a deque.
    """
    return error_type.endswith("_type") or error_type == "sequence_str"


def _is_output_function(schema: CoreSchema) -> bool:
    """
    Tell an output function from a type, both of which can have a ``call`` core schema.

    A NamedTuple, wrapped or not, calls a class. A reused parameter model wraps the schema in ``definitions``.
    """
    if schema["type"] == "definitions":
        schema = schema["schema"]
    return schema["type"] == "call" and not inspect.isclass(schema["function"])
