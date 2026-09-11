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
Provider-neutral structured-output layer for ``@task.llm_batch``.

This module knows nothing about any provider -- it only knows Pydantic. It
turns ``output_type`` into a JSON Schema adapters can translate into their own
request shape (:class:`OutputSpec`), and turns an adapter's extracted
response back into a validated value or an explicit failure
(:class:`ValidationOutcome`).

Deliberately does **not** call
:func:`~airflow.providers.common.ai.utils.output_type.rehydrate_pydantic_output`:
that helper's failure mode is "validation failed -> return the raw string
unchanged", which is correct for the HITL round-trip it was built for but
wrong here -- a batch item that fails validation must be recorded as failed
(``status: "invalid_output"``), never silently downgraded into a
string that merely looks like a success.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, PydanticUserError, TypeAdapter, ValidationError

from airflow.providers.common.ai.exceptions import LLMBatchOutputTypeError

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import ExtractedOutput

#: Pydantic's ``ValidationError`` can be very long when a schema has many
#: fields; truncated so 100k failed items don't write 100k long error blobs
#: into the JSONL result file.
_ERROR_MESSAGE_MAX_LENGTH = 2000

_SCHEMA_NAME_DISALLOWED_CHARS = re.compile(r"[^a-zA-Z0-9_-]")
_SCHEMA_NAME_MAX_LENGTH = 64


@dataclass(frozen=True)
class OutputSpec:
    """
    The translation of ``output_type`` into a provider-agnostic structured-output request.

    Built once per task execution (the schema is identical for every request
    in the batch), then passed to the adapter's
    :meth:`~airflow.providers.common.ai.batch.base.BatchAdapter.build_output_directive`
    and, on the way back, to :func:`validate_extracted_output`.
    """

    output_type: type
    is_structured: bool
    json_schema: dict[str, Any]
    schema_name: str
    type_adapter: TypeAdapter


def _normalize_schema_name(name: str) -> str:
    """
    Normalize a type name into the character set both OpenAI and Anthropic accept for a schema/tool name.

    Takes the intersection of the two providers' allowed character sets
    (alphanumeric, ``_``, ``-``) rather than tracking each provider's exact
    rules separately.
    """
    normalized = _SCHEMA_NAME_DISALLOWED_CHARS.sub("_", name)[:_SCHEMA_NAME_MAX_LENGTH]
    return normalized or "output"


def build_output_spec(output_type: type) -> OutputSpec:
    """
    Build the :class:`OutputSpec` for a ``@task.llm_batch`` ``output_type``.

    ``output_type is str`` (the default) produces an unstructured spec: no
    schema is generated and adapters skip ``build_output_directive`` /
    ``extract_output`` validation entirely. Any other type -- a ``BaseModel``
    subclass, or another type ``TypeAdapter`` supports (``int``,
    ``list[str]``, ...), matching what ``@task.llm``'s ``output_type``
    already accepts -- produces a JSON Schema.

    :raises LLMBatchOutputTypeError: ``output_type`` cannot produce a JSON
        Schema at all (e.g. a bare class with no Pydantic-compatible fields).
        Raised eagerly, before any request is built or submitted.
    """
    if output_type is str:
        return OutputSpec(
            output_type=output_type,
            is_structured=False,
            json_schema={},
            schema_name="",
            type_adapter=TypeAdapter(str),
        )

    try:
        type_adapter = TypeAdapter(output_type)
        if isinstance(output_type, type) and issubclass(output_type, BaseModel):
            json_schema = output_type.model_json_schema()
        else:
            json_schema = type_adapter.json_schema()
    except PydanticUserError as e:
        raise LLMBatchOutputTypeError(
            f"output_type={output_type!r} cannot produce a JSON Schema for structured batch "
            f"output ({e}). Use a Pydantic BaseModel subclass, or a type TypeAdapter supports "
            "(int, list[str], ...)."
        ) from e

    schema_name = _normalize_schema_name(getattr(output_type, "__name__", ""))
    return OutputSpec(
        output_type=output_type,
        is_structured=True,
        json_schema=json_schema,
        schema_name=schema_name,
        type_adapter=type_adapter,
    )


@dataclass(frozen=True)
class ValidationOutcome:
    """The result of validating one item's :class:`ExtractedOutput` against an :class:`OutputSpec`."""

    ok: bool
    #: ``ok=True``: the validated value, dumped JSON-native (``mode="json"``)
    #: so ``datetime``/``Enum``/``UUID`` fields serialize instead of raising
    #: at ``json.dumps`` time -- the failure that would otherwise only show
    #: up after 100k requests had already been processed.
    value: Any | None = None
    #: ``ok=False``: the original text, preserved for the JSONL row's
    #: ``raw_output`` field so the caller can see what the model actually
    #: returned instead of just that it failed.
    raw_text: str | None = None
    #: ``ok=False``: a truncated, human-readable explanation.
    error_message: str | None = None


def _truncate_error(message: str) -> str:
    if len(message) <= _ERROR_MESSAGE_MAX_LENGTH:
        return message
    return message[:_ERROR_MESSAGE_MAX_LENGTH] + "… (truncated)"


def validate_extracted_output(extracted: ExtractedOutput, spec: OutputSpec) -> ValidationOutcome:
    """
    Validate one item's extracted output against ``spec``, never raising.

    "The model returned something that doesn't match ``output_type``" is
    batch *data*, not an exception -- the caller records ``ok=False`` items as
    ``status: "invalid_output"`` and keeps processing the rest of the stream.

    When ``spec.is_structured`` is ``False`` (``output_type is str``), the
    text passes through unchanged with no validation at all.
    """
    if not spec.is_structured:
        return ValidationOutcome(ok=True, value=extracted.text)

    if extracted.kind == "absent":
        return ValidationOutcome(
            ok=False,
            raw_text=extracted.text,
            error_message="The model did not return structured output matching the requested schema.",
        )

    try:
        if extracted.kind == "json_text":
            validated = spec.type_adapter.validate_json(extracted.text or "")
        else:
            validated = spec.type_adapter.validate_python(extracted.value)
    except (ValidationError, json.JSONDecodeError, ValueError, TypeError) as e:
        raw_text = extracted.text if extracted.kind == "json_text" else _dump_raw_value(extracted.value)
        return ValidationOutcome(ok=False, raw_text=raw_text, error_message=_truncate_error(str(e)))

    return ValidationOutcome(ok=True, value=spec.type_adapter.dump_python(validated, mode="json"))


def _dump_raw_value(value: Any) -> str | None:
    """Best-effort stringification of a ``json_value`` payload for ``raw_output`` on failure."""
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
