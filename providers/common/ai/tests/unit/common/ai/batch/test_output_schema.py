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
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from enum import Enum

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.batch.base import ExtractedOutput
from airflow.providers.common.ai.batch.output_schema import (
    ValidationOutcome,
    build_output_spec,
    validate_extracted_output,
)
from airflow.providers.common.ai.exceptions import LLMBatchOutputTypeError
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output


class Diagnosis(BaseModel):
    name: str
    age: int


class Severity(str, Enum):
    LOW = "low"
    HIGH = "high"


class Event(BaseModel):
    happened_at: datetime
    severity: Severity


class TestBuildOutputSpecStrIsUnstructured:
    def test_str_output_type_is_not_structured(self):
        spec = build_output_spec(str)
        assert spec.is_structured is False
        assert spec.json_schema == {}


class TestBuildOutputSpecStructuredTypes:
    def test_base_model_produces_schema(self):
        spec = build_output_spec(Diagnosis)
        assert spec.is_structured is True
        assert spec.json_schema["properties"].keys() == {"name", "age"}
        assert spec.schema_name == "Diagnosis"

    @pytest.mark.parametrize(
        ("output_type", "inner_type"),
        [(int, "integer"), (list[str], "array"), (str | None, None)],
    )
    def test_non_object_roots_are_wrapped_in_an_object_both_providers_accept(self, output_type, inner_type):
        spec = build_output_spec(output_type)
        assert spec.is_structured is True
        assert spec.wrapped is True
        assert spec.json_schema["type"] == "object"
        assert spec.json_schema["required"] == ["response"]
        inner = spec.json_schema["properties"]["response"]
        if inner_type is not None:
            assert inner["type"] == inner_type
        else:
            assert "anyOf" in inner

    def test_object_roots_are_not_wrapped(self):
        spec = build_output_spec(Diagnosis)
        assert spec.wrapped is False
        assert spec.json_schema["type"] == "object"
        assert "age" in spec.json_schema["properties"]

    @pytest.mark.parametrize(
        ("output_type", "payload", "expected"),
        [
            (int, '{"response": 42}', 42),
            (list[str], '{"response": ["a", "b"]}', ["a", "b"]),
            (str | None, '{"response": null}', None),
        ],
    )
    def test_wrapped_output_is_unwrapped_on_validation(self, output_type, payload, expected):
        spec = build_output_spec(output_type)
        outcome = validate_extracted_output(ExtractedOutput(kind="json_text", text=payload), spec)
        assert outcome.ok is True
        assert outcome.value == expected

    def test_wrapped_output_without_the_response_key_is_invalid(self):
        spec = build_output_spec(int)
        outcome = validate_extracted_output(ExtractedOutput(kind="json_value", value={"answer": 1}), spec)
        assert outcome.ok is False
        assert "response" in outcome.error_message

    def test_unstructured_null_text_is_invalid_output_not_success(self):
        outcome = validate_extracted_output(ExtractedOutput(kind="absent"), build_output_spec(str))
        assert outcome.ok is False
        assert "no text content" in outcome.error_message


class TestBuildOutputSpecUnrepresentableType:
    def test_unrepresentable_type_raises_llm_batch_output_type_error(self):
        """Non-Pydantic-compatible types must fail eagerly, before any request is built."""
        with pytest.raises(LLMBatchOutputTypeError, match="cannot produce a JSON Schema"):
            build_output_spec(threading.Lock)


class TestValidateExtractedOutputUnstructured:
    def test_unstructured_passes_through_with_no_validation(self):
        spec = build_output_spec(str)
        outcome = validate_extracted_output(ExtractedOutput(kind="text", text="hello"), spec)
        assert outcome == ValidationOutcome(ok=True, value="hello")


class TestValidateExtractedOutputStructuredSuccess:
    def test_json_text_kind_validates_and_dumps_json_native(self):
        spec = build_output_spec(Diagnosis)
        extracted = ExtractedOutput(kind="json_text", text='{"name": "Ann", "age": 30}')
        outcome = validate_extracted_output(extracted, spec)
        assert outcome.ok is True
        assert outcome.value == {"name": "Ann", "age": 30}

    def test_json_value_kind_validates_from_a_parsed_dict(self):
        """Anthropic's tool-use ``input`` arrives already parsed -- no dumps/loads round trip needed."""
        spec = build_output_spec(Diagnosis)
        extracted = ExtractedOutput(kind="json_value", value={"name": "Ann", "age": 30})
        outcome = validate_extracted_output(extracted, spec)
        assert outcome.ok is True
        assert outcome.value == {"name": "Ann", "age": 30}

    def test_datetime_and_enum_fields_serialize_with_mode_json(self):
        """
        Dumping with ``mode="json"`` is not optional -- without it, ``datetime``/``Enum``
        fields are Python objects and a downstream ``json.dumps`` on the JSONL row would raise,
        the kind of failure that only surfaces after 100k requests have already run.
        """
        spec = build_output_spec(Event)
        extracted = ExtractedOutput(
            kind="json_value",
            value={"happened_at": datetime(2026, 9, 11, tzinfo=timezone.utc), "severity": Severity.HIGH},
        )
        outcome = validate_extracted_output(extracted, spec)
        assert outcome.ok is True
        json.dumps(outcome.value)  # must not raise
        assert outcome.value["severity"] == "high"


class TestValidateExtractedOutputFourFailureModes:
    """
    The four failure modes for ``validate_extracted_output``: absent output, malformed JSON,
    and schema mismatch, covered below; the fourth -- an unrepresentable ``output_type`` -- is
    covered separately by ``TestBuildOutputSpecUnrepresentableType``, since it fails at
    spec-build time, before validation is ever reached.
    """

    def test_absent_kind_fails(self):
        spec = build_output_spec(Diagnosis)
        outcome = validate_extracted_output(ExtractedOutput(kind="absent"), spec)
        assert outcome.ok is False
        assert outcome.value is None
        assert outcome.error_message is not None

    def test_malformed_json_text_fails_and_preserves_raw_text(self):
        spec = build_output_spec(Diagnosis)
        bad_json = '{"name": "Ann", "age": '  # truncated JSON
        outcome = validate_extracted_output(ExtractedOutput(kind="json_text", text=bad_json), spec)
        assert outcome.ok is False
        assert outcome.raw_text == bad_json

    def test_schema_mismatch_fails_and_preserves_raw_text(self):
        spec = build_output_spec(Diagnosis)
        bad_json = '{"name": "Ann", "age": "not-a-number"}'
        outcome = validate_extracted_output(ExtractedOutput(kind="json_text", text=bad_json), spec)
        assert outcome.ok is False
        assert outcome.raw_text == bad_json
        assert outcome.error_message is not None

    def test_error_message_is_truncated(self):
        class ManyFields(BaseModel):
            a: int
            b: int
            c: int
            d: int
            e: int

        spec = build_output_spec(ManyFields)
        bad_json = '{"a": "x", "b": "x", "c": "x", "d": "x", "e": "x"}'
        outcome = validate_extracted_output(ExtractedOutput(kind="json_text", text=bad_json), spec)
        assert outcome.ok is False
        assert len(outcome.error_message) <= 2000 + len("… (truncated)")


class TestNotRehydratePydanticOutput:
    """
    A regression guard proving this module's failure semantics are the opposite of
    ``rehydrate_pydantic_output``.

    ``rehydrate_pydantic_output`` silently downgrades a validation failure to
    the raw string -- correct for the HITL round trip, wrong for batch, where
    a failure must be recorded as ``invalid_output``, not mistaken for a
    successful string result.
    """

    def test_same_bad_input_diverges_between_the_two_functions(self):
        bad_json = '{"name": "Ann", "age": "not-a-number"}'

        rehydrated = rehydrate_pydantic_output(Diagnosis, bad_json, serialize_output=True)
        assert rehydrated == bad_json  # silently returns the raw string unchanged

        spec = build_output_spec(Diagnosis)
        outcome = validate_extracted_output(ExtractedOutput(kind="json_text", text=bad_json), spec)
        assert outcome.ok is False  # explicitly reports failure instead
