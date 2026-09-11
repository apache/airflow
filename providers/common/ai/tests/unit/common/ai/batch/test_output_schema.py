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

    def test_int_produces_schema(self):
        spec = build_output_spec(int)
        assert spec.is_structured is True
        assert spec.json_schema["type"] == "integer"

    def test_list_of_str_produces_schema(self):
        spec = build_output_spec(list[str])
        assert spec.is_structured is True
        assert spec.json_schema["type"] == "array"


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
        §6: dumping with ``mode="json"`` is not optional -- without it, ``datetime``/``Enum``
        fields are Python objects and a downstream ``json.dumps`` on the JSONL row would raise,
        the kind of failure that only surfaces after 100k requests have already run.
        """
        import json

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
    """§10.4 / Step 5 acceptance (c): absent, malformed JSON, schema mismatch, unrepresentable output_type."""

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

    def test_unrepresentable_output_type_fails_at_spec_build_time(self):
        """The fourth failure mode is not a ``ValidationOutcome`` at all -- it never gets that far."""
        with pytest.raises(LLMBatchOutputTypeError):
            build_output_spec(threading.Lock)

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
    Step 5 acceptance (d) / plan "發現 D": a regression guard proving this module's failure
    semantics are the opposite of ``rehydrate_pydantic_output``.

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
