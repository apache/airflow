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

import logging
from typing import Any

import pytest
from pydantic import BaseModel
from pydantic_ai import NativeOutput, PromptedOutput, ToolOutput

from airflow.providers.common.ai.utils.output_type import (
    dump_output_to_json,
    rehydrate_pydantic_output,
)


class A(BaseModel):
    x: int


class B(BaseModel):
    y: int


UNION_OUTPUT_TYPE: list[Any] = [A, B]


class Widget:
    """Plain class pydantic cannot build a schema for."""

    def __init__(self, n: int) -> None:
        self.n = n

    def __str__(self) -> str:
        return f"Widget({self.n})"


CALLS: list[int] = []


def make_widget(n: int) -> Widget:
    """A pydantic-ai output function: called with validated *arguments*, not a schema for its return value."""
    CALLS.append(n)
    return Widget(n)


class TestRehydratePydanticOutput:
    def test_returns_model_instance(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=False)
        assert isinstance(result, A)
        assert result.x == 7

    def test_returns_dict_when_serialize_output(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=True)
        assert result == {"x": 7}

    def test_returns_model_instances_for_generic_alias_of_base_model(self):
        # ``list[A]`` is not ``isinstance(..., type)`` -- it must still reach TypeAdapter
        # rather than fall back to plain json.loads and hand back a list of dicts.
        result = rehydrate_pydantic_output(list[A], '[{"x": 7}]', serialize_output=False)
        assert result == [A(x=7)]
        assert isinstance(result[0], A)

    def test_returns_dicts_for_generic_alias_of_base_model_when_serialize_output(self):
        result = rehydrate_pydantic_output(list[A], '[{"x": 7}]', serialize_output=True)
        assert result == [{"x": 7}]
        assert not isinstance(result[0], BaseModel)

    def test_returns_raw_for_str_output_type(self):
        result = rehydrate_pydantic_output(str, "anything", serialize_output=False)
        assert result == "anything"

    @pytest.mark.parametrize(
        ("output_type", "raw", "expected"),
        [(int, "5", 5), (bool, "true", True), (list[str], '["a", "b"]', ["a", "b"])],
        ids=["int", "bool", "list"],
    )
    def test_validates_other_types_with_type_adapter(self, output_type, raw, expected):
        assert rehydrate_pydantic_output(output_type, raw, serialize_output=False) == expected

    def test_returns_raw_when_type_adapter_rejects(self):
        result = rehydrate_pydantic_output(int, "not-a-number", serialize_output=False)
        assert result == "not-a-number"

    def test_returns_raw_on_invalid_json(self):
        result = rehydrate_pydantic_output(A, "not-json", serialize_output=False)
        assert result == "not-json"

    def test_returns_raw_on_schema_mismatch(self):
        # ``A`` requires ``x: int`` -- this payload should fail validation
        result = rehydrate_pydantic_output(A, '{"y": "no-x-field"}', serialize_output=False)
        assert result == '{"y": "no-x-field"}'

    def test_falls_back_to_json_when_output_type_has_no_schema(self):
        # pydantic-ai accepts a ``[A, B]`` union list, but TypeAdapter cannot build a
        # schema for it and the failure is not a ValidationError, so it must not escape.
        result = rehydrate_pydantic_output(UNION_OUTPUT_TYPE, '{"x": 7}', serialize_output=False)
        assert result == {"x": 7}

    def test_returns_raw_when_output_type_has_no_schema_and_raw_is_not_json(self):
        result = rehydrate_pydantic_output(UNION_OUTPUT_TYPE, "not-json", serialize_output=False)
        assert result == "not-json"

    @pytest.mark.parametrize(
        "marker",
        [ToolOutput(A), NativeOutput(A), PromptedOutput(A)],
        ids=["tool-output", "native-output", "prompted-output"],
    )
    def test_unwraps_output_marker_wrapping_a_single_type(self, marker):
        # A marker wrapping one type unwraps to that type and validates normally --
        # it must not fall back to json.loads and hand back a bare dict.
        result = rehydrate_pydantic_output(marker, '{"x": 7}', serialize_output=False)
        assert result == A(x=7)

    @pytest.mark.parametrize(
        "marker",
        [NativeOutput([A, B]), PromptedOutput([A, B])],
        ids=["native-output", "prompted-output"],
    )
    def test_falls_back_when_marker_wraps_a_sequence(self, marker):
        result = rehydrate_pydantic_output(marker, '{"x": 7}', serialize_output=False)
        assert result == {"x": 7}

    def test_output_function_falls_back_without_invoking_it(self):
        # TypeAdapter(make_widget) builds a schema for its *arguments*, not its return
        # value, and does not raise -- validating against it would call make_widget a
        # second time with reviewer-controlled input instead of falling back to JSON.
        CALLS.clear()
        result = rehydrate_pydantic_output(make_widget, '{"n": 3}', serialize_output=False)
        assert result == {"n": 3}
        assert CALLS == []


class TestDumpOutputToJson:
    def test_returns_str_unchanged(self):
        assert dump_output_to_json("plain text") == "plain text"

    def test_uses_model_dump_json_for_base_model(self):
        assert dump_output_to_json(A(x=7)) == '{"x":7}'

    @pytest.mark.parametrize(
        ("output", "expected"),
        [(["tag-a", "tag-b"], '["tag-a","tag-b"]'), (True, "true"), ({"k": 1}, '{"k":1}')],
        ids=["list", "bool", "dict"],
    )
    def test_dumps_other_types_as_json(self, output, expected):
        assert dump_output_to_json(output) == expected

    def test_falls_back_to_str_when_type_has_no_schema(self):
        # Reachable via a pydantic-ai output function: a non-schema-able return type
        # only warns at agent-build time, so serialization must not raise here.
        assert dump_output_to_json(Widget(3)) == "Widget(3)"

    def test_logs_warning_when_falling_back_to_str(self, caplog):
        with caplog.at_level(logging.WARNING):
            dump_output_to_json(Widget(3))
        assert any(r.levelno == logging.WARNING and "Widget" in r.message for r in caplog.records)
