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

from enum import Enum
from typing import Literal

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError
from airflow.providers.common.ai.utils.output_type import rehydrate_pydantic_output


class A(BaseModel):
    x: int


class Color(str, Enum):
    RED = "red"


class TestRehydratePydanticOutput:
    def test_returns_model_instance(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=False)
        assert isinstance(result, A)
        assert result.x == 7

    def test_returns_dict_when_serialize_output(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=True)
        assert result == {"x": 7}

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

    @pytest.mark.parametrize(
        ("output_type", "raw", "expected"),
        [
            (Literal["a", "b"], "a", "a"),
            (str | None, "hello", "hello"),
            (Color, "red", Color.RED),
        ],
        ids=["literal", "optional-str", "str-enum"],
    )
    def test_accepts_bare_text_for_string_valued_types(self, output_type, raw, expected):
        result = rehydrate_pydantic_output(output_type, raw, serialize_output=False)
        assert result == expected
        assert type(result) is type(expected)

    def test_returns_raw_for_output_function_without_calling_it(self):
        calls = []

        def clean(text: str) -> str:
            calls.append(text)
            return text

        assert rehydrate_pydantic_output(clean, "A", serialize_output=False) == "A"
        assert calls == []

    def test_error_reports_the_text_attempt_for_bare_text_types(self):
        with pytest.raises(ReviewedOutputValidationError, match="Input should be 'a' or 'b'"):
            rehydrate_pydantic_output(Literal["a", "b"], "c", serialize_output=False)

    def test_error_truncates_long_received_text(self):
        with pytest.raises(ReviewedOutputValidationError) as exc_info:
            rehydrate_pydantic_output(int, "x" * 500, serialize_output=False)

        assert "x" * 200 + "..." in str(exc_info.value)
        assert "x" * 201 not in str(exc_info.value)

    @pytest.mark.parametrize(
        ("output_type", "raw", "type_name"),
        [
            (int, "thirty-one", "int"),
            (list[str], "a, b, c", "list[str]"),
            (A, '{"y": "no-x-field"}', "A"),
        ],
        ids=["int", "list", "basemodel"],
    )
    def test_raises_when_type_adapter_rejects(self, output_type, raw, type_name):
        with pytest.raises(ReviewedOutputValidationError) as exc_info:
            rehydrate_pydantic_output(output_type, raw, serialize_output=False)

        message = str(exc_info.value)
        assert f"output_type {type_name}." in message
        assert repr(raw) in message
