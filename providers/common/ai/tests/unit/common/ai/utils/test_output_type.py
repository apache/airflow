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

from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Literal, NamedTuple

import pytest
from pydantic import BaseModel, ConfigDict, Field, computed_field

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError
from airflow.providers.common.ai.utils.output_type import dump_output_to_json, rehydrate_pydantic_output


class A(BaseModel):
    x: int


class Color(str, Enum):
    RED = "red"


class ValidationAlias(BaseModel):
    user_name: str = Field(alias="userName")


class SerializationAlias(BaseModel):
    user_name: str = Field(serialization_alias="userName")


class SerializedByAlias(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)
    user_name: str = Field(serialization_alias="userName")


class ComputedForbidExtra(BaseModel):
    model_config = ConfigDict(extra="forbid")
    a: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def double(self) -> int:
        return self.a * 2


class Address(BaseModel):
    city: str


class Order(BaseModel):
    billing: Address
    shipping: Address


class NoSchema:
    """A plain class pydantic cannot build a schema for."""


@dataclass
class UnresolvedForwardReference:
    value: Missing  # type: ignore[name-defined]  # noqa: F821


class Point(NamedTuple):
    x: int
    y: str


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
            (str | None, "hello\n", "hello\n"),
            (Literal["a", "b"], "b\n", "b"),
            (Color, " red ", Color.RED),
        ],
        ids=[
            "literal",
            "optional-str",
            "str-enum",
            "optional-str-keeps-its-newline",
            "literal-with-newline",
            "str-enum-with-spaces",
        ],
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

        def route(order: Order) -> str:
            calls.append(order.billing.city)
            return order.billing.city

        assert rehydrate_pydantic_output(clean, "Paris", serialize_output=False) == "Paris"
        assert rehydrate_pydantic_output(route, "Paris", serialize_output=False) == "Paris"
        assert calls == []

    @pytest.mark.parametrize("output_type", [NoSchema, UnresolvedForwardReference])
    def test_returns_raw_for_type_pydantic_cannot_build_a_schema_for(self, output_type):
        assert rehydrate_pydantic_output(output_type, "anything", serialize_output=False) == "anything"

    @pytest.mark.parametrize("raw", ['"hi"', "null", "42"])
    def test_returns_any_text_for_a_str_output_type_unparsed(self, raw):
        assert rehydrate_pydantic_output(str, raw, serialize_output=False) == raw

    @pytest.mark.parametrize("output_type", [Point, Annotated[Point, "x"]], ids=["bare", "annotated"])
    def test_validates_a_named_tuple_even_though_its_schema_is_a_call(self, output_type):
        result = rehydrate_pydantic_output(output_type, '[1, "a"]', serialize_output=False)

        assert result == Point(1, "a")
        assert type(result) is Point

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [('{"user_name": "kax"}', "kax"), ('{"userName": "kax"}', "kax")],
        ids=["field-name", "alias"],
    )
    def test_accepts_a_field_name_or_an_alias(self, raw, expected):
        result = rehydrate_pydantic_output(ValidationAlias, raw, serialize_output=False)

        assert result.user_name == expected

    def test_returns_raw_with_a_warning_for_an_unedited_output_that_does_not_validate_back(self, caplog):
        assert rehydrate_pydantic_output(int, "null", serialize_output=False, is_edited=False) == "null"
        assert any("Returning the output unchanged" in message for message in caplog.messages)

    def test_error_truncates_long_received_text(self):
        with pytest.raises(ReviewedOutputValidationError) as exc_info:
            rehydrate_pydantic_output(int, "x" * 500, serialize_output=False)

        assert "x" * 200 + "..." in str(exc_info.value)
        assert "x" * 201 not in str(exc_info.value)

    @pytest.mark.parametrize(
        ("output_type", "raw", "type_name", "detail"),
        [
            (int, "thirty-one", "int", "valid integer"),
            (list[str], "a, b, c", "list[str]", "Invalid JSON"),
            (A, '{"y": "no-x-field"}', "A", "Field required"),
        ],
        ids=["int", "list", "basemodel"],
    )
    def test_raises_when_type_adapter_rejects(self, output_type, raw, type_name, detail):
        with pytest.raises(ReviewedOutputValidationError) as exc_info:
            rehydrate_pydantic_output(output_type, raw, serialize_output=False)

        message = str(exc_info.value)
        assert f"output_type {type_name}." in message
        assert repr(raw) in message
        assert detail in message

    @pytest.mark.parametrize(
        ("output_type", "output"),
        [
            (ValidationAlias, ValidationAlias(userName="kax")),
            (SerializationAlias, SerializationAlias(user_name="kax")),
            (SerializedByAlias, SerializedByAlias(user_name="kax")),
            (list[ValidationAlias], [ValidationAlias(userName="kax")]),
            (ComputedForbidExtra, ComputedForbidExtra(a=1)),
            (list[ComputedForbidExtra], [ComputedForbidExtra(a=1)]),
        ],
        ids=[
            "validation-alias",
            "serialization-alias",
            "serialize-by-alias",
            "list-of-aliased",
            "computed-field-forbid-extra",
            "list-of-computed-field-forbid-extra",
        ],
    )
    def test_a_dumped_output_validates_back(self, output_type, output):
        raw = dump_output_to_json(output)

        assert rehydrate_pydantic_output(output_type, raw, serialize_output=False) == output

    @pytest.mark.parametrize(
        ("output_type", "raw", "detail"),
        [
            (A, '{"x": 1,}', "trailing comma"),
            (list[str], '["a","b",]', "trailing comma"),
            (Literal["a", "b"], "c", "Input should be 'a' or 'b'"),
        ],
        ids=["model-typo", "list-typo", "bare-text"],
    )
    def test_error_reports_the_json_error_for_a_json_typo_and_the_text_error_otherwise(
        self, output_type, raw, detail
    ):
        with pytest.raises(ReviewedOutputValidationError, match=detail):
            rehydrate_pydantic_output(output_type, raw, serialize_output=False)
