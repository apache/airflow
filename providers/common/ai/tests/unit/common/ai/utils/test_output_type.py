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

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Literal, NamedTuple
from unittest import mock

import pytest
from pydantic import BaseModel, ConfigDict, Field, computed_field, model_validator
from pydantic_ai import NativeOutput, PromptedOutput, ToolOutput

from airflow.providers.common.ai.exceptions import ReviewedOutputValidationError
from airflow.providers.common.ai.utils.output_type import (
    _is_container_mismatch,
    dump_output_to_json,
    is_model_dump_required,
    rehydrate_pydantic_output,
)


class A(BaseModel):
    x: int


class SubA(A):
    pass


class B(BaseModel):
    y: str


class FromAttributesModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    x: int


@dataclass
class DataclassOutput:
    a: int


class Color(str, Enum):
    RED = "red"


class ValidationAlias(BaseModel):
    user_name: str = Field(alias="userName")


class SerializationAlias(BaseModel):
    user_name: str = Field(serialization_alias="userName")


class SerializedByAlias(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)
    user_name: str = Field(serialization_alias="userName")


class DictOnlyBeforeValidator(BaseModel):
    """A model whose before-validator assumes dict input, so it breaks on a bare string."""

    amount: int

    @model_validator(mode="before")
    @classmethod
    def default_amount(cls, data):
        amount = data.get("amount", 0)
        return {**data, "amount": amount}


class ValidatorWithOperationalError(BaseModel):
    """A model whose validator fails for a reason that has nothing to do with the review text."""

    amount: int

    @model_validator(mode="before")
    @classmethod
    def look_up_amount(cls, data):
        raise TimeoutError("lookup service did not answer")


class AliasNamedLikeAnotherField(BaseModel):
    """A model that validates by name only, with an alias that is another field's name."""

    model_config = ConfigDict(validate_by_alias=False, validate_by_name=True)
    source: str
    destination: str = Field(alias="source")


class IndexingBeforeValidator(BaseModel):
    """A model whose before-validator indexes into its input, so a missing key or item raises."""

    first: str

    @model_validator(mode="before")
    @classmethod
    def take_first_item(cls, data):
        return {"first": data["items"][0]}


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
    # Reuses Address, so pydantic wraps a function taking an Order in a ``definitions`` schema.
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


class TestIsModelDumpRequired:
    @pytest.mark.parametrize(
        ("output", "output_type", "serialize_output", "expected"),
        [
            (A(x=1), [A, int], False, True),
            (A(x=1), (A, int), False, True),
            (A(x=1), ToolOutput(A), False, True),
            (A(x=1), A, False, False),
            (A(x=1), A | int, False, False),
            (A(x=1), list[A], False, False),
            (A(x=1), A, True, True),
            (SubA(x=1), A, False, True),
            (5, [A, int], False, False),
        ],
        ids=[
            "list-of-types",
            "tuple-of-types",
            "output-marker",
            "single-model",
            "union",
            "generic-list",
            "serialize-output",
            "subclass-instance",
            "not-a-model",
        ],
    )
    def test_is_true_when_the_worker_registers_no_class_for_the_output(
        self, output, output_type, serialize_output, expected
    ):
        result = is_model_dump_required(output, output_type, serialize_output=serialize_output)

        assert result is expected

    @mock.patch("airflow.providers.common.ai.utils.output_type.iter_pydantic_models", None)
    def test_is_true_without_the_sdk_walker(self):
        assert is_model_dump_required(A(x=1), A, serialize_output=False) is True


class TestRehydratePydanticOutput:
    def test_returns_model_instance(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=False)
        assert isinstance(result, A)
        assert result.x == 7

    def test_returns_dict_when_serialize_output(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=True)
        assert result == {"x": 7}

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

    @pytest.mark.parametrize(
        ("output_type", "raw", "expected"),
        [
            ([str, int], '"x"', "x"),
            ([int, float], "1.5", 1.5),
            ((int, float), "2", 2),
            ([A, int], '{"x": 7}', {"x": 7}),
        ],
        ids=["str-int-string", "int-float", "tuple", "model-in-a-list-of-types-is-dumped"],
    )
    def test_validates_list_output_type_as_union(self, output_type, raw, expected):
        assert rehydrate_pydantic_output(output_type, raw, serialize_output=False) == expected

    @pytest.mark.parametrize(
        "output_type",
        [[], [ToolOutput(int), ToolOutput(str)]],
        ids=["empty-list", "unsupported-operand"],
    )
    def test_returns_raw_when_list_cannot_be_unioned(self, output_type):
        assert rehydrate_pydantic_output(output_type, "1", serialize_output=False) == "1"

    def test_rejects_edit_matching_no_list_candidate(self):
        with pytest.raises(ReviewedOutputValidationError):
            rehydrate_pydantic_output([int, float], '"x"', serialize_output=False)

    @pytest.mark.parametrize(
        "takes_an_order", [False, True], ids=["call-schema", "definitions-wrapped-schema"]
    )
    def test_returns_raw_for_output_function_without_calling_it(self, takes_an_order):
        calls = []

        def clean(text: str) -> str:
            calls.append(text)
            return text

        def route(order: Order) -> str:
            calls.append(order.billing.city)
            return order.billing.city

        output_function = route if takes_an_order else clean

        assert rehydrate_pydantic_output(output_function, "Paris", serialize_output=False) == "Paris"
        assert calls == []

    @pytest.mark.parametrize("output_type", [NoSchema, UnresolvedForwardReference])
    def test_returns_raw_for_type_pydantic_cannot_build_a_schema_for(self, output_type):
        assert rehydrate_pydantic_output(output_type, "anything", serialize_output=False) == "anything"

    @pytest.mark.parametrize(
        "output_type",
        [ToolOutput(A), NativeOutput(A), PromptedOutput(A)],
        ids=["tool-output", "native-output", "prompted-output"],
    )
    @pytest.mark.parametrize("is_edited", [False, True], ids=["unedited", "edited"])
    def test_returns_raw_for_a_pydantic_ai_output_marker(self, output_type, is_edited):
        result = rehydrate_pydantic_output(
            output_type, '{"x": 1}', serialize_output=False, is_edited=is_edited
        )

        assert result == '{"x": 1}'

    @pytest.mark.parametrize(
        "raw",
        ["anything", '"hi"', "null", "42"],
        ids=["plain-text", "json-string", "json-null", "json-number"],
    )
    def test_returns_any_text_for_a_str_output_type_unparsed(self, raw):
        assert rehydrate_pydantic_output(str, raw, serialize_output=False) == raw

    @pytest.mark.parametrize("output_type", [Point, Annotated[Point, "x"]], ids=["bare", "annotated"])
    def test_validates_a_named_tuple_even_though_its_schema_is_a_call(self, output_type):
        result = rehydrate_pydantic_output(output_type, '[1, "a"]', serialize_output=False)

        assert result == Point(1, "a")
        assert type(result) is Point

    @pytest.mark.parametrize(
        "raw", ['{"user_name": "kax"}', '{"userName": "kax"}'], ids=["field-name", "alias"]
    )
    def test_accepts_a_field_name_or_an_alias(self, raw):
        result = rehydrate_pydantic_output(ValidationAlias, raw, serialize_output=False)

        assert type(result) is ValidationAlias
        assert result.user_name == "kax"

    def test_returns_raw_with_a_warning_for_an_unedited_output_that_does_not_validate_back(self, caplog):
        assert rehydrate_pydantic_output(int, "null", serialize_output=False, is_edited=False) == "null"
        assert any("Returning the output unchanged" in message for message in caplog.messages)

    def test_error_truncates_long_received_text(self):
        with pytest.raises(ReviewedOutputValidationError) as exc_info:
            rehydrate_pydantic_output(int, "x" * 500, serialize_output=False)

        assert "x" * 200 + "..." in str(exc_info.value)
        assert "x" * 201 not in str(exc_info.value)

    @pytest.mark.parametrize(
        ("raw", "detail"),
        [
            ('{"amount": "ten"}', "valid integer"),
            ('{"amount": 1,}', "trailing comma"),
            ('["x"]', "has no attribute 'get'"),
            ('"x"', "has no attribute 'get'"),
            ("42", "has no attribute 'get'"),
        ],
        ids=["invalid-field", "json-typo", "json-array", "json-string", "json-number"],
    )
    def test_a_before_validator_failing_on_bare_text_reports_the_json_error(self, raw, detail):
        with pytest.raises(ReviewedOutputValidationError, match=detail):
            rehydrate_pydantic_output(DictOnlyBeforeValidator, raw, serialize_output=False)

    @pytest.mark.parametrize(
        ("raw", "detail"),
        [("{}", "'items'"), ('{"items": []}', "list index out of range")],
        ids=["key-error", "index-error"],
    )
    def test_a_before_validator_indexing_a_wrong_shape_reports_a_failed_conversion(self, raw, detail):
        with pytest.raises(ReviewedOutputValidationError, match=detail):
            rehydrate_pydantic_output(IndexingBeforeValidator, raw, serialize_output=False)

    @pytest.mark.parametrize("is_edited", [False, True], ids=["unedited", "edited"])
    def test_a_validator_error_unrelated_to_the_text_propagates(self, is_edited):
        with pytest.raises(TimeoutError, match="lookup service"):
            rehydrate_pydantic_output(
                ValidatorWithOperationalError, '{"amount": 1}', serialize_output=False, is_edited=is_edited
            )

    def test_a_before_validator_failing_on_bare_text_leaves_an_unedited_output_unchanged(self):
        raw = '{"amount": "ten"}'

        result = rehydrate_pydantic_output(
            DictOnlyBeforeValidator, raw, serialize_output=False, is_edited=False
        )

        assert result == raw

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
            (A | B, '{"x": 1,}', "trailing comma"),
            (list[int] | dict[str, int], "[1,]", "trailing comma"),
            (DataclassOutput, '{"a": 1,}', "trailing comma"),
            (frozenset[int], "[1,]", "trailing comma"),
            (Point, '[1, "a",]', "trailing comma"),
            (Literal["a", "b"], "c", "Input should be 'a' or 'b'"),
            (A | int, "hello", "valid integer"),
            (Literal["a", "b"] | A, '{"x": 1,}', "trailing comma"),
            (Literal["a", "b"] | A, ' {"x": 1,}', "trailing comma"),
            (int, "3.5", "fractional part"),
        ],
        ids=[
            "model-typo",
            "list-typo",
            "model-union-typo",
            "container-union-typo",
            "dataclass-typo",
            "frozenset-typo",
            "named-tuple-typo",
            "bare-text",
            "bare-text-for-a-model-or-int",
            "json-typo-for-a-literal-or-model",
            "json-typo-after-whitespace",
            "valid-json-of-the-wrong-kind",
        ],
    )
    def test_error_reports_the_json_error_for_a_json_typo_and_the_text_error_otherwise(
        self, output_type, raw, detail
    ):
        with pytest.raises(ReviewedOutputValidationError, match=detail):
            rehydrate_pydantic_output(output_type, raw, serialize_output=False)

    @pytest.mark.parametrize(
        "output_type",
        [
            A,
            FromAttributesModel,
            DataclassOutput,
            dict[str, int],
            list[int],
            tuple[int, str],
            set[int],
            frozenset[int],
            Point,
            deque[int],
            Sequence[int],
        ],
        ids=[
            "model_type",
            "model_attributes_type",
            "dataclass_type",
            "dict_type",
            "list_type",
            "tuple_type",
            "set_type",
            "frozen_set_type",
            "arguments_or_named_tuple_type",
            "list_or_deque_type",
            "sequence_str",
        ],
    )
    def test_error_reports_the_json_error_for_plain_text_sent_to_a_structured_type(self, output_type):
        # Each id is the error pydantic raises when the whole text meets the type (pydantic 2.14 renamed
        # the NamedTuple and deque ones). The JSON error says more.
        with pytest.raises(ReviewedOutputValidationError, match="Invalid JSON"):
            rehydrate_pydantic_output(output_type, "hello", serialize_output=False)

    def test_a_union_with_str_keeps_an_edit_that_is_not_the_model_as_text(self):
        raw = '{"x": "not-a-number"}'

        assert rehydrate_pydantic_output(A | str, raw, serialize_output=False) == raw


def test_a_model_that_turned_off_validation_by_alias_reads_its_dump_back_unchanged():
    output = AliasNamedLikeAnotherField(source="SRC", destination="DST")

    result = rehydrate_pydantic_output(
        AliasNamedLikeAnotherField, dump_output_to_json(output), serialize_output=False, is_edited=False
    )

    assert result == output


class TestIsContainerMismatch:
    @pytest.mark.parametrize(
        "error_type",
        [
            "model_type",
            "dict_type",
            "list_type",
            "arguments_type",
            # Names pydantic 2.14 introduced, which the locked pydantic cannot raise.
            "named_tuple_type",
            "deque_type",
            "sequence_str",
        ],
    )
    def test_a_structured_type_refusing_the_whole_text(self, error_type):
        assert _is_container_mismatch(error_type)

    @pytest.mark.parametrize(
        "error_type", ["int_parsing", "bool_parsing", "uuid_parsing", "literal_error", "enum", "json_invalid"]
    )
    def test_a_type_that_takes_text_rejecting_its_value(self, error_type):
        assert not _is_container_mismatch(error_type)
