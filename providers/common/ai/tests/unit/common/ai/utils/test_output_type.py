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

from pydantic import BaseModel

from airflow.providers.common.ai.utils.output_type import (
    iter_base_model_classes,
    rehydrate_pydantic_output,
)


class A(BaseModel):
    x: int


class B(BaseModel):
    y: str


class C(BaseModel):
    z: float


class TestIterBaseModelClasses:
    def test_single_class(self):
        assert set(iter_base_model_classes(A)) == {A}

    def test_str_skipped(self):
        assert set(iter_base_model_classes(str)) == set()

    def test_optional(self):
        assert set(iter_base_model_classes(A | None)) == {A}

    def test_union(self):
        assert set(iter_base_model_classes(A | B)) == {A, B}

    def test_list_of_models(self):
        assert set(iter_base_model_classes(list[A])) == {A}

    def test_dict_with_model_values(self):
        assert set(iter_base_model_classes(dict[str, A])) == {A}

    def test_nested_union_list_optional(self):
        assert set(iter_base_model_classes(list[A | B | None])) == {A, B}

    def test_mixed_with_primitives(self):
        assert set(iter_base_model_classes(A | str | int | B)) == {A, B}

    def test_three_models(self):
        assert set(iter_base_model_classes(A | B | C)) == {A, B, C}


class TestRehydratePydanticOutput:
    def test_returns_model_instance(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=False)
        assert isinstance(result, A)
        assert result.x == 7

    def test_returns_dict_when_serialize_output(self):
        result = rehydrate_pydantic_output(A, '{"x": 7}', serialize_output=True)
        assert result == {"x": 7}

    def test_returns_raw_for_non_basemodel(self):
        result = rehydrate_pydantic_output(str, "anything", serialize_output=False)
        assert result == "anything"

    def test_returns_raw_on_invalid_json(self):
        result = rehydrate_pydantic_output(A, "not-json", serialize_output=False)
        assert result == "not-json"

    def test_returns_raw_on_schema_mismatch(self):
        # ``A`` requires ``x: int`` -- this payload should fail validation
        result = rehydrate_pydantic_output(A, '{"y": "no-x-field"}', serialize_output=False)
        assert result == '{"y": "no-x-field"}'
