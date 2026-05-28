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

from airflow.providers.common.ai.utils.output_type import iter_base_model_classes


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
