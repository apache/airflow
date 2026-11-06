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

import pytest
from ci.prek.check_i18n_json import natural_sort_key, sort_keys_naturally


class TestNaturalSortKey:
    # Pairs in the order the `natural-compare` npm package that `jsonc/sort-keys` used puts them.
    @pytest.mark.parametrize(
        ("first", "second"),
        [
            ("B", "a"),
            ("a", "ab"),
            ("a2", "a10"),
            ("x9y", "x10y"),
            ("a01", "a1"),
            ("a0", "a1"),
            ("item010", "item2x"),
            ("a b", "a.b"),
            ("a.b", "a-b"),
            ("a.b", "a_b"),
            ("a_b", "ab"),
            ("dag_run", "dagRun"),
            ("_x", "Z9"),
            ("~a", "z9"),
        ],
    )
    def test_orders_like_natural_compare(self, first, second):
        assert natural_sort_key(first) < natural_sort_key(second)


class TestSortKeysNaturally:
    def test_sorts_nested_objects_and_objects_in_arrays(self):
        value = {
            "item10": 1,
            "item2": {"b": 1, "a": 2},
            "Item": [{"z": 1, "y": 2}, "keep"],
        }

        result = sort_keys_naturally(value)

        assert json.dumps(result) == json.dumps(
            {
                "Item": [{"y": 2, "z": 1}, "keep"],
                "item2": {"a": 2, "b": 1},
                "item10": 1,
            }
        )
