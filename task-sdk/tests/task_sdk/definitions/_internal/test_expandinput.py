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

import pytest

from airflow.sdk.definitions._internal.expandinput import (
    BatchedExpandInput,
    DictOfListsExpandInput,
    ListOfDictsExpandInput,
)


class TestExpandInput:
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
            (
                {"a": [1, 2], "b": [10, 20]},
                [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}],
            ),
            ({"a": (x for x in [1, 2])}, [{"a": 1}, {"a": 2}]),
        ],
    )
    def test_dict_of_lists_expand_input_iter_values(self, actual, expected):
        expand_input = DictOfListsExpandInput(actual)

        with pytest.raises(RuntimeError, match="Length of DictOfListsExpandInput is not yet known"):
            len(expand_input)

        result = list(expand_input.iter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    def test_list_of_dicts_expand_input_iter_values(self, actual, expected):
        expand_input = ListOfDictsExpandInput(actual)

        with pytest.raises(RuntimeError, match="Length of ListOfDictsExpandInput is not yet known"):
            len(expand_input)

        result = list(expand_input.iter_values({}))
        assert result == expected
        assert len(expand_input) == len(expected)


class TestBatchedExpandInput:
    @pytest.mark.parametrize(
        "size",
        [pytest.param(-1), pytest.param(0), pytest.param(1)],
    )
    def test_invalid_size_raises(self, size: int):
        inner = DictOfListsExpandInput({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="batch size must be at least 2"):
            BatchedExpandInput(inner, size=size)

    @pytest.mark.parametrize(
        ("size", "map_index", "items", "expected"),
        [
            (2, 0, [1, 2, 3, 4, 5], [1, 3, 5]),
            (2, 1, [1, 2, 3, 4, 5], [2, 4]),
            (3, 0, [1, 2, 3, 4, 5, 6], [1, 4]),
            (3, 1, [1, 2, 3, 4, 5, 6], [2, 5]),
            (3, 2, [1, 2, 3, 4, 5, 6], [3, 6]),
        ],
    )
    def test_iter_values_striding(self, size: int, map_index: int, items: list, expected: list):
        inner = DictOfListsExpandInput({"a": items})
        batched = BatchedExpandInput(inner, size=size)
        context = {"ti": type("TI", (), {"map_index": map_index})()}

        with pytest.raises(RuntimeError, match="Length of BatchedExpandInput is not yet known"):
            len(batched)

        result = [combo["a"] for combo in batched.iter_values(context)]
        assert result == expected
        assert len(batched) == len(expected)
        # delegate length must remain the full item count, not the batch slice
        assert len(inner) == len(items)
