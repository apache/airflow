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
from task_sdk.definitions.conftest import make_xcom_arg

from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput, ListOfDictsExpandInput


class TestExpandInput:
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
            ({"a": [1, 2], "b": [10, 20]}, [{"a": 1, "b": 10}, {"a": 2, "b": 20}]),
            ({"a": (x for x in [1, 2])}, [{"a": 1}, {"a": 2}]),
            ({"a": make_xcom_arg([1, 2])}, [{"a": 1}, {"a": 2}]),
        ],
    )
    def test_dict_of_lists_expand_input_iter_values(self, actual, expected):
        result = list(DictOfListsExpandInput(actual).iter_values({}))
        assert result == expected

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
            ([make_xcom_arg([{"a": 1}, {"a": 2}])], [{"a": 1}, {"a": 2}]),  # XComArg input
        ],
    )
    def test_list_of_dicts_expand_input_iter_values(self, actual, expected):
        result = list(ListOfDictsExpandInput(actual).iter_values({}))
        assert result == expected
