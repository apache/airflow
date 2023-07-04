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

from airflow_breeze.utils.exclude_from_matrix import excluded_combos, representative_combos


@pytest.mark.parametrize(
    "list_1, list_2, expected_representative_list",
    [
        (["3.8", "3.9"], ["1", "2"], [("3.8", "1"), ("3.9", "2")]),
        (["3.8", "3.9"], ["1", "2", "3"], [("3.8", "1"), ("3.9", "2"), ("3.8", "3")]),
        (["3.8", "3.9"], ["1", "2", "3", "4"], [("3.8", "1"), ("3.9", "2"), ("3.8", "3"), ("3.9", "4")]),
        (
            ["3.8", "3.9", "3.10"],
            ["1", "2", "3", "4"],
            [("3.8", "1"), ("3.9", "2"), ("3.10", "3"), ("3.8", "4")],
        ),
    ],
)
def test_exclude_from_matrix(
    list_1: list[str],
    list_2: list[str],
    expected_representative_list: dict[str, str],
):
    representative_list = representative_combos(list_1, list_2)
    exclusion_list = excluded_combos(list_1, list_2)
    assert representative_list == expected_representative_list
    assert len(representative_list) == len(list_1) * len(list_2) - len(exclusion_list)
    assert len(set(representative_list) & set(exclusion_list)) == 0
