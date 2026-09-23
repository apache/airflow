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

from airflow.utils.strings import get_random_string, to_boolean


def test_get_random_string_uses_requested_length_and_choices() -> None:
    assert get_random_string(length=3, choices="a") == "aaa"


def test_get_random_string_defaults_to_eight_alphanumeric_characters() -> None:
    value = get_random_string()

    assert len(value) == 8
    assert value.isalnum()


@pytest.mark.parametrize(
    ("input_string", "expected_result"),
    [
        (" yes ", True),
        (" 1\n", True),
        ("\tON", True),
        (" no ", False),
        (" 0\n", False),
        ("\tOFF", False),
    ],
)
def test_to_boolean_strips_whitespace(input_string: str, expected_result: bool) -> None:
    assert to_boolean(input_string) is expected_result


def test_to_boolean_returns_false_for_none() -> None:
    assert to_boolean(None) is False
