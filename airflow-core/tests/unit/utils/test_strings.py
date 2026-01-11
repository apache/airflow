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

from airflow.utils.strings import to_boolean


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
