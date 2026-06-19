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

from airflow_shared.dagnode.ui_color import is_chakra_color_token


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("blue.500", True),
        ("red.50", True),
        ("corporate.500", True),
        ("team1.300", True),
        ("#fff", False),
        ("#e8b7e4", False),
        ("CornflowerBlue", False),
        ("blue", False),
        ("rgb(1, 2, 3)", False),
        ("", False),
        (None, False),
        ("blue.", False),
        (".500", False),
        ("blue.500.foo", False),
        ("blue.5e2", False),
        # Non-ASCII digit/letter must not slip through the plain checks.
        ("blue.²", False),
        ("1blue.500", False),
    ],
)
def test_is_chakra_color_token(value, expected):
    assert is_chakra_color_token(value) is expected
