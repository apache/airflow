#
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

from airflow_shared.timezones.rfc3339 import is_iso8601_datetime, is_rfc3339_datetime


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2024-01-01T00:00:00+00:00", True),
        ("2024-01-01T00:00:00Z", True),
        ("2024-01-01t00:00:00z", True),
        ("2024-01-01T00:00:00.123456Z", True),
        ("2024-02-29T00:00:00Z", True),
        ("2024-01-01T00:00:00-23:59", True),
        ("2024-01-01T00:00:00", False),
        ("2024-01-01 00:00:00Z", False),
        ("2024-01-01", False),
        ("", False),
        ("garbage", False),
        ("{{ ds }}", False),
        ("2024-01-01T00:00Z", False),
        ("2024-01-01T24:00:00Z", False),
        ("2024-01-01T00:60:00Z", False),
        ("2024-01-01T00:00:60Z", False),
        ("2024-13-01T00:00:00Z", False),
        ("2023-02-29T00:00:00Z", False),
        ("0000-01-01T00:00:00Z", False),
        ("2024-01-01T00:00:00+0000", False),
        ("2024-01-01T00:00:00+24:00", False),
        ("2024-01-01T00:00:00+00:60", False),
        ("2024-01-01T00:00:00Z\n", False),
        ("2024-01-01T0٠:00:00Z", False),
        (None, True),
        (1, True),
        ([], True),
    ],
)
def test_is_rfc3339_datetime(value, expected):
    assert is_rfc3339_datetime(value) is expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2024-01-01T00:00:00+00:00", True),
        ("2024-01-01T00:00:00", True),
        ("2024-01-01 00:00:00+00:00", True),
        ("2024-01-01T00:00Z", True),
        ("2024-01-01", False),
        ("03:04:05", False),
        ("", False),
        ("garbage", False),
        ("{{ ds }}", False),
    ],
)
def test_is_iso8601_datetime(value, expected):
    assert is_iso8601_datetime(value) is expected
