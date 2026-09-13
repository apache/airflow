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

import logging

import pendulum
import pytest

from airflow_shared.search.formatter import ISO8601Formatter


@pytest.mark.parametrize(
    ("timezone", "timestamp", "datefmt", "expected"),
    [
        ("UTC", 0.123, None, "1970-01-01T00:00:00.123+0000"),
        ("Asia/Hong_Kong", 0.123, None, "1970-01-01T08:00:00.123+0800"),
        ("UTC", 0.123, "%Y/%m/%d", "1970/01/01.123+0000"),
        ("America/New_York", 1710053999.123, None, "2024-03-10T01:59:59.123-0500"),
        ("America/New_York", 1710054000.123, None, "2024-03-10T03:00:00.123-0400"),
    ],
)
def test_format_time(timezone, timestamp, datefmt, expected):
    record = logging.makeLogRecord({"created": timestamp, "msecs": 123})
    with pendulum.tz.test_local_timezone(pendulum.timezone(timezone)):
        assert ISO8601Formatter().formatTime(record, datefmt) == expected


def test_format_time_without_milliseconds_or_offset():
    formatter = ISO8601Formatter()
    formatter.default_msec_format = None
    formatter.default_tz_format = ""
    record = logging.makeLogRecord({"created": 0, "msecs": 0})
    with pendulum.tz.test_local_timezone(pendulum.timezone("UTC")):
        assert formatter.formatTime(record) == "1970-01-01T00:00:00"
