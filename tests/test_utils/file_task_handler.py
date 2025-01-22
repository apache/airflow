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

from contextlib import suppress

from airflow.utils.log.file_task_handler import (
    _parse_timestamp,
)


def log_str_to_parsed_log_stream(log_sample: str):
    lines = log_sample.splitlines()
    timestamp = None
    next_timestamp = None
    for idx, line in enumerate(lines):
        if line:
            with suppress(Exception):
                # next_timestamp unchanged if line can't be parsed
                next_timestamp = _parse_timestamp(line)
            if next_timestamp:
                timestamp = next_timestamp
            yield timestamp, idx, line


def mark_test_for_stream_based_read_log_method(func):
    """Test case for new stream-based read log method"""
    return func


def mark_test_for_old_read_log_method(func):
    """Test case for old read log method"""
    return func
