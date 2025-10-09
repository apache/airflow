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

import itertools
from collections.abc import Generator, Iterable
from datetime import datetime
from typing import TYPE_CHECKING

import pendulum

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if TYPE_CHECKING:
    from airflow.utils.log.file_task_handler import ParsedLog, StructuredLogMessage


def extract_events(logs: Iterable[StructuredLogMessage], skip_source_info=True) -> list[str]:
    """Helper function to return just the event (a.k.a message) from a list of StructuredLogMessage"""
    logs = iter(logs)
    if skip_source_info:

        def is_source_group(log: StructuredLogMessage) -> bool:
            return not hasattr(log, "timestamp") or log.event == "::endgroup::" or hasattr(log, "sources")

        logs = itertools.dropwhile(is_source_group, logs)

    return [s.event for s in logs]


def convert_list_to_stream(input_list: list[str]) -> Generator[str, None, None]:
    """
    Convert a list of strings to a stream-like object.
    This function yields each string in the list one by one.
    """
    yield from input_list


def mock_parsed_logs_factory(
    event_prefix: str,
    start_datetime: datetime,
    count: int,
) -> list[ParsedLog]:
    """
    Create a list of ParsedLog objects with the specified start datetime and count.
    Each ParsedLog object contains a timestamp and a list of StructuredLogMessage objects.
    """
    if AIRFLOW_V_3_0_PLUS:
        from airflow.utils.log.file_task_handler import StructuredLogMessage

    return [
        (
            pendulum.instance(start_datetime + pendulum.duration(seconds=i)),
            i,
            StructuredLogMessage(
                timestamp=pendulum.instance(start_datetime + pendulum.duration(seconds=i)),
                event=f"{event_prefix} Event {i}",
            ),
        )
        for i in range(count)
    ]
