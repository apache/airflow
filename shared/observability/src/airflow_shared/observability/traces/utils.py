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

import calendar
import logging

TRACE_ID = 0
SPAN_ID = 16

log = logging.getLogger(__name__)


def datetime_to_nano(datetime) -> int | None:
    """Convert datetime to nanoseconds."""
    if datetime:
        if datetime.tzinfo is None:
            # There is no timezone info, handle it the same as UTC.
            timestamp = calendar.timegm(datetime.timetuple()) + datetime.microsecond / 1e6
        else:
            # The datetime is timezone-aware. Use timestamp directly.
            timestamp = datetime.timestamp()
        return int(timestamp * 1e9)
    return None


def parse_traceparent(traceparent_str: str | None = None) -> dict:
    """Parse traceparent string: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01."""
    if traceparent_str is None:
        return {}
    tokens = traceparent_str.split("-")
    if len(tokens) != 4:
        raise ValueError("The traceparent string does not have the correct format.")
    return {"version": tokens[0], "trace_id": tokens[1], "parent_id": tokens[2], "flags": tokens[3]}


def parse_tracestate(tracestate_str: str | None = None) -> dict:
    """Parse tracestate string: rojo=00f067aa0ba902b7,congo=t61rcWkgMzE."""
    if tracestate_str is None or len(tracestate_str) == 0:
        return {}
    tokens = tracestate_str.split(",")
    result = {}
    for pair in tokens:
        if "=" in pair:
            key, value = pair.split("=")
            result[key.strip()] = value.strip()
    return result
