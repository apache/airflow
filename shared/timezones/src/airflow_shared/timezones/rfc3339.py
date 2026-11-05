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

import re
from datetime import date, datetime

from pendulum.parsing import parse_iso8601

_RFC3339_DATETIME = re.compile(
    r"\d{4}-(?:0[1-9]|1[0-2])-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?"
    r"(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)",
    re.ASCII,
)


# jsonschema checks "date-time" only when the unmaintained ``rfc3339-validator`` is installed.
def is_rfc3339_datetime(value: object) -> bool:
    """
    Return whether ``value`` is an RFC 3339 date-time string, as a JSON Schema format checker.

    The offset is required, ``T`` and ``Z`` are case-insensitive, and fractional seconds are allowed.
    Leap seconds (``:60``) are rejected because ``datetime`` cannot represent them.
    Non-strings return ``True``: a format applies only to strings, ``type`` rejects the rest.
    """
    if not isinstance(value, str):
        return True
    value = value.upper()
    if _RFC3339_DATETIME.fullmatch(value) is None:
        return False
    try:
        date.fromisoformat(value[:10])
    except ValueError:
        return False
    return True


def is_iso8601_datetime(value: str) -> bool:
    """Return whether ``value`` is an ISO 8601 date-time, the Airflow 2 fallback for non-RFC 3339 values."""
    try:
        return isinstance(parse_iso8601(value), datetime)
    except ValueError:
        return False
