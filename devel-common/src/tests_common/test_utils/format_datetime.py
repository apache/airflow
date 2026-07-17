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

from datetime import datetime
from typing import overload


@overload
def from_datetime_to_zulu(dt: datetime) -> str: ...
@overload
def from_datetime_to_zulu(dt: None) -> None: ...
def from_datetime_to_zulu(dt: datetime | None) -> str | None:
    """
    Format a UTC datetime to an ISO-8601 Zulu string with milliseconds.

    - If `dt` is None, return None (useful for optional fields in tests).
    - This function assumes `dt` already represents UTC (e.g. tzinfo=UTC) and
      does not perform timezone conversion.
    """
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@overload
def from_datetime_to_zulu_without_ms(dt: datetime) -> str: ...
@overload
def from_datetime_to_zulu_without_ms(dt: None) -> None: ...
def from_datetime_to_zulu_without_ms(dt: datetime | None) -> str | None:
    """
    Format a UTC datetime to an ISO-8601 Zulu string without milliseconds.

    - If `dt` is None, return None (useful for optional fields in tests).
    - This function assumes `dt` already represents UTC and does not convert tz.
    """
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
