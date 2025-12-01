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

from typing import TYPE_CHECKING

from airflow._shared.timezones.timezone import parse_timezone
from airflow.sdk.serialization.serializers.timezone import (
    deserialize as deserialize_timezone,
    serialize as serialize_timezone,
)
from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    import datetime

    from airflow.sdk.serialization.serde import U

__version__ = 2

serializers = ["datetime.date", "datetime.datetime", "datetime.timedelta", "pendulum.datetime.DateTime"]
deserializers = serializers

TIMESTAMP = "timestamp"
TIMEZONE = "tz"


def serialize(o: object) -> tuple[U, str, int, bool]:
    from datetime import date, datetime, timedelta

    if isinstance(o, datetime):
        qn = qualname(o)

        tz = serialize_timezone(o.tzinfo) if o.tzinfo else None

        return {TIMESTAMP: o.timestamp(), TIMEZONE: tz}, qn, __version__, True

    if isinstance(o, date):
        return o.isoformat(), qualname(o), __version__, True

    if isinstance(o, timedelta):
        return o.total_seconds(), qualname(o), __version__, True

    return "", "", 0, False


def deserialize(cls: type, version: int, data: dict | str) -> datetime.date | datetime.timedelta:
    import datetime

    from pendulum import DateTime

    tz: datetime.tzinfo | None = None
    if isinstance(data, dict) and TIMEZONE in data:
        if version == 1:
            # try to deserialize unsupported timezones
            timezone_mapping = {
                "EDT": parse_timezone(-4 * 3600),
                "CDT": parse_timezone(-5 * 3600),
                "MDT": parse_timezone(-6 * 3600),
                "PDT": parse_timezone(-7 * 3600),
                "CEST": parse_timezone("CET"),
            }
            if data[TIMEZONE] in timezone_mapping:
                tz = timezone_mapping[data[TIMEZONE]]
            else:
                tz = parse_timezone(data[TIMEZONE])
        else:
            tz = (
                deserialize_timezone(data[TIMEZONE][1], data[TIMEZONE][2], data[TIMEZONE][0])
                if data[TIMEZONE]
                else None
            )

    if cls is datetime.datetime and isinstance(data, dict):
        return datetime.datetime.fromtimestamp(float(data[TIMESTAMP]), tz=tz)

    if cls is DateTime and isinstance(data, dict):
        return DateTime.fromtimestamp(float(data[TIMESTAMP]), tz=tz)

    if cls is datetime.timedelta and isinstance(data, str | float):
        return datetime.timedelta(seconds=float(data))

    if cls is datetime.date and isinstance(data, str):
        return datetime.date.fromisoformat(data)

    raise TypeError(f"unknown date/time format {qualname(cls)}")
