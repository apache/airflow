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

import datetime
from typing import TYPE_CHECKING, Any, cast

from airflow.utils.module_loading import qualname

if TYPE_CHECKING:
    from airflow.serialization.serde import U


serializers = [
    "pendulum.tz.timezone.FixedTimezone",
    "pendulum.tz.timezone.Timezone",
    "zoneinfo.ZoneInfo",
]

deserializers = serializers

__version__ = 1


def serialize(o: object) -> tuple[U, str, int, bool]:
    """
    Encode a Pendulum Timezone for serialization.

    Airflow only supports timezone objects that implements Pendulum's Timezone
    interface. We try to keep as much information as possible to make conversion
    round-tripping possible (see ``decode_timezone``). We need to special-case
    UTC; Pendulum implements it as a FixedTimezone (i.e. it gets encoded as
    0 without the special case), but passing 0 into ``pendulum.timezone`` does
    not give us UTC (but ``+00:00``).
    """
    from pendulum.tz.timezone import FixedTimezone

    name = qualname(o)

    if isinstance(o, FixedTimezone):
        if o.offset == 0:
            return "UTC", name, __version__, True
        return o.offset, name, __version__, True

    tz_name = _get_tzinfo_name(cast("datetime.tzinfo", o))
    if tz_name is not None:
        return tz_name, name, __version__, True

    if cast("datetime.tzinfo", o).utcoffset(None) == datetime.timedelta(0):
        return "UTC", qualname(FixedTimezone), __version__, True

    return "", "", 0, False


def deserialize(classname: str, version: int, data: object) -> Any:
    from airflow.utils.timezone import parse_timezone

    if not isinstance(data, (str, int)):
        raise TypeError(f"{data} is not of type int or str but of {type(data)}")

    if version > __version__:
        raise TypeError(f"serialized {version} of {classname} > {__version__}")

    if classname == "backports.zoneinfo.ZoneInfo" and isinstance(data, str):
        from zoneinfo import ZoneInfo

        return ZoneInfo(data)

    return parse_timezone(data)


# ported from pendulum.tz.timezone._get_tzinfo_name
def _get_tzinfo_name(tzinfo: datetime.tzinfo | None) -> str | None:
    if tzinfo is None:
        return None

    if hasattr(tzinfo, "key"):
        # zoneinfo timezone
        return tzinfo.key
    if hasattr(tzinfo, "name"):
        # Pendulum timezone
        return tzinfo.name
    if hasattr(tzinfo, "zone"):
        # pytz timezone
        return tzinfo.zone  # type: ignore[no-any-return]

    return None
