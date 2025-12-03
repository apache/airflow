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

import datetime as dt
from importlib import metadata
from typing import TYPE_CHECKING, overload

import pendulum
from dateutil.relativedelta import relativedelta
from packaging import version
from pendulum.datetime import DateTime

if TYPE_CHECKING:
    from pendulum.tz.timezone import FixedTimezone, Timezone


_PENDULUM3 = version.parse(metadata.version("pendulum")).major == 3
# UTC Timezone as a tzinfo instance. Actual value depends on pendulum version:
# - Timezone("UTC") in pendulum 3
# - FixedTimezone(0, "UTC") in pendulum 2
utc = pendulum.UTC


def is_localized(value: dt.datetime) -> bool:
    """
    Determine if a given datetime.datetime is aware.

    The concept is defined in Python documentation. Assuming the tzinfo is
    either None or a proper ``datetime.tzinfo`` instance, ``value.utcoffset()``
    implements the appropriate logic.

    .. seealso:: http://docs.python.org/library/datetime.html#datetime.tzinfo
    """
    return value.utcoffset() is not None


def is_naive(value):
    """
    Determine if a given datetime.datetime is naive.

    The concept is defined in Python documentation. Assuming the tzinfo is
    either None or a proper ``datetime.tzinfo`` instance, ``value.utcoffset()``
    implements the appropriate logic.

    .. seealso:: http://docs.python.org/library/datetime.html#datetime.tzinfo
    """
    return value.utcoffset() is None


def utcnow() -> dt.datetime:
    """Get the current date and time in UTC."""
    return dt.datetime.now(tz=utc)


@overload
def convert_to_utc(value: None) -> None: ...


@overload
def convert_to_utc(value: dt.datetime) -> DateTime: ...


def convert_to_utc(value: dt.datetime | None) -> DateTime | None:
    """
    Create a datetime with the default timezone added if none is associated.

    :param value: datetime
    :return: datetime with tzinfo
    """
    if value is None:
        return value

    if not is_localized(value):
        value = pendulum.instance(value, TIMEZONE)

    return pendulum.instance(value.astimezone(utc))


@overload
def make_aware(value: None, timezone: dt.tzinfo | None = None) -> None: ...


@overload
def make_aware(value: DateTime, timezone: dt.tzinfo | None = None) -> DateTime: ...


@overload
def make_aware(value: dt.datetime, timezone: dt.tzinfo | None = None) -> dt.datetime: ...


def make_aware(value: dt.datetime | None, timezone: dt.tzinfo | None = None) -> dt.datetime | None:
    """
    Make a naive datetime.datetime in a given time zone aware.

    :param value: datetime
    :param timezone: timezone
    :return: localized datetime in settings.TIMEZONE or timezone
    """
    if timezone is None:
        timezone = TIMEZONE

    if not value:
        return None

    # Check that we won't overwrite the timezone of an aware datetime.
    if is_localized(value):
        raise ValueError(f"make_aware expects a naive datetime, got {value}")
    # In case we move clock back we want to schedule the run at the time of the second
    # instance of the same clock time rather than the first one.
    # Fold parameter has no impact in other cases, so we can safely set it to 1 here
    value = value.replace(fold=1)
    localized = getattr(timezone, "localize", None)
    if localized is not None:
        # This method is available for pytz time zones
        return localized(value)
    convert = getattr(timezone, "convert", None)
    if convert is not None:
        # For pendulum
        return convert(value)
    # This may be wrong around DST changes!
    return value.replace(tzinfo=timezone)


def make_naive(value, timezone=None):
    """
    Make an aware datetime.datetime naive in a given time zone.

    :param value: datetime
    :param timezone: timezone
    :return: naive datetime
    """
    if timezone is None:
        timezone = TIMEZONE

    # Emulate the behavior of astimezone() on Python < 3.6.
    if is_naive(value):
        raise ValueError("make_naive() cannot be applied to a naive datetime")

    date = value.astimezone(timezone)

    # cross library compatibility
    naive = dt.datetime(
        date.year, date.month, date.day, date.hour, date.minute, date.second, date.microsecond
    )

    return naive


def datetime(*args, **kwargs):
    """
    Wrap around datetime.datetime to add settings.TIMEZONE if tzinfo not specified.

    :return: datetime.datetime
    """
    if "tzinfo" not in kwargs:
        kwargs["tzinfo"] = TIMEZONE

    return dt.datetime(*args, **kwargs)


def parse(string: str, timezone=None, *, strict=False) -> DateTime:
    """
    Parse a time string and return an aware datetime.

    :param string: time string
    :param timezone: the timezone
    :param strict: if False, it will fall back on the dateutil parser if unable to parse with pendulum
    """
    return pendulum.parse(string, tz=timezone or TIMEZONE, strict=strict)  # type: ignore


@overload
def coerce_datetime(v: None, tz: dt.tzinfo | None = None) -> None: ...


@overload
def coerce_datetime(v: DateTime, tz: dt.tzinfo | None = None) -> DateTime: ...


@overload
def coerce_datetime(v: dt.datetime, tz: dt.tzinfo | None = None) -> DateTime: ...


def coerce_datetime(v: dt.datetime | None, tz: dt.tzinfo | None = None) -> DateTime | None:
    """
    Convert ``v`` into a timezone-aware ``pendulum.DateTime``.

    * If ``v`` is *None*, *None* is returned.
    * If ``v`` is a naive datetime, it is converted to an aware Pendulum DateTime.
    * If ``v`` is an aware datetime, it is converted to a Pendulum DateTime.
      Note that ``tz`` is **not** taken into account in this case; the datetime
      will maintain its original tzinfo!
    """
    if v is None:
        return None
    if isinstance(v, DateTime):
        return v if v.tzinfo else make_aware(v, tz)
    # Only dt.datetime is left here.
    return pendulum.instance(v if v.tzinfo else make_aware(v, tz))


def td_format(td_object: None | dt.timedelta | float | int) -> str | None:
    """
    Format a timedelta object or float/int into a readable string for time duration.

    For example timedelta(seconds=3752) would become `1h:2M:32s`.
    If the time is less than a second, the return will be `<1s`.
    """
    if not td_object:
        return None
    if isinstance(td_object, dt.timedelta):
        delta = relativedelta() + td_object
    else:
        delta = relativedelta(seconds=int(td_object))
    # relativedelta for timedelta cannot convert days to months
    # so calculate months by assuming 30 day months and normalize
    months, delta.days = divmod(delta.days, 30)
    delta = delta.normalized() + relativedelta(months=months)

    def _format_part(key: str) -> str:
        value = int(getattr(delta, key))
        if value < 1:
            return ""
        # distinguish between month/minute following strftime format
        # and take first char of each unit, i.e. years='y', days='d'
        if key == "minutes":
            key = key.upper()
        key = key[0]
        return f"{value}{key}"

    parts = map(_format_part, ("years", "months", "days", "hours", "minutes", "seconds"))
    joined = ":".join(part for part in parts if part)
    if not joined:
        return "<1s"
    return joined


def parse_timezone(name: str | int) -> FixedTimezone | Timezone:
    """
    Parse timezone and return one of the pendulum Timezone.

    Provide the same interface as ``pendulum.timezone(name)``

    :param name: Either IANA timezone or offset to UTC in seconds.

    :meta private:
    """
    if _PENDULUM3:
        # This only presented in pendulum 3 and code do not reached into the pendulum 2
        return pendulum.timezone(name)  # type: ignore[operator]
    # In pendulum 2 this refers to the function, in pendulum 3 refers to the module
    return pendulum.tz.timezone(name)  # type: ignore[operator]


def local_timezone() -> FixedTimezone | Timezone:
    """
    Return local timezone.

    Provide the same interface as ``pendulum.tz.local_timezone()``

    :meta private:
    """
    return pendulum.tz.local_timezone()


TIMEZONE: FixedTimezone | Timezone = utc


def initialize(default_timezone: str) -> None:
    """
    Initialize the default timezone for the timezone library.

    Automatically called by airflow-core and task-sdk during their initialization.
    """
    global TIMEZONE
    if default_timezone == "system":
        TIMEZONE = local_timezone()
    else:
        TIMEZONE = parse_timezone(default_timezone)


def from_timestamp(timestamp: int | float, tz: str | FixedTimezone | Timezone = utc) -> DateTime:
    """
    Parse timestamp and return DateTime in a given time zone.

    :param timestamp: epoch time in seconds.
    :param tz: In which timezone should return a resulting object.
        Could be either one of pendulum timezone, IANA timezone or `local` literal.

    :meta private:
    """
    result = coerce_datetime(dt.datetime.fromtimestamp(timestamp, tz=utc))
    if tz != utc or tz != "UTC":
        if isinstance(tz, str) and tz.lower() == "local":
            tz = local_timezone()
        result = result.in_timezone(tz)
    return result
