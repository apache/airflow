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
from typing import overload

import pendulum
from dateutil.relativedelta import relativedelta
from pendulum.datetime import DateTime

# UTC time zone as a tzinfo instance.
utc = pendulum.tz.timezone("UTC")


def is_localized(value):
    """
    Determine if a given datetime.datetime is aware.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return value.utcoffset() is not None


def is_naive(value):
    """
    Determine if a given datetime.datetime is naive.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return value.utcoffset() is None


def utcnow() -> dt.datetime:
    """
    Get the current date and time in UTC

    :return:
    """
    # pendulum utcnow() is not used as that sets a TimezoneInfo object
    # instead of a Timezone. This is not picklable and also creates issues
    # when using replace()
    result = dt.datetime.utcnow()
    result = result.replace(tzinfo=utc)

    return result


def utc_epoch() -> dt.datetime:
    """
    Gets the epoch in the users timezone

    :return:
    """
    # pendulum utcnow() is not used as that sets a TimezoneInfo object
    # instead of a Timezone. This is not picklable and also creates issues
    # when using replace()
    result = dt.datetime(1970, 1, 1)
    result = result.replace(tzinfo=utc)

    return result


@overload
def convert_to_utc(value: None) -> None:
    ...


@overload
def convert_to_utc(value: dt.datetime) -> DateTime:
    ...


def convert_to_utc(value: dt.datetime | None) -> DateTime | None:
    """
    Returns the datetime with the default timezone added if timezone
    information was not associated

    :param value: datetime
    :return: datetime with tzinfo
    """
    if value is None:
        return value

    if not is_localized(value):
        from airflow.settings import TIMEZONE

        value = pendulum.instance(value, TIMEZONE)

    return pendulum.instance(value.astimezone(utc))


@overload
def make_aware(value: None, timezone: dt.tzinfo | None = None) -> None:
    ...


@overload
def make_aware(value: DateTime, timezone: dt.tzinfo | None = None) -> DateTime:
    ...


@overload
def make_aware(value: dt.datetime, timezone: dt.tzinfo | None = None) -> dt.datetime:
    ...


def make_aware(value: dt.datetime | None, timezone: dt.tzinfo | None = None) -> dt.datetime | None:
    """
    Make a naive datetime.datetime in a given time zone aware.

    :param value: datetime
    :param timezone: timezone
    :return: localized datetime in settings.TIMEZONE or timezone
    """
    if timezone is None:
        from airflow.settings import TIMEZONE

        timezone = TIMEZONE

    if not value:
        return None

    # Check that we won't overwrite the timezone of an aware datetime.
    if is_localized(value):
        raise ValueError(f"make_aware expects a naive datetime, got {value}")
    if hasattr(value, "fold"):
        # In case of python 3.6 we want to do the same that pendulum does for python3.5
        # i.e in case we move clock back we want to schedule the run at the time of the second
        # instance of the same clock time rather than the first one.
        # Fold parameter has no impact in other cases so we can safely set it to 1 here
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
        from airflow.settings import TIMEZONE

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
    Wrapper around datetime.datetime that adds settings.TIMEZONE if tzinfo not specified

    :return: datetime.datetime
    """
    if "tzinfo" not in kwargs:
        from airflow.settings import TIMEZONE

        kwargs["tzinfo"] = TIMEZONE

    return dt.datetime(*args, **kwargs)


def parse(string: str, timezone=None) -> DateTime:
    """
    Parse a time string and return an aware datetime

    :param string: time string
    :param timezone: the timezone
    """
    from airflow.settings import TIMEZONE

    return pendulum.parse(string, tz=timezone or TIMEZONE, strict=False)  # type: ignore


@overload
def coerce_datetime(v: None, tz: dt.tzinfo | None = None) -> None:
    ...


@overload
def coerce_datetime(v: DateTime, tz: dt.tzinfo | None = None) -> DateTime:
    ...


@overload
def coerce_datetime(v: dt.datetime, tz: dt.tzinfo | None = None) -> DateTime:
    ...


def coerce_datetime(v: dt.datetime | None, tz: dt.tzinfo | None = None) -> DateTime | None:
    """Convert ``v`` into a timezone-aware ``pendulum.DateTime``.

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
