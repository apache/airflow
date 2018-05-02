# -*- coding: utf-8 -*-
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
#
import datetime as dt
import pendulum

from airflow.settings import TIMEZONE


# UTC time zone as a tzinfo instance.
utc = pendulum.timezone('UTC')


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


def utcnow():
    """
    Get the current date and time in UTC
    :return:
    """

    # pendulum utcnow() is not used as that sets a TimezoneInfo object
    # instead of a Timezone. This is not pickable and also creates issues
    # when using replace()
    d = dt.datetime.utcnow()
    d = d.replace(tzinfo=utc)

    return d


def utc_epoch():
    """
    Gets the epoch in the users timezone
    :return:
    """

    # pendulum utcnow() is not used as that sets a TimezoneInfo object
    # instead of a Timezone. This is not pickable and also creates issues
    # when using replace()
    d = dt.datetime(1970, 1, 1)
    d = d.replace(tzinfo=utc)

    return d


def convert_to_utc(value):
    """
    Returns the datetime with the default timezone added if timezone
    information was not associated
    :param value: datetime
    :return: datetime with tzinfo
    """
    if not value:
        return value

    if not is_localized(value):
        value = pendulum.instance(value, TIMEZONE)

    return value.astimezone(utc)


def make_aware(value, timezone=None):
    """
    Make a naive datetime.datetime in a given time zone aware.

    :param value: datetime
    :param timezone: timezone
    :return: localized datetime in settings.TIMEZONE or timezone

    """
    if timezone is None:
        timezone = TIMEZONE

    # Check that we won't overwrite the timezone of an aware datetime.
    if is_localized(value):
        raise ValueError(
            "make_aware expects a naive datetime, got %s" % value)

    if hasattr(timezone, 'localize'):
        # This method is available for pytz time zones.
        return timezone.localize(value)
    elif hasattr(timezone, 'convert'):
        # For pendulum
        return timezone.convert(value)
    else:
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

    o = value.astimezone(timezone)

    # cross library compatibility
    naive = dt.datetime(o.year,
                        o.month,
                        o.day,
                        o.hour,
                        o.minute,
                        o.second,
                        o.microsecond)

    return naive


def datetime(*args, **kwargs):
    """
    Wrapper around datetime.datetime that adds settings.TIMEZONE if tzinfo not specified

    :return: datetime.datetime
    """
    if 'tzinfo' not in kwargs:
        kwargs['tzinfo'] = TIMEZONE

    return dt.datetime(*args, **kwargs)


def parse(string):
    """
    Parse a time string and return an aware datetime
    :param string: time string
    """
    return pendulum.parse(string, tz=TIMEZONE)
