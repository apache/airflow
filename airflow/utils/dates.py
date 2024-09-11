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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Collection

from croniter import croniter

from airflow.typing_compat import Literal
from airflow.utils import timezone

cron_presets: dict[str, str] = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
}

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta  # for doctest


def round_time(
    dt: datetime,
    delta: str | timedelta | relativedelta,
    start_date: datetime = timezone.make_aware(datetime.min),
):
    """
    Return ``start_date + i * delta`` for given ``i`` where the result is closest to ``dt``.

    .. code-block:: pycon

        >>> round_time(datetime(2015, 1, 1, 6), timedelta(days=1))
        datetime.datetime(2015, 1, 1, 0, 0)
        >>> round_time(datetime(2015, 1, 2), relativedelta(months=1))
        datetime.datetime(2015, 1, 1, 0, 0)
        >>> round_time(datetime(2015, 9, 16, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
        datetime.datetime(2015, 9, 16, 0, 0)
        >>> round_time(datetime(2015, 9, 15, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
        datetime.datetime(2015, 9, 15, 0, 0)
        >>> round_time(datetime(2015, 9, 14, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
        datetime.datetime(2015, 9, 14, 0, 0)
        >>> round_time(datetime(2015, 9, 13, 0, 0), timedelta(1), datetime(2015, 9, 14, 0, 0))
        datetime.datetime(2015, 9, 14, 0, 0)
    """
    if isinstance(delta, str):
        # It's cron based, so it's easy
        time_zone = start_date.tzinfo
        start_date = timezone.make_naive(start_date, time_zone)
        cron = croniter(delta, start_date)
        prev = cron.get_prev(datetime)
        if prev == start_date:
            return timezone.make_aware(start_date, time_zone)
        else:
            return timezone.make_aware(prev, time_zone)

    # Ignore the microseconds of dt
    dt -= timedelta(microseconds=dt.microsecond)

    # We are looking for a datetime in the form start_date + i * delta
    # which is as close as possible to dt. Since delta could be a relative
    # delta we don't know its exact length in seconds so we cannot rely on
    # division to find i. Instead we employ a binary search algorithm, first
    # finding an upper and lower limit and then dissecting the interval until
    # we have found the closest match.

    # We first search an upper limit for i for which start_date + upper * delta
    # exceeds dt.
    upper = 1
    while start_date + upper * delta < dt:
        # To speed up finding an upper limit we grow this exponentially by a
        # factor of 2
        upper *= 2

    # Since upper is the first value for which start_date + upper * delta
    # exceeds dt, upper // 2 is below dt and therefore forms a lower limited
    # for the i we are looking for
    lower = upper // 2

    # We now continue to intersect the interval between
    # start_date + lower * delta and start_date + upper * delta
    # until we find the closest value
    while True:
        # Invariant: start + lower * delta < dt <= start + upper * delta
        # If start_date + (lower + 1)*delta exceeds dt, then either lower or
        # lower+1 has to be the solution we are searching for
        if start_date + (lower + 1) * delta >= dt:
            # Check if start_date + (lower + 1)*delta or
            # start_date + lower*delta is closer to dt and return the solution
            if (start_date + (lower + 1) * delta) - dt <= dt - (start_date + lower * delta):
                return start_date + (lower + 1) * delta
            else:
                return start_date + lower * delta

        # We intersect the interval and either replace the lower or upper
        # limit with the candidate
        candidate = lower + (upper - lower) // 2
        if start_date + candidate * delta >= dt:
            upper = candidate
        else:
            lower = candidate

    # in the special case when start_date > dt the search for upper will
    # immediately stop for upper == 1 which results in lower = upper // 2 = 0
    # and this function returns start_date.


TimeUnit = Literal["days", "hours", "minutes", "seconds"]


def infer_time_unit(time_seconds_arr: Collection[float]) -> TimeUnit:
    """
    Determine the most appropriate time unit for given durations (in seconds).

    e.g. 5400 seconds => 'minutes', 36000 seconds => 'hours'
    """
    if not time_seconds_arr:
        return "hours"
    max_time_seconds = max(time_seconds_arr)
    if max_time_seconds <= 60 * 2:
        return "seconds"
    elif max_time_seconds <= 60 * 60 * 2:
        return "minutes"
    elif max_time_seconds <= 24 * 60 * 60 * 2:
        return "hours"
    else:
        return "days"


def scale_time_units(time_seconds_arr: Collection[float], unit: TimeUnit) -> Collection[float]:
    """Convert an array of time durations in seconds to the specified time unit."""
    if unit == "minutes":
        factor = 60
    elif unit == "hours":
        factor = 60 * 60
    elif unit == "days":
        factor = 24 * 60 * 60
    else:
        factor = 1
    return [x / factor for x in time_seconds_arr]


def parse_execution_date(execution_date_str):
    """Parse execution date string to datetime object."""
    return timezone.parse(execution_date_str)


def datetime_to_nano(datetime) -> int:
    """Convert datetime to nanoseconds."""
    return int(datetime.timestamp() * 1000000000)
