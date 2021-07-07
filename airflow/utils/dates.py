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

import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from croniter import croniter
from dateutil.relativedelta import relativedelta  # for doctest

from airflow.utils import timezone

cron_presets: Dict[str, str] = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@quarterly': '0 0 1 */3 *',
    '@yearly': '0 0 1 1 *',
}


def date_range(
    start_date: datetime,
    end_date: Optional[datetime] = None,
    num: Optional[int] = None,
    delta: Optional[Union[str, timedelta, relativedelta]] = None,
) -> List[datetime]:
    """
    Get a set of dates as a list based on a start, end and delta, delta
    can be something that can be added to `datetime.datetime`
    or a cron expression as a `str`

    .. code-block:: pycon
        >>> from airflow.utils.dates import datterange
        >>> from datetime import datetime, timedelta
        >>> date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta=timedelta(1))
        [datetime.datetime(2016, 1, 1, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 1, 2, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 1, 3, 0, 0, tzinfo=Timezone('UTC'))]
        >>> date_range(datetime(2016, 1, 1), datetime(2016, 1, 3), delta="0 0 * * *")
        [datetime.datetime(2016, 1, 1, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 1, 2, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 1, 3, 0, 0, tzinfo=Timezone('UTC'))]
        >>> date_range(datetime(2016, 1, 1), datetime(2016, 3, 3), delta="0 0 0 * *")
        [datetime.datetime(2016, 1, 1, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 2, 1, 0, 0, tzinfo=Timezone('UTC')),
        datetime.datetime(2016, 3, 1, 0, 0, tzinfo=Timezone('UTC'))]

    :param start_date: anchor date to start the series from
    :type start_date: datetime.datetime
    :param end_date: right boundary for the date range
    :type end_date: datetime.datetime
    :param num: alternatively to end_date, you can specify the number of
        number of entries you want in the range. This number can be negative,
        output will always be sorted regardless
    :type num: int
    :param delta: step length. It can be datetime.timedelta or cron expression as string
    :type delta: datetime.timedelta or str or dateutil.relativedelta
    """
    warnings.warn(
        "`airflow.utils.dates.date_range()` is deprecated. Please use `airflow.timetables`.",
        category=DeprecationWarning,
        stacklevel=2,
    )

    if not delta:
        return []
    if end_date:
        if start_date > end_date:
            raise Exception("Wait. start_date needs to be before end_date")
        if num:
            raise Exception("Wait. Either specify end_date OR num")
    if not end_date and not num:
        end_date = timezone.utcnow()

    delta_iscron = False
    time_zone = start_date.tzinfo

    abs_delta: Union[timedelta, relativedelta]
    if isinstance(delta, str):
        delta_iscron = True
        if timezone.is_localized(start_date):
            start_date = timezone.make_naive(start_date, time_zone)
        cron = croniter(cron_presets.get(delta, delta), start_date)
    elif isinstance(delta, timedelta):
        abs_delta = abs(delta)
    elif isinstance(delta, relativedelta):
        abs_delta = abs(delta)
    else:
        raise Exception("Wait. delta must be either datetime.timedelta or cron expression as str")

    dates = []
    if end_date:
        if timezone.is_naive(start_date) and not timezone.is_naive(end_date):
            end_date = timezone.make_naive(end_date, time_zone)
        while start_date <= end_date:  # type: ignore
            if timezone.is_naive(start_date):
                dates.append(timezone.make_aware(start_date, time_zone))
            else:
                dates.append(start_date)

            if delta_iscron:
                start_date = cron.get_next(datetime)
            else:
                start_date += abs_delta
    else:
        num_entries: int = num  # type: ignore
        for _ in range(abs(num_entries)):
            if timezone.is_naive(start_date):
                dates.append(timezone.make_aware(start_date, time_zone))
            else:
                dates.append(start_date)

            if delta_iscron and num_entries > 0:
                start_date = cron.get_next(datetime)
            elif delta_iscron:
                start_date = cron.get_prev(datetime)
            elif num_entries > 0:
                start_date += abs_delta
            else:
                start_date -= abs_delta

    return sorted(dates)


def round_time(dt, delta, start_date=timezone.make_aware(datetime.min)):
    """
    Returns the datetime of the form start_date + i * delta
    which is closest to dt for any non-negative integer i.
    Note that delta may be a datetime.timedelta or a dateutil.relativedelta

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


def infer_time_unit(time_seconds_arr):
    """
    Determine the most appropriate time unit for an array of time durations
    specified in seconds.
    e.g. 5400 seconds => 'minutes', 36000 seconds => 'hours'
    """
    if len(time_seconds_arr) == 0:
        return 'hours'
    max_time_seconds = max(time_seconds_arr)
    if max_time_seconds <= 60 * 2:
        return 'seconds'
    elif max_time_seconds <= 60 * 60 * 2:
        return 'minutes'
    elif max_time_seconds <= 24 * 60 * 60 * 2:
        return 'hours'
    else:
        return 'days'


def scale_time_units(time_seconds_arr, unit):
    """Convert an array of time durations in seconds to the specified time unit."""
    if unit == 'minutes':
        return list(map(lambda x: x / 60, time_seconds_arr))
    elif unit == 'hours':
        return list(map(lambda x: x / (60 * 60), time_seconds_arr))
    elif unit == 'days':
        return list(map(lambda x: x / (24 * 60 * 60), time_seconds_arr))
    return time_seconds_arr


def days_ago(n, hour=0, minute=0, second=0, microsecond=0):
    """
    Get a datetime object representing `n` days ago. By default the time is
    set to midnight.
    """
    today = timezone.utcnow().replace(hour=hour, minute=minute, second=second, microsecond=microsecond)
    return today - timedelta(days=n)


def parse_execution_date(execution_date_str):
    """Parse execution date string to datetime object."""
    return timezone.parse(execution_date_str)
