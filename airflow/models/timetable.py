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

from datetime import datetime, timedelta

import holidays
from dateutil import parser, tz


class BaseSeries:
    interval = None

    def __call__(self, time):
        raise NotImplementedError()


class Minute(BaseSeries):
    UNIT = 'minute'

    def __init__(self, minute=None, minutes=None, interval=None):
        assert not (minute and minutes)
        super().__init__()
        self.interval = timedelta(minutes=interval or 1)
        if minutes:
            self.minutes = set(minutes)
        elif minute:
            self.minutes = {minute}
        else:
            self.minutes = {i for i in range(60)}

    def __call__(self, time):
        return time.minute in self.minutes


class Hour(BaseSeries):
    UNIT = 'hour'

    def __init__(self, hour=None, hours=None, interval=None):
        assert not (hour and hours)
        super().__init__()
        if hours:
            self.hours = set(hours)
        elif hour:
            self.hours = {hour}
        else:
            self.hours = {i for i in range(24)}

        self.interval = timedelta(hours=interval or 1)

    def __call__(self, time):
        return time.hour in self.hours


class Day(BaseSeries):
    UNIT = 'day'

    def __init__(self, day=None, days=None, interval=None):
        assert not (day and days)
        super().__init__()
        if days:
            self.days = set(days)
        elif day:
            self.days = {day}
        else:
            self.days = {i for i in range(1, 32)}

        self.interval = timedelta(days=interval or 1)

    def __call__(self, time):
        return time.day in self.days


class Weekday(BaseSeries):
    ALL_DAYS = ['Mon', 'Tues', 'Wed', 'Thurs', 'Fri', 'Sat', 'Sun']
    WEEKDAYS = ['Mon', 'Tues', 'Wed', 'Thurs', 'Fri']
    UNIT = 'day'

    def __init__(self, day=None, days=None, interval=None):
        assert not (day and days)
        super().__init__()
        if days:
            self.days = set(days)
        elif day:
            self.days = {day}
        else:
            self.days = set(self.WEEKDAYS)

        self.interval = timedelta(days=interval or 1)

    def __call__(self, time):
        return self.ALL_DAYS[time.weekday()] in self.days


class WorkingDay(Weekday):
    def __init__(self, *args, **kwargs):
        if not kwargs.get('days'):
            kwargs['days'] = self.WEEKDAYS
        self.dates = set(kwargs.get("dates", []))
        kwargs.pop('dates')
        super().__init__(*args, **kwargs)
        self.day_or_next_work_day = kwargs.get("day_or_next_work_day", True)
        self.holidays = kwargs.get('holidays', holidays.UnitedStates())

    def __call__(self, cur_date):
        is_weekend = lambda x: x.weekday() in [5, 6]
        is_holiday = lambda x: x in self.holidays
        matches = lambda x: cur_date.day in self.dates

        if is_holiday(cur_date) or is_weekend(cur_date):
            return False

        if matches(cur_date):
            return True

        if self.day_or_next_work_day:
            cur_date -= timedelta(days=1)
            if not (is_holiday(cur_date) or is_weekend(cur_date)):
                return False
            while is_holiday(cur_date) or is_weekend(cur_date):
                if matches(cur_date):
                    return True
                cur_date -= timedelta(days=1)

        return False


def interval(unit):
    def wrapped(count=1):
        return timedelta(**{unit: count})

    return wrapped


minutes = interval("minutes")
hours = interval("hours")
days = interval("days")
weeks = interval("weeks")


class Timetable:
    def __init__(
        self,
        include=None,
        exclude=None,
        start=None,
        end=None,
        timezone=None,
        data_interval_length=None,
        historic_data_length=None,
        execution_delay=None,
    ):
        self.include = include or []
        self.exclude = exclude or []
        self.start = parser.parse(start) if start else datetime.now()
        self.end = parser.parse(end) if end else None

        local_tz = tz.gettz(timezone) if timezone else tz.tzutc()
        self._time = datetime.now(tz=local_tz)
        self.data_interval = data_interval_length or timedelta()
        self.previous_data = historic_data_length or timedelta()
        self.execution_delay = execution_delay or timedelta()
        self.intervals = self._sort_intervals()

    def _sort_intervals(self):
        return sorted(self.include, key=lambda i: i.interval)

    def should_execute(self):
        for included in self.include:
            if not included(self._time):
                return False
        for excluded in self.exclude:
            if excluded(self._time):
                return False
        return True

    def _zero_smaller_units(self, time, unit):
        if unit == 'minute':
            return time.replace(second=0, microsecond=0)
        if unit == 'hour':
            return time.replace(minute=0, second=0, microsecond=0)
        if unit == 'day':
            return time.replace(hour=0, minute=0, second=0, microsecond=0)
        if unit == 'month':
            return time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if unit == 'year':
            return time.replace(month=0, day=1, hour=0, minute=0, second=0, microsecond=0)

    def get_next_time_step(self):
        """
        This code looks gross, but rather than march forward in time one minute at a time and then checking if the new time matches the execution rules, this puts interval times in order by size (minute -> hour -> day) and adjusts the least granular interval that doesn't match the date. TODO: Explain this better.

        TlDR: It looks gross cuz I wanted to make it fast, which it semi is.
        """
        next_time = self._time
        current_unit = None
        for interval in self.intervals:
            if current_unit and interval.UNIT == current_unit:
                continue
            current_unit = interval.UNIT
            next_time = self._zero_smaller_units(next_time, interval.UNIT)
            next_time += interval.interval
            while not interval(next_time):
                next_time += interval.interval
                if getattr(next_time, current_unit) < getattr(self._time, current_unit):
                    break
            if interval(next_time):
                return next_time
        # TODO: Handle the case where none match
        return next_time

    def get_execution_delay(self):
        if callable(self.execution_delay):
            return self.execution_delay(self)
        return self._time + self.data_interval + self.execution_delay

    def get_historical_data_start(self):
        if callable(self.previous_data):
            return self.previous_data(self)
        return self._time - self.previous_data

    def get_data_interval_end(self):
        if callable(self.data_interval):
            return self.data_interval(self)
        return self._time + self.data_interval

    def get_data_interval_start(self):
        return self._time

    def get_series(self, count):
        end_date = self._time + timedelta(days=365)
        series = []
        while count and (self._time < end_date):
            if self.should_execute():
                series.append(
                    (
                        self.get_data_interval_start(),
                        self.get_historical_data_start(),
                        self.get_data_interval_end(),
                        self.get_execution_delay(),
                    )
                )
                count -= 1
            self._time = self.get_next_time_step()
        return series


# tt = Timetable(
#     include=[
#         Minute(minutes=[4, 5, 13]),
#         Hour(hours=[3, 17]),
#         Weekday(days=["Mon", "Wed", "Thurs"]),
#     ],
#     exclude=[
#       Day(days=[20, 21, 22, 23, 24, 25, 26, 27, 28, 29]),
#     ],
#     timezone='America/Los Angeles'
# )

# tt = Timetable(
#     include=[
#         Weekday(days=["Mon", "Tues", "Wed", "Thurs", "Fri"]),
#     ],
#     data_interval_length=days(1),
#     historic_data_length=days(7),
#     execution_delay=hours(2),
# )

# tt = Timetable(
#     include=[
#         Weekday(days=["Mon", "Tues", "Wed", "Thurs", "Fri"]),
#     ],
#     data_interval_length=days(1),
# )

# def delay_until_start_of_next_interval(timetable):
#   return timetable.get_next_time_step()

# tt = Timetable(
#    include=[
#        Weekday(),
#    ],
#    data_interval_length=days(1),
#    execution_delay=delay_until_start_of_next_interval
# )

tt = Timetable(
    include=[
        Hour(8),
        WorkingDay(dates=[1, 15]),
    ],
)
print(tt)

# tt = Timetable(
#     include=[
#         Weekday(days=["Mon", "Tues", "Wed", "Thurs", "Fri"]),
#     ],
#     filters=[
#       TradingDays('NYSE')
#     ],
#     data_interval_length=days(1),
#     timezone='America/Los Angeles'
# )

# tt = Timetable(
#     filters=[Sunset()],
# )

series = tt.get_series(100)
for times in series:
    print([t.strftime("%a, %b %d, %y %H:%M:%S") for t in times])
print(len(series))
