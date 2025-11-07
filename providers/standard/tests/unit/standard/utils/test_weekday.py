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

from enum import Enum

import pytest

from airflow.providers.standard.utils.weekday import WeekDay


class TestWeekDay:
    def test_weekday_enum_length(self):
        assert len(WeekDay) == 7

    def test_weekday_name_value(self):
        weekdays = "MONDAY TUESDAY WEDNESDAY THURSDAY FRIDAY SATURDAY SUNDAY"
        weekdays = weekdays.split()
        for i, weekday in enumerate(weekdays, start=1):
            weekday_enum = WeekDay(i)
            assert weekday_enum == i
            assert int(weekday_enum) == i
            assert weekday_enum.name == weekday
            assert weekday_enum in WeekDay
            assert 0 < weekday_enum < 8
            assert isinstance(weekday_enum, WeekDay)
            assert isinstance(weekday_enum, int)
            assert isinstance(weekday_enum, Enum)

    @pytest.mark.parametrize(
        ("weekday", "expected"),
        [
            ("Monday", 1),
            (WeekDay.MONDAY, 1),
        ],
        ids=["with-string", "with-enum"],
    )
    def test_convert(self, weekday, expected):
        result = WeekDay.convert(weekday)
        assert result == expected

    def test_convert_with_incorrect_input(self):
        invalid = "Sun"
        error_message = rf'Invalid Week Day passed: "{invalid}"'
        with pytest.raises(AttributeError, match=error_message):
            WeekDay.convert(invalid)

    @pytest.mark.parametrize(
        ("weekday", "expected"),
        [
            ("Monday", {WeekDay.MONDAY}),
            (WeekDay.MONDAY, {WeekDay.MONDAY}),
            ({"Thursday": "1"}, {WeekDay.THURSDAY}),
            (["Thursday"], {WeekDay.THURSDAY}),
            (["Thursday", WeekDay.MONDAY], {WeekDay.MONDAY, WeekDay.THURSDAY}),
        ],
        ids=[
            "with-string",
            "with-enum",
            "with-dict",
            "with-list",
            "with-mix",
        ],
    )
    def test_validate_week_day(self, weekday, expected):
        result = WeekDay.validate_week_day(weekday)
        assert expected == result

    def test_validate_week_day_with_invalid_type(self):
        invalid_week_day = 5
        with pytest.raises(
            TypeError,
            match=f"Unsupported Type for week_day parameter: {type(invalid_week_day)}."
            "Input should be iterable type:"
            "str, set, list, dict or Weekday enum type",
        ):
            WeekDay.validate_week_day(invalid_week_day)
