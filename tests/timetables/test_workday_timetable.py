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

import pendulum
import pytest

from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.settings import TIMEZONE
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=TIMEZONE)  # This is a Saturday.

WEEK_1_WEEKDAYS = [
    pendulum.DateTime(2021, 9, 6, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 8, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 9, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 10, tzinfo=TIMEZONE),
]

WEEK_1_SATURDAY = pendulum.DateTime(2021, 9, 11, tzinfo=TIMEZONE)

WEEK_2_MONDAY = pendulum.DateTime(2021, 9, 13, tzinfo=TIMEZONE)
WEEK_2_TUESDAY = pendulum.DateTime(2021, 9, 14, tzinfo=TIMEZONE)


@pytest.fixture()
def restriction():
    return TimeRestriction(earliest=START_DATE, latest=None, catchup=True)


@pytest.fixture()
def timetable():
    return AfterWorkdayTimetable()


@pytest.mark.parametrize(
    "start, end",
    list(zip(WEEK_1_WEEKDAYS[:-1], WEEK_1_WEEKDAYS[1:])),
)
def test_dag_run_info_interval(start: pendulum.DateTime, end: pendulum.DateTime):
    expected_info = DagRunInfo(run_after=end, data_interval=DataInterval(start, end))
    assert DagRunInfo.interval(start, end) == expected_info


def test_first_schedule(timetable: Timetable, restriction: TimeRestriction):
    """Since DAG starts on Saturday, the first ever run covers the next Monday and schedules on Tuesday."""
    next_info = timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    assert next_info == DagRunInfo.interval(WEEK_1_WEEKDAYS[0], WEEK_1_WEEKDAYS[1])


@pytest.mark.parametrize(
    "last_automated_data_interval, expected_next_info",
    [
        pytest.param(
            DataInterval(day, day + datetime.timedelta(days=1)),
            DagRunInfo.interval(
                day + datetime.timedelta(days=1),
                day + datetime.timedelta(days=2),
            ),
        )
        for day in WEEK_1_WEEKDAYS[:-1]  # Data intervals for Monday to Tuesday.
    ],
)
def test_subsequent_weekday_schedule(
    timetable: Timetable,
    restriction: TimeRestriction,
    last_automated_data_interval: DataInterval,
    expected_next_info: DagRunInfo,
):
    """The next four subsequent runs cover the next four weekdays each."""
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=restriction,
    )
    assert next_info == expected_next_info


def test_next_schedule_after_friday(timetable: Timetable, restriction: TimeRestriction):
    """The run after Friday's run covers Monday."""
    last_automated_data_interval = DataInterval(WEEK_1_WEEKDAYS[-1], WEEK_1_SATURDAY)
    expected_next_info = DagRunInfo.interval(WEEK_2_MONDAY, WEEK_2_TUESDAY)

    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=restriction,
    )
    assert next_info == expected_next_info
