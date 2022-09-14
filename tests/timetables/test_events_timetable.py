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

import pendulum
import pytest

from airflow.settings import TIMEZONE
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.events import EventsTimetable

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=TIMEZONE)  # Precedes all events

EVENT_DATES = [
    pendulum.DateTime(2021, 9, 6, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 8, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 8, tzinfo=TIMEZONE),  # deliberate duplicate, should be ignored
    pendulum.DateTime(2021, 10, 9, tzinfo=TIMEZONE),  # deliberately out of order
    pendulum.DateTime(2021, 9, 10, tzinfo=TIMEZONE),
]

EVENT_DATES_SORTED = [
    pendulum.DateTime(2021, 9, 6, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 8, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 9, 10, tzinfo=TIMEZONE),
    pendulum.DateTime(2021, 10, 9, tzinfo=TIMEZONE),
]

NON_EVENT_DATE = pendulum.DateTime(2021, 10, 1, tzinfo=TIMEZONE)
MOST_RECENT_EVENT = pendulum.DateTime(2021, 9, 10, tzinfo=TIMEZONE)


@pytest.fixture()
def restriction():
    return TimeRestriction(earliest=START_DATE, latest=None, catchup=True)


@pytest.fixture()
def unrestricted_timetable():
    return EventsTimetable(event_dates=EVENT_DATES)


@pytest.fixture()
def restricted_timetable():
    return EventsTimetable(event_dates=EVENT_DATES, restrict_to_events=True)


@pytest.mark.parametrize(
    "start, end",
    list(zip(EVENT_DATES, EVENT_DATES)),
)
def test_dag_run_info_interval(start: pendulum.DateTime, end: pendulum.DateTime):
    expected_info = DagRunInfo(run_after=end, data_interval=DataInterval(start, end))
    assert DagRunInfo.interval(start, end) == expected_info


def test_manual_with_unrestricted(unrestricted_timetable: Timetable, restriction: TimeRestriction):
    """When not using strict event dates, manual runs have run_after as the data interval"""
    manual_run_data_interval = unrestricted_timetable.infer_manual_data_interval(run_after=NON_EVENT_DATE)
    expected_data_interval = DataInterval.exact(NON_EVENT_DATE)
    assert expected_data_interval == manual_run_data_interval


def test_manual_with_restricted_middle(restricted_timetable: Timetable, restriction: TimeRestriction):
    """
    Test that when using strict event dates, manual runs after the first event have the
    most recent event's date as the start interval
    """
    manual_run_data_interval = restricted_timetable.infer_manual_data_interval(run_after=NON_EVENT_DATE)
    expected_data_interval = DataInterval.exact(MOST_RECENT_EVENT)
    assert expected_data_interval == manual_run_data_interval


def test_manual_with_restricted_before(restricted_timetable: Timetable, restriction: TimeRestriction):
    """
    Test that when using strict event dates, manual runs before the first event have the first event's date
    as the start interval
    """
    manual_run_data_interval = restricted_timetable.infer_manual_data_interval(run_after=START_DATE)
    expected_data_interval = DataInterval.exact(EVENT_DATES[0])
    assert expected_data_interval == manual_run_data_interval


@pytest.mark.parametrize(
    "last_automated_data_interval, expected_next_info",
    [
        pytest.param(
            DataInterval(day, day),
            DagRunInfo.interval(
                EVENT_DATES_SORTED[event_num + 1],
                EVENT_DATES_SORTED[event_num + 1],
            ),
        )
        for event_num, day in enumerate(EVENT_DATES_SORTED[:-1])
    ]
    + [pytest.param(DataInterval(EVENT_DATES_SORTED[-1], EVENT_DATES_SORTED[-1]), None)],
)
def test_subsequent_weekday_schedule(
    unrestricted_timetable: Timetable,
    restriction: TimeRestriction,
    last_automated_data_interval: DataInterval,
    expected_next_info: DagRunInfo,
):
    """The next four subsequent runs cover the next four weekdays each."""
    next_info = unrestricted_timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=restriction,
    )
    assert next_info == expected_next_info
