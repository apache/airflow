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
import time_machine

from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.simple import ContinuousTimetable

BEFORE_DATE = pendulum.datetime(2023, 3, 1, tz="UTC")
START_DATE = pendulum.datetime(2023, 3, 3, tz="UTC")
DURING_DATE = pendulum.datetime(2023, 3, 6, tz="UTC")
END_DATE = pendulum.datetime(2023, 3, 10, tz="UTC")
AFTER_DATE = pendulum.datetime(2023, 3, 12, tz="UTC")


@pytest.fixture
def restriction():
    return TimeRestriction(earliest=START_DATE, latest=END_DATE, catchup=True)


@pytest.fixture
def timetable():
    return ContinuousTimetable()


def test_no_runs_without_start_date(timetable):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=None, latest=None, catchup=False),
    )
    assert next_info is None


@time_machine.travel(DURING_DATE)
def test_first_run_after_start_date_correct_interval(timetable, restriction):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    assert next_info.run_after == DURING_DATE
    assert next_info.data_interval.start == START_DATE
    assert next_info.data_interval.end == DURING_DATE


@time_machine.travel(BEFORE_DATE)
def test_first_run_before_start_date_correct_interval(timetable, restriction):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    assert next_info.run_after == START_DATE
    assert next_info.data_interval.start == START_DATE
    assert next_info.data_interval.end == START_DATE


@time_machine.travel(DURING_DATE)
def test_run_uses_utcnow(timetable, restriction):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(START_DATE, DURING_DATE),
        restriction=restriction,
    )

    assert next_info.run_after == DURING_DATE


@time_machine.travel(AFTER_DATE)
def test_no_runs_after_end_date(timetable, restriction):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(START_DATE, DURING_DATE),
        restriction=restriction,
    )

    assert next_info is None
