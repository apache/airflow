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
import datetime
from typing import Optional

import freezegun
import pendulum
import pytest

from airflow.settings import TIMEZONE
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=TIMEZONE)

PREV_DATA_INTERVAL_START = START_DATE
PREV_DATA_INTERVAL_END = START_DATE + datetime.timedelta(days=1)
PREV_DATA_INTERVAL = DataInterval(start=PREV_DATA_INTERVAL_START, end=PREV_DATA_INTERVAL_END)

CURRENT_TIME = pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE)

HOURLY_CRON_TIMETABLE = CronDataIntervalTimetable("@hourly", TIMEZONE)
HOURLY_DELTA_TIMETABLE = DeltaDataIntervalTimetable(datetime.timedelta(hours=1))


@pytest.mark.parametrize(
    "timetable",
    [pytest.param(HOURLY_CRON_TIMETABLE, id="cron"), pytest.param(HOURLY_DELTA_TIMETABLE, id="delta")],
)
@pytest.mark.parametrize(
    "last_automated_data_interval",
    [pytest.param(None, id="first-run"), pytest.param(PREV_DATA_INTERVAL, id="subsequent")],
)
@freezegun.freeze_time(CURRENT_TIME)
def test_no_catchup_next_info_starts_at_current_time(
    timetable: Timetable,
    last_automated_data_interval: Optional[DataInterval],
) -> None:
    """If ``catchup=False``, the next data interval ends at the current time."""
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=START_DATE, latest=None, catchup=False),
    )
    expected_start = CURRENT_TIME - datetime.timedelta(hours=1)
    assert next_info == DagRunInfo.interval(start=expected_start, end=CURRENT_TIME)


@pytest.mark.parametrize(
    "timetable",
    [pytest.param(HOURLY_CRON_TIMETABLE, id="cron"), pytest.param(HOURLY_DELTA_TIMETABLE, id="delta")],
)
def test_catchup_next_info_starts_at_previous_interval_end(timetable: Timetable) -> None:
    """If ``catchup=True``, the next interval starts at the previous's end."""
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=PREV_DATA_INTERVAL,
        restriction=TimeRestriction(earliest=START_DATE, latest=None, catchup=True),
    )
    expected_end = PREV_DATA_INTERVAL_END + datetime.timedelta(hours=1)
    assert next_info == DagRunInfo.interval(start=PREV_DATA_INTERVAL_END, end=expected_end)
