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
import typing

import dateutil.relativedelta
import pendulum
import pendulum.tz
import pytest
import time_machine

from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.timetables.trigger import CronTriggerTimetable

TIMEZONE = pendulum.tz.timezone("UTC")
START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=TIMEZONE)

PREV_DATA_INTERVAL_END = START_DATE + datetime.timedelta(days=1)
PREV_DATA_INTERVAL_EXACT = DataInterval.exact(PREV_DATA_INTERVAL_END)

CURRENT_TIME = pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE)
YESTERDAY = CURRENT_TIME - datetime.timedelta(days=1)

HOURLY_CRON_TRIGGER_TIMETABLE = CronTriggerTimetable("@hourly", timezone=TIMEZONE)

DELTA_FROM_MIDNIGHT = datetime.timedelta(minutes=30, hours=16)


@pytest.mark.parametrize(
    "last_automated_data_interval, next_start_time",
    [
        pytest.param(
            None,
            CURRENT_TIME + DELTA_FROM_MIDNIGHT,
            id="first-run",
        ),
        pytest.param(
            PREV_DATA_INTERVAL_EXACT,
            CURRENT_TIME + DELTA_FROM_MIDNIGHT,
            id="before-now",
        ),
        pytest.param(
            DataInterval.exact(CURRENT_TIME + DELTA_FROM_MIDNIGHT),
            CURRENT_TIME + datetime.timedelta(days=1) + DELTA_FROM_MIDNIGHT,
            id="after-now",
        ),
    ],
)
@time_machine.travel(CURRENT_TIME)
def test_daily_cron_trigger_no_catchup_first_starts_at_next_schedule(
    last_automated_data_interval: DataInterval | None,
    next_start_time: pendulum.DateTime,
) -> None:
    """If ``catchup=False`` and start_date is a day before"""
    timetable = CronTriggerTimetable("30 16 * * *", timezone=TIMEZONE)
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=YESTERDAY, latest=None, catchup=False),
    )
    assert next_info == DagRunInfo.exact(next_start_time)


@pytest.mark.parametrize(
    "current_time, earliest, expected",
    [
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=TIMEZONE),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=TIMEZONE)),
            id="current_time_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=TIMEZONE),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=TIMEZONE)),
            id="current_time_not_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=TIMEZONE),
            pendulum.DateTime(2199, 12, 31, 22, 30, 0, tzinfo=TIMEZONE),
            DagRunInfo.exact(pendulum.DateTime(2199, 12, 31, 23, 0, 0, tzinfo=TIMEZONE)),
            id="future_start_date",
        ),
    ],
)
def test_hourly_cron_trigger_no_catchup_next_info(
    current_time: pendulum.DateTime,
    earliest: pendulum.DateTime,
    expected: DagRunInfo,
) -> None:
    with time_machine.travel(current_time):
        next_info = HOURLY_CRON_TRIGGER_TIMETABLE.next_dagrun_info(
            last_automated_data_interval=PREV_DATA_INTERVAL_EXACT,
            restriction=TimeRestriction(earliest=earliest, latest=None, catchup=False),
        )
    assert next_info == expected


@pytest.mark.parametrize(
    "last_automated_data_interval, earliest, expected",
    [
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=TIMEZONE)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=TIMEZONE)),
            id="last_automated_on_boundary",
        ),
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=TIMEZONE)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=TIMEZONE)),
            id="last_automated_not_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=TIMEZONE),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=TIMEZONE)),
            id="no_last_automated_with_earliest_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=TIMEZONE),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=TIMEZONE)),
            id="no_last_automated_with_earliest_not_on_boundary",
        ),
        pytest.param(
            None,
            None,
            None,
            id="no_last_automated_no_earliest",
        ),
    ],
)
def test_hourly_cron_trigger_catchup_next_info(
    last_automated_data_interval: DataInterval | None,
    earliest: pendulum.DateTime | None,
    expected: DagRunInfo | None,
) -> None:
    next_info = HOURLY_CRON_TRIGGER_TIMETABLE.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=earliest, latest=None, catchup=True),
    )
    assert next_info == expected


def test_cron_trigger_next_info_with_interval():
    # Runs every Monday on 16:30, covering the day before the run.
    timetable = CronTriggerTimetable(
        "30 16 * * MON",
        timezone=TIMEZONE,
        interval=datetime.timedelta(hours=16, minutes=30),
    )

    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            pendulum.DateTime(2022, 8, 1, tzinfo=TIMEZONE),
            pendulum.DateTime(2022, 8, 1, 16, 30, tzinfo=TIMEZONE),
        ),
        restriction=TimeRestriction(earliest=START_DATE, latest=None, catchup=True),
    )
    assert next_info == DagRunInfo.interval(
        pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
        pendulum.DateTime(2022, 8, 8, 16, 30, tzinfo=TIMEZONE),
    )


def test_validate_success() -> None:
    HOURLY_CRON_TRIGGER_TIMETABLE.validate()


def test_validate_failure() -> None:
    timetable = CronTriggerTimetable("0 0 1 13 0", timezone=TIMEZONE)

    with pytest.raises(AirflowTimetableInvalid) as ctx:
        timetable.validate()
    assert str(ctx.value) == "[0 0 1 13 0] is not acceptable, out of range"


@pytest.mark.parametrize(
    "timetable, data",
    [
        (HOURLY_CRON_TRIGGER_TIMETABLE, {"expression": "0 * * * *", "timezone": "UTC", "interval": 0}),
        (
            CronTriggerTimetable("0 0 1 12 *", timezone=TIMEZONE, interval=datetime.timedelta(hours=2)),
            {"expression": "0 0 1 12 *", "timezone": "UTC", "interval": 7200.0},
        ),
        (
            CronTriggerTimetable(
                "0 0 1 12 0",
                timezone=pendulum.tz.timezone("Asia/Taipei"),
                interval=dateutil.relativedelta.relativedelta(weekday=dateutil.relativedelta.MO),
            ),
            {"expression": "0 0 1 12 0", "timezone": "Asia/Taipei", "interval": {"weekday": [0]}},
        ),
    ],
)
def test_serialization(timetable: CronTriggerTimetable, data: dict[str, typing.Any]) -> None:
    assert timetable.serialize() == data

    tt = CronTriggerTimetable.deserialize(data)
    assert isinstance(tt, CronTriggerTimetable)
    assert tt._expression == timetable._expression
    assert tt._timezone == timetable._timezone
    assert tt._interval == timetable._interval
