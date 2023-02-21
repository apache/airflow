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

import dateutil.relativedelta
import pendulum
import pytest
import time_machine

from airflow.exceptions import AirflowTimetableInvalid
from airflow.settings import TIMEZONE
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=TIMEZONE)

PREV_DATA_INTERVAL_START = START_DATE
PREV_DATA_INTERVAL_END = START_DATE + datetime.timedelta(days=1)
PREV_DATA_INTERVAL = DataInterval(start=PREV_DATA_INTERVAL_START, end=PREV_DATA_INTERVAL_END)
PREV_DATA_INTERVAL_EXACT = DataInterval.exact(PREV_DATA_INTERVAL_END)

CURRENT_TIME = pendulum.DateTime(2021, 9, 7, tzinfo=TIMEZONE)
YESTERDAY = CURRENT_TIME - datetime.timedelta(days=1)
OLD_INTERVAL = DataInterval(start=YESTERDAY, end=CURRENT_TIME)

HOURLY_CRON_TIMETABLE = CronDataIntervalTimetable("@hourly", TIMEZONE)
HOURLY_TIMEDELTA_TIMETABLE = DeltaDataIntervalTimetable(datetime.timedelta(hours=1))
HOURLY_RELATIVEDELTA_TIMETABLE = DeltaDataIntervalTimetable(dateutil.relativedelta.relativedelta(hours=1))

CRON_TIMETABLE = CronDataIntervalTimetable("30 16 * * *", TIMEZONE)
DELTA_FROM_MIDNIGHT = datetime.timedelta(minutes=30, hours=16)


@pytest.mark.parametrize(
    "last_automated_data_interval",
    [pytest.param(None, id="first-run"), pytest.param(PREV_DATA_INTERVAL, id="subsequent")],
)
@time_machine.travel(CURRENT_TIME)
def test_no_catchup_first_starts_at_current_time(
    last_automated_data_interval: DataInterval | None,
) -> None:
    """If ``catchup=False`` and start_date is a day before"""
    next_info = CRON_TIMETABLE.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=YESTERDAY, latest=None, catchup=False),
    )
    expected_start = YESTERDAY + DELTA_FROM_MIDNIGHT
    assert next_info == DagRunInfo.interval(start=expected_start, end=CURRENT_TIME + DELTA_FROM_MIDNIGHT)


@pytest.mark.parametrize(
    "earliest",
    [pytest.param(None, id="none"), pytest.param(START_DATE, id="start_date")],
)
@pytest.mark.parametrize(
    "catchup",
    [pytest.param(True, id="catchup_true"), pytest.param(False, id="catchup_false")],
)
@time_machine.travel(CURRENT_TIME)
def test_new_schedule_interval_next_info_starts_at_new_time(
    earliest: pendulum.DateTime | None,
    catchup: bool,
) -> None:
    """First run after DAG has new schedule interval."""
    next_info = CRON_TIMETABLE.next_dagrun_info(
        last_automated_data_interval=OLD_INTERVAL,
        restriction=TimeRestriction(earliest=earliest, latest=None, catchup=catchup),
    )
    expected_start = YESTERDAY + datetime.timedelta(hours=16, minutes=30)
    expected_end = CURRENT_TIME + datetime.timedelta(hours=16, minutes=30)
    assert next_info == DagRunInfo.interval(start=expected_start, end=expected_end)


@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(HOURLY_CRON_TIMETABLE, id="cron"),
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
@pytest.mark.parametrize(
    "last_automated_data_interval",
    [pytest.param(None, id="first-run"), pytest.param(PREV_DATA_INTERVAL, id="subsequent")],
)
@time_machine.travel(CURRENT_TIME)
def test_no_catchup_next_info_starts_at_current_time(
    timetable: Timetable,
    last_automated_data_interval: DataInterval | None,
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
    [
        pytest.param(HOURLY_CRON_TIMETABLE, id="cron"),
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
def test_catchup_next_info_starts_at_previous_interval_end(timetable: Timetable) -> None:
    """If ``catchup=True``, the next interval starts at the previous's end."""
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=PREV_DATA_INTERVAL,
        restriction=TimeRestriction(earliest=START_DATE, latest=None, catchup=True),
    )
    expected_end = PREV_DATA_INTERVAL_END + datetime.timedelta(hours=1)
    assert next_info == DagRunInfo.interval(start=PREV_DATA_INTERVAL_END, end=expected_end)


@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(HOURLY_CRON_TIMETABLE, id="cron"),
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
def test_validate_success(timetable: Timetable) -> None:
    timetable.validate()


@pytest.mark.parametrize(
    "timetable, error_message",
    [
        pytest.param(
            CronDataIntervalTimetable("0 0 1 13 0", TIMEZONE),
            "[0 0 1 13 0] is not acceptable, out of range",
            id="invalid-cron",
        ),
        pytest.param(
            DeltaDataIntervalTimetable(datetime.timedelta()),
            "schedule interval must be positive, not datetime.timedelta(0)",
            id="zero-timedelta",
        ),
        pytest.param(
            DeltaDataIntervalTimetable(dateutil.relativedelta.relativedelta()),
            "schedule interval must be positive, not relativedelta()",
            id="zero-relativedelta",
        ),
        pytest.param(
            DeltaDataIntervalTimetable(datetime.timedelta(days=-1)),
            # Dynamically formatted since different Python versions display timedelta differently.
            f"schedule interval must be positive, not {datetime.timedelta(days=-1)!r}",
            id="negative-timedelta",
        ),
        pytest.param(
            DeltaDataIntervalTimetable(dateutil.relativedelta.relativedelta(days=-1)),
            "schedule interval must be positive, not relativedelta(days=-1)",
            id="negative-relativedelta",
        ),
    ],
)
def test_validate_failure(timetable: Timetable, error_message: str) -> None:
    with pytest.raises(AirflowTimetableInvalid) as ctx:
        timetable.validate()
    assert str(ctx.value) == error_message


def test_cron_interval_timezone_from_string():
    timetable = CronDataIntervalTimetable("@hourly", "UTC")
    assert timetable.serialize()["timezone"] == "UTC"


@pytest.mark.parametrize(
    "trigger_at, expected_interval",
    [
        # Arbitrary trigger time.
        pytest.param(
            pendulum.DateTime(2022, 8, 8, 1, tzinfo=TIMEZONE),
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
            ),
            id="adhoc",
        ),
        # Trigger time falls exactly on interval boundary.
        pytest.param(
            pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
            ),
            id="exact",
        ),
    ],
)
def test_cron_infer_manual_data_interval_alignment(
    trigger_at: pendulum.DateTime,
    expected_interval: DataInterval,
) -> None:
    timetable = CronDataIntervalTimetable("@daily", TIMEZONE)
    assert timetable.infer_manual_data_interval(run_after=trigger_at) == expected_interval


@pytest.mark.parametrize(
    "last_data_interval, expected_info",
    [
        pytest.param(
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
            ),
            DagRunInfo.interval(
                pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 9, tzinfo=TIMEZONE),
            ),
            id="exact",
        ),
        pytest.param(
            # Previous data interval does not align with the current timetable.
            # This is possible if the user edits a DAG with existing runs.
            DataInterval(
                pendulum.DateTime(2022, 8, 7, 1, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 8, 1, tzinfo=TIMEZONE),
            ),
            DagRunInfo.interval(
                pendulum.DateTime(2022, 8, 8, tzinfo=TIMEZONE),
                pendulum.DateTime(2022, 8, 9, tzinfo=TIMEZONE),
            ),
            id="changed",
        ),
    ],
)
def test_cron_next_dagrun_info_alignment(last_data_interval: DataInterval, expected_info: DagRunInfo):
    timetable = CronDataIntervalTimetable("@daily", TIMEZONE)
    info = timetable.next_dagrun_info(
        last_automated_data_interval=last_data_interval,
        restriction=TimeRestriction(None, None, True),
    )
    assert info == expected_info
