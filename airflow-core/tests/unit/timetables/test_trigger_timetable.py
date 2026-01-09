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
import pytest
import time_machine

from airflow._shared.timezones.timezone import utc
from airflow.exceptions import AirflowTimetableInvalid
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.trigger import (
    CronTriggerTimetable,
    DeltaTriggerTimetable,
    MultipleCronTriggerTimetable,
)

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=utc)

PREV_DATA_INTERVAL_END = START_DATE + datetime.timedelta(days=1)
PREV_DATA_INTERVAL_EXACT = DataInterval.exact(PREV_DATA_INTERVAL_END)

CURRENT_TIME = pendulum.DateTime(2021, 9, 7, tzinfo=utc)
YESTERDAY = CURRENT_TIME - datetime.timedelta(days=1)

HOURLY_CRON_TRIGGER_TIMETABLE = CronTriggerTimetable("@hourly", timezone=utc, run_immediately=True)

HOURLY_TIMEDELTA_TIMETABLE = DeltaTriggerTimetable(datetime.timedelta(hours=1))
HOURLY_RELATIVEDELTA_TIMETABLE = DeltaTriggerTimetable(dateutil.relativedelta.relativedelta(hours=1))

DELTA_FROM_MIDNIGHT = datetime.timedelta(minutes=30, hours=16)


@pytest.mark.parametrize(
    ("last_automated_data_interval", "next_start_time"),
    [
        pytest.param(
            None,
            YESTERDAY + DELTA_FROM_MIDNIGHT,
            id="first-run",
        ),
        pytest.param(
            DataInterval.exact(YESTERDAY + DELTA_FROM_MIDNIGHT),
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
    timetable = CronTriggerTimetable(
        "30 16 * * *",
        timezone=utc,
        run_immediately=False,  # Should have no effect since earliest is not None
    )
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=YESTERDAY, latest=None, catchup=False),
    )
    assert next_info == DagRunInfo.exact(next_start_time)


@pytest.mark.parametrize(
    ("last_automated_data_interval", "next_start_time"),
    [
        pytest.param(
            None,
            CURRENT_TIME,
            id="first-run",
        ),
        pytest.param(
            DataInterval.exact(YESTERDAY + DELTA_FROM_MIDNIGHT),
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
@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(DeltaTriggerTimetable(datetime.timedelta(days=1)), id="timedelta"),
        pytest.param(DeltaTriggerTimetable(dateutil.relativedelta.relativedelta(days=1)), id="relativedelta"),
    ],
)
@time_machine.travel(CURRENT_TIME)
def test_daily_delta_trigger_no_catchup_first_starts_at_next_schedule(
    last_automated_data_interval: DataInterval | None,
    next_start_time: pendulum.DateTime,
    timetable: Timetable,
) -> None:
    """If ``catchup=False`` and start_date is a day before"""
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=YESTERDAY, latest=None, catchup=False),
    )
    assert next_info == DagRunInfo.exact(next_start_time)


@pytest.mark.parametrize(
    ("current_time", "earliest", "expected"),
    [
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            id="current_time_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            id="current_time_not_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="current_time_miss_one_interval_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 1, 30, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="current_time_miss_one_interval_not_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            pendulum.DateTime(2199, 12, 31, 22, 30, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2199, 12, 31, 23, 0, 0, tzinfo=utc)),
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
    ("current_time", "earliest", "expected"),
    [
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            id="current_time_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc)),
            id="current_time_not_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="current_time_miss_one_interval_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 1, 30, 0, tzinfo=utc),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 30, 0, tzinfo=utc)),
            id="current_time_miss_one_interval_not_on_boundary",
        ),
        pytest.param(
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            pendulum.DateTime(2199, 12, 31, 22, 30, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2199, 12, 31, 22, 30, 0, tzinfo=utc)),
            id="future_start_date",
        ),
    ],
)
@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
def test_hourly_delta_trigger_no_catchup_next_info(
    current_time: pendulum.DateTime,
    earliest: pendulum.DateTime,
    expected: DagRunInfo,
    timetable: Timetable,
) -> None:
    with time_machine.travel(current_time):
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=PREV_DATA_INTERVAL_EXACT,
            restriction=TimeRestriction(earliest=earliest, latest=None, catchup=False),
        )
    assert next_info == expected


@pytest.mark.parametrize(
    ("last_automated_data_interval", "earliest", "expected"),
    [
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="last_automated_on_boundary",
        ),
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="last_automated_not_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            id="no_last_automated_with_earliest_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="no_last_automated_with_earliest_not_on_boundary",
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


@pytest.mark.parametrize(
    ("last_automated_data_interval", "earliest", "expected"),
    [
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 0, 0, tzinfo=utc)),
            id="last_automated_on_boundary",
        ),
        pytest.param(
            DataInterval.exact(pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc)),
            START_DATE,
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 1, 30, 0, tzinfo=utc)),
            id="last_automated_not_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 0, 0, tzinfo=utc)),
            id="no_last_automated_with_earliest_on_boundary",
        ),
        pytest.param(
            None,
            pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc),
            DagRunInfo.exact(pendulum.DateTime(2022, 7, 27, 0, 30, 0, tzinfo=utc)),
            id="no_last_automated_with_earliest_not_on_boundary",
        ),
    ],
)
@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
def test_hourly_delta_trigger_catchup_next_info(
    last_automated_data_interval: DataInterval | None,
    earliest: pendulum.DateTime | None,
    expected: DagRunInfo | None,
    timetable: Timetable,
) -> None:
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=last_automated_data_interval,
        restriction=TimeRestriction(earliest=earliest, latest=None, catchup=True),
    )
    assert next_info == expected


def test_cron_trigger_next_info_with_interval():
    # Runs every Monday on 16:30, covering the day before the run.
    timetable = CronTriggerTimetable(
        "30 16 * * MON",
        timezone=utc,
        interval=datetime.timedelta(hours=16, minutes=30),
    )

    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(
            pendulum.DateTime(2022, 8, 1, tzinfo=utc),
            pendulum.DateTime(2022, 8, 1, 16, 30, tzinfo=utc),
        ),
        restriction=TimeRestriction(earliest=START_DATE, latest=None, catchup=True),
    )
    assert next_info == DagRunInfo.interval(
        pendulum.DateTime(2022, 8, 8, tzinfo=utc),
        pendulum.DateTime(2022, 8, 8, 16, 30, tzinfo=utc),
    )


@pytest.mark.parametrize(
    "timetable",
    [
        pytest.param(HOURLY_CRON_TRIGGER_TIMETABLE, id="cron"),
        pytest.param(HOURLY_TIMEDELTA_TIMETABLE, id="timedelta"),
        pytest.param(HOURLY_RELATIVEDELTA_TIMETABLE, id="relativedelta"),
    ],
)
def test_validate_success(timetable: Timetable) -> None:
    timetable.validate()


@pytest.mark.parametrize(
    ("timetable", "message"),
    [
        pytest.param(
            CronTriggerTimetable("0 0 1 13 0", timezone=utc),
            "[0 0 1 13 0] is not acceptable, out of range",
            id="cron",
        ),
        pytest.param(
            DeltaTriggerTimetable(datetime.timedelta(days=-1)),
            "schedule interval must be positive, not datetime.timedelta(days=-1)",
            id="timedelta",
        ),
        pytest.param(
            DeltaTriggerTimetable(dateutil.relativedelta.relativedelta(days=-1)),
            "schedule interval must be positive, not relativedelta(days=-1)",
            id="relativedelta",
        ),
    ],
)
def test_validate_failure(timetable: Timetable, message: str) -> None:
    with pytest.raises(AirflowTimetableInvalid) as ctx:
        timetable.validate()
    assert str(ctx.value) == message


@pytest.mark.parametrize(
    ("timetable", "data"),
    [
        pytest.param(
            HOURLY_CRON_TRIGGER_TIMETABLE,
            {"expression": "0 * * * *", "run_immediately": True, "timezone": "UTC", "interval": 0.0},
            id="hourly",
        ),
        pytest.param(
            CronTriggerTimetable(
                "0 0 1 12 *", timezone=utc, run_immediately=False, interval=datetime.timedelta(hours=2)
            ),
            {"expression": "0 0 1 12 *", "run_immediately": False, "timezone": "UTC", "interval": 7200.0},
            id="interval",
        ),
        pytest.param(
            CronTriggerTimetable(
                "0 0 1 12 0",
                timezone="Asia/Taipei",
                run_immediately=False,
                interval=dateutil.relativedelta.relativedelta(weekday=dateutil.relativedelta.MO),
            ),
            {
                "expression": "0 0 1 12 0",
                "run_immediately": False,
                "timezone": "Asia/Taipei",
                "interval": {"weekday": [0]},
            },
            id="non-utc-interval",
        ),
    ],
)
def test_cron_trigger_serialization(timetable: CronTriggerTimetable, data: dict[str, typing.Any]) -> None:
    assert timetable.serialize() == data

    tt = CronTriggerTimetable.deserialize(data)
    assert isinstance(tt, CronTriggerTimetable)
    assert tt._expression == timetable._expression
    assert tt._timezone == timetable._timezone
    assert tt._interval == timetable._interval
    assert tt._run_immediately == timetable._run_immediately


@pytest.mark.parametrize(
    ("timetable", "data"),
    [
        pytest.param(
            HOURLY_TIMEDELTA_TIMETABLE,
            {"delta": 3600.0, "interval": 0.0},
            id="timedelta",
        ),
        pytest.param(
            DeltaTriggerTimetable(
                datetime.timedelta(hours=3),
                interval=dateutil.relativedelta.relativedelta(weekday=dateutil.relativedelta.MO),
            ),
            {"delta": 10800.0, "interval": {"weekday": [0]}},
            id="timedelta-interval",
        ),
        pytest.param(
            HOURLY_RELATIVEDELTA_TIMETABLE,
            {"delta": {"hours": 1}, "interval": 0.0},
            id="relativedelta",
        ),
        pytest.param(
            DeltaTriggerTimetable(
                dateutil.relativedelta.relativedelta(weekday=dateutil.relativedelta.MO),
                interval=datetime.timedelta(days=7),
            ),
            {"delta": {"weekday": [0]}, "interval": 604800.0},
            id="relativedelta-interval",
        ),
    ],
)
def test_delta_trigger_serialization(timetable: DeltaTriggerTimetable, data: dict[str, typing.Any]) -> None:
    assert timetable.serialize() == data

    tt = DeltaTriggerTimetable.deserialize(data)
    assert isinstance(tt, DeltaTriggerTimetable)
    assert tt._delta == timetable._delta
    assert tt._interval == timetable._interval


JUST_AFTER = pendulum.datetime(year=2024, month=8, day=15, hour=3, minute=5)
WAY_AFTER = pendulum.datetime(year=2024, month=8, day=15, hour=12, minute=5)
PREVIOUS = DagRunInfo.exact(pendulum.datetime(year=2024, month=8, day=15, hour=3))
NEXT = DagRunInfo.exact(pendulum.datetime(year=2024, month=8, day=16, hour=3))


@pytest.mark.parametrize("catchup", [True, False])
@pytest.mark.parametrize(
    ("run_immediately", "current_time", "correct_interval"),
    [
        (True, WAY_AFTER, PREVIOUS),
        (False, JUST_AFTER, PREVIOUS),
        (False, WAY_AFTER, NEXT),
        (datetime.timedelta(minutes=10), JUST_AFTER, PREVIOUS),
        (datetime.timedelta(minutes=10), WAY_AFTER, NEXT),
    ],
)
def test_run_immediately(catchup, run_immediately, current_time, correct_interval):
    timetable = CronTriggerTimetable(
        "0 3 * * *",
        timezone=utc,
        run_immediately=run_immediately,
    )
    with time_machine.travel(current_time):
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=None, latest=None, catchup=catchup),
        )
        assert next_info == correct_interval


@pytest.mark.parametrize("catchup", [True, False])
def test_run_immediately_fast_dag(catchup):
    timetable = CronTriggerTimetable(
        "*/10 3 * * *",  # Runs every 10 minutes, so falls back to 5 min hardcoded limit on buffer time
        timezone=utc,
        run_immediately=False,
    )
    with time_machine.travel(JUST_AFTER, tick=False):
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=TimeRestriction(earliest=None, latest=None, catchup=catchup),
        )
        assert next_info == PREVIOUS


@pytest.mark.parametrize(
    ("start_date", "expected"),
    [
        (pendulum.datetime(2025, 1, 1), pendulum.datetime(2025, 1, 1)),
        (pendulum.datetime(2025, 1, 1, minute=5), pendulum.datetime(2025, 1, 1, minute=30)),
        (pendulum.datetime(2025, 1, 1, minute=35), pendulum.datetime(2025, 1, 1, hour=1)),
    ],
)
def test_multi_run_first(start_date, expected):
    timetable = MultipleCronTriggerTimetable("@hourly", "30 * * * *", timezone=utc)
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=start_date, latest=None, catchup=True),
    )
    assert next_info == DagRunInfo.exact(expected)


@pytest.mark.parametrize(
    ("last", "expected"),
    [
        (pendulum.datetime(2025, 1, 1), pendulum.datetime(2025, 1, 1, minute=30)),
        (pendulum.datetime(2025, 1, 1, minute=30), pendulum.datetime(2025, 1, 1, hour=1)),
    ],
)
def test_multi_run_next_catchup(last, expected):
    timetable = MultipleCronTriggerTimetable("@hourly", "30 * * * *", timezone=utc)
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval.exact(last),
        restriction=TimeRestriction(earliest=None, latest=None, catchup=True),
    )
    assert next_info == DagRunInfo.exact(expected)


@pytest.mark.parametrize(
    ("last", "current_time", "expected"),
    [
        pytest.param(
            pendulum.datetime(2025, 2, 1),
            pendulum.datetime(2025, 2, 1, minute=30),
            pendulum.datetime(2025, 2, 1, minute=30),
            id="exact",
        ),
        pytest.param(
            pendulum.datetime(2025, 2, 1),
            pendulum.datetime(2025, 2, 1, hour=1, minute=59),
            pendulum.datetime(2025, 2, 1, hour=1, minute=30),
            id="choose-closest-past",
        ),
        pytest.param(
            pendulum.datetime(2025, 2, 1),
            pendulum.datetime(2025, 2, 1, minute=29),
            pendulum.datetime(2025, 2, 1, minute=30),
            id="no-past-choose-closest-future",
        ),
    ],
)
def test_multi_run_next_no_catchup(last, current_time, expected):
    timetable = MultipleCronTriggerTimetable("@hourly", "30 * * * *", timezone=utc)
    with time_machine.travel(current_time):
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=DataInterval.exact(last),
            restriction=TimeRestriction(earliest=None, latest=None, catchup=False),
        )
    assert next_info == DagRunInfo.exact(expected)


def test_multi_serialization():
    timetable = MultipleCronTriggerTimetable(
        "@every 30s",
        "*/2 * * * *",
        timezone="UTC",
        interval=datetime.timedelta(minutes=10),
    )
    data = timetable.serialize()
    assert data == {
        "expressions": ["@every 30s", "*/2 * * * *"],
        "timezone": "UTC",
        "interval": 600.0,
        "run_immediately": False,
    }

    tt = MultipleCronTriggerTimetable.deserialize(data)
    assert isinstance(tt, MultipleCronTriggerTimetable)
    assert len(tt._timetables) == 2
    assert tt._timetables[0]._expression == "@every 30s"
    assert tt._timetables[1]._expression == "*/2 * * * *"
    assert tt._timetables[0]._timezone == tt._timetables[1]._timezone == utc
    assert tt._timetables[0]._interval == tt._timetables[1]._interval == datetime.timedelta(minutes=10)
    assert tt._timetables[0]._run_immediately == tt._timetables[1]._run_immediately is False
