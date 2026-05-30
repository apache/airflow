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
from sqlalchemy import select

from airflow._shared.timezones.timezone import utc
from airflow.exceptions import AirflowTimetableInvalid
from airflow.models import DagModel
from airflow.sdk import CronPartitionTimetable
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.trigger import (
    CronPartitionTimetable as CoreCronPartitionTimetable,
    CronTriggerTimetable,
    DeltaTriggerTimetable,
    MultipleCronTriggerTimetable,
)
from airflow.utils.types import DagRunType

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


def test_partition_key_uses_timetable_timezone():
    """Regression: partition_key reflects the local partition date, not the UTC instant.

    For an Asia/Taipei daily timetable the 2026-02-15 partition fires at Taipei
    midnight = 2026-02-14T16:00:00Z.  partition_date stays that UTC instant, but
    partition_key must read as the local label "2026-02-15T00:00:00" rather than
    the UTC "2026-02-14T16:00:00".
    """
    timetable = CoreCronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")
    info = timetable.next_dagrun_info_v2(
        last_dagrun_info=None,
        restriction=TimeRestriction(
            earliest=pendulum.datetime(2026, 2, 14, 12, tz="Asia/Taipei"),
            latest=None,
            catchup=True,
        ),
    )
    assert info is not None
    # Key formatted in the timetable timezone (the local calendar label).
    assert info.partition_key == "2026-02-15T00:00:00"
    # partition_date stays the UTC instant of Taipei midnight — only the key formatting changed.
    assert info.partition_date == pendulum.datetime(2026, 2, 14, 16, 0, 0, tz="UTC")


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


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_latest_run_no_history(dag_maker, session):
    start_date = pendulum.datetime(2026, 1, 1)
    with dag_maker(
        "test",
        start_date=start_date,
        catchup=True,
        schedule=CronPartitionTimetable(
            "0 * * * *",
            timezone=pendulum.UTC,
        ),
    ):
        pass
    dag_maker.sync_dagbag_to_db()
    session.commit()
    dm = session.scalar(select(DagModel))
    assert dm.next_dagrun is None
    assert dm.next_dagrun_partition_date == start_date
    assert dm.next_dagrun_partition_key == "2026-01-01T00:00:00"


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_latest_run_with_run(dag_maker, session):
    """
    This ensures that the dag processor will figure out the next run correctly
    """
    start_date = pendulum.datetime(2026, 1, 1)
    with dag_maker(
        "test",
        start_date=start_date,
        catchup=True,
        schedule=CronPartitionTimetable(
            "0 0 * * *",
            timezone=pendulum.UTC,
        ),
    ):
        pass
    dag_maker.sync_dagbag_to_db()
    dag_maker.create_dagrun(
        run_id="abc1234",
        logical_date=None,
        data_interval=None,
        run_type="scheduled",
        run_after=start_date + datetime.timedelta(days=3),
        partition_key="anything",
        session=session,
    )
    session.commit()
    dag_maker.sync_dag_to_db()
    session.commit()
    dm = session.scalar(select(DagModel))
    assert dm.next_dagrun is None
    assert dm.next_dagrun_partition_date == start_date + datetime.timedelta(days=4)
    assert dm.next_dagrun_partition_key == "2026-01-05T00:00:00"


@pytest.mark.db_test
@pytest.mark.parametrize(
    ("schedule", "expected"),
    [
        pytest.param(
            CronPartitionTimetable(
                "0 0 * * *",
                timezone=pendulum.UTC,
            ),
            DagRunInfo(
                run_after=START_DATE + datetime.timedelta(days=3),
                data_interval=None,
                partition_date=START_DATE + datetime.timedelta(days=3),
                partition_key="key-1",
            ),
            id="cron-partition",
        ),
        pytest.param(
            "0 0 * * *",
            DagRunInfo(
                run_after=START_DATE + datetime.timedelta(days=3),
                data_interval=DataInterval(
                    start=START_DATE + datetime.timedelta(days=2),
                    end=START_DATE + datetime.timedelta(days=3),
                ),
                partition_date=None,
                partition_key=None,
            ),
            id="cron-trigger",
        ),
    ],
)
def test_run_info_from_dag_run(schedule, expected, dag_maker, session):
    with dag_maker(
        "test",
        start_date=START_DATE,
        catchup=True,
        schedule=schedule,
    ):
        pass
    dag_maker.sync_dagbag_to_db()
    dr = dag_maker.create_dagrun(
        run_id="abc1234",
        logical_date=None,
        data_interval=None,
        run_type="scheduled",
        run_after=START_DATE + datetime.timedelta(days=3),
        partition_key=expected.partition_key,
        partition_date=expected.partition_date,
        session=session,
    )
    info = dag_maker.serialized_dag.timetable.run_info_from_dag_run(dag_run=dr)
    assert info == expected


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
@pytest.mark.parametrize(
    ("schedule", "expected"),
    [
        (
            CronPartitionTimetable(
                "0 0 * * *",
                timezone=pendulum.UTC,
            ),
            DagRunInfo(
                run_after=START_DATE,
                data_interval=None,
                partition_date=START_DATE,
                partition_key="2021-09-04T00:00:00",
            ),
        ),
        (
            "0 0 * * *",
            DagRunInfo(
                run_after=START_DATE + datetime.timedelta(days=1),
                data_interval=DataInterval(
                    start=START_DATE,
                    end=START_DATE + datetime.timedelta(days=1),
                ),
                partition_date=None,
                partition_key=None,
            ),
        ),
    ],
)
def test_next_dagrun_info_v2(schedule, expected, dag_maker, session):
    """
    This ensures that the dag processor will figure out the next run correctly
    """
    with dag_maker(
        "test",
        start_date=START_DATE,
        catchup=True,
        schedule=schedule,
    ):
        pass
    dag_maker.sync_dagbag_to_db()
    serdag = dag_maker.serialized_dag
    timetable = serdag.timetable
    info = timetable.next_dagrun_info_v2(
        last_dagrun_info=None,
        restriction=serdag._time_restriction,
    )
    assert info == expected


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
@pytest.mark.parametrize(
    ("schedule", "partition_key", "expected"),
    [
        (
            CronPartitionTimetable(
                "0 0 * * *",
                timezone=pendulum.UTC,
            ),
            "key-1",
            DagRunInfo(
                run_after=START_DATE,
                data_interval=None,
                partition_date=START_DATE,
                partition_key="2021-09-04T00:00:00",
            ),
        ),
        (
            "0 0 * * *",
            None,
            DagRunInfo(
                run_after=START_DATE + datetime.timedelta(days=1),
                data_interval=DataInterval(
                    start=START_DATE,
                    end=START_DATE + datetime.timedelta(days=1),
                ),
                partition_date=None,
                partition_key=None,
            ),
        ),
    ],
)
def test_next_run_info_from_dag_model(schedule, partition_key, expected, dag_maker, session):
    with dag_maker(
        "test",
        start_date=START_DATE,
        catchup=True,
        schedule=schedule,
    ):
        pass
    dag_maker.sync_dagbag_to_db()
    dm = dag_maker.dag_model
    info = dag_maker.serialized_dag.timetable.next_run_info_from_dag_model(dag_model=dm)
    assert info == expected


def test_generate_run_id_without_partition_key() -> None:
    """
    Tests the generate_run_id method of CronPartitionTimetable.

    generate_run_id shouldn't break even if when the run is manually trigger (partition_key might be missing).
    """
    cron_partitioned_timetabe = CoreCronPartitionTimetable(
        "0 * * * *",
        timezone=pendulum.UTC,
    )
    run_id = cron_partitioned_timetabe.generate_run_id(
        run_type=DagRunType.MANUAL,
        run_after=pendulum.DateTime(2025, 6, 7, 8, 9, tzinfo=pendulum.UTC),
        data_interval=None,
    )
    assert run_id.startswith("manual__2025-06-07T08:09:00+00:00__")


# ---------------------------------------------------------------------------
# run_after_window_for_partition_window unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("run_offset", "from_date", "to_date", "expected_earliest_date", "expected_latest_date"),
    [
        pytest.param(
            0,
            pendulum.datetime(2026, 2, 18, tz="UTC"),
            pendulum.datetime(2026, 2, 20, tz="UTC"),
            pendulum.datetime(2026, 2, 17, tz="UTC"),
            pendulum.datetime(2026, 2, 21, tz="UTC"),
            id="offset_0_degenerates_to_plus_minus_1_day",
        ),
        pytest.param(
            1,
            pendulum.datetime(2026, 2, 18, tz="UTC"),
            pendulum.datetime(2026, 2, 20, tz="UTC"),
            # forward: partition = _get_next(run_after)
            # invert:  run_after ≈ _get_prev(partition)
            # lower: _get_prev(_get_prev(from)) - 1 day
            # with daily "0 0 * * *" Asia/Taipei (UTC+8): Taipei midnight of 2026-02-18
            # is 2026-02-17T16:00Z.  _get_prev(2026-02-18T00:00Z) → 2026-02-17T16:00Z (prev tick)
            # then _get_prev(2026-02-17T16:00Z) → 2026-02-16T16:00Z, minus 1d → 2026-02-15T16:00Z
            pendulum.datetime(2026, 2, 15, 16, 0, 0, tz="UTC"),
            # upper: _get_next(_get_prev(to+1tick)) + 1d
            # _get_next(2026-02-20T00:00Z) → 2026-02-20T16:00Z, _get_prev → 2026-02-19T16:00Z, +1d
            # wait — re-derive: upper_start = _get_next(to_date) = _get_next(2026-02-20T00Z)
            # = 2026-02-20T16Z; then invert_func(_get_prev) once: 2026-02-19T16Z; +1d = 2026-02-20T16Z
            pendulum.datetime(2026, 2, 20, 16, 0, 0, tz="UTC"),
            id="offset_1_earliest_shifted_earlier",
        ),
        pytest.param(
            2,
            pendulum.datetime(2026, 2, 18, tz="UTC"),
            pendulum.datetime(2026, 2, 20, tz="UTC"),
            # Two _get_prev steps from _get_prev(from_date), then -1 day.
            pendulum.datetime(2026, 2, 14, 16, 0, 0, tz="UTC"),
            pendulum.datetime(2026, 2, 19, 16, 0, 0, tz="UTC"),
            id="offset_2_earliest_shifted_2_intervals_earlier",
        ),
        pytest.param(
            -1,
            pendulum.datetime(2026, 2, 18, tz="UTC"),
            pendulum.datetime(2026, 2, 20, tz="UTC"),
            # forward (offset<0): partition = _get_prev(run_after)
            # invert: run_after ≈ _get_next(partition)
            # lower_start = _get_prev(from_date) → 2026-02-17T16Z
            # _get_next(2026-02-17T16Z) → 2026-02-18T16Z, -1d → 2026-02-17T16Z
            pendulum.datetime(2026, 2, 17, 16, 0, 0, tz="UTC"),
            # upper_start = _get_next(to_date) = _get_next(2026-02-20T00Z) → 2026-02-20T16Z
            # _get_next(2026-02-20T16Z) → 2026-02-21T16Z, +1d → 2026-02-22T16Z
            pendulum.datetime(2026, 2, 22, 16, 0, 0, tz="UTC"),
            id="offset_minus1_latest_shifted_later",
        ),
        pytest.param(
            -2,
            pendulum.datetime(2026, 2, 18, tz="UTC"),
            pendulum.datetime(2026, 2, 20, tz="UTC"),
            # lower_start = _get_prev(2026-02-18T00Z) = 2026-02-17T16Z
            # upper_start = _get_next(2026-02-20T00Z) = 2026-02-20T16Z
            # iter1: earliest=_get_next(2026-02-17T16Z)=2026-02-18T16Z, latest=_get_next(2026-02-20T16Z)=2026-02-21T16Z
            # iter2: earliest=_get_next(2026-02-18T16Z)=2026-02-19T16Z, latest=_get_next(2026-02-21T16Z)=2026-02-22T16Z
            # -1d/+1d: earliest=2026-02-18T16Z, latest=2026-02-23T16Z
            pendulum.datetime(2026, 2, 18, 16, 0, 0, tz="UTC"),
            pendulum.datetime(2026, 2, 23, 16, 0, 0, tz="UTC"),
            id="offset_minus2_latest_shifted_2_intervals_later",
        ),
    ],
)
def test_run_after_window_for_partition_window(
    run_offset, from_date, to_date, expected_earliest_date, expected_latest_date
) -> None:
    """run_after_window_for_partition_window returns correct (earliest, latest) bounds."""
    timetable = CoreCronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=run_offset)
    earliest, latest = timetable.run_after_window_for_partition_window(
        from_date=from_date,
        to_date=to_date,
    )
    assert earliest == expected_earliest_date
    assert latest == expected_latest_date


def test_run_after_window_for_partition_window_timezone_day_boundary() -> None:
    """Timezone offset: Asia/Taipei midnight partitions are UTC-prior-day; ±1-day buffer must absorb this."""
    # A Taipei "2026-02-18" partition fires at 2026-02-17T16:00Z.
    # from_date / to_date expressed in UTC as the local Taipei midnight.
    timetable = CoreCronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=0)
    from_date = pendulum.datetime(2026, 2, 17, 16, 0, 0, tz="UTC")  # Taipei 2026-02-18 midnight in UTC
    to_date = pendulum.datetime(2026, 2, 19, 16, 0, 0, tz="UTC")  # Taipei 2026-02-20 midnight in UTC
    earliest, latest = timetable.run_after_window_for_partition_window(
        from_date=from_date,
        to_date=to_date,
    )
    # earliest must be ≤ 2026-02-17T16:00Z so iter can reach it.
    assert earliest <= from_date
    # latest must be ≥ 2026-02-19T16:00Z so iter can reach it.
    assert latest >= to_date


def test_run_after_window_offset_at_tick_endpoint_not_dropped() -> None:
    """When from_date is exactly on a cron tick, offset>0 must still cover that tick (Q1)."""
    # from_date = 2026-02-17T16:00Z, which IS a cron tick for "0 0 * * *" Asia/Taipei
    # (= Taipei 2026-02-18 midnight).
    timetable = CoreCronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=1)
    from_date = pendulum.datetime(2026, 2, 17, 16, 0, 0, tz="UTC")  # exactly one tick
    to_date = pendulum.datetime(2026, 2, 17, 16, 0, 0, tz="UTC")
    earliest, latest = timetable.run_after_window_for_partition_window(
        from_date=from_date,
        to_date=to_date,
    )
    # The run that produces partition 2026-02-17T16Z (i.e. the prior tick) must be reachable.
    expected_run_after = pendulum.datetime(2026, 2, 16, 16, 0, 0, tz="UTC")
    assert earliest <= expected_run_after <= latest


def test_run_after_window_dst_boundary_america_new_york() -> None:
    """DST probe: America/New_York spring-forward (2026-03-08).

    On spring-forward night the 02:00 clock-hour is skipped. The over-generate
    strategy (extra cron tick + ±1 day buffer) must ensure no partition is
    silently dropped for a DST-affected timezone.
    """
    # "0 0 * * *" in America/New_York: midnight ET.
    # Spring-forward 2026: clocks go forward at 2026-03-08T02:00 ET
    # (EST=UTC-5 → EDT=UTC-4).  2026-03-07 midnight ET = 2026-03-07T05:00Z,
    # 2026-03-08 midnight ET = 2026-03-08T05:00Z (no skip at midnight).
    timetable = CoreCronPartitionTimetable("0 0 * * *", timezone="America/New_York", run_offset=1)
    # Partition window spanning the DST transition: 2026-03-07 and 2026-03-08.
    from_date = pendulum.datetime(2026, 3, 7, 5, 0, 0, tz="UTC")  # NY midnight 2026-03-07
    to_date = pendulum.datetime(2026, 3, 8, 5, 0, 0, tz="UTC")  # NY midnight 2026-03-08
    earliest, latest = timetable.run_after_window_for_partition_window(
        from_date=from_date,
        to_date=to_date,
    )
    # With offset=1 the run_after that produces 2026-03-07 partition is the
    # prior tick (2026-03-06T05:00Z).  earliest must be ≤ that.
    expected_run = pendulum.datetime(2026, 3, 6, 5, 0, 0, tz="UTC")
    assert earliest <= expected_run
    # The run_after that produces 2026-03-08 partition is 2026-03-07T05:00Z.
    assert earliest <= pendulum.datetime(2026, 3, 7, 5, 0, 0, tz="UTC") <= latest
