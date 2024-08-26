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
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.utils.timezone import utc

START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=utc)

PREV_DATA_INTERVAL_START = START_DATE
PREV_DATA_INTERVAL_END = START_DATE + datetime.timedelta(days=1)
PREV_DATA_INTERVAL = DataInterval(start=PREV_DATA_INTERVAL_START, end=PREV_DATA_INTERVAL_END)
PREV_DATA_INTERVAL_EXACT = DataInterval.exact(PREV_DATA_INTERVAL_END)

CURRENT_TIME = pendulum.DateTime(2021, 9, 7, tzinfo=utc)
YESTERDAY = CURRENT_TIME - datetime.timedelta(days=1)
OLD_INTERVAL = DataInterval(start=YESTERDAY, end=CURRENT_TIME)

HOURLY_CRON_TIMETABLE = CronDataIntervalTimetable("@hourly", utc)
HOURLY_TIMEDELTA_TIMETABLE = DeltaDataIntervalTimetable(datetime.timedelta(hours=1))
HOURLY_RELATIVEDELTA_TIMETABLE = DeltaDataIntervalTimetable(dateutil.relativedelta.relativedelta(hours=1))

CRON_TIMETABLE = CronDataIntervalTimetable("30 16 * * *", utc)
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
def test_new_schedule_next_info_starts_at_new_time(
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
            CronDataIntervalTimetable("0 0 1 13 0", utc),
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
            pendulum.DateTime(2022, 8, 8, 1, tzinfo=utc),
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=utc),
                pendulum.DateTime(2022, 8, 8, tzinfo=utc),
            ),
            id="adhoc",
        ),
        # Trigger time falls exactly on interval boundary.
        pytest.param(
            pendulum.DateTime(2022, 8, 8, tzinfo=utc),
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=utc),
                pendulum.DateTime(2022, 8, 8, tzinfo=utc),
            ),
            id="exact",
        ),
    ],
)
def test_cron_infer_manual_data_interval_alignment(
    trigger_at: pendulum.DateTime,
    expected_interval: DataInterval,
) -> None:
    timetable = CronDataIntervalTimetable("@daily", utc)
    assert timetable.infer_manual_data_interval(run_after=trigger_at) == expected_interval


@pytest.mark.parametrize(
    "last_data_interval, expected_info",
    [
        pytest.param(
            DataInterval(
                pendulum.DateTime(2022, 8, 7, tzinfo=utc),
                pendulum.DateTime(2022, 8, 8, tzinfo=utc),
            ),
            DagRunInfo.interval(
                pendulum.DateTime(2022, 8, 8, tzinfo=utc),
                pendulum.DateTime(2022, 8, 9, tzinfo=utc),
            ),
            id="exact",
        ),
        pytest.param(
            # Previous data interval does not align with the current timetable.
            # This is possible if the user edits a DAG with existing runs.
            DataInterval(
                pendulum.DateTime(2022, 8, 7, 1, tzinfo=utc),
                pendulum.DateTime(2022, 8, 8, 1, tzinfo=utc),
            ),
            DagRunInfo.interval(
                pendulum.DateTime(2022, 8, 8, tzinfo=utc),
                pendulum.DateTime(2022, 8, 9, tzinfo=utc),
            ),
            id="changed",
        ),
    ],
)
def test_cron_next_dagrun_info_alignment(last_data_interval: DataInterval, expected_info: DagRunInfo):
    timetable = CronDataIntervalTimetable("@daily", utc)
    info = timetable.next_dagrun_info(
        last_automated_data_interval=last_data_interval,
        restriction=TimeRestriction(None, None, True),
    )
    assert info == expected_info


class TestCronIntervalDst:
    """Test cron interval timetable can correctly enter a DST boundary.

    Zurich (Switzerland) is chosen since it is +1/+2 DST, making it a bit easier
    to get my head around the mental timezone conversion.

    In 2023, DST entered on 26th Mar, 2am local clocks (1am UTC) were turned
    forward to 3am. DST exited on 29th Oct, 3am local clocks (1am UTC) were
    turned backward to 2am (making the 2:XX hour fold).
    """

    def test_entering_exact(self) -> None:
        timetable = CronDataIntervalTimetable("0 3 * * *", timezone="Europe/Zurich")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2023, 3, 24, tz=utc),
            latest=None,
            catchup=True,
        )

        # Last run before DST. Interval starts and ends on 2am UTC (local time is +1).
        next_info = timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 24, 2, tz=utc),
            pendulum.datetime(2023, 3, 25, 2, tz=utc),
        )

        # Crossing the DST switch. Interval starts on 2am UTC (local time +1)
        # but ends on 1am UTC (local time is +2).
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 25, 2, tz=utc),
            pendulum.datetime(2023, 3, 26, 1, tz=utc),
        )

        # In DST. Interval starts and ends on 1am UTC (local time is +2).
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 26, 1, tz=utc),
            pendulum.datetime(2023, 3, 27, 1, tz=utc),
        )

    def test_entering_skip(self) -> None:
        timetable = CronDataIntervalTimetable("0 2 * * *", timezone="Europe/Zurich")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2023, 3, 24, tz=utc),
            latest=None,
            catchup=True,
        )

        # Last run before DST. Interval starts and ends on 1am UTC (local time is +1).
        next_info = timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 24, 1, tz=utc),
            pendulum.datetime(2023, 3, 25, 1, tz=utc),
        )

        # Crossing the DST switch. Interval starts on 1am UTC (local time +1)
        # and ends on 1am UTC (local time is +2) since the 2am wall clock time
        # does not logically exist due to entering DST.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 25, 1, tz=utc),
            pendulum.datetime(2023, 3, 26, 1, tz=utc),
        )

        # In DST. Interval starts on 1am UTC (local time is +2 but 2am local
        # time is not possible) and ends on 0am UTC.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 3, 26, 1, tz=utc),
            pendulum.datetime(2023, 3, 27, 0, tz=utc),
        )

    def test_exiting_exact(self) -> None:
        timetable = CronDataIntervalTimetable("0 3 * * *", timezone="Europe/Zurich")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2023, 10, 27, tz=utc),
            latest=None,
            catchup=True,
        )

        # Last run in DST. Interval starts and ends on 1am UTC (local time is +2).
        next_info = timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 27, 1, tz=utc),
            pendulum.datetime(2023, 10, 28, 1, tz=utc),
        )

        # Crossing the DST switch. Interval starts on 1am UTC (local time +2)
        # and ends on 2am UTC (local time +1).
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 28, 1, tz=utc),
            pendulum.datetime(2023, 10, 29, 2, tz=utc),
        )

        # Out of DST. Interval starts and ends on 2am UTC (local time is +1).
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 29, 2, tz=utc),
            pendulum.datetime(2023, 10, 30, 2, tz=utc),
        )

    def test_exiting_fold(self) -> None:
        timetable = CronDataIntervalTimetable("0 2 * * *", timezone="Europe/Zurich")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2023, 10, 27, tz=utc),
            latest=None,
            catchup=True,
        )

        # Last run before folding. Interval starts and ends on 0am UTC (local
        # time is +2).
        next_info = timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 27, 0, tz=utc),
            pendulum.datetime(2023, 10, 28, 0, tz=utc),
        )

        # Account for folding. Interval starts on 0am UTC (local time +2) and
        # ends on 1am UTC (local time +1). There are two 2am local times on the
        # 29th due to folding. We end on the second one (fold=1. There's no
        # logical reason here; this is simply what Airflow has been doing since
        # a long time ago, and there's no point breaking it.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 28, 0, tz=utc),
            pendulum.datetime(2023, 10, 29, 1, tz=utc),
        )

        # Stepping out of DST. Interval starts from the folded 2am local time
        # (1am UTC out of DST) since that is when the previous interval ended.
        # It ends at 1am UTC (local time is +1) normally.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2023, 10, 29, 1, tz=utc),
            pendulum.datetime(2023, 10, 30, 1, tz=utc),
        )


class TestCronIntervalDstNonTrivial:
    """These tests are similar to TestCronIntervalDst but with a different cron.

    The original test cases are from apache/airflow#7999. In 2020 at Los Angeles,
    DST started on 8th Mar; 10am UTC was turned from 2am UTC-8 to 3am UTC-7.
    """

    def test_7_to_8_entering(self):
        timetable = CronDataIntervalTimetable("0 7-8 * * *", timezone="America/Los_Angeles")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2020, 3, 7, tz=utc),
            latest=None,
            catchup=True,
        )

        # Triggers as expected before the interval touches the DST transition.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 7, 7 + 8, tz=utc),
            pendulum.datetime(2020, 3, 7, 8 + 8, tz=utc),
        )

        # This interval ends an hour early since it includes the DST switch!
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 7, 8 + 8, tz=utc),
            pendulum.datetime(2020, 3, 8, 7 + 7, tz=utc),
        )

        # We're fully into DST so the interval is as expected.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 8, 7 + 7, tz=utc),
            pendulum.datetime(2020, 3, 8, 8 + 7, tz=utc),
        )

    def test_7_and_9_entering(self):
        timetable = CronDataIntervalTimetable("0 7,9 * * *", timezone="America/Los_Angeles")
        restriction = TimeRestriction(
            earliest=pendulum.datetime(2020, 3, 7, tz=utc),
            latest=None,
            catchup=True,
        )

        # Triggers as expected before the interval touches the DST transition.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=None,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 7, 7 + 8, tz=utc),
            pendulum.datetime(2020, 3, 7, 9 + 8, tz=utc),
        )

        # This interval ends an hour early since it includes the DST switch!
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 7, 9 + 8, tz=utc),
            pendulum.datetime(2020, 3, 8, 7 + 7, tz=utc),
        )

        # We're fully into DST so the interval is as expected.
        next_info = timetable.next_dagrun_info(
            last_automated_data_interval=next_info.data_interval,
            restriction=restriction,
        )
        assert next_info
        assert next_info.data_interval == DataInterval(
            pendulum.datetime(2020, 3, 8, 7 + 7, tz=utc),
            pendulum.datetime(2020, 3, 8, 9 + 7, tz=utc),
        )


def test_fold_scheduling():
    timetable = CronDataIntervalTimetable("*/30 * * * *", timezone="Europe/Zurich")
    restriction = TimeRestriction(
        earliest=pendulum.datetime(2023, 10, 28, 23, 30, tz=utc),  # Locally 1:30 (DST).
        latest=None,
        catchup=True,
    )

    # Still in DST, acting normally.
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    assert next_info
    assert next_info.data_interval == DataInterval(
        pendulum.datetime(2023, 10, 28, 23, 30, tz=utc),
        pendulum.datetime(2023, 10, 29, 0, 0, tz=utc),  # Locally 2am (DST).
    )
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=next_info.data_interval,
        restriction=restriction,
    )
    assert next_info
    assert next_info.data_interval == DataInterval(
        pendulum.datetime(2023, 10, 29, 0, 0, tz=utc),
        pendulum.datetime(2023, 10, 29, 0, 30, tz=utc),  # Locally 2:30 (DST).
    )

    # Crossing into fold.
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=next_info.data_interval,
        restriction=restriction,
    )
    assert next_info
    assert next_info.data_interval == DataInterval(
        pendulum.datetime(2023, 10, 29, 0, 30, tz=utc),
        pendulum.datetime(2023, 10, 29, 1, 0, tz=utc),  # Locally 2am (fold, not DST).
    )

    # In the "fold zone".
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=next_info.data_interval,
        restriction=restriction,
    )
    assert next_info
    assert next_info.data_interval == DataInterval(
        pendulum.datetime(2023, 10, 29, 1, 0, tz=utc),
        pendulum.datetime(2023, 10, 29, 1, 30, tz=utc),  # Locally 2am (fold, not DST).
    )

    # Stepping out of fold.
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=next_info.data_interval,
        restriction=restriction,
    )
    assert next_info
    assert next_info.data_interval == DataInterval(
        pendulum.datetime(2023, 10, 29, 1, 30, tz=utc),
        pendulum.datetime(2023, 10, 29, 2, 0, tz=utc),  # Locally 3am (not DST).
    )
