from __future__ import annotations

import pendulum
import pytest
import time_machine

from airflow.timetables.base import TimeRestriction
from airflow.timetables.simple import ContinuousTimetable

START_DATE = pendulum.datetime(2023, 3, 3, tz="UTC")
DURING_DATE = pendulum.datetime(2023, 3, 6, tz="UTC")
END_DATE = pendulum.datetime(2023, 3, 10, tz="UTC")
AFTER_DATE = pendulum.datetime(2023, 3, 12, tz="UTC")


@pytest.fixture()
def restriction():
    return TimeRestriction(earliest=START_DATE, latest=END_DATE, catchup=True)


@pytest.fixture()
def timetable():
    return ContinuousTimetable()


def test_no_runs_without_start_date(timetable):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=None, latest=None, catchup=False),
    )
    assert next_info is None


@time_machine.travel(DURING_DATE)
def test_first_run_matches_start_date(timetable, restriction):
    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    assert next_info.run_after == START_DATE


@time_machine.travel(DURING_DATE)
def test_run_uses_utcnow(timetable, restriction):

    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=START_DATE,
        restriction=restriction,
    )

    assert next_info.run_after == DURING_DATE


@time_machine.travel(AFTER_DATE)
def test_no_runs_after_end_date(timetable, restriction):

    next_info = timetable.next_dagrun_info(
        last_automated_data_interval=START_DATE,
        restriction=restriction,
    )

    assert next_info is None
