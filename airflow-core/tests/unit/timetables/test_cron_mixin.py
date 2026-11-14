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

from airflow.timetables._cron import CronMixin, _localize_all_folds

SAMPLE_TZ = "UTC"


def test_valid_cron_expression():
    cm = CronMixin("* * 1 * *", SAMPLE_TZ)  # every day at midnight
    assert isinstance(cm.description, str)
    assert "Every minute" in cm.description or "month" in cm.description


def test_invalid_cron_expression():
    cm = CronMixin("invalid cron", SAMPLE_TZ)
    assert cm.description == ""


def test_dom_and_dow_conflict():
    cm = CronMixin("* * 1 * 1", SAMPLE_TZ)  # 1st of month or Monday
    desc = cm.description

    assert "(or)" in desc
    assert "Every minute, on day 1 of the month" in desc
    assert "Every minute, only on Monday" in desc


@pytest.mark.parametrize(
    ("timezone", "friday_last_tick", "monday_first_tick"),
    [
        pytest.param(
            "Europe/Berlin",
            pendulum.datetime(2026, 10, 23, 21, 0, tz="UTC"),  # Fri 23:00 CEST
            pendulum.datetime(2026, 10, 25, 23, 0, tz="UTC"),  # Mon 00:00 CET
            id="berlin-fall-back",
        ),
        pytest.param(
            "America/Chicago",
            pendulum.datetime(2026, 10, 31, 4, 0, tz="UTC"),  # Fri 2026-10-30 23:00 CDT
            pendulum.datetime(2026, 11, 2, 6, 0, tz="UTC"),  # Mon 00:00 CST
            id="chicago-fall-back",
        ),
        pytest.param(
            "Australia/Sydney",
            pendulum.datetime(2026, 4, 3, 12, 0, tz="UTC"),  # Fri 23:00 AEDT
            pendulum.datetime(2026, 4, 5, 14, 0, tz="UTC"),  # Mon 00:00 AEST
            id="sydney-fall-back",
        ),
        pytest.param(
            "Europe/Berlin",
            pendulum.datetime(2027, 3, 26, 22, 0, tz="UTC"),  # Fri 23:00 CET
            pendulum.datetime(2027, 3, 28, 22, 0, tz="UTC"),  # Mon 00:00 CEST
            id="berlin-spring-forward",
        ),
    ],
)
def test_weekday_hourly_across_weekend_dst(
    timezone: str, friday_last_tick: pendulum.DateTime, monday_first_tick: pendulum.DateTime
):
    mixin = CronMixin("0 * * * 1-5", timezone)

    assert mixin._get_next(friday_last_tick) == monday_first_tick
    assert mixin._get_prev(monday_first_tick) == friday_last_tick


@pytest.mark.parametrize(
    ("timezone", "cron", "current", "expected"),
    [
        pytest.param(
            "America/Havana",
            "0 * * * 0",
            pendulum.datetime(2025, 10, 27, 3, 0, tz="UTC"),
            # Sun 00:00 -04:00, first of the folded hour
            pendulum.datetime(2025, 11, 2, 4, 0, tz="UTC"),
            id="havana-target-in-fold-hour",
        ),
        pytest.param(
            "Europe/Berlin",
            "0 * * * *",
            pendulum.datetime(2026, 10, 25, 0, 0, tz="UTC"),
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            id="berlin-fold-first-step",
        ),
        pytest.param(
            "Europe/Berlin",
            "0 * * * *",
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            pendulum.datetime(2026, 10, 25, 2, 0, tz="UTC"),
            id="berlin-fold-second-step",
        ),
        pytest.param(
            "America/Havana",
            "0 * * * 0",
            pendulum.datetime(2025, 3, 8, 4, 0, tz="UTC"),
            # Sun 00:00 is skipped by the spring-forward gap, so the next tick is the following hour
            pendulum.datetime(2025, 3, 9, 5, 0, tz="UTC"),
            id="havana-next-target-in-gap",
        ),
        pytest.param(
            "America/Godthab",
            "0 * * * 6",
            pendulum.datetime(2026, 10, 25, 0, 0, tz="UTC"),
            # Sat 23:00 -02:00 is followed by its fold twin Sat 23:00 -03:00
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            id="godthab-next-fold-twin",
        ),
        pytest.param(
            "America/Santiago",
            "0 * * * 6",
            pendulum.datetime(2026, 4, 5, 2, 0, tz="UTC"),
            # Sat 23:00 -03:00 is followed by its fold twin Sat 23:00 -04:00
            pendulum.datetime(2026, 4, 5, 3, 0, tz="UTC"),
            id="santiago-next-fold-twin",
        ),
        pytest.param(
            "America/Godthab",
            "0,30 * * * 6",
            pendulum.datetime(2026, 10, 25, 0, 30, tz="UTC"),
            # Sat 23:00 -02:00 (second pass of the fold hour) comes before the twin Sat 23:30 -02:00
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            id="godthab-next-half-hour-fold",
        ),
        pytest.param(
            "Australia/Lord_Howe",
            "0,10 * * * 0",
            pendulum.datetime(2026, 4, 4, 14, 10, tz="UTC"),
            # Sun 02:00 +10:30, first tick after the 30 minute fall-back
            pendulum.datetime(2026, 4, 4, 15, 30, tz="UTC"),
            id="lord-howe-next-half-hour-dst",
        ),
        pytest.param(
            "Australia/Lord_Howe",
            "0,45 * * * 0",
            pendulum.datetime(2026, 4, 4, 14, 10, tz="UTC"),
            # Sun 01:45 +11:00, the first pass of the folded 01:30-02:00 range
            pendulum.datetime(2026, 4, 4, 14, 45, tz="UTC"),
            id="lord-howe-next-tick-in-fold",
        ),
        pytest.param(
            "Antarctica/Troll",
            "0,10 * * * 0",
            pendulum.datetime(2026, 10, 25, 0, 15, tz="UTC"),
            # Sun 01:00 +01:00 after the two hour fall-back, not the later twin of Sun 02:00 +02:00
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            id="troll-next-two-hour-fold",
        ),
        pytest.param(
            "America/Havana",
            "0 * 2 3,11 *",
            pendulum.datetime(2025, 3, 3, 4, 0, tz="UTC"),
            # Sun 00:00 -04:00, the earlier of the two midnights; both ends of the span are -05:00
            pendulum.datetime(2025, 11, 2, 4, 0, tz="UTC"),
            id="havana-next-sparse-cron-target-in-fold",
        ),
        pytest.param(
            "America/Godthab",
            "0 * * * 0",
            pendulum.datetime(2026, 10, 25, 0, 30, tz="UTC"),
            # current is Sat 23:30 -01:00, the first pass of the fall-back fold; only Sunday has ticks,
            # so the answer is Sun 00:00 -02:00, not the second pass of current's own hour
            pendulum.datetime(2026, 10, 25, 2, 0, tz="UTC"),
            id="godthab-next-day-boundary-fold",
        ),
    ],
)
def test_get_next_across_dst(
    timezone: str, cron: str, current: pendulum.DateTime, expected: pendulum.DateTime
):
    assert CronMixin(cron, timezone)._get_next(current) == expected


@pytest.mark.parametrize(
    ("timezone", "cron", "current", "expected"),
    [
        pytest.param(
            "America/Godthab",
            "0 * * * 6",
            pendulum.datetime(2026, 4, 4, 1, 0, tz="UTC"),
            # Sat 22:00 -02:00; Sat 23:00 is skipped
            pendulum.datetime(2026, 3, 29, 0, 0, tz="UTC"),
            id="godthab-target-in-gap",
        ),
        pytest.param(
            "Europe/Berlin",
            "0 * * * *",
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            pendulum.datetime(2026, 10, 25, 0, 0, tz="UTC"),
            id="berlin-fold-first-step",
        ),
        pytest.param(
            "Europe/Berlin",
            "0 * * * *",
            pendulum.datetime(2026, 10, 25, 2, 0, tz="UTC"),
            pendulum.datetime(2026, 10, 25, 1, 0, tz="UTC"),
            id="berlin-fold-second-step",
        ),
        pytest.param(
            "America/Havana",
            "0 * * * 0",
            pendulum.datetime(2025, 11, 2, 5, 0, tz="UTC"),
            # Sun 00:00 -04:00, the earlier of the two midnights
            pendulum.datetime(2025, 11, 2, 4, 0, tz="UTC"),
            id="havana-prev-fold-twin",
        ),
        pytest.param(
            "America/Havana",
            "0,30 * * * 0",
            pendulum.datetime(2025, 11, 2, 5, 0, tz="UTC"),
            # Sun 00:30 -04:00 comes after Sun 00:00 -04:00 but before the later Sun 00:00 -05:00
            pendulum.datetime(2025, 11, 2, 4, 30, tz="UTC"),
            id="havana-prev-half-hour-fold",
        ),
        pytest.param(
            "America/Havana",
            "0 * * * 0",
            pendulum.datetime(2025, 3, 9, 5, 0, tz="UTC"),
            # Sat 23:00 is one wall-clock hour back but not a tick; the previous Sun 00:00 is a week earlier
            pendulum.datetime(2025, 3, 3, 4, 0, tz="UTC"),
            id="havana-prev-gap-at-one-hour",
        ),
        pytest.param(
            "Australia/Lord_Howe",
            "0,10 * * * 0",
            pendulum.datetime(2026, 10, 3, 15, 30, tz="UTC"),
            # Sat 01:40 +10:30; Sun 02:10 is skipped by the 30 minute gap
            pendulum.datetime(2026, 10, 3, 14, 40, tz="UTC"),
            id="lord-howe-prev-target-in-half-hour-gap",
        ),
        pytest.param(
            "America/Havana",
            "0 * 2 3,11 *",
            pendulum.datetime(2025, 11, 2, 5, 0, tz="UTC"),
            # Sun 00:00 -04:00, the first pass of current's own wall time; both ends are -05:00
            pendulum.datetime(2025, 11, 2, 4, 0, tz="UTC"),
            id="havana-prev-sparse-cron-fold-twin",
        ),
        pytest.param(
            "America/Havana",
            "0,30 * * * 6",
            pendulum.datetime(2025, 11, 2, 5, 0, tz="UTC"),
            # current is Sun 00:00 -05:00, the second pass of the fall-back fold; only Saturday has ticks,
            # so the answer is Sat 23:30 -04:00
            pendulum.datetime(2025, 11, 2, 3, 30, tz="UTC"),
            id="havana-prev-day-boundary-fold",
        ),
    ],
)
def test_get_prev_across_dst(
    timezone: str, cron: str, current: pendulum.DateTime, expected: pendulum.DateTime
):
    assert CronMixin(cron, timezone)._get_prev(current) == expected


@pytest.mark.parametrize(
    ("timezone", "naive", "expected"),
    [
        pytest.param(
            pendulum.timezone("UTC"),
            datetime.datetime(2026, 6, 1, 12, 0),
            [pendulum.datetime(2026, 6, 1, 12, 0, tz="UTC")],
            id="ordinary-time",
        ),
        pytest.param(
            pendulum.timezone("America/Havana"),
            datetime.datetime(2025, 11, 2, 0, 0),
            [pendulum.datetime(2025, 11, 2, 4, 0, tz="UTC"), pendulum.datetime(2025, 11, 2, 5, 0, tz="UTC")],
            id="havana-fold",
        ),
        pytest.param(
            pendulum.timezone("Europe/London"),
            datetime.datetime(2026, 10, 25, 1, 30),
            [
                pendulum.datetime(2026, 10, 25, 0, 30, tz="UTC"),
                pendulum.datetime(2026, 10, 25, 1, 30, tz="UTC"),
            ],
            id="london-fold",
        ),
        pytest.param(
            pendulum.timezone("America/Godthab"),
            datetime.datetime(2026, 3, 28, 23, 0),
            [],
            id="godthab-spring-forward-gap",
        ),
        pytest.param(
            pendulum.fixed_timezone(7200),
            datetime.datetime(2026, 6, 1, 12, 0),
            [pendulum.datetime(2026, 6, 1, 10, 0, tz="UTC")],
            id="fixed-timezone",
        ),
    ],
)
def test_localize_all_folds(
    timezone: pendulum.Timezone | pendulum.FixedTimezone,
    naive: datetime.datetime,
    expected: list[pendulum.DateTime],
):
    assert _localize_all_folds(naive, timezone) == expected
