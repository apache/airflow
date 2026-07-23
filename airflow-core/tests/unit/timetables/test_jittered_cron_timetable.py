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

from datetime import timedelta

import pendulum
import pytest
import time_machine

from airflow._shared.timezones.timezone import utc
from airflow.sdk import JitteredCronTimetable as SdkJitteredCronTimetable
from airflow.serialization.decoders import decode_timetable
from airflow.serialization.encoders import encode_timetable
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.trigger import CronTriggerTimetable, JitteredCronTimetable

CRON = "0 0 * * *"  # daily at midnight
START_DATE = pendulum.DateTime(2021, 9, 4, tzinfo=utc)
# seed/window known to produce a non-zero offset (used across the behavioural tests)
SEED = "my_dag"
MAX_JITTER = timedelta(hours=1)


def _catchup_run_afters(timetable, count, *, earliest):
    """Return the ``run_after`` of the first ``count`` catchup runs, feeding each back in."""
    run_afters = []
    last = None
    for _ in range(count):
        info = timetable.next_dagrun_info(
            last_automated_data_interval=last,
            restriction=TimeRestriction(earliest=earliest, latest=None, catchup=True),
        )
        assert info is not None
        run_afters.append(info.run_after)
        last = DataInterval.exact(info.run_after)
    return run_afters


@pytest.mark.parametrize(
    "timezone_name",
    [
        pytest.param("UTC", id="utc"),
        # Daily runs across the 2026-03-08 spring-forward in this zone: the offset must be
        # applied identically on both sides of the transition.
        pytest.param("America/New_York", id="dst-spring-forward"),
    ],
)
def test_jittered_runs_equal_base_runs_plus_offset(timezone_name):
    """Every jittered run is the plain-cron run shifted by the fixed per-DAG offset.

    Cron/DST correctness is delegated to ``CronTriggerTimetable``; this asserts the offset is
    applied consistently across a catchup sequence (including a DST transition), i.e. the
    strip -> cron -> apply overrides never drift.
    """
    earliest = pendulum.datetime(2026, 3, 6, tz=timezone_name)
    base = CronTriggerTimetable(CRON, timezone=timezone_name)
    jittered = JitteredCronTimetable(CRON, timezone=timezone_name, seed=SEED, max_jitter=MAX_JITTER)
    offset = jittered._offset
    assert offset > timedelta(0), "seed/window must produce a real shift, else the test is vacuous"

    base_runs = _catchup_run_afters(base, 5, earliest=earliest)
    jittered_runs = _catchup_run_afters(jittered, 5, earliest=earliest)

    assert jittered_runs == [run + offset for run in base_runs]


@pytest.mark.parametrize("catchup", [True, False])
def test_zero_max_jitter_matches_cron_trigger(catchup):
    """A zero window yields a zero offset, so the timetable behaves exactly like its parent."""
    base = CronTriggerTimetable(CRON, timezone=utc)
    jittered = JitteredCronTimetable(CRON, timezone=utc, seed=SEED, max_jitter=timedelta(0))
    assert jittered._offset == timedelta(0)

    last = DataInterval.exact(pendulum.DateTime(2022, 7, 26, tzinfo=utc))
    restriction = TimeRestriction(earliest=START_DATE, latest=None, catchup=catchup)
    # travel so the no-catchup branch (which reads utcnow()) is deterministic
    with time_machine.travel(pendulum.DateTime(2022, 7, 27, 5, 30, tzinfo=utc)):
        assert jittered.next_dagrun_info(
            last_automated_data_interval=last, restriction=restriction
        ) == base.next_dagrun_info(last_automated_data_interval=last, restriction=restriction)


def test_offsets_are_deterministic_bounded_and_spread():
    """Same seed -> same offset (stable hash, not process-salted); offsets stay in [0, max_jitter) and spread."""
    assert (
        JitteredCronTimetable(CRON, timezone=utc, seed="dag_a", max_jitter=MAX_JITTER)._offset
        == JitteredCronTimetable(CRON, timezone=utc, seed="dag_a", max_jitter=MAX_JITTER)._offset
    )

    offsets = [
        JitteredCronTimetable(CRON, timezone=utc, seed=f"dag_{i}", max_jitter=MAX_JITTER)._offset
        for i in range(25)
    ]
    assert all(timedelta(0) <= offset < MAX_JITTER for offset in offsets)
    assert len(set(offsets)) > 1, "distinct seeds should not all collide on one slot"


def test_serialize_round_trip_preserves_offset():
    """The core ``serialize``/``deserialize`` round-trips seed + window (as seconds) and the derived offset."""
    tt = JitteredCronTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    data = tt.serialize()
    assert data["seed"] == SEED
    assert data["max_jitter"] == 3600.0  # serialized as plain seconds

    restored = JitteredCronTimetable.deserialize(data)
    assert isinstance(restored, JitteredCronTimetable)
    assert restored._seed == tt._seed
    assert restored._max_jitter == tt._max_jitter
    assert restored._offset == tt._offset


def test_encode_decode_round_trip_across_layers():
    """SDK class -> encode_timetable (dispatch keyed on the SDK class) -> decode_timetable -> core class."""
    sdk_tt = SdkJitteredCronTimetable(CRON, timezone="UTC", seed=SEED, max_jitter=MAX_JITTER)

    restored = decode_timetable(encode_timetable(sdk_tt))

    assert isinstance(restored, JitteredCronTimetable)  # rebuilt as the core scheduler-side class
    assert restored._max_jitter == MAX_JITTER
    expected_offset = JitteredCronTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)._offset
    assert restored._offset == expected_offset
