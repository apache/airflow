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
from airflow.sdk import (
    CronDataIntervalTimetable as SdkCronDataIntervalTimetable,
    CronPartitionTimetable as SdkCronPartitionTimetable,
    CronTriggerTimetable as SdkCronTriggerTimetable,
    MultipleCronTriggerTimetable as SdkMultipleCronTriggerTimetable,
)
from airflow.serialization.decoders import decode_timetable
from airflow.serialization.encoders import encode_timetable
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.trigger import (
    CronPartitionTimetable,
    CronTriggerTimetable,
    MultipleCronTriggerTimetable,
)

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

    Cron/DST correctness is delegated to the un-jittered timetable; this asserts the offset is
    applied consistently across a catchup sequence (including a DST transition), i.e. the
    strip -> cron -> apply wrappers never drift.
    """
    earliest = pendulum.datetime(2026, 3, 6, tz=timezone_name)
    base = CronTriggerTimetable(CRON, timezone=timezone_name)
    jittered = CronTriggerTimetable(CRON, timezone=timezone_name, seed=SEED, max_jitter=MAX_JITTER)
    offset = jittered._offset
    assert offset > timedelta(0), "seed/window must produce a real shift, else the test is vacuous"

    base_runs = _catchup_run_afters(base, 5, earliest=earliest)
    jittered_runs = _catchup_run_afters(jittered, 5, earliest=earliest)

    assert jittered_runs == [run + offset for run in base_runs]


@pytest.mark.parametrize("catchup", [True, False])
def test_zero_max_jitter_matches_plain_cron(catchup):
    """A zero window yields a zero offset, so the timetable behaves exactly like a plain cron one."""
    base = CronTriggerTimetable(CRON, timezone=utc)
    jittered = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=timedelta(0))
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
        CronTriggerTimetable(CRON, timezone=utc, seed="dag_a", max_jitter=MAX_JITTER)._offset
        == CronTriggerTimetable(CRON, timezone=utc, seed="dag_a", max_jitter=MAX_JITTER)._offset
    )

    offsets = [
        CronTriggerTimetable(CRON, timezone=utc, seed=f"dag_{i}", max_jitter=MAX_JITTER)._offset
        for i in range(25)
    ]
    assert all(timedelta(0) <= offset < MAX_JITTER for offset in offsets)
    assert len(set(offsets)) > 1, "distinct seeds should not all collide on one slot"


def test_serialize_round_trip_preserves_offset():
    """The core ``serialize``/``deserialize`` round-trips seed + window (as seconds) and the derived offset."""
    tt = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    data = tt.serialize()
    assert data["seed"] == SEED
    assert data["max_jitter"] == 3600.0  # serialized as plain seconds

    restored = CronTriggerTimetable.deserialize(data)
    assert isinstance(restored, CronTriggerTimetable)
    assert restored._seed == tt._seed
    assert restored._max_jitter == tt._max_jitter
    assert restored._offset == tt._offset


def test_encode_decode_round_trip_across_layers():
    """SDK class -> encode_timetable (dispatch keyed on the SDK class) -> decode_timetable -> core class."""
    sdk_tt = SdkCronTriggerTimetable(CRON, timezone="UTC", seed=SEED, max_jitter=MAX_JITTER)

    restored = decode_timetable(encode_timetable(sdk_tt))

    assert isinstance(restored, CronTriggerTimetable)  # rebuilt as the core scheduler-side class
    assert restored._max_jitter == MAX_JITTER
    expected_offset = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)._offset
    assert restored._offset == expected_offset


def test_sub_second_max_jitter_is_bounded_and_does_not_divide_by_zero():
    """A sub-second window keeps full precision (microsecond math) instead of truncating to a 0s divisor."""
    window = timedelta(milliseconds=500)
    offset = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=window)._offset
    assert timedelta(0) <= offset < window


@pytest.mark.parametrize(
    "timetable_cls",
    [
        pytest.param(CronTriggerTimetable, id="trigger-core"),
        pytest.param(SdkCronTriggerTimetable, id="trigger-sdk"),
        pytest.param(CronDataIntervalTimetable, id="interval-core"),
        pytest.param(SdkCronDataIntervalTimetable, id="interval-sdk"),
        pytest.param(CronPartitionTimetable, id="partition-core"),
        pytest.param(SdkCronPartitionTimetable, id="partition-sdk"),
        pytest.param(MultipleCronTriggerTimetable, id="multiple-core"),
        pytest.param(SdkMultipleCronTriggerTimetable, id="multiple-sdk"),
    ],
)
def test_empty_seed_requires_zero_jitter(timetable_cls):
    """An empty seed with a real window would give every DAG the same offset -> reject in both layers."""
    with pytest.raises(ValueError, match="seed"):
        timetable_cls(CRON, timezone="UTC", seed="", max_jitter=MAX_JITTER)

    # ...but an empty seed is fine when jitter is off (degrades to a plain cron timetable).
    timetable_cls(CRON, timezone="UTC", seed="", max_jitter=timedelta(0))


def test_jitter_is_part_of_equality():
    """Timetables differing only in jitter produce different schedules, so they must not compare equal."""
    plain = CronTriggerTimetable(CRON, timezone=utc)
    jittered = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    assert plain != jittered

    # Identical jitter settings are equal and hash-consistent (the eq/hash invariant).
    same = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    assert jittered == same
    assert hash(jittered) == hash(same)


def test_data_interval_jitter_shifts_whole_window_uniformly():
    """For a data-interval timetable the offset moves both bounds and the fire time equally.

    The window keeps its length and consecutive runs stay contiguous; only the calendar
    alignment of the boundaries shifts (00:35 -> 00:35 instead of 00:00 -> 00:00).
    """
    restriction = TimeRestriction(
        earliest=pendulum.DateTime(2026, 3, 6, tzinfo=utc), latest=None, catchup=True
    )
    plain = CronDataIntervalTimetable(CRON, timezone=utc)
    jittered = CronDataIntervalTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    offset = jittered._offset
    assert offset > timedelta(0)

    p = plain.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    j = jittered.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    assert j.data_interval.start == p.data_interval.start + offset
    assert j.data_interval.end == p.data_interval.end + offset
    assert j.run_after == j.data_interval.end
    # pendulum ``DateTime - DateTime`` is an Interval compared by endpoints, so compare durations
    assert (j.data_interval.end - j.data_interval.start).total_seconds() == (
        p.data_interval.end - p.data_interval.start
    ).total_seconds()

    following = jittered.next_dagrun_info(
        last_automated_data_interval=j.data_interval, restriction=restriction
    )
    assert following.data_interval.start == j.data_interval.end


def test_data_interval_serialize_round_trip_preserves_offset():
    """``CronDataIntervalTimetable`` persists seed + window and rebuilds the same offset."""
    tt = CronDataIntervalTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    data = tt.serialize()
    assert data["seed"] == SEED
    assert data["max_jitter"] == 3600.0

    restored = CronDataIntervalTimetable.deserialize(data)
    assert isinstance(restored, CronDataIntervalTimetable)
    assert restored._offset == tt._offset


@pytest.mark.parametrize(
    "timetable_cls", [CronTriggerTimetable, CronDataIntervalTimetable, CronPartitionTimetable]
)
def test_unjittered_timetable_serializes_without_jitter_keys(timetable_cls):
    """Without jitter the serialized form is unchanged, so existing DAGs are not re-serialized on upgrade."""
    data = timetable_cls(CRON, timezone=utc).serialize()
    assert "seed" not in data
    assert "max_jitter" not in data
    assert timetable_cls.deserialize(data)._offset == timedelta(0)


def test_data_interval_encode_decode_round_trip_across_layers():
    """SDK ``CronDataIntervalTimetable`` -> encode -> decode -> core class with the same offset."""
    sdk_tt = SdkCronDataIntervalTimetable(CRON, timezone="UTC", seed=SEED, max_jitter=MAX_JITTER)
    restored = decode_timetable(encode_timetable(sdk_tt))
    assert isinstance(restored, CronDataIntervalTimetable)
    expected = CronDataIntervalTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)._offset
    assert restored._offset == expected


def test_cron_partition_serialize_round_trip_preserves_offset():
    """``CronPartitionTimetable`` persists seed + window and rebuilds the same offset."""
    tt = CronPartitionTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)
    data = tt.serialize()
    assert data["seed"] == SEED
    assert data["max_jitter"] == 3600.0
    assert CronPartitionTimetable.deserialize(data)._offset == tt._offset


def test_multiple_cron_children_share_one_offset():
    """Every cron in a ``MultipleCronTriggerTimetable`` gets the same seed, so the whole DAG shifts in lockstep.

    The offset depends only on the seed, not the cron expression, so it also equals the offset a
    single ``CronTriggerTimetable`` with the same seed would get.
    """
    tt = MultipleCronTriggerTimetable(
        "10 1 * * *", "40 2 * * *", timezone=utc, seed=SEED, max_jitter=MAX_JITTER
    )
    offsets = {t._offset for t in tt._timetables}
    assert len(offsets) == 1
    assert offsets.pop() == CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)._offset


def test_multiple_cron_serialize_round_trip_preserves_offset():
    """``MultipleCronTriggerTimetable`` persists the shared seed + window and rebuilds the same child offsets."""
    tt = MultipleCronTriggerTimetable(
        "10 1 * * *", "40 2 * * *", timezone=utc, seed=SEED, max_jitter=MAX_JITTER
    )
    data = tt.serialize()
    assert data["seed"] == SEED
    assert data["max_jitter"] == 3600.0
    restored = MultipleCronTriggerTimetable.deserialize(data)
    assert [t._offset for t in restored._timetables] == [t._offset for t in tt._timetables]

    unjittered = MultipleCronTriggerTimetable("10 1 * * *", "40 2 * * *", timezone=utc).serialize()
    assert "seed" not in unjittered
    assert "max_jitter" not in unjittered


@pytest.mark.parametrize(
    ("sdk_timetable", "core_cls"),
    [
        pytest.param(
            SdkCronPartitionTimetable(CRON, timezone="UTC", seed=SEED, max_jitter=MAX_JITTER),
            CronPartitionTimetable,
            id="partition",
        ),
        pytest.param(
            SdkMultipleCronTriggerTimetable(
                "10 1 * * *", "40 2 * * *", timezone="UTC", seed=SEED, max_jitter=MAX_JITTER
            ),
            MultipleCronTriggerTimetable,
            id="multiple",
        ),
    ],
)
def test_partition_and_multiple_encode_decode_round_trip_across_layers(sdk_timetable, core_cls):
    """SDK class -> encode -> decode -> core class, with the jitter offset intact."""
    restored = decode_timetable(encode_timetable(sdk_timetable))
    assert isinstance(restored, core_cls)
    expected = CronTriggerTimetable(CRON, timezone=utc, seed=SEED, max_jitter=MAX_JITTER)._offset
    if core_cls is MultipleCronTriggerTimetable:
        offsets = [t._offset for t in restored._timetables]
    else:
        offsets = [restored._offset]
    assert offsets == [expected] * len(offsets)
