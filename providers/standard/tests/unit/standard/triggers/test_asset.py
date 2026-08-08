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
"""
Tests for :class:`AssetEventTrigger`.

The ``TestRun`` cases are database-backed (``db_test``): the trigger fetches from real
``AssetEvent`` rows via a DB-backed ``SUPERVISOR_COMMS`` (see the ``conftest.py`` one directory up),
imports the ``process_result`` callable by path, applies it, and fires once the count is satisfied.
Only the pure helpers and the serialization contract are tested without a DB.
"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.standard.sensors.asset import AssetEventSensor
from airflow.providers.standard.triggers.asset import (
    AssetEventTrigger,
    _count_satisfied,
    _serialize_events,
)
from airflow.triggers.base import TriggerEvent

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="AssetEventTrigger requires Airflow 3.4+")

ASSET_NAME = "my_asset"
ASSET_URI = "s3://bucket/key"
ALIAS_NAME = "my_alias"


# top-level (importable) process_result callables
def only_us_partitions(events: list[Any]) -> list[Any]:
    """Keep only events whose partition key targets the ``us`` region."""
    return [event for event in events if (event.partition_key or "").startswith("us|")]


def dedup_by_partition_key(events: list[Any]) -> list[Any]:
    """Keep the first event seen for each distinct partition key (order preserved)."""
    seen: set[str | None] = set()
    out: list[Any] = []
    for event in events:
        if event.partition_key in seen:
            continue
        seen.add(event.partition_key)
        out.append(event)
    return out


def drop_all(events: list[Any]) -> list[Any]:
    """A process_result that always yields nothing (keeps the trigger waiting)."""
    return []


def duplicate_all(events: list[Any]) -> list[Any]:
    """A process_result that returns more events than it received."""
    return events + events


def boom(events: list[Any]) -> list[Any]:
    raise RuntimeError("process_result exploded")


class _Stop(Exception):
    """Sentinel raised from a patched ``asyncio.sleep`` to break the polling loop."""


def _keys(event: TriggerEvent) -> list[str | None]:
    return [e["partition_key"] for e in event.payload["events"]]


@pytest.mark.parametrize(
    ("count", "expected_count", "result"),
    [(0, -1, False), (1, -1, True), (5, -1, True), (2, 2, True), (1, 2, False), (3, 2, False)],
)
def test_count_satisfied(count, expected_count, result):
    assert _count_satisfied(count, expected_count) is result


def test_serialize_events_uses_model_dump():
    class FakeEvent:
        def model_dump(self, mode=None):
            return {"dumped": mode}

    assert _serialize_events([FakeEvent(), {"plain": 1}]) == [{"dumped": "json"}, {"plain": 1}]


def test_serialize_round_trip():
    trigger = AssetEventTrigger(
        name="my_asset",
        uri="s3://bucket/key",
        partition_key_regexp_pattern="da.*",
        extra={"k": "v"},
        expected_count=2,
        process_result_path="my.module.func",
        poke_interval=30,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow.providers.standard.triggers.asset.AssetEventTrigger"
    assert kwargs == {
        "name": "my_asset",
        "uri": "s3://bucket/key",
        "alias_name": None,
        "after": None,
        "before": None,
        "ascending": True,
        "limit": None,
        "partition_key": None,
        "partition_key_regexp_pattern": "da.*",
        "extra": {"k": "v"},
        "expected_count": 2,
        "process_result_path": "my.module.func",
        "poke_interval": 30,
    }
    # Re-instantiation from the serialized kwargs must work.
    AssetEventTrigger(**kwargs)


def test_rejects_invalid_expected_count():
    with pytest.raises(ValueError, match="expected_count"):
        AssetEventTrigger(name="my_asset", expected_count=-2)


def test_serialize_round_trip_with_datetimes():
    from datetime import datetime, timezone

    after = datetime(2024, 1, 1, tzinfo=timezone.utc)
    before = datetime(2024, 1, 9, tzinfo=timezone.utc)
    trigger = AssetEventTrigger(name="my_asset", after=after, before=before)
    _, kwargs = trigger.serialize()
    assert kwargs["after"] == after
    assert kwargs["before"] == before
    reborn = AssetEventTrigger(**kwargs)
    assert reborn.after == after
    assert reborn.before == before


@pytest.mark.db_test
class TestRun:
    """run() against real ``AssetEvent`` rows (no patching of the fetch)."""

    @pytest.mark.asyncio
    async def test_fires_at_least_one(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(name=ASSET_NAME, poke_interval=0.01)  # expected_count=-1
        event = await trigger.run().__anext__()
        assert isinstance(event, TriggerEvent)
        assert event.payload["status"] == "success"
        assert len(event.payload["events"]) == 5

    @pytest.mark.asyncio
    async def test_fires_on_exact_count(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(name=ASSET_NAME, expected_count=5, poke_interval=0.01)
        event = await trigger.run().__anext__()
        assert len(event.payload["events"]) == 5

    @pytest.mark.asyncio
    async def test_exact_partition_key_filter(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(name=ASSET_NAME, partition_key="us|2024-01-01", poke_interval=0.01)
        event = await trigger.run().__anext__()
        assert _keys(event) == ["us|2024-01-01", "us|2024-01-01"]

    @pytest.mark.asyncio
    async def test_extra_filter(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(name=ASSET_NAME, extra={"region": "eu"}, poke_interval=0.01)
        event = await trigger.run().__anext__()
        assert _keys(event) == ["eu|2024-01-01"]

    @pytest.mark.asyncio
    async def test_by_alias(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(alias_name=ALIAS_NAME, poke_interval=0.01)
        event = await trigger.run().__anext__()
        assert len(event.payload["events"]) == 5

    @pytest.mark.asyncio
    async def test_applies_process_result_filter(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(
            name=ASSET_NAME,
            expected_count=3,
            process_result_path=f"{__name__}.only_us_partitions",
            poke_interval=0.01,
        )
        event = await trigger.run().__anext__()
        assert _keys(event) == ["us|2024-01-01", "us|2024-01-02", "us|2024-01-01"]

    @pytest.mark.asyncio
    async def test_applies_process_result_dedup(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(
            name=ASSET_NAME,
            expected_count=4,
            process_result_path=f"{__name__}.dedup_by_partition_key",
            poke_interval=0.01,
        )
        event = await trigger.run().__anext__()
        assert _keys(event) == ["us|2024-01-01", "us|2024-01-02", "eu|2024-01-01", None]

    @pytest.mark.asyncio
    async def test_sleeps_until_matching_event_appears(
        self, asset_event_rows, db_supervisor_comms, add_asset_event, mocker
    ):
        # No event matches the filter yet; a new one is inserted "during" the first sleep, after
        # which the next fetch finds it and the trigger fires. This exercises the real polling loop.
        async def _insert(_seconds):
            add_asset_event(id=100, day=9, partition_key="zz|only", extra={"region": "zz"})

        sleep_mock = mocker.patch("asyncio.sleep", new=mock.AsyncMock(side_effect=_insert))
        trigger = AssetEventTrigger(name=ASSET_NAME, partition_key="zz|only", poke_interval=5)
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "success"
        assert _keys(event) == ["zz|only"]
        sleep_mock.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_process_result_reduces_below_expected_keeps_waiting(
        self, asset_event_rows, db_supervisor_comms, add_asset_event, mocker
    ):
        # only_us -> 3 events initially (< expected 4), so the trigger must sleep; a 4th "us" event
        # is inserted during that sleep, after which the trigger fires.
        async def _insert(_seconds):
            add_asset_event(id=101, day=9, partition_key="us|2024-01-09", extra={"region": "us"})

        sleep_mock = mocker.patch("asyncio.sleep", new=mock.AsyncMock(side_effect=_insert))
        trigger = AssetEventTrigger(
            name=ASSET_NAME,
            expected_count=4,
            process_result_path=f"{__name__}.only_us_partitions",
            poke_interval=5,
        )
        event = await trigger.run().__anext__()
        assert len(event.payload["events"]) == 4
        assert all((k or "").startswith("us|") for k in _keys(event))
        sleep_mock.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_callable_serialized_by_sensor_runs_in_trigger(self, asset_event_rows, db_supervisor_comms):
        # End-to-end: pass a *real callable* to the sensor. The sensor's serialization util turns it
        # into a dotted path and hands it to the trigger; we then run the exact trigger the sensor
        # built and confirm the trigger imports and executes that callable against the DB.
        sensor = AssetEventSensor(
            task_id="s",
            name=ASSET_NAME,
            process_result=only_us_partitions,  # a real function object, not an import path
            expected_count=99,  # force the initial poke to fail so the sensor defers
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        trigger = exc.value.trigger
        assert isinstance(trigger, AssetEventTrigger)
        assert trigger.process_result_path.endswith("test_asset.only_us_partitions")

        # Relax the (deliberately unreachable) expectation so the same trigger can fire, then run it.
        trigger.expected_count = 3
        trigger.poke_interval = 0.01
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "success"
        assert _keys(event) == ["us|2024-01-01", "us|2024-01-02", "us|2024-01-01"]

    @pytest.mark.asyncio
    async def test_empty_extra_is_no_filter(self, asset_event_rows, db_supervisor_comms):
        # An empty ``extra`` dict must be treated as "no filter", not as an impossible match.
        trigger = AssetEventTrigger(name=ASSET_NAME, extra={}, poke_interval=0.01)
        event = await trigger.run().__anext__()
        assert len(event.payload["events"]) == 5

    @pytest.mark.asyncio
    async def test_process_result_returns_more_events(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(
            name=ASSET_NAME,
            expected_count=10,
            process_result_path=f"{__name__}.duplicate_all",
            poke_interval=0.01,
        )
        event = await trigger.run().__anext__()
        assert len(event.payload["events"]) == 10

    @pytest.mark.asyncio
    async def test_process_result_exception_propagates(self, asset_event_rows, db_supervisor_comms):
        trigger = AssetEventTrigger(
            name=ASSET_NAME, process_result_path=f"{__name__}.boom", poke_interval=0.01
        )
        with pytest.raises(RuntimeError, match="process_result exploded"):
            await trigger.run().__anext__()

    @pytest.mark.asyncio
    async def test_empty_process_result_keeps_waiting(self, asset_event_rows, db_supervisor_comms, mocker):
        # process_result drops everything -> count never satisfied -> the trigger must sleep.
        # A patched sleep raises to break the otherwise-infinite polling loop after one iteration.
        async def _stop(_seconds):
            raise _Stop()

        sleep_mock = mocker.patch("asyncio.sleep", new=mock.AsyncMock(side_effect=_stop))
        trigger = AssetEventTrigger(
            name=ASSET_NAME, process_result_path=f"{__name__}.drop_all", poke_interval=3
        )
        with pytest.raises(_Stop):
            await trigger.run().__anext__()
        sleep_mock.assert_awaited_once_with(3)
