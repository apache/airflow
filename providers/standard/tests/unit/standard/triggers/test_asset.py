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

from typing import Any
from unittest import mock

import pytest

from airflow.providers.standard.triggers.asset import (
    AssetEventTrigger,
    _count_satisfied,
    _serialize_events,
)
from airflow.triggers.base import TriggerEvent

FETCH_PATH = "airflow.providers.standard.triggers.asset._fetch_asset_events"


def keep_filter(events: list[Any]) -> list[Any]:
    """A top-level (importable) process_result callable used in trigger tests."""
    return [event for event in events if event["partition_key"] == "keep"]


def _events(*partition_keys: str) -> list[dict[str, Any]]:
    return [{"id": i, "partition_key": pk} for i, pk in enumerate(partition_keys)]


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


class TestAssetEventTriggerRun:
    @pytest.mark.asyncio
    async def test_fires_when_count_satisfied(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("a", "b"))
        trigger = AssetEventTrigger(name="my_asset")  # expected_count=-1
        event = await trigger.run().__anext__()
        assert isinstance(event, TriggerEvent)
        assert event.payload == {"status": "success", "events": _events("a", "b")}

    @pytest.mark.asyncio
    async def test_sleeps_until_count_satisfied(self, mocker):
        mocker.patch(FETCH_PATH, side_effect=[[], _events("a")])
        sleep_mock = mocker.patch("asyncio.sleep", new=mock.AsyncMock())
        trigger = AssetEventTrigger(name="my_asset", poke_interval=5)
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "success"
        assert event.payload["events"] == _events("a")
        sleep_mock.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_applies_process_result_before_count(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("keep", "drop", "keep"))
        trigger = AssetEventTrigger(
            name="my_asset",
            expected_count=2,
            process_result_path=f"{__name__}.keep_filter",
        )
        event = await trigger.run().__anext__()
        assert event.payload["events"] == [
            {"id": 0, "partition_key": "keep"},
            {"id": 2, "partition_key": "keep"},
        ]

    @pytest.mark.asyncio
    async def test_process_result_reduces_below_expected_keeps_waiting(self, mocker):
        # After process_result there is only 1 "keep", below the expected 2, so it must sleep.
        mocker.patch(
            FETCH_PATH,
            side_effect=[_events("keep", "drop"), _events("keep", "keep", "drop")],
        )
        sleep_mock = mocker.patch("asyncio.sleep", new=mock.AsyncMock())
        trigger = AssetEventTrigger(
            name="my_asset",
            expected_count=2,
            process_result_path=f"{__name__}.keep_filter",
        )
        event = await trigger.run().__anext__()
        assert event.payload["events"] == _events("keep", "keep")
        sleep_mock.assert_awaited_once()
