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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import Asset, AssetAlias, PokeReturnValue, TaskDeferred
from airflow.providers.standard.sensors.asset import AssetEventSensor
from airflow.providers.standard.triggers.asset import AssetEventTrigger

FETCH_PATH = "airflow.providers.standard.sensors.asset._fetch_asset_events"


def sample_process_result(events: list[Any]) -> list[Any]:
    """A top-level (importable) process_result callable used in deferrable tests."""
    return events


def _events(*partition_keys: str) -> list[dict[str, Any]]:
    return [{"id": i, "partition_key": pk} for i, pk in enumerate(partition_keys)]


class TestAssetEventSensor:
    def test_requires_a_target(self):
        with pytest.raises(ValueError, match="One of `asset`, `name`, `uri`, or `alias_name`"):
            AssetEventSensor(task_id="s")

    def test_target_from_asset(self):
        sensor = AssetEventSensor(task_id="s", asset=Asset(name="my_asset", uri="s3://bucket/key"))
        assert sensor.name == "my_asset"
        assert sensor.uri == "s3://bucket/key"
        assert sensor.alias_name is None

    def test_target_from_asset_alias(self):
        sensor = AssetEventSensor(task_id="s", asset=AssetAlias(name="my_alias"))
        assert sensor.alias_name == "my_alias"
        assert sensor.name is None
        assert sensor.uri is None

    def test_target_rejects_bad_type(self):
        with pytest.raises(TypeError, match="must be an Asset or AssetAlias"):
            AssetEventSensor(task_id="s", asset=object())  # type: ignore[arg-type]

    def test_poke_at_least_one(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("a", "b"))
        sensor = AssetEventSensor(task_id="s", name="my_asset")  # expected_count=-1 by default
        result = sensor.poke({})
        assert isinstance(result, PokeReturnValue)
        assert bool(result) is True
        assert result.xcom_value == _events("a", "b")

    def test_poke_at_least_one_no_events(self, mocker):
        mocker.patch(FETCH_PATH, return_value=[])
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        result = sensor.poke({})
        assert bool(result) is False
        assert result.xcom_value is None

    @pytest.mark.parametrize(
        ("expected_count", "num_events", "done"),
        [(2, 2, True), (3, 2, False), (1, 2, False)],
    )
    def test_poke_exact_count(self, mocker, expected_count, num_events, done):
        mocker.patch(FETCH_PATH, return_value=_events(*[str(i) for i in range(num_events)]))
        sensor = AssetEventSensor(task_id="s", name="my_asset", expected_count=expected_count)
        assert bool(sensor.poke({})) is done

    def test_poke_applies_process_result_before_count(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("a", "a", "b"))

        def dedup(events):
            seen, out = set(), []
            for event in events:
                if event["partition_key"] not in seen:
                    seen.add(event["partition_key"])
                    out.append(event)
            return out

        sensor = AssetEventSensor(task_id="s", name="my_asset", expected_count=2, process_result=dedup)
        result = sensor.poke({})
        assert bool(result) is True
        assert result.xcom_value == [
            {"id": 0, "partition_key": "a"},
            {"id": 2, "partition_key": "b"},
        ]

    def test_poke_process_result_by_import_path(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("a"))
        sensor = AssetEventSensor(
            task_id="s",
            name="my_asset",
            process_result=f"{__name__}.sample_process_result",
        )
        assert bool(sensor.poke({})) is True

    def test_execute_deferrable_defers(self, mocker):
        mocker.patch(FETCH_PATH, return_value=[])
        sensor = AssetEventSensor(
            task_id="s",
            name="my_asset",
            partition_key_regexp_pattern="da.*",
            extra={"k": "v"},
            expected_count=3,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        trigger = exc.value.trigger
        assert isinstance(trigger, AssetEventTrigger)
        assert trigger.name == "my_asset"
        assert trigger.expected_count == 3
        assert trigger.partition_key_regexp_pattern == "da.*"
        assert trigger.extra == {"k": "v"}
        assert trigger.process_result_path is None

    def test_execute_deferrable_short_circuits(self, mocker):
        mocker.patch(FETCH_PATH, return_value=_events("a", "b"))
        sensor = AssetEventSensor(task_id="s", name="my_asset", deferrable=True)
        # Already satisfied on the initial poke, so it should not defer.
        assert sensor.execute({}) == _events("a", "b")

    def test_execute_deferrable_passes_process_result_path(self, mocker):
        mocker.patch(FETCH_PATH, return_value=[])
        sensor = AssetEventSensor(
            task_id="s",
            name="my_asset",
            process_result=sample_process_result,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})
        assert exc.value.trigger.process_result_path.endswith("test_asset.sample_process_result")

    def test_execute_deferrable_rejects_non_importable_callable(self, mocker):
        mocker.patch(FETCH_PATH, return_value=[])
        sensor = AssetEventSensor(
            task_id="s",
            name="my_asset",
            process_result=lambda events: events,
            deferrable=True,
        )
        with pytest.raises(ValueError, match="top-level importable function"):
            sensor.execute({})

    def test_execute_complete_success(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        events = _events("a")
        assert sensor.execute_complete({}, {"status": "success", "events": events}) == events

    def test_execute_complete_failure(self):
        sensor = AssetEventSensor(task_id="s", name="my_asset")
        with pytest.raises(AirflowException, match="unexpected trigger event"):
            sensor.execute_complete({}, {"status": "boom"})
