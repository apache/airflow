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

from datetime import datetime, timezone
from typing import Any
from unittest.mock import call

import pytest

from airflow.providers.common.compat.sdk import Asset, AssetAlias, PokeReturnValue
from airflow.providers.standard.sensors.asset import AssetEventSensor

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

if AIRFLOW_V_3_4_PLUS:
    from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
    from airflow.sdk.execution_time.comms import (
        AssetEventsResult,
        AssetResult,
        GetAssetByName,
        GetAssetByUri,
        GetAssetEventByAsset,
        GetAssetEventByAssetAlias,
    )
    from airflow.sdk.execution_time.context import InletEventsAccessors

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="AssetEventSensor requires Airflow 3.4+")

ASSET_NAME = "my_asset"
ASSET_URI = "s3://bucket/key"
ALIAS_NAME = "my_alias"


def _get_timestamp(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=timezone.utc)


def _get_context(sensor: AssetEventSensor) -> dict[str, Any]:
    return {"inlet_events": InletEventsAccessors(sensor.inlets)}


def only_us_partitions(events: list[Any]) -> list[Any]:
    return [event for event in events if (event.partition_key or "").startswith("us|")]


def dedup_by_partition_key(events: list[Any]) -> list[Any]:
    seen: set[str | None] = set()
    result = []
    for event in events:
        if event.partition_key not in seen:
            seen.add(event.partition_key)
            result.append(event)
    return result


@pytest.fixture
def asset():
    return Asset(name=ASSET_NAME, uri=ASSET_URI)


@pytest.fixture
def events_response():
    partition_keys = ["us|2024-01-01", "us|2024-01-02", "eu|2024-01-01", "us|2024-01-01", None]
    return AssetEventsResult(
        asset_events=[
            AssetEventResponse(
                id=index,
                timestamp=_get_timestamp(index),
                partition_key=key,
                extra={"region": key.split("|")[0]} if key else {},
                asset=AssetResponse(name=ASSET_NAME, uri=ASSET_URI, group="asset", extra={}),
                created_dagruns=[],
                source_dag_id="producer",
                source_task_id="emit",
                source_run_id="run_1",
                source_map_index=-1,
            )
            for index, key in enumerate(partition_keys, start=1)
        ]
    )


class TestInit:
    def test_requires_a_target(self):
        with pytest.raises(ValueError, match="obj.*name.*uri.*alias_name"):
            AssetEventSensor(task_id="s")

    @pytest.mark.parametrize("use_alias", [False, True])
    def test_declares_target_as_inlet(self, asset, use_alias):
        obj = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        sensor = AssetEventSensor(task_id="s", obj=obj)
        assert sensor.obj == obj
        assert sensor.inlets == [obj]

    @pytest.mark.parametrize("include_target", [False, True])
    def test_preserves_existing_inlets_without_duplicates(self, asset, include_target):
        other_asset = Asset("s3://bucket/other")
        inlets = [other_asset, asset] if include_target else [other_asset]
        original_inlets = inlets.copy()
        sensor = AssetEventSensor(task_id="s", obj=asset, inlets=inlets)
        assert sensor.inlets == [other_asset, asset]
        assert inlets == original_inlets

    def test_target_rejects_bad_type(self):
        with pytest.raises(TypeError, match="Asset.*AssetAlias"):
            AssetEventSensor(task_id="s", obj=object())

    @pytest.mark.parametrize(
        "target",
        [
            {"name": ASSET_NAME},
            {"uri": ASSET_URI},
            {"name": ASSET_NAME, "uri": ASSET_URI},
            {"alias_name": ALIAS_NAME},
        ],
    )
    @pytest.mark.parametrize("use_alias", [False, True])
    def test_rejects_obj_with_raw_selector(self, asset, target, use_alias):
        obj = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        with pytest.raises(ValueError, match="obj"):
            AssetEventSensor(task_id="s", obj=obj, **target)

    @pytest.mark.parametrize(
        "target",
        [{"name": ASSET_NAME}, {"uri": ASSET_URI}, {"name": ASSET_NAME, "uri": ASSET_URI}],
    )
    def test_rejects_alias_with_asset_selector(self, target):
        with pytest.raises(ValueError, match="alias_name"):
            AssetEventSensor(task_id="s", alias_name=ALIAS_NAME, **target)

    @pytest.mark.parametrize("expected_count", [1.5, "1", True, None])
    def test_rejects_noninteger_expected_count(self, asset, expected_count):
        with pytest.raises(TypeError, match="expected_count"):
            AssetEventSensor(task_id="s", obj=asset, expected_count=expected_count)

    def test_rejects_negative_expected_count(self, asset):
        with pytest.raises(ValueError, match="expected_count"):
            AssetEventSensor(task_id="s", obj=asset, expected_count=-1)

    @pytest.mark.parametrize("limit", [0.5, "1", True])
    def test_rejects_noninteger_limit(self, asset, limit):
        with pytest.raises(TypeError, match="limit"):
            AssetEventSensor(task_id="s", obj=asset, limit=limit)

    @pytest.mark.parametrize("limit", [0, -1])
    def test_rejects_nonpositive_limit(self, asset, limit):
        with pytest.raises(ValueError, match="limit"):
            AssetEventSensor(task_id="s", obj=asset, limit=limit, expected_count=0)

    def test_rejects_invalid_count_policy(self, asset):
        with pytest.raises(ValueError, match="count_policy"):
            AssetEventSensor(task_id="s", obj=asset, count_policy="unknown")

    @pytest.mark.parametrize("count_policy", ["minimum", "exact"])
    def test_rejects_limit_below_expected_count_without_processing(self, asset, count_policy):
        with pytest.raises(ValueError, match="limit"):
            AssetEventSensor(task_id="s", obj=asset, limit=1, expected_count=3, count_policy=count_policy)

    def test_requires_airflow_3_4(self, mocker, asset):
        mocker.patch("airflow.providers.standard.sensors.asset.AIRFLOW_V_3_4_PLUS", False)
        with pytest.raises(RuntimeError, match="requires Apache Airflow 3.4"):
            AssetEventSensor(task_id="s", obj=asset)

    def test_template_fields(self):
        assert set(AssetEventSensor.template_fields) == {
            "partition_key",
            "partition_key_regexp_pattern",
            "extra",
            "after",
            "before",
        }

    def test_renders_filters_without_changing_lineage(self, asset):
        sensor = AssetEventSensor(
            task_id="s",
            obj=asset,
            partition_key="{{ ds }}",
            partition_key_regexp_pattern="{{ params.region }}.*",
            extra={"region": "{{ params.region }}"},
            after="{{ ds }}T00:00:00+00:00",
            before="{{ ds }}T23:59:59+00:00",
        )
        sensor.render_template_fields({"ds": "2024-01-01", "params": {"region": "us"}})
        assert sensor.partition_key == "2024-01-01"
        assert sensor.partition_key_regexp_pattern == "us.*"
        assert sensor.extra == {"region": "us"}
        assert sensor.after == "2024-01-01T00:00:00+00:00"
        assert sensor.before == "2024-01-01T23:59:59+00:00"
        assert sensor.obj == asset
        assert sensor.inlets == [asset]


class TestPoke:
    def test_returns_serialized_events(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(task_id="s", obj=asset)
        result = sensor.poke(_get_context(sensor))
        assert isinstance(result, PokeReturnValue)
        assert bool(result) is True
        assert result.xcom_value == [event.model_dump(mode="json") for event in events_response.asset_events]
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI)
        )

    @pytest.mark.parametrize(
        ("count_policy", "expected_count", "actual_count", "done"),
        [
            ("minimum", 1, 0, False),
            ("minimum", 1, 1, True),
            ("minimum", 3, 2, False),
            ("minimum", 3, 3, True),
            ("minimum", 3, 5, True),
            ("minimum", 0, 0, True),
            ("exact", 0, 0, True),
            ("exact", 0, 1, False),
            ("exact", 3, 2, False),
            ("exact", 3, 3, True),
            ("exact", 3, 5, False),
        ],
    )
    def test_count_policy(
        self, asset, events_response, mock_supervisor_comms, count_policy, expected_count, actual_count, done
    ):
        events = events_response.asset_events[:actual_count]
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=events)
        sensor = AssetEventSensor(
            task_id="s", obj=asset, count_policy=count_policy, expected_count=expected_count
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is done
        assert result.xcom_value == ([event.model_dump(mode="json") for event in events] if done else None)

    @pytest.mark.parametrize("use_alias", [False, True])
    @pytest.mark.parametrize("string_bounds", [False, True])
    def test_forwards_all_filters(self, asset, mock_supervisor_comms, use_alias, string_bounds):
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=[])
        obj = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        sensor = AssetEventSensor(
            task_id="s",
            obj=obj,
            after=_get_timestamp(1).isoformat() if string_bounds else _get_timestamp(1),
            before=_get_timestamp(9).isoformat() if string_bounds else _get_timestamp(9),
            ascending=False,
            limit=7,
            partition_key="us|2024-01-01",
            partition_key_regexp_pattern="us.*",
            extra={"region": "us", "status": "validated"},
        )
        sensor.poke(_get_context(sensor))
        filters = {
            "after": _get_timestamp(1),
            "before": _get_timestamp(9),
            "ascending": False,
            "limit": 7,
            "partition_key": "us|2024-01-01",
            "partition_key_regexp_pattern": "us.*",
            "extra": {"region": "us", "status": "validated"},
        }
        message = (
            GetAssetEventByAssetAlias(alias_name=ALIAS_NAME, **filters)
            if use_alias
            else GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, **filters)
        )
        mock_supervisor_comms.send.assert_called_once_with(message)

    @pytest.mark.parametrize("selector", ["name", "uri"])
    def test_resolves_asset_reference(self, selector, events_response, mock_supervisor_comms):
        target = {"name": ASSET_NAME} if selector == "name" else {"uri": ASSET_URI}
        sensor = AssetEventSensor(task_id="s", **target)
        assert sensor.obj == Asset.ref(**target)
        assert sensor.inlets == [sensor.obj]
        mock_supervisor_comms.send.side_effect = [
            AssetResult(name=ASSET_NAME, uri=ASSET_URI, group="asset", extra={}),
            events_response,
        ]
        assert bool(sensor.poke(_get_context(sensor))) is True
        resolution = GetAssetByName(name=ASSET_NAME) if selector == "name" else GetAssetByUri(uri=ASSET_URI)
        query = GetAssetEventByAsset(
            name=ASSET_NAME if selector == "name" else None,
            uri=ASSET_URI if selector == "uri" else None,
        )
        assert mock_supervisor_comms.send.call_args_list == [call(resolution), call(query)]

    @pytest.mark.parametrize("use_alias", [False, True])
    def test_raw_target(self, asset, events_response, mock_supervisor_comms, use_alias):
        target = {"alias_name": ALIAS_NAME} if use_alias else {"name": ASSET_NAME, "uri": ASSET_URI}
        sensor = AssetEventSensor(task_id="s", **target)
        expected_obj = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        assert sensor.obj == expected_obj
        assert sensor.inlets == [expected_obj]
        mock_supervisor_comms.send.return_value = events_response
        assert bool(sensor.poke(_get_context(sensor))) is True
        query = (
            GetAssetEventByAssetAlias(alias_name=ALIAS_NAME)
            if use_alias
            else GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI)
        )
        mock_supervisor_comms.send.assert_called_once_with(query)

    def test_fetches_fresh_events_on_every_poke(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.side_effect = [AssetEventsResult(asset_events=[]), events_response]
        sensor = AssetEventSensor(task_id="s", obj=asset)
        context = _get_context(sensor)
        assert bool(sensor.poke(context)) is False
        assert bool(sensor.poke(context)) is True
        query = GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI)
        assert mock_supervisor_comms.send.call_args_list == [call(query), call(query)]

    def test_execute_returns_xcom_payload(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(task_id="s", obj=asset)
        result = sensor.execute(_get_context(sensor))
        assert result == [event.model_dump(mode="json") for event in events_response.asset_events]


class TestProcessResult:
    @pytest.mark.parametrize(
        ("process_result", "expected_count", "expected_ids", "done"),
        [
            (only_us_partitions, 3, [1, 2, 4], True),
            (f"{__name__}.only_us_partitions", 3, [1, 2, 4], True),
            (dedup_by_partition_key, 4, [1, 2, 3, 5], True),
            (only_us_partitions, 5, None, False),
            (lambda events: [], 1, None, False),
            (lambda events: [], 0, [], True),
        ],
    )
    def test_processes_events_before_counting(
        self,
        asset,
        events_response,
        mock_supervisor_comms,
        process_result,
        expected_count,
        expected_ids,
        done,
    ):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(
            task_id="s",
            obj=asset,
            process_result=process_result,
            expected_count=expected_count,
            count_policy="exact",
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is done
        if done:
            assert [event["id"] for event in result.xcom_value] == expected_ids
        else:
            assert result.xcom_value is None

    def test_processing_can_expand_limited_results(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = AssetEventsResult(
            asset_events=events_response.asset_events[:1]
        )
        sensor = AssetEventSensor(
            task_id="s",
            obj=asset,
            limit=1,
            expected_count=2,
            count_policy="exact",
            process_result=lambda events: events + events,
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is True
        assert [event["id"] for event in result.xcom_value] == [1, 1]

    def test_processing_receives_only_limited_results(
        self, asset, events_response, mock_supervisor_comms, mocker
    ):
        mock_supervisor_comms.send.return_value = AssetEventsResult(
            asset_events=[events_response.asset_events[2]]
        )
        process_result = mocker.create_autospec(only_us_partitions, side_effect=only_us_partitions)
        sensor = AssetEventSensor(
            task_id="s",
            obj=asset,
            after=_get_timestamp(3),
            ascending=True,
            limit=1,
            process_result=process_result,
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is False
        assert result.xcom_value is None
        process_result.assert_called_once()
        assert [event.id for event in process_result.call_args.args[0]] == [3]
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetEventByAsset(
                name=ASSET_NAME, uri=ASSET_URI, after=_get_timestamp(3), ascending=True, limit=1
            )
        )

    def test_processing_can_return_json_values(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(
            task_id="s",
            obj=asset,
            process_result=lambda events: [{"partition": event.partition_key} for event in events],
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is True
        assert result.xcom_value == [
            {"partition": event.partition_key} for event in events_response.asset_events
        ]

    def test_exception_propagates(self, asset, events_response, mock_supervisor_comms):
        def boom(events):
            raise RuntimeError("process_result exploded")

        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(task_id="s", obj=asset, process_result=boom)
        with pytest.raises(RuntimeError, match="process_result exploded"):
            sensor.poke(_get_context(sensor))
