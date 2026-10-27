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

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import call

import pytest

from airflow.providers.common.compat.sdk import (
    DAG,
    AirflowOptionalProviderFeatureException,
    Asset,
    AssetAlias,
    PokeReturnValue,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS, AIRFLOW_V_3_4_PLUS

if AIRFLOW_V_3_2_PLUS:
    from airflow.exceptions import AirflowRescheduleException
    from airflow.providers.standard.sensors import asset as asset_sensor
    from airflow.providers.standard.sensors.asset import AssetEventSensor, AssetEventTimeBound, RunStart
    from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
    from airflow.sdk.execution_time.comms import (
        AssetEventsResult,
        AssetResult,
        GetAssetByName,
        GetAssetByUri,
        GetAssetEventByAsset,
        GetAssetEventByAssetAlias,
        TaskRescheduleStartDate,
    )
    from airflow.sdk.execution_time.context import InletEventsAccessors

    from tests_common.test_utils.compat import OperatorSerialization

    class _ParameterBound:
        def resolve(self, context):
            return context["params"]["events_after"]

    class _InheritedParameterBound(AssetEventTimeBound):
        def resolve(self, context):
            return context["params"]["events_after"]


pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="AssetEventSensor requires Airflow 3.2+")
requires_3_4 = pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Filter requires Airflow 3.4+")

ASSET_NAME = "my_asset"
ASSET_URI = "s3://bucket/key"
ALIAS_NAME = "my_alias"


def _get_timestamp(day: int) -> datetime:
    return datetime(2024, 1, day, tzinfo=UTC)


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


@pytest.fixture(
    params=[
        ("RunStart", "dag_run.start_date", 8),
        ("LogicalDate", "logical_date", 1),
        ("DataIntervalStart", "data_interval_start", 2),
        ("DataIntervalEnd", "data_interval_end", 3),
    ],
    ids=lambda case: case[0],
)
def time_bound_case(request):
    name, anchor, day = request.param
    return getattr(asset_sensor, name), anchor, _get_timestamp(day)


@pytest.fixture
def time_bound_context():
    return {
        "dag_run": SimpleNamespace(start_date=_get_timestamp(8)),
        "logical_date": _get_timestamp(1),
        "data_interval_start": _get_timestamp(2),
        "data_interval_end": _get_timestamp(3),
    }


class TestInit:
    @pytest.mark.parametrize("use_alias", [False, True])
    def test_declares_target_as_inlet(self, asset, use_alias):
        target = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        sensor = AssetEventSensor(task_id="s", asset=target)
        assert sensor.asset == target
        assert sensor.inlets == [target]

    @pytest.mark.parametrize("include_target", [False, True])
    def test_preserves_existing_inlets_without_duplicates(self, asset, include_target):
        other_asset = Asset("s3://bucket/other")
        inlets = [other_asset, asset] if include_target else [other_asset]
        original_inlets = inlets.copy()
        sensor = AssetEventSensor(task_id="s", asset=asset, inlets=inlets)
        assert sensor.inlets == [other_asset, asset]
        assert inlets == original_inlets

    def test_rejects_negative_expected_count(self, asset):
        with pytest.raises(ValueError, match="expected_count"):
            AssetEventSensor(task_id="s", asset=asset, expected_count=-1)

    @pytest.mark.parametrize("limit", [0, -1])
    def test_rejects_nonpositive_limit(self, asset, limit):
        with pytest.raises(ValueError, match="limit"):
            AssetEventSensor(task_id="s", asset=asset, limit=limit, expected_count=0)

    def test_rejects_invalid_count_policy(self, asset):
        with pytest.raises(ValueError, match="count_policy"):
            AssetEventSensor(task_id="s", asset=asset, count_policy="unknown")

    @pytest.mark.parametrize(
        ("count_policy", "limit", "expected_count"),
        [("minimum", 1, 3), ("exact", 1, 3), ("exact", 3, 3)],
    )
    def test_rejects_insufficient_limit_without_processing(self, asset, count_policy, limit, expected_count):
        with pytest.raises(ValueError, match="limit"):
            AssetEventSensor(
                task_id="s",
                asset=asset,
                limit=limit,
                expected_count=expected_count,
                count_policy=count_policy,
            )

    @pytest.mark.parametrize(("count_policy", "limit"), [("minimum", 3), ("exact", 4)])
    def test_accepts_sufficient_limit_without_processing(self, asset, count_policy, limit):
        sensor = AssetEventSensor(
            task_id="s", asset=asset, limit=limit, expected_count=3, count_policy=count_policy
        )
        assert sensor.limit == limit

    @pytest.mark.parametrize(
        "filters",
        [{"partition_key": "a"}, {"partition_key_regexp_pattern": "a.*"}, {"extra": {"k": "v"}}],
    )
    def test_partition_and_extra_filters_require_airflow_3_4(self, mocker, asset, filters):
        mocker.patch("airflow.providers.standard.sensors.asset.AIRFLOW_V_3_4_PLUS", False)
        with pytest.raises(AirflowOptionalProviderFeatureException, match="need Airflow 3.4"):
            AssetEventSensor(task_id="s", asset=asset, **filters)

    def test_other_filters_do_not_require_airflow_3_4(self, mocker, asset):
        mocker.patch("airflow.providers.standard.sensors.asset.AIRFLOW_V_3_4_PLUS", False)
        sensor = AssetEventSensor(task_id="s", asset=asset, after=_get_timestamp(1), limit=1)
        assert sensor.limit == 1

    def test_template_fields(self):
        assert set(AssetEventSensor.template_fields) == {
            "partition_key",
            "partition_key_regexp_pattern",
            "extra",
            "after",
            "before",
        }

    @requires_3_4
    def test_renders_filters_without_changing_lineage(self, asset):
        sensor = AssetEventSensor(
            task_id="s",
            asset=asset,
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
        assert sensor.asset == asset
        assert sensor.inlets == [asset]


class TestTimeBounds:
    @pytest.mark.parametrize("inherit_protocol", [False, True], ids=["plain", "subclass"])
    @pytest.mark.parametrize("as_string", [False, True], ids=["datetime", "iso"])
    def test_custom_resolver_needs_only_resolve(
        self, asset, events_response, mock_supervisor_comms, inherit_protocol, as_string
    ):
        bound = _InheritedParameterBound() if inherit_protocol else _ParameterBound()
        timestamp = _get_timestamp(8)
        sensor = AssetEventSensor(task_id="s", asset=asset, after=bound)
        context = {
            **_get_context(sensor),
            "params": {"events_after": timestamp.isoformat() if as_string else timestamp},
        }
        sensor.render_template_fields(context)
        mock_supervisor_comms.send.side_effect = [AssetEventsResult(asset_events=[]), events_response]
        assert not sensor.poke(context)
        assert sensor.poke(context)
        query = GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, after=timestamp)
        assert mock_supervisor_comms.send.call_args_list == [call(query), call(query)]

    def test_custom_resolver_serialization_is_optional(self, asset):
        with DAG("test_custom_bound_serialization"):
            sensor = AssetEventSensor(task_id="s", asset=asset, after=_ParameterBound())
        encoded = json.loads(json.dumps(OperatorSerialization.serialize_operator(sensor)))
        assert isinstance(encoded["after"], str)
        assert "_ParameterBound" in encoded["after"]

    @pytest.mark.parametrize("lookback", [timedelta(0), timedelta(hours=1, microseconds=7)])
    def test_resolves_selected_timestamp(self, time_bound_case, time_bound_context, lookback):
        bound_type, _, timestamp = time_bound_case
        assert bound_type(lookback=lookback).resolve(time_bound_context) == timestamp - lookback

    def test_rejects_negative_lookback(self):
        with pytest.raises(ValueError, match="lookback.*non-negative"):
            RunStart(lookback=timedelta(microseconds=-1))

    @pytest.mark.parametrize("missing", [False, True], ids=["none", "absent"])
    def test_rejects_missing_timestamp(self, time_bound_case, time_bound_context, missing):
        bound_type, anchor, _ = time_bound_case
        if anchor == "dag_run.start_date":
            time_bound_context["dag_run"].start_date = None
            if missing:
                del time_bound_context["dag_run"]
        elif missing:
            del time_bound_context[anchor]
        else:
            time_bound_context[anchor] = None
        with pytest.raises(ValueError, match=rf"{bound_type.__name__} requires"):
            bound_type().resolve(time_bound_context)

    def test_displays_bound_configuration_in_serialized_task(self, asset, time_bound_case):
        bound_type, _, _ = time_bound_case
        with DAG("test_bounds_serialization"):
            first = AssetEventSensor(
                task_id="first", asset=asset, after=bound_type(lookback=timedelta(hours=1))
            )
            second = AssetEventSensor(
                task_id="second", asset=asset, after=bound_type(lookback=timedelta(hours=2))
            )
        first_encoded = json.loads(json.dumps(OperatorSerialization.serialize_operator(first)))
        second_encoded = json.loads(json.dumps(OperatorSerialization.serialize_operator(second)))
        assert first_encoded["after"] == f"{bound_type.__name__}(lookback=datetime.timedelta(seconds=3600))"
        assert second_encoded["after"] == f"{bound_type.__name__}(lookback=datetime.timedelta(seconds=7200))"

    @pytest.mark.parametrize("field", ["after", "before"])
    def test_renders_and_queries_bound(
        self, asset, time_bound_case, time_bound_context, field, mock_supervisor_comms
    ):
        bound_type, _, timestamp = time_bound_case
        lookback = timedelta(hours=1)
        bound = bound_type(lookback=lookback)
        sensor = AssetEventSensor(task_id="s", asset=asset, **{field: bound})
        context = {**time_bound_context, **_get_context(sensor)}
        sensor.render_template_fields(context)
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=[])
        sensor.poke(context)
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, **{field: timestamp - lookback})
        )
        assert bound.lookback == lookback

    @pytest.mark.parametrize("as_string", [False, True])
    def test_custom_callable_resolves_before_polling(
        self, asset, events_response, mock_supervisor_comms, mocker, as_string
    ):
        timestamp = _get_timestamp(8)

        def get_bound(*, context, jinja_env):
            return timestamp.isoformat() if as_string else timestamp

        get_bound_spy = mocker.create_autospec(get_bound, side_effect=get_bound)
        sensor = AssetEventSensor(task_id="s", asset=asset, after=get_bound_spy)
        context = _get_context(sensor)
        sensor.render_template_fields(context)
        mock_supervisor_comms.send.side_effect = [AssetEventsResult(asset_events=[]), events_response]
        assert not sensor.poke(context)
        assert sensor.poke(context)
        get_bound_spy.assert_called_once()
        query = GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, after=timestamp)
        assert mock_supervisor_comms.send.call_args_list == [call(query), call(query)]

    @pytest.mark.parametrize("mapped_bound", [False, True], ids=["partial", "expand"])
    def test_mapped_sensor(
        self, asset, events_response, create_runtime_ti, mock_supervisor_comms, mapped_bound
    ):
        bound = RunStart(lookback=timedelta(hours=1))
        with DAG("test_mapped_bounds"):
            if mapped_bound:
                mapped = AssetEventSensor.partial(task_id="s", asset=asset).expand(after=[bound, bound])
            else:
                mapped = AssetEventSensor.partial(task_id="s", asset=asset, after=bound).expand(limit=[1, 2])
        ti = create_runtime_ti(task=mapped, map_index=1, start_date=_get_timestamp(8), logical_date=None)
        context = ti.get_template_context()
        ti.render_templates(context=context)
        mock_supervisor_comms.send.return_value = events_response
        assert ti.task.poke(context)
        mock_supervisor_comms.send.assert_called_with(
            GetAssetEventByAsset(
                name=ASSET_NAME,
                uri=ASSET_URI,
                after=_get_timestamp(8) - timedelta(hours=1),
                limit=None if mapped_bound else 2,
            )
        )

    @pytest.mark.parametrize("mapped_bound", [False, True], ids=["partial", "expand"])
    def test_mapped_custom_resolver(
        self, asset, events_response, create_runtime_ti, mock_supervisor_comms, mapped_bound
    ):
        bound = _ParameterBound()
        timestamp = _get_timestamp(8)
        with DAG("test_mapped_custom_bound"):
            if mapped_bound:
                mapped = AssetEventSensor.partial(
                    task_id="s", asset=asset, params={"events_after": timestamp.isoformat()}
                ).expand(after=[bound, bound])
            else:
                mapped = AssetEventSensor.partial(
                    task_id="s",
                    asset=asset,
                    after=bound,
                    params={"events_after": timestamp.isoformat()},
                ).expand(limit=[1, 2])
        ti = create_runtime_ti(task=mapped, map_index=1)
        context = ti.get_template_context()
        ti.render_templates(context=context)
        mock_supervisor_comms.send.return_value = events_response
        assert ti.task.poke(context)
        mock_supervisor_comms.send.assert_called_with(
            GetAssetEventByAsset(
                name=ASSET_NAME,
                uri=ASSET_URI,
                after=timestamp,
                limit=None if mapped_bound else 2,
            )
        )

    def test_reconstructed_sensor_keeps_bound_after_rescheduling(
        self, asset, events_response, create_runtime_ti, mock_supervisor_comms, time_machine
    ):
        time_machine.move_to(_get_timestamp(10), tick=False)
        first = AssetEventSensor(task_id="s", asset=asset, after=RunStart(), mode="reschedule")
        ti = create_runtime_ti(task=first, start_date=_get_timestamp(8))
        context = ti.get_template_context()
        ti.render_templates(context=context)
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=[])
        with pytest.raises(AirflowRescheduleException):
            ti.task.execute(context)

        time_machine.shift(timedelta(hours=1))
        resumed = AssetEventSensor(task_id="s", asset=asset, after=RunStart(), mode="reschedule")
        resumed_ti = create_runtime_ti(task=resumed, start_date=_get_timestamp(8), task_reschedule_count=1)
        resumed_context = resumed_ti.get_template_context()
        resumed_ti.render_templates(context=resumed_context)
        mock_supervisor_comms.send.side_effect = [
            TaskRescheduleStartDate(start_date=_get_timestamp(10)),
            events_response,
        ]
        assert resumed_ti.task.execute(resumed_context)
        query = GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, after=_get_timestamp(8))
        assert [c for c in mock_supervisor_comms.send.call_args_list if c == call(query)] == [
            call(query),
            call(query),
        ]


class TestPoke:
    def test_returns_serialized_events(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(task_id="s", asset=asset)
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
            task_id="s", asset=asset, count_policy=count_policy, expected_count=expected_count
        )
        result = sensor.poke(_get_context(sensor))
        assert bool(result) is done
        assert result.xcom_value == ([event.model_dump(mode="json") for event in events] if done else None)

    @requires_3_4
    @pytest.mark.parametrize("use_alias", [False, True])
    @pytest.mark.parametrize("string_bounds", [False, True])
    def test_forwards_all_filters(self, asset, mock_supervisor_comms, use_alias, string_bounds):
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=[])
        target = AssetAlias(name=ALIAS_NAME) if use_alias else asset
        sensor = AssetEventSensor(
            task_id="s",
            asset=target,
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
        sensor = AssetEventSensor(task_id="s", asset=Asset.ref(**target))
        assert sensor.inlets == [Asset.ref(**target)]
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

    def test_fetches_fresh_events_on_every_poke(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.side_effect = [AssetEventsResult(asset_events=[]), events_response]
        sensor = AssetEventSensor(task_id="s", asset=asset)
        context = _get_context(sensor)
        assert bool(sensor.poke(context)) is False
        assert bool(sensor.poke(context)) is True
        query = GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI)
        assert mock_supervisor_comms.send.call_args_list == [call(query), call(query)]

    def test_execute_returns_xcom_payload(self, asset, events_response, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = events_response
        sensor = AssetEventSensor(task_id="s", asset=asset)
        result = sensor.execute(_get_context(sensor))
        assert result == [event.model_dump(mode="json") for event in events_response.asset_events]

    def test_mapped_task(self, asset, events_response, create_runtime_ti, mock_supervisor_comms):
        with DAG("test_dag"):
            mapped = AssetEventSensor.partial(task_id="s", asset=asset).expand(
                after=[_get_timestamp(1), _get_timestamp(2)]
            )
        ti = create_runtime_ti(task=mapped, map_index=1)
        context = ti.get_template_context()
        ti.render_templates(context=context)
        mock_supervisor_comms.send.return_value = events_response
        assert bool(ti.task.poke(context)) is True
        mock_supervisor_comms.send.assert_called_with(
            GetAssetEventByAsset(name=ASSET_NAME, uri=ASSET_URI, after=_get_timestamp(2))
        )


class TestProcessResult:
    @pytest.mark.parametrize(
        ("process_result", "expected_count", "expected_ids", "done"),
        [
            (only_us_partitions, 3, [1, 2, 4], True),
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
            asset=asset,
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
            asset=asset,
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
            asset=asset,
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
            asset=asset,
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
        sensor = AssetEventSensor(task_id="s", asset=asset, process_result=boom)
        with pytest.raises(RuntimeError, match="process_result exploded"):
            sensor.poke(_get_context(sensor))
