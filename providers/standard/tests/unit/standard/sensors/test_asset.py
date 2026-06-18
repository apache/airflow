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

from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowFailException, Asset, TaskDeferred
from airflow.providers.standard.sensors.asset import AssetPartitionSensor
from airflow.providers.standard.triggers.asset import AssetPartitionTrigger
from airflow.sdk import timezone
from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    ErrorResponse,
    GetAssetEventByAsset,
)


class TestAssetPartitionSensor:
    def test_template_fields(self):
        assert AssetPartitionSensor.template_fields == ("partition_key",)

    def test_poke_returns_true_when_partition_event_exists(self, monkeypatch):
        comms = mock.Mock()
        comms.send.return_value = AssetEventsResult(
            asset_events=[
                AssetEventResponse(
                    id=1,
                    timestamp=timezone.utcnow(),
                    asset=AssetResponse(name="orders", uri="s3://warehouse/orders", group="asset"),
                    partition_key="2024-01-01",
                    created_dagruns=[],
                )
            ],
        )
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)

        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
        )

        assert sensor.poke({}) is True
        comms.send.assert_called_once_with(
            GetAssetEventByAsset(
                name="orders",
                uri="s3://warehouse/orders",
                partition_key="2024-01-01",
                ascending=False,
                limit=1,
            )
        )

    def test_poke_returns_false_when_partition_event_is_missing(self, monkeypatch):
        comms = mock.Mock()
        comms.send.return_value = AssetEventsResult(asset_events=[])
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)

        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
        )

        assert sensor.poke({}) is False

    def test_poke_raises_runtime_error_for_supervisor_error(self, monkeypatch):
        comms = mock.Mock()
        comms.send.return_value = ErrorResponse(error=ErrorType.ASSET_NOT_FOUND)
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)

        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
        )

        with pytest.raises(AirflowRuntimeError):
            sensor.poke({})

    def test_poke_raises_for_unexpected_supervisor_response(self, monkeypatch):
        comms = mock.Mock()
        comms.send.return_value = object()
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)

        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
        )

        with pytest.raises(TypeError, match="Unexpected response from supervisor"):
            sensor.poke({})

    def test_execute_defers_when_partition_event_is_missing(self, monkeypatch):
        comms = mock.Mock()
        comms.send.return_value = AssetEventsResult(asset_events=[])
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)

        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            sensor.execute({})

        assert isinstance(exc.value.trigger, AssetPartitionTrigger)
        assert exc.value.trigger.asset_name == "orders"
        assert exc.value.trigger.asset_uri == "s3://warehouse/orders"
        assert exc.value.trigger.partition_key == "2024-01-01"

    def test_execute_complete_raises_for_trigger_error(self):
        sensor = AssetPartitionSensor(
            task_id="wait_orders",
            asset=Asset(name="orders", uri="s3://warehouse/orders"),
            partition_key="2024-01-01",
        )

        with pytest.raises(AirflowFailException, match="failed"):
            sensor.execute_complete({}, {"status": "error", "message": "failed"})
