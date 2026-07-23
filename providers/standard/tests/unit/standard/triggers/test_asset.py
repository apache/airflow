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

from airflow.providers.standard.triggers.asset import AssetPartitionTrigger
from airflow.sdk import timezone
from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import AssetEventsResult, ErrorResponse, GetAssetEventByAsset
from airflow.triggers.base import TriggerEvent


class TestAssetPartitionTrigger:
    def test_serialization(self):
        after = timezone.datetime(2024, 1, 1)
        trigger = AssetPartitionTrigger(
            asset_name="orders",
            asset_uri="s3://warehouse/orders",
            partition_key="2024-01-01",
            after=after,
            poke_interval=10,
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.standard.triggers.asset.AssetPartitionTrigger"
        assert kwargs == {
            "asset_name": "orders",
            "asset_uri": "s3://warehouse/orders",
            "partition_key": "2024-01-01",
            "after": after,
            "poke_interval": 10,
        }

    @pytest.mark.asyncio
    async def test_run_yields_success_when_partition_event_exists(self, monkeypatch):
        comms = mock.Mock()
        comms.asend = mock.AsyncMock(
            return_value=AssetEventsResult(
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
        )
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
        trigger = AssetPartitionTrigger(
            asset_name="orders",
            asset_uri="s3://warehouse/orders",
            partition_key="2024-01-01",
            poke_interval=0,
        )

        assert await trigger.run().__anext__() == TriggerEvent({"status": "success"})
        comms.asend.assert_awaited_once_with(
            GetAssetEventByAsset(
                name="orders",
                uri="s3://warehouse/orders",
                partition_key="2024-01-01",
                ascending=False,
                limit=1,
            )
        )

    @pytest.mark.asyncio
    async def test_run_yields_error_for_supervisor_error(self, monkeypatch):
        comms = mock.Mock()
        comms.asend = mock.AsyncMock(return_value=ErrorResponse(error=ErrorType.ASSET_NOT_FOUND))
        monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
        trigger = AssetPartitionTrigger(
            asset_name="orders",
            asset_uri="s3://warehouse/orders",
            partition_key="2024-01-01",
            poke_interval=0,
        )

        assert await trigger.run().__anext__() == TriggerEvent(
            {"status": "error", "message": "ASSET_NOT_FOUND: None"}
        )
