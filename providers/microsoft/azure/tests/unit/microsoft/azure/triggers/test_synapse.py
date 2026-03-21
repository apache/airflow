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

import asyncio
import time
from unittest import mock

import pytest

from airflow.providers.microsoft.azure.triggers.synapse import (
    AzureSynapsePipelineRunStatus,
    AzureSynapsePipelineTrigger,
)
from airflow.triggers.base import TriggerEvent

AZURE_SYNAPSE_CONN_ID = "azure_synapse_default"
AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT = "azure_synapse_workspace_dev_endpoint"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
POKE_INTERVAL = 5
AZ_PIPELINE_END_TIME = time.time() + 60 * 60 * 24 * 7
MODULE = "airflow.providers.microsoft.azure"


class TestAzureSynapsePipelineTrigger:
    TRIGGER = AzureSynapsePipelineTrigger(
        run_id=RUN_ID,
        azure_synapse_conn_id=AZURE_SYNAPSE_CONN_ID,
        azure_synapse_workspace_dev_endpoint=AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
        check_interval=POKE_INTERVAL,
        end_time=AZ_PIPELINE_END_TIME,
    )

    def test_synapse_trigger_serialization(self):
        classpath, kwargs = self.TRIGGER.serialize()

        assert classpath == f"{MODULE}.triggers.synapse.AzureSynapsePipelineTrigger"

        assert kwargs == {
            "run_id": RUN_ID,
            "azure_synapse_conn_id": AZURE_SYNAPSE_CONN_ID,
            "azure_synapse_workspace_dev_endpoint": AZURE_SYNAPSE_WORKSPACE_DEV_ENDPOINT,
            "check_interval": POKE_INTERVAL,
            "end_time": AZ_PIPELINE_END_TIME,
        }

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ("Succeeded", "success"),
            ("Failed", "failure"),
            ("Cancelled", "failure"),
        ],
    )
    def test_build_trigger_event_terminal_states(self, status, expected):
        event = self.TRIGGER._build_trigger_event(status)

        assert event is not None
        assert event.payload["status"] == expected
        assert event.payload["run_id"] == RUN_ID

    @pytest.mark.parametrize(
        "status",
        ["Queued", "InProgress", "Canceling"],
    )
    def test_build_trigger_event_non_terminal_states(self, status):
        event = self.TRIGGER._build_trigger_event(status)
        assert event is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_run_success(self, mock_status):
        mock_status.return_value = AzureSynapsePipelineRunStatus.SUCCEEDED

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)

        assert actual == TriggerEvent(
            {
                "status": "success",
                "message": f"Pipeline run {RUN_ID} succeeded.",
                "run_id": RUN_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_run_failed(self, mock_status):
        mock_status.return_value = AzureSynapsePipelineRunStatus.FAILED

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)

        assert actual == TriggerEvent(
            {
                "status": "failure",
                "message": f"Pipeline run {RUN_ID} finished with state Failed.",
                "run_id": RUN_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_waiting(self, mock_status):
        mock_status.return_value = AzureSynapsePipelineRunStatus.QUEUED

        task = asyncio.create_task(self.TRIGGER.run().__anext__())

        await asyncio.sleep(0.5)

        assert task.done() is False

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_exception(self, mock_status):
        mock_status.side_effect = Exception("API failure")

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": "API failure",
                    "run_id": RUN_ID,
                }
            )
        ]
