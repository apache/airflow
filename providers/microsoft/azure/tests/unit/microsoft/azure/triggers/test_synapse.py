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

import time
from unittest import mock

import pytest
from azure.core.exceptions import ServiceRequestError

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
            ("Failed", "error"),
            ("Cancelled", "error"),
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
                "status": "error",
                "message": f"Pipeline run {RUN_ID} finished with state Failed.",
                "run_id": RUN_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.cancel_pipeline_run")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_exception(self, mock_status, mock_cancel):
        mock_status.side_effect = Exception("API failure")

        events = [event async for event in self.TRIGGER.run()]

        mock_cancel.assert_awaited_once_with(RUN_ID)

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": "API failure",
                    "run_id": RUN_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.synapse.asyncio.sleep", new_callable=mock.AsyncMock)
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.refresh_conn")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_refresh_on_service_request_error(self, mock_status, mock_refresh, _mock_sleep):
        mock_status.side_effect = [
            ServiceRequestError("token expired"),
            AzureSynapsePipelineRunStatus.SUCCEEDED,
        ]

        events = [event async for event in self.TRIGGER.run()]

        mock_refresh.assert_awaited_once()

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Pipeline run {RUN_ID} succeeded.",
                    "run_id": RUN_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.synapse.time")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.cancel_pipeline_run")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_timeout_pipeline_already_succeeded(self, mock_status, mock_cancel, mock_time):
        mock_status.return_value = AzureSynapsePipelineRunStatus.SUCCEEDED

        mock_time.time.return_value = AZ_PIPELINE_END_TIME + 60

        events = [event async for event in self.TRIGGER.run()]

        mock_cancel.assert_not_awaited()

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Pipeline run {RUN_ID} succeeded.",
                    "run_id": RUN_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.synapse.time")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.cancel_pipeline_run")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_timeout_cancellation(self, mock_status, mock_cancel, mock_time):

        mock_status.return_value = AzureSynapsePipelineRunStatus.IN_PROGRESS

        mock_time.time.return_value = AZ_PIPELINE_END_TIME + 60

        events = [event async for event in self.TRIGGER.run()]

        mock_cancel.assert_awaited_once_with(RUN_ID)

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"Timeout waiting for pipeline run {RUN_ID}.",
                    "run_id": RUN_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.synapse.time")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.cancel_pipeline_run")
    @mock.patch(f"{MODULE}.hooks.synapse.AzureSynapsePipelineAsyncHook.get_pipeline_run_status")
    async def test_trigger_timeout_cancel_failure_still_yields_timeout(
        self, mock_status, mock_cancel, mock_time
    ):
        mock_status.return_value = AzureSynapsePipelineRunStatus.IN_PROGRESS
        mock_cancel.side_effect = Exception("cancel failed")

        mock_time.time.return_value = AZ_PIPELINE_END_TIME + 60

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"Timeout waiting for pipeline run {RUN_ID}.",
                    "run_id": RUN_ID,
                }
            )
        ]
