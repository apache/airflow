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

from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsAsyncHook
from airflow.providers.microsoft.azure.triggers.ai_agents import AzureAIAgentVersionTrigger

MODULE = "airflow.providers.microsoft.azure.triggers.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_NAME = "agent-123"


def build_trigger(**overrides):
    kwargs = {
        "azure_ai_agents_conn_id": CONN_ID,
        "endpoint": ENDPOINT,
        "api_version": "v1",
        "agent_name": AGENT_NAME,
        "agent_version": "1",
        "timeout": 10,
        "poll_interval": 2,
        **overrides,
    }
    return AzureAIAgentVersionTrigger(**kwargs)


async def get_trigger_event(trigger):
    generator = trigger.run()
    event = await anext(generator)
    await generator.aclose()
    return event


class TestAzureAIAgentVersionTrigger:
    def test_serialize(self):
        trigger = build_trigger(api_version="v2")

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.microsoft.azure.triggers.ai_agents.AzureAIAgentVersionTrigger"
        assert kwargs == {
            "azure_ai_agents_conn_id": CONN_ID,
            "endpoint": ENDPOINT,
            "api_version": "v2",
            "agent_name": AGENT_NAME,
            "agent_version": "1",
            "timeout": 10,
            "poll_interval": 2,
        }

    def test_serialize_default_endpoint(self):
        trigger = AzureAIAgentVersionTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=10,
            poll_interval=2,
        )

        _, kwargs = trigger.serialize()

        assert kwargs["endpoint"] is None

    @mock.patch(f"{MODULE}._serialize_resource", autospec=True)
    def test_build_trigger_event_does_not_serialize_intermediate_version(self, mock_serialize):
        trigger = build_trigger()

        assert trigger._build_trigger_event({"status": "creating"}) is None
        mock_serialize.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "close", autospec=True)
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_success(self, mock_get_version, mock_close):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "active"}
        trigger = build_trigger()

        event = await get_trigger_event(trigger)

        assert event.payload == {
            "status": "success",
            "message": f"Azure AI Hosted agent {AGENT_NAME} version 1 is active.",
            "version": {"name": AGENT_NAME, "version": "1", "status": "active"},
        }
        mock_close.assert_awaited_once()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep", autospec=True)
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_polls_until_success(self, mock_get_version, mock_sleep):
        creating = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        active = {"name": AGENT_NAME, "version": "1", "status": "active"}
        mock_get_version.side_effect = [creating, creating, active]
        trigger = build_trigger(poll_interval=1)

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "success"
        assert mock_get_version.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_awaited_with(1)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_failure(self, mock_get_version):
        mock_get_version.return_value = {
            "name": AGENT_NAME,
            "version": "1",
            "status": "failed",
            "error": {"message": "boom"},
        }
        trigger = build_trigger()

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "error"
        assert "boom" in event.payload["message"]
        assert event.payload["version"]["status"] == "failed"

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_failure_without_error_details(self, mock_get_version):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "failed"}
        trigger = build_trigger()

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "error"
        assert event.payload["message"] == (
            f"Azure AI Hosted agent {AGENT_NAME} version 1 failed: No error details were returned."
        )
        assert event.payload["version"]["status"] == "failed"

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_unknown_status(self, mock_get_version):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "paused"}
        trigger = build_trigger()

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "error"
        assert "unknown status paused" in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_timeout(self, mock_get_version, mock_monotonic, mock_sleep):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        trigger = build_trigger(timeout=0)

        event = await get_trigger_event(trigger)

        assert event.payload == {
            "status": "timeout",
            "message": f"Timeout waiting for Azure AI Hosted agent {AGENT_NAME} version 1.",
            "version": {"name": AGENT_NAME, "version": "1"},
        }
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "close", autospec=True)
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_exception(self, mock_get_version, mock_close):
        mock_get_version.side_effect = RuntimeError("boom")
        trigger = build_trigger()

        event = await get_trigger_event(trigger)

        assert event.payload == {
            "status": "error",
            "message": f"Failed while polling Azure AI Hosted agent {AGENT_NAME} version 1: boom",
            "version": {"name": AGENT_NAME, "version": "1"},
        }
        mock_close.assert_awaited_once()
