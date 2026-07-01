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
from airflow.providers.microsoft.azure.triggers.ai_agents import (
    AzureAIAgentDeleteTrigger,
    AzureAIAgentVersionTrigger,
)

MODULE = "airflow.providers.microsoft.azure.triggers.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_NAME = "agent-123"


async def get_trigger_event(trigger):
    return await anext(trigger.run())


class TestAzureAIAgentVersionTrigger:
    def test_serialize(self):
        trigger = AzureAIAgentVersionTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v2",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=10,
            poll_interval=2,
        )

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

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_success(self, mock_get_version):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "active"}
        trigger = AzureAIAgentVersionTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=10,
            poll_interval=2,
        )

        event = await get_trigger_event(trigger)

        assert event.payload == {
            "status": "success",
            "message": f"Azure AI Hosted agent {AGENT_NAME} version 1 is active.",
            "version": {"name": AGENT_NAME, "version": "1", "status": "active"},
        }

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_failure(self, mock_get_version):
        mock_get_version.return_value = {
            "name": AGENT_NAME,
            "version": "1",
            "status": "failed",
            "error": {"message": "boom"},
        }
        trigger = AzureAIAgentVersionTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=10,
            poll_interval=2,
        )

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "error"
        assert "boom" in event.payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_agent_version", autospec=True)
    async def test_run_timeout(self, mock_get_version, mock_monotonic, mock_sleep):
        mock_get_version.return_value = {"name": AGENT_NAME, "version": "1", "status": "creating"}
        trigger = AzureAIAgentVersionTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=0,
            poll_interval=2,
        )

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "timeout"
        mock_sleep.assert_not_called()


class TestAzureAIAgentDeleteTrigger:
    def test_serialize(self):
        trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v2",
            agent_name=AGENT_NAME,
            agent_version=None,
            timeout=10,
            poll_interval=2,
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.microsoft.azure.triggers.ai_agents.AzureAIAgentDeleteTrigger"
        assert kwargs == {
            "azure_ai_agents_conn_id": CONN_ID,
            "endpoint": ENDPOINT,
            "api_version": "v2",
            "agent_name": AGENT_NAME,
            "agent_version": None,
            "timeout": 10,
            "poll_interval": 2,
        }

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_is_agent_deleted", autospec=True)
    async def test_run_success(self, mock_is_deleted):
        mock_is_deleted.return_value = True
        trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version="1",
            timeout=10,
            poll_interval=2,
        )

        event = await get_trigger_event(trigger)

        assert event.payload == {
            "status": "success",
            "message": f"Azure AI Hosted agent {AGENT_NAME} was deleted.",
            "agent_name": AGENT_NAME,
            "agent_version": "1",
        }
        mock_is_deleted.assert_awaited_once_with(mock.ANY, agent_name=AGENT_NAME, agent_version="1")

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep", autospec=True)
    @mock.patch(f"{MODULE}.time.monotonic", autospec=True, side_effect=[0, 0])
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_is_agent_deleted", autospec=True)
    async def test_run_timeout(self, mock_is_deleted, mock_monotonic, mock_sleep):
        mock_is_deleted.return_value = False
        trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            api_version="v1",
            agent_name=AGENT_NAME,
            agent_version=None,
            timeout=0,
            poll_interval=2,
        )

        event = await get_trigger_event(trigger)

        assert event.payload["status"] == "timeout"
        mock_sleep.assert_not_called()
