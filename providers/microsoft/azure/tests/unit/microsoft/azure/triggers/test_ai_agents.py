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

from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.microsoft.azure.triggers.ai_agents import (
    AzureAIAgentDeleteTrigger,
    AzureAIAgentRunTrigger,
)
from airflow.triggers.base import TriggerEvent

MODULE = "airflow.providers.microsoft.azure.triggers.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_ID = "agent_123"
RUN_ID = "run_123"
THREAD_ID = "thread_123"


def create_run(status: str) -> SimpleNamespace:
    return SimpleNamespace(id=RUN_ID, thread_id=THREAD_ID, status=status)


class TestAzureAIAgentRunTrigger:
    def setup_method(self):
        self.trigger = AzureAIAgentRunTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            thread_id=THREAD_ID,
            run_id=RUN_ID,
            timeout=10,
            poll_interval=2,
        )

    def test_serialize(self):
        classpath, kwargs = self.trigger.serialize()

        assert classpath == f"{MODULE}.AzureAIAgentRunTrigger"
        assert kwargs == {
            "azure_ai_agents_conn_id": CONN_ID,
            "endpoint": ENDPOINT,
            "thread_id": THREAD_ID,
            "run_id": RUN_ID,
            "timeout": 10,
            "poll_interval": 2,
        }

    def test_build_trigger_event_success(self):
        event = self.trigger._build_trigger_event(create_run("completed"))

        assert event == TriggerEvent(
            {
                "status": "success",
                "message": f"Azure AI Agent run {RUN_ID} completed.",
                "run": {"id": RUN_ID, "thread_id": THREAD_ID, "status": "completed"},
            }
        )

    def test_build_trigger_event_failure(self):
        event = self.trigger._build_trigger_event(create_run("failed"))

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": f"Azure AI Agent run {RUN_ID} finished with status failed.",
                "run": {"id": RUN_ID, "thread_id": THREAD_ID, "status": "failed"},
            }
        )

    def test_build_trigger_event_non_terminal(self):
        assert self.trigger._build_trigger_event(create_run("in_progress")) is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep")
    @mock.patch(f"{MODULE}.asyncio.to_thread")
    async def test_run_sleeps_until_success(self, mock_to_thread, mock_sleep):
        mock_to_thread.side_effect = [create_run("in_progress"), create_run("completed")]

        events = [event async for event in self.trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Azure AI Agent run {RUN_ID} completed.",
                    "run": {"id": RUN_ID, "thread_id": THREAD_ID, "status": "completed"},
                }
            )
        ]
        mock_sleep.assert_awaited_once_with(2)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.time.monotonic", side_effect=[1, 2])
    async def test_run_timeout(self, mock_monotonic):
        trigger = AzureAIAgentRunTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            thread_id=THREAD_ID,
            run_id=RUN_ID,
            timeout=0,
            poll_interval=2,
        )

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent run {RUN_ID}.",
                    "run_id": RUN_ID,
                    "thread_id": THREAD_ID,
                }
            )
        ]


class TestAzureAIAgentDeleteTrigger:
    def setup_method(self):
        self.trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            agent_id=AGENT_ID,
            timeout=10,
            poll_interval=2,
        )

    def test_serialize(self):
        classpath, kwargs = self.trigger.serialize()

        assert classpath == f"{MODULE}.AzureAIAgentDeleteTrigger"
        assert kwargs == {
            "azure_ai_agents_conn_id": CONN_ID,
            "endpoint": ENDPOINT,
            "agent_id": AGENT_ID,
            "timeout": 10,
            "poll_interval": 2,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep")
    @mock.patch(f"{MODULE}.asyncio.to_thread")
    async def test_run_sleeps_until_deleted(self, mock_to_thread, mock_sleep):
        mock_to_thread.side_effect = [False, True]

        events = [event async for event in self.trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Azure AI Agent {AGENT_ID} was deleted.",
                    "agent_id": AGENT_ID,
                }
            )
        ]
        mock_sleep.assert_awaited_once_with(2)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.time.monotonic", side_effect=[1, 2])
    async def test_run_timeout(self, mock_monotonic):
        trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            agent_id=AGENT_ID,
            timeout=0,
            poll_interval=2,
        )

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent {AGENT_ID} deletion.",
                    "agent_id": AGENT_ID,
                }
            )
        ]
