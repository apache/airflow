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

pytest.importorskip("azure.ai.agents")

from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsAsyncHook
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


def create_run(status: str, **kwargs) -> SimpleNamespace:
    return SimpleNamespace(id=RUN_ID, thread_id=THREAD_ID, status=status, **kwargs)


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

    def test_build_trigger_event_completed_with_incomplete_details(self):
        event = self.trigger._build_trigger_event(
            create_run("completed", incomplete_details={"reason": "max_completion_tokens"})
        )

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"Azure AI Agent run {RUN_ID} completed with incomplete output: max_completion_tokens."
                ),
                "run": {
                    "id": RUN_ID,
                    "thread_id": THREAD_ID,
                    "status": "completed",
                    "incomplete_details": {"reason": "max_completion_tokens"},
                },
            }
        )

    def test_build_trigger_event_failure(self):
        event = self.trigger._build_trigger_event(create_run("cancelled"))

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": f"Azure AI Agent run {RUN_ID} finished with status cancelled.",
                "run": {"id": RUN_ID, "thread_id": THREAD_ID, "status": "cancelled"},
            }
        )

    def test_build_trigger_event_requires_action(self):
        event = self.trigger._build_trigger_event(
            create_run("requires_action", required_action={"type": "submit_tool_outputs"})
        )

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"Azure AI Agent run {RUN_ID} requires tool outputs, but "
                    "RunAzureAIAgentOperator does not support tool-output submission."
                ),
                "run": {
                    "id": RUN_ID,
                    "thread_id": THREAD_ID,
                    "status": "requires_action",
                    "required_action": {"type": "submit_tool_outputs"},
                },
            }
        )

    def test_build_trigger_event_non_terminal(self):
        assert self.trigger._build_trigger_event(create_run("in_progress")) is None

    def test_build_trigger_event_cancelling_is_non_terminal(self):
        assert self.trigger._build_trigger_event(create_run("cancelling")) is None

    def test_build_trigger_event_unknown_status(self):
        event = self.trigger._build_trigger_event(create_run("validating"))

        assert event == TriggerEvent(
            {
                "status": "error",
                "message": f"Azure AI Agent run {RUN_ID} reached unknown status validating.",
                "run": {"id": RUN_ID, "thread_id": THREAD_ID, "status": "validating"},
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep")
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_run")
    async def test_run_sleeps_until_success(self, mock_get_run, mock_sleep):
        mock_get_run.side_effect = [create_run("in_progress"), create_run("completed")]

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
    @mock.patch(f"{MODULE}.asyncio.sleep")
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_run")
    @mock.patch(f"{MODULE}.time.monotonic", side_effect=[1, 1])
    async def test_run_timeout(self, mock_monotonic, mock_get_run, mock_sleep):
        trigger = AzureAIAgentRunTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            thread_id=THREAD_ID,
            run_id=RUN_ID,
            timeout=0,
            poll_interval=2,
        )
        mock_get_run.return_value = create_run("in_progress")

        events = [event async for event in trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent run {RUN_ID}.",
                    "run": {"id": RUN_ID, "thread_id": THREAD_ID},
                }
            )
        ]
        mock_get_run.assert_awaited_once_with(thread_id=THREAD_ID, run_id=RUN_ID)
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_get_run")
    async def test_run_exception_has_context(self, mock_get_run):
        mock_get_run.side_effect = RuntimeError("service unavailable")

        events = [event async for event in self.trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"Failed while polling Azure AI Agent run {RUN_ID}: service unavailable",
                    "run": {"id": RUN_ID, "thread_id": THREAD_ID},
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
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_is_agent_deleted")
    async def test_run_sleeps_until_deleted(self, mock_is_agent_deleted, mock_sleep):
        mock_is_agent_deleted.side_effect = [False, True]

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
    @mock.patch(f"{MODULE}.asyncio.sleep")
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_is_agent_deleted")
    @mock.patch(f"{MODULE}.time.monotonic", side_effect=[1, 1])
    async def test_run_timeout(self, mock_monotonic, mock_is_agent_deleted, mock_sleep):
        trigger = AzureAIAgentDeleteTrigger(
            azure_ai_agents_conn_id=CONN_ID,
            endpoint=ENDPOINT,
            agent_id=AGENT_ID,
            timeout=0,
            poll_interval=2,
        )
        mock_is_agent_deleted.return_value = False

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
        mock_is_agent_deleted.assert_awaited_once_with(agent_id=AGENT_ID)
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    @mock.patch.object(AzureAIAgentsAsyncHook, "async_is_agent_deleted")
    async def test_run_exception_has_context(self, mock_is_agent_deleted):
        mock_is_agent_deleted.side_effect = RuntimeError("service unavailable")

        events = [event async for event in self.trigger.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"Failed while polling Azure AI Agent {AGENT_ID} deletion: service unavailable",
                    "agent_id": AGENT_ID,
                }
            )
        ]
