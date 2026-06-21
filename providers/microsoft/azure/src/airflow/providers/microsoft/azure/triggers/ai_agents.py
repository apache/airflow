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
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.microsoft.azure.hooks.ai_agents import (
    RUN_FAILURE_STATUSES,
    RUN_INTERMEDIATE_STATUSES,
    RUN_SUCCESS_STATUSES,
    AzureAIAgentsAsyncHook,
    build_incomplete_run_message,
    build_run_failure_message,
    build_run_payload,
    get_incomplete_details,
    get_run_status,
    serialize_resource,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureAIAgentRunTrigger(BaseTrigger):
    """
    Trigger that polls an Azure AI Agent run until it reaches a terminal status.

    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    :param thread_id: Thread id for the run.
    :param run_id: Run id to poll.
    :param timeout: Time in seconds to wait for the run to complete.
    :param poll_interval: Poll interval in seconds.
    """

    def __init__(
        self,
        *,
        azure_ai_agents_conn_id: str,
        endpoint: str | None,
        thread_id: str,
        run_id: str,
        timeout: float,
        poll_interval: float,
    ) -> None:
        super().__init__()
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.thread_id = thread_id
        self.run_id = run_id
        self.timeout = timeout
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "azure_ai_agents_conn_id": self.azure_ai_agents_conn_id,
                "endpoint": self.endpoint,
                "thread_id": self.thread_id,
                "run_id": self.run_id,
                "timeout": self.timeout,
                "poll_interval": self.poll_interval,
            },
        )

    def _build_trigger_event(self, run: Any) -> TriggerEvent | None:
        """Build a terminal TriggerEvent for a run."""
        status = get_run_status(run)
        serialized_run = serialize_resource(run)
        if status in RUN_SUCCESS_STATUSES:
            incomplete_details = get_incomplete_details(run)
            if incomplete_details:
                return TriggerEvent(
                    {
                        "status": "error",
                        "message": build_incomplete_run_message(
                            run_id=self.run_id, incomplete_details=incomplete_details
                        ),
                        "run": serialized_run,
                    }
                )
            return TriggerEvent(
                {
                    "status": "success",
                    "message": f"Azure AI Agent run {self.run_id} completed.",
                    "run": serialized_run,
                }
            )
        if status in RUN_FAILURE_STATUSES:
            return TriggerEvent(
                {
                    "status": "error",
                    "message": build_run_failure_message(run_id=self.run_id, status=status),
                    "run": serialized_run,
                }
            )
        if status not in RUN_INTERMEDIATE_STATUSES:
            return TriggerEvent(
                {
                    "status": "error",
                    "message": f"Azure AI Agent run {self.run_id} reached unknown status {status}.",
                    "run": serialized_run,
                }
            )
        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the run status until terminal state or timeout."""
        hook = AzureAIAgentsAsyncHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

        try:
            end_time = time.monotonic() + self.timeout
            while True:
                run = await hook.async_get_run(thread_id=self.thread_id, run_id=self.run_id)
                event = self._build_trigger_event(run)
                if event:
                    yield event
                    return
                if time.monotonic() >= end_time:
                    break

                await asyncio.sleep(self.poll_interval)

            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent run {self.run_id}.",
                    "run": build_run_payload(run_id=self.run_id, thread_id=self.thread_id),
                }
            )
        except Exception as e:
            self.log.exception("Exception occurred while waiting for Azure AI Agent run.")
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Failed while polling Azure AI Agent run {self.run_id}: {e}",
                    "run": build_run_payload(run_id=self.run_id, thread_id=self.thread_id),
                }
            )


class AzureAIAgentDeleteTrigger(BaseTrigger):
    """
    Trigger that polls an Azure AI Agent until it is no longer retrievable.

    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    :param agent_id: Agent id to poll for deletion.
    :param timeout: Time in seconds to wait for deletion to complete.
    :param poll_interval: Poll interval in seconds.
    """

    def __init__(
        self,
        *,
        azure_ai_agents_conn_id: str,
        endpoint: str | None,
        agent_id: str,
        timeout: float,
        poll_interval: float,
    ) -> None:
        super().__init__()
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.agent_id = agent_id
        self.timeout = timeout
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "azure_ai_agents_conn_id": self.azure_ai_agents_conn_id,
                "endpoint": self.endpoint,
                "agent_id": self.agent_id,
                "timeout": self.timeout,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the agent until the service reports it as deleted."""
        hook = AzureAIAgentsAsyncHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

        try:
            end_time = time.monotonic() + self.timeout
            while True:
                if await hook.async_is_agent_deleted(agent_id=self.agent_id):
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Azure AI Agent {self.agent_id} was deleted.",
                            "agent_id": self.agent_id,
                        }
                    )
                    return
                if time.monotonic() >= end_time:
                    break

                await asyncio.sleep(self.poll_interval)

            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent {self.agent_id} deletion.",
                    "agent_id": self.agent_id,
                }
            )
        except Exception as e:
            self.log.exception("Exception occurred while waiting for Azure AI Agent deletion.")
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Failed while polling Azure AI Agent {self.agent_id} deletion: {e}",
                    "agent_id": self.agent_id,
                }
            )
