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

from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

RUN_FAILURE_STATUSES = {"failed", "cancelled", "expired", "incomplete"}
RUN_SUCCESS_STATUSES = {"completed"}


def serialize_resource(resource: Any) -> Any:
    """Serialize an Azure SDK object into XCom-safe primitives."""
    if resource is None or isinstance(resource, str | int | float | bool):
        return resource
    if isinstance(resource, list | tuple):
        return [serialize_resource(item) for item in resource]
    if isinstance(resource, dict):
        return {key: serialize_resource(value) for key, value in resource.items()}
    if hasattr(resource, "as_dict"):
        return serialize_resource(resource.as_dict())
    if hasattr(resource, "model_dump"):
        return serialize_resource(resource.model_dump())
    if hasattr(resource, "__dict__"):
        return {
            key: serialize_resource(value) for key, value in vars(resource).items() if not key.startswith("_")
        }
    return resource


def get_resource_attr(resource: Any, attr: str) -> Any:
    """Get an attribute from an SDK resource or mapping."""
    if isinstance(resource, dict):
        return resource.get(attr)
    return getattr(resource, attr, None)


def get_run_status(run: Any) -> str:
    """Return a normalized run status string."""
    status = get_resource_attr(run, "status")
    if hasattr(status, "value"):
        status = status.value
    if status is None:
        raise ValueError("Azure AI Agent run did not include a status.")
    return str(status).lower()


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
                    "message": f"Azure AI Agent run {self.run_id} finished with status {status}.",
                    "run": serialized_run,
                }
            )
        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the run status until terminal state or timeout."""
        hook = AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

        try:
            end_time = time.monotonic() + self.timeout
            while time.monotonic() <= end_time:
                run = await asyncio.to_thread(
                    hook.get_run,
                    thread_id=self.thread_id,
                    run_id=self.run_id,
                )
                event = self._build_trigger_event(run)
                if event:
                    yield event
                    return

                await asyncio.sleep(self.poll_interval)

            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure AI Agent run {self.run_id}.",
                    "run_id": self.run_id,
                    "thread_id": self.thread_id,
                }
            )
        except Exception as e:
            self.log.exception("Exception occurred while waiting for Azure AI Agent run.")
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(e),
                    "run_id": self.run_id,
                    "thread_id": self.thread_id,
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
        hook = AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

        try:
            end_time = time.monotonic() + self.timeout
            while time.monotonic() <= end_time:
                if await asyncio.to_thread(hook.is_agent_deleted, agent_id=self.agent_id):
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Azure AI Agent {self.agent_id} was deleted.",
                            "agent_id": self.agent_id,
                        }
                    )
                    return

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
                    "message": str(e),
                    "agent_id": self.agent_id,
                }
            )
