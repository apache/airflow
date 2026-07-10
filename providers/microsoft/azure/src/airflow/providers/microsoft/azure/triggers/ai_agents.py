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

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.microsoft.azure._ai_agents import (
    VERSION_FAILURE_STATUSES,
    VERSION_INTERMEDIATE_STATUSES,
    VERSION_SUCCESS_STATUSES,
    _get_resource_attr,
    _get_version_status,
    _serialize_resource,
)
from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureAIAgentVersionTrigger(BaseTrigger):
    """
    Trigger that polls an Azure AI Hosted agent version until it becomes active.

    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override. Default is ``None``.
    :param api_version: Foundry Agent Service API version.
    :param agent_name: Hosted agent name.
    :param agent_version: Hosted agent version to poll.
    :param timeout: Time in seconds to wait for the version to become active.
    :param poll_interval: Poll interval in seconds.
    """

    def __init__(
        self,
        *,
        azure_ai_agents_conn_id: str,
        endpoint: str | None = None,
        api_version: str,
        agent_name: str,
        agent_version: str,
        timeout: float,
        poll_interval: float,
    ) -> None:
        super().__init__()
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version
        self.agent_name = agent_name
        self.agent_version = agent_version
        self.timeout = timeout
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "azure_ai_agents_conn_id": self.azure_ai_agents_conn_id,
                "endpoint": self.endpoint,
                "api_version": self.api_version,
                "agent_name": self.agent_name,
                "agent_version": self.agent_version,
                "timeout": self.timeout,
                "poll_interval": self.poll_interval,
            },
        )

    def _build_trigger_event(self, version: Any) -> TriggerEvent | None:
        """Build a terminal TriggerEvent for a Hosted agent version."""
        status = _get_version_status(version)
        if status in VERSION_INTERMEDIATE_STATUSES:
            return None

        serialized_version = _serialize_resource(version)
        if status in VERSION_SUCCESS_STATUSES:
            return TriggerEvent(
                {
                    "status": "success",
                    "message": (
                        f"Azure AI Hosted agent {self.agent_name} version {self.agent_version} is active."
                    ),
                    "version": serialized_version,
                }
            )
        if status in VERSION_FAILURE_STATUSES:
            error = _get_resource_attr(version, "error") or "No error details were returned"
            return TriggerEvent(
                {
                    "status": "error",
                    "message": (
                        f"Azure AI Hosted agent {self.agent_name} version {self.agent_version} failed: "
                        f"{error}."
                    ),
                    "version": serialized_version,
                }
            )
        return TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"Azure AI Hosted agent {self.agent_name} version {self.agent_version} "
                    f"reached unknown status {status}."
                ),
                "version": serialized_version,
            }
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the Hosted agent version status until terminal state or timeout."""
        hook = AzureAIAgentsAsyncHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
            api_version=self.api_version,
        )

        try:
            end_time = time.monotonic() + self.timeout
            while True:
                version = await hook.async_get_agent_version(
                    agent_name=self.agent_name,
                    agent_version=self.agent_version,
                )
                event = self._build_trigger_event(version)
                if event:
                    yield event
                    return
                if time.monotonic() >= end_time:
                    yield TriggerEvent(
                        {
                            "status": "timeout",
                            "message": (
                                f"Timeout waiting for Azure AI Hosted agent {self.agent_name} "
                                f"version {self.agent_version}."
                            ),
                            "version": {"name": self.agent_name, "version": self.agent_version},
                        }
                    )
                    return

                await asyncio.sleep(self.poll_interval)
        except Exception as e:
            self.log.exception(
                "Exception while polling Azure AI Hosted agent %s version %s.",
                self.agent_name,
                self.agent_version,
            )
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": (
                        f"Failed while polling Azure AI Hosted agent {self.agent_name} "
                        f"version {self.agent_version}: {e}"
                    ),
                    "version": {"name": self.agent_name, "version": self.agent_version},
                }
            )
        finally:
            await hook.close()
