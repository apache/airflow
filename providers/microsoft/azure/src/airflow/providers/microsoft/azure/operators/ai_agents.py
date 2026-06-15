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
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsHook
from airflow.providers.microsoft.azure.triggers.ai_agents import (
    RUN_FAILURE_STATUSES,
    RUN_SUCCESS_STATUSES,
    AzureAIAgentDeleteTrigger,
    AzureAIAgentRunTrigger,
    get_resource_attr,
    get_run_status,
    serialize_resource,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


def validate_execute_complete_event(event: dict[str, Any] | None) -> dict[str, Any]:
    """Validate a trigger event and raise a specific exception for non-success events."""
    if event is None:
        raise RuntimeError("Trigger returned no event.")

    status = event.get("status")
    message = event.get("message", "No message returned from trigger.")
    if status == "success":
        return event
    if status == "timeout":
        raise TimeoutError(message)
    if status == "error":
        raise RuntimeError(message)
    raise ValueError(f"Unexpected trigger event status: {status!r}")


class CreateAzureAIAgentOperator(BaseOperator):
    """
    Create an Azure AI Agent.

    :param model: Model deployment name to use for the agent.
    :param config: Additional keyword arguments passed to ``AgentsClient.create_agent``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    """

    template_fields: Sequence[str] = ("model", "config", "azure_ai_agents_conn_id", "endpoint")
    template_fields_renderers = {"config": "json"}
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        model: str,
        config: dict[str, Any] | None = None,
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.model = model
        self.config = config or {}
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

    def execute(self, context: Context) -> Any:
        """Create an Azure AI Agent and return the created resource."""
        self.log.info("Creating Azure AI Agent.")
        agent = self.hook.create_agent(model=self.model, **self.config)
        return serialize_resource(agent)


class UpdateAzureAIAgentOperator(BaseOperator):
    """
    Update an Azure AI Agent.

    :param agent_id: Agent id to update.
    :param config: Additional keyword arguments passed to ``AgentsClient.update_agent``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    """

    template_fields: Sequence[str] = ("agent_id", "config", "azure_ai_agents_conn_id", "endpoint")
    template_fields_renderers = {"config": "json"}
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_id: str,
        config: dict[str, Any] | None = None,
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_id = agent_id
        self.config = config or {}
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

    def execute(self, context: Context) -> Any:
        """Update an Azure AI Agent and return the updated resource."""
        self.log.info("Updating Azure AI Agent %s.", self.agent_id)
        agent = self.hook.update_agent(agent_id=self.agent_id, **self.config)
        return serialize_resource(agent)


class RunAzureAIAgentOperator(BaseOperator):
    """
    Create a thread and run an Azure AI Agent.

    :param agent_id: Agent id to run.
    :param config: Additional keyword arguments passed to ``AgentsClient.create_thread_and_run``.
    :param wait_for_completion: Whether to wait until the run reaches a terminal status.
    :param poll_interval: Time in seconds between status checks.
    :param timeout: Time in seconds to wait for the run to complete.
    :param deferrable: Run in deferrable mode.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    """

    template_fields: Sequence[str] = ("agent_id", "config", "azure_ai_agents_conn_id", "endpoint")
    template_fields_renderers = {"config": "json"}
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_id: str,
        config: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        poll_interval: float = 30.0,
        timeout: float = 60 * 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_id = agent_id
        self.config = config or {}
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

    def execute(self, context: Context) -> Any:
        """Start an Azure AI Agent run and optionally wait for completion."""
        self.log.info("Running Azure AI Agent %s.", self.agent_id)
        run = self.hook.run_agent(agent_id=self.agent_id, **self.config)
        if not self.wait_for_completion:
            return serialize_resource(run)

        thread_id = get_resource_attr(run, "thread_id")
        run_id = get_resource_attr(run, "id")
        if not thread_id or not run_id:
            raise ValueError("Azure AI Agent run response must include both id and thread_id.")

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=AzureAIAgentRunTrigger(
                    azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
                    endpoint=self.endpoint,
                    thread_id=thread_id,
                    run_id=run_id,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        return self._wait_for_run_completion(thread_id=thread_id, run_id=run_id)

    def _wait_for_run_completion(self, *, thread_id: str, run_id: str) -> Any:
        end_time = time.monotonic() + self.timeout
        while time.monotonic() <= end_time:
            run = self.hook.get_run(thread_id=thread_id, run_id=run_id)
            status = get_run_status(run)
            if status in RUN_SUCCESS_STATUSES:
                return serialize_resource(run)
            if status in RUN_FAILURE_STATUSES:
                raise RuntimeError(f"Azure AI Agent run {run_id} finished with status {status}.")

            self.log.info(
                "Azure AI Agent run %s is in status %s. Sleeping for %s seconds.",
                run_id,
                status,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

        raise TimeoutError(f"Timeout waiting for Azure AI Agent run {run_id}.")

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> Any:
        """Resume after the run trigger completes."""
        validated_event = validate_execute_complete_event(event)
        self.log.info(validated_event["message"])
        return validated_event["run"]


class DeleteAzureAIAgentOperator(BaseOperator):
    """
    Delete an Azure AI Agent and wait until it is no longer retrievable.

    :param agent_id: Agent id to delete.
    :param poll_interval: Time in seconds between status checks.
    :param timeout: Time in seconds to wait for deletion to complete.
    :param deferrable: Run in deferrable mode.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    """

    template_fields: Sequence[str] = ("agent_id", "azure_ai_agents_conn_id", "endpoint")
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_id: str,
        poll_interval: float = 30.0,
        timeout: float = 60 * 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_id = agent_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
        )

    def execute(self, context: Context) -> None:
        """Delete an Azure AI Agent and wait for deletion to complete."""
        self.log.info("Deleting Azure AI Agent %s.", self.agent_id)
        self.hook.delete_agent(agent_id=self.agent_id)

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=AzureAIAgentDeleteTrigger(
                    azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
                    endpoint=self.endpoint,
                    agent_id=self.agent_id,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        self._wait_for_agent_deletion()

    def _wait_for_agent_deletion(self) -> None:
        end_time = time.monotonic() + self.timeout
        while time.monotonic() <= end_time:
            if self.hook.is_agent_deleted(agent_id=self.agent_id):
                self.log.info("Azure AI Agent %s was deleted.", self.agent_id)
                return

            self.log.info(
                "Azure AI Agent %s is still retrievable. Sleeping for %s seconds.",
                self.agent_id,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

        raise TimeoutError(f"Timeout waiting for Azure AI Agent {self.agent_id} deletion.")

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> None:
        """Resume after the delete trigger completes."""
        validated_event = validate_execute_complete_event(event)
        self.log.info(validated_event["message"])
