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

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.hooks.ai_agents import (
    VERSION_FAILURE_STATUSES,
    VERSION_INTERMEDIATE_STATUSES,
    VERSION_SUCCESS_STATUSES,
    AzureAIAgentsHook,
    get_agent_version,
    get_resource_attr,
    get_version_status,
    serialize_resource,
)
from airflow.providers.microsoft.azure.triggers.ai_agents import (
    AzureAIAgentDeleteTrigger,
    AzureAIAgentVersionTrigger,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


def validate_execute_complete_event(
    event: dict[str, Any] | None, *, require_version: bool = False
) -> dict[str, Any]:
    """Validate a trigger event and raise a specific exception for non-success events."""
    if event is None:
        raise RuntimeError("Trigger returned no event.")

    status = event.get("status")
    message = event.get("message", "No message returned from trigger.")
    if status == "success":
        if require_version and "version" not in event:
            raise RuntimeError("Trigger success event did not include version payload.")
        return event
    if status == "timeout":
        raise TimeoutError(message)
    if status == "error":
        raise RuntimeError(message)
    raise ValueError(f"Unexpected trigger event status: {status!r}")


class AzureAIHostedAgentBaseOperator(BaseOperator):
    """Base operator for Azure AI Hosted agent operations."""

    azure_ai_agents_conn_id: str
    endpoint: str | None
    api_version: str

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
            api_version=self.api_version,
        )


class CreateAzureAIAgentOperator(AzureAIHostedAgentBaseOperator):
    """
    Create an Azure AI Hosted agent from a container image definition.

    :param agent_name: Hosted agent name.
    :param definition: Hosted agent definition. Include ``container_configuration.image`` with
        the Azure Container Registry image URI.
    :param wait_for_completion: Whether to wait until the created version reaches ``active``.
    :param poll_interval: Time in seconds between status checks.
    :param timeout: Time in seconds to wait for the version to become active.
    :param deferrable: Run in deferrable mode when ``wait_for_completion`` is ``True``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    :param api_version: Foundry Agent Service API version.
    """

    template_fields: Sequence[str] = (
        "agent_name",
        "definition",
        "azure_ai_agents_conn_id",
        "endpoint",
        "api_version",
    )
    template_fields_renderers = {"definition": "json"}
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_name: str,
        definition: dict[str, Any],
        wait_for_completion: bool = True,
        poll_interval: float = 30.0,
        timeout: float = 60 * 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_name = agent_name
        self.definition = definition
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    def execute(self, context: Context) -> Any:
        """Create an Azure AI Hosted agent and optionally wait for the version to become active."""
        self.log.info("Creating Azure AI Hosted agent %s.", self.agent_name)
        version = self.hook.create_agent(agent_name=self.agent_name, definition=self.definition)
        agent_version = get_agent_version(version)
        if not self.wait_for_completion:
            return serialize_resource(version)
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=AzureAIAgentVersionTrigger(
                    azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
                    endpoint=self.endpoint,
                    api_version=self.api_version,
                    agent_name=self.agent_name,
                    agent_version=agent_version,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        return self._wait_for_version(agent_version=agent_version)

    def _wait_for_version(self, *, agent_version: str) -> Any:
        end_time = time.monotonic() + self.timeout
        while True:
            version = self.hook.get_agent_version(agent_name=self.agent_name, agent_version=agent_version)
            status = get_version_status(version)
            if status in VERSION_SUCCESS_STATUSES:
                return serialize_resource(version)
            if status in VERSION_FAILURE_STATUSES:
                error = get_resource_attr(version, "error")
                raise RuntimeError(
                    f"Azure AI Hosted agent {self.agent_name} version {agent_version} failed: {error}."
                )
            if status not in VERSION_INTERMEDIATE_STATUSES:
                raise RuntimeError(
                    f"Azure AI Hosted agent {self.agent_name} version {agent_version} "
                    f"reached unknown status {status}."
                )
            if time.monotonic() >= end_time:
                break

            self.log.info(
                "Azure AI Hosted agent %s version %s is in status %s. Sleeping for %s seconds.",
                self.agent_name,
                agent_version,
                status,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

        raise TimeoutError(
            f"Timeout waiting for Azure AI Hosted agent {self.agent_name} version {agent_version}."
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> Any:
        """Resume after the version trigger completes."""
        validated_event = validate_execute_complete_event(event, require_version=True)
        self.log.info(validated_event["message"])
        return validated_event["version"]


class UpdateAzureAIAgentOperator(CreateAzureAIAgentOperator):
    """
    Create a new Azure AI Hosted agent version.

    The Hosted agent API publishes updates as immutable versions.
    """

    def execute(self, context: Context) -> Any:
        """Create a new Azure AI Hosted agent version and optionally wait for it to become active."""
        self.log.info("Creating a new Azure AI Hosted agent %s version.", self.agent_name)
        version = self.hook.create_agent_version(agent_name=self.agent_name, definition=self.definition)
        agent_version = get_agent_version(version)
        if not self.wait_for_completion:
            return serialize_resource(version)
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=AzureAIAgentVersionTrigger(
                    azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
                    endpoint=self.endpoint,
                    api_version=self.api_version,
                    agent_name=self.agent_name,
                    agent_version=agent_version,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        return self._wait_for_version(agent_version=agent_version)


class RunAzureAIAgentOperator(AzureAIHostedAgentBaseOperator):
    """
    Invoke an Azure AI Hosted agent.

    :param agent_name: Hosted agent name.
    :param input_data: Request payload for the selected protocol.
    :param protocol: Hosted agent protocol to use. Supported values are ``responses`` and ``invocations``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    :param api_version: Foundry Agent Service API version.
    """

    template_fields: Sequence[str] = (
        "agent_name",
        "input_data",
        "protocol",
        "azure_ai_agents_conn_id",
        "endpoint",
        "api_version",
    )
    template_fields_renderers = {"input_data": "json"}
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_name: str,
        input_data: dict[str, Any],
        protocol: str = "responses",
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_name = agent_name
        self.input_data = input_data
        self.protocol = protocol
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    def execute(self, context: Context) -> Any:
        """Invoke an Azure AI Hosted agent and return the response payload."""
        self.log.info("Invoking Azure AI Hosted agent %s with %s protocol.", self.agent_name, self.protocol)
        if self.protocol == "responses":
            return serialize_resource(
                self.hook.invoke_agent_responses(agent_name=self.agent_name, input_data=self.input_data)
            )
        if self.protocol == "invocations":
            return serialize_resource(
                self.hook.invoke_agent_invocations(agent_name=self.agent_name, input_data=self.input_data)
            )
        raise ValueError("protocol must be either 'responses' or 'invocations'.")


class DeleteAzureAIAgentOperator(AzureAIHostedAgentBaseOperator):
    """
    Delete an Azure AI Hosted agent or one Hosted agent version.

    :param agent_name: Hosted agent name.
    :param agent_version: Optional Hosted agent version. When omitted, the whole agent is deleted.
    :param force: Whether to cascade-delete active sessions when deleting the whole agent.
    :param wait_for_completion: Whether to wait until the resource is deleted.
    :param poll_interval: Time in seconds between status checks.
    :param timeout: Time in seconds to wait for deletion to complete.
    :param deferrable: Run in deferrable mode when ``wait_for_completion`` is ``True``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint override.
    :param api_version: Foundry Agent Service API version.
    """

    template_fields: Sequence[str] = (
        "agent_name",
        "agent_version",
        "azure_ai_agents_conn_id",
        "endpoint",
        "api_version",
    )
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_name: str,
        agent_version: str | None = None,
        force: bool = False,
        wait_for_completion: bool = True,
        poll_interval: float = 30.0,
        timeout: float = 60 * 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_name = agent_name
        self.agent_version = agent_version
        self.force = force
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    def execute(self, context: Context) -> None:
        """Delete an Azure AI Hosted agent or version and optionally wait for deletion."""
        if self.agent_version is None:
            self.log.info("Deleting Azure AI Hosted agent %s.", self.agent_name)
            self.hook.delete_agent(agent_name=self.agent_name, force=self.force)
        else:
            self.log.info(
                "Deleting Azure AI Hosted agent %s version %s.", self.agent_name, self.agent_version
            )
            self.hook.delete_agent_version(agent_name=self.agent_name, agent_version=self.agent_version)

        if not self.wait_for_completion:
            return
        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=AzureAIAgentDeleteTrigger(
                    azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
                    endpoint=self.endpoint,
                    api_version=self.api_version,
                    agent_name=self.agent_name,
                    agent_version=self.agent_version,
                    timeout=self.timeout,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        self._wait_for_deletion()

    def _wait_for_deletion(self) -> None:
        end_time = time.monotonic() + self.timeout
        while True:
            if self.agent_version is None:
                is_deleted = self.hook.is_agent_deleted(agent_name=self.agent_name)
            else:
                is_deleted = self.hook.is_agent_version_deleted(
                    agent_name=self.agent_name, agent_version=self.agent_version
                )
            if is_deleted:
                self.log.info("Azure AI Hosted agent %s was deleted.", self.agent_name)
                return
            if time.monotonic() >= end_time:
                break

            self.log.info(
                "Azure AI Hosted agent %s is still retrievable. Sleeping for %s seconds.",
                self.agent_name,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

        raise TimeoutError(f"Timeout waiting for Azure AI Hosted agent {self.agent_name} deletion.")

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> None:
        """Resume after the delete trigger completes."""
        validated_event = validate_execute_complete_event(event)
        self.log.info(validated_event["message"])
