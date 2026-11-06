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
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure._ai_agents import (
    VERSION_FAILURE_STATUSES,
    VERSION_INTERMEDIATE_STATUSES,
    VERSION_SUCCESS_STATUSES,
    _get_agent_version,
    _get_resource_attr,
    _get_version_status,
    _serialize_resource,
)
from airflow.providers.microsoft.azure.hooks.ai_agents import AzureAIAgentsHook
from airflow.providers.microsoft.azure.triggers.ai_agents import AzureAIAgentVersionTrigger

if TYPE_CHECKING:
    from azure.ai.projects.models import AgentBlueprintReference
    from pydantic import JsonValue

    from airflow.sdk import Context


class CreateAzureAIAgentOperator(BaseOperator):
    """
    Create an Azure AI Hosted agent version from a container image definition.

    Creating a version also creates the Hosted agent itself on first use.

    :param agent_name: Hosted agent name.
    :param definition: Hosted agent definition. Include ``container_configuration.image`` with
        the Azure Container Registry image URI.
    :param metadata: Optional metadata attached to the Hosted agent. Default is ``None``.
    :param description: Optional human-readable Hosted agent description. Default is ``None``.
    :param blueprint_reference: Optional managed identity blueprint reference. Default is ``None``.
    :param wait_for_completion: Whether to wait until the created version reaches ``active``.
        Default is ``True``.
    :param poll_interval: Time in seconds between status checks. Default is ``30.0``.
    :param timeout: Time in seconds to wait for the version to become active. Default is ``3600``.
    :param deferrable: Run in deferrable mode when ``wait_for_completion`` is ``True``.
        Default is configured by ``operators.default_deferrable``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
        Default is ``azure_ai_agents_default``.
    :param endpoint: Optional Azure AI Foundry project endpoint override. Default is ``None``.
    :param api_version: Foundry Agent Service API version. Default is ``v1``.
    """

    template_fields: Sequence[str] = (
        "agent_name",
        "definition",
        "metadata",
        "description",
        "blueprint_reference",
        "azure_ai_agents_conn_id",
        "endpoint",
        "api_version",
    )
    template_fields_renderers = {
        "definition": "json",
        "metadata": "json",
        "blueprint_reference": "json",
    }
    ui_color = "#0678d4"

    def __init__(
        self,
        *,
        agent_name: str,
        definition: dict[str, Any],
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        blueprint_reference: AgentBlueprintReference | None = None,
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
        self.metadata = metadata
        self.description = description
        self.blueprint_reference = blueprint_reference
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
            api_version=self.api_version,
        )

    def execute(self, context: Context) -> dict[str, JsonValue]:
        """Create an Azure AI Hosted agent version and optionally wait for it to become active."""
        self.log.info("Creating Azure AI Hosted agent %s version.", self.agent_name)
        version = self.hook.create_agent_version(
            agent_name=self.agent_name,
            definition=self.definition,
            metadata=self.metadata,
            description=self.description,
            blueprint_reference=self.blueprint_reference,
        )
        if not self.wait_for_completion:
            return cast("dict[str, JsonValue]", _serialize_resource(version))
        agent_version = _get_agent_version(version)
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

    def _wait_for_version(self, *, agent_version: str) -> dict[str, JsonValue]:
        end_time = time.monotonic() + self.timeout
        while True:
            version = self.hook.get_agent_version(agent_name=self.agent_name, agent_version=agent_version)
            status = _get_version_status(version)
            if status in VERSION_SUCCESS_STATUSES:
                return cast("dict[str, JsonValue]", _serialize_resource(version))
            if status in VERSION_FAILURE_STATUSES:
                error = _get_resource_attr(version, "error") or "No error details were returned"
                raise RuntimeError(
                    f"Azure AI Hosted agent {self.agent_name} version {agent_version} failed: {error}."
                )
            if status not in VERSION_INTERMEDIATE_STATUSES:
                raise RuntimeError(
                    f"Azure AI Hosted agent {self.agent_name} version {agent_version} "
                    f"reached unknown status {status}."
                )
            if time.monotonic() >= end_time:
                raise TimeoutError(
                    f"Timeout waiting for Azure AI Hosted agent {self.agent_name} version {agent_version}."
                )

            self.log.info(
                "Azure AI Hosted agent %s version %s is in status %s. Sleeping for %s seconds.",
                self.agent_name,
                agent_version,
                status,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> dict[str, JsonValue]:
        """Resume after the version trigger completes."""
        if event is None:
            raise RuntimeError("Trigger returned no event.")

        status = event.get("status")
        message = event.get("message", "No message returned from trigger.")
        if status == "success":
            if "version" not in event:
                raise RuntimeError("Trigger success event did not include version payload.")
            self.log.info(message)
            return event["version"]
        if status == "timeout":
            raise TimeoutError(message)
        if status == "error":
            raise RuntimeError(message)
        raise ValueError(f"Unexpected trigger event status: {status!r}")


class UpdateAzureAIAgentOperator(CreateAzureAIAgentOperator):
    """
    Create a new version of an existing Azure AI Hosted agent.

    The Hosted agent API publishes updates as new immutable versions, so this operator accepts
    the same parameters as :class:`CreateAzureAIAgentOperator`.
    """


class RunAzureAIAgentOperator(BaseOperator):
    """
    Invoke an Azure AI Hosted agent.

    :param agent_name: Hosted agent name.
    :param input_data: Request payload for the selected protocol.
    :param protocol: Hosted agent protocol to use. Supported values are ``responses`` and
        ``invocations``. Default is ``responses``.
    :param agent_session_id: Optional session id used by the ``invocations`` protocol.
        Default is ``None``.
    :param user_isolation_key: Optional per-user isolation key for endpoint-scoped resources.
        Default is ``None``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
        Default is ``azure_ai_agents_default``.
    :param endpoint: Optional Azure AI Foundry project endpoint override. Default is ``None``.
    :param api_version: Foundry Agent Service API version. Default is ``v1``.
    """

    template_fields: Sequence[str] = (
        "agent_name",
        "input_data",
        "protocol",
        "agent_session_id",
        "user_isolation_key",
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
        agent_session_id: str | None = None,
        user_isolation_key: str | None = None,
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_name = agent_name
        self.input_data = input_data
        self.protocol = protocol
        self.agent_session_id = agent_session_id
        self.user_isolation_key = user_isolation_key
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
            api_version=self.api_version,
        )

    def execute(self, context: Context) -> JsonValue:
        """Invoke an Azure AI Hosted agent and return the response payload."""
        self.log.info("Invoking Azure AI Hosted agent %s with %s protocol.", self.agent_name, self.protocol)
        if self.protocol == "responses":
            return self.hook.invoke_agent_responses(
                agent_name=self.agent_name,
                input_data=self.input_data,
                user_isolation_key=self.user_isolation_key,
            )
        if self.protocol == "invocations":
            return self.hook.invoke_agent_invocations(
                agent_name=self.agent_name,
                input_data=self.input_data,
                agent_session_id=self.agent_session_id,
                user_isolation_key=self.user_isolation_key,
            )
        raise ValueError("protocol must be either 'responses' or 'invocations'.")


class DeleteAzureAIAgentOperator(BaseOperator):
    """
    Delete an Azure AI Hosted agent or one Hosted agent version.

    :param agent_name: Hosted agent name.
    :param agent_version: Optional Hosted agent version. When omitted, the whole agent is deleted.
        Default is ``None``.
    :param force: Whether to cascade-delete active sessions when deleting the agent or version.
        Default is ``False``.
    :param wait_for_completion: Whether to wait until the resource is deleted. Default is ``True``.
    :param poll_interval: Time in seconds between status checks. Default is ``30.0``.
    :param timeout: Time in seconds to wait for deletion to complete. Default is ``3600``.
    :param azure_ai_agents_conn_id: Azure AI Agents connection id.
        Default is ``azure_ai_agents_default``.
    :param endpoint: Optional Azure AI Foundry project endpoint override. Default is ``None``.
    :param api_version: Foundry Agent Service API version. Default is ``v1``.
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
        self.azure_ai_agents_conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

    @cached_property
    def hook(self) -> AzureAIAgentsHook:
        """Create and return an AzureAIAgentsHook."""
        return AzureAIAgentsHook(
            azure_ai_agents_conn_id=self.azure_ai_agents_conn_id,
            endpoint=self.endpoint,
            api_version=self.api_version,
        )

    def execute(self, context: Context) -> dict[str, JsonValue]:
        """Delete an Azure AI Hosted agent or version and optionally wait for deletion."""
        delete_response: Any
        if self.agent_version is None:
            self.log.info("Deleting Azure AI Hosted agent %s.", self.agent_name)
            delete_response = self.hook.delete_agent(agent_name=self.agent_name, force=self.force)
        else:
            self.log.info(
                "Deleting Azure AI Hosted agent %s version %s.", self.agent_name, self.agent_version
            )
            delete_response = self.hook.delete_agent_version(
                agent_name=self.agent_name, agent_version=self.agent_version, force=self.force
            )

        if self.wait_for_completion:
            self._wait_for_deletion()
        return cast("dict[str, JsonValue]", _serialize_resource(delete_response))

    def _wait_for_deletion(self) -> None:
        end_time = time.monotonic() + self.timeout
        resource_description = f"agent {self.agent_name}"
        if self.agent_version is not None:
            resource_description += f" version {self.agent_version}"
        while True:
            if self.agent_version is None:
                is_deleted = self.hook.is_agent_deleted(agent_name=self.agent_name)
            else:
                is_deleted = self.hook.is_agent_version_deleted(
                    agent_name=self.agent_name, agent_version=self.agent_version
                )
            if is_deleted:
                self.log.info("Azure AI Hosted %s was deleted.", resource_description)
                return
            if time.monotonic() >= end_time:
                raise TimeoutError(f"Timeout waiting for Azure AI Hosted {resource_description} deletion.")

            self.log.info(
                "Azure AI Hosted %s is still retrievable. Sleeping for %s seconds.",
                resource_description,
                self.poll_interval,
            )
            time.sleep(self.poll_interval)
