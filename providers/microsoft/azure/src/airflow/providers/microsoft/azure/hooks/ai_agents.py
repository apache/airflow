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

from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

from azure.ai.projects import AIProjectClient
from azure.ai.projects.aio import AIProjectClient as AsyncAIProjectClient
from azure.core.exceptions import ResourceNotFoundError
from azure.core.rest import HttpRequest
from azure.identity import ClientSecretCredential
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure._ai_agents import (
    VERSION_DELETED_STATUS,
    _get_version_status,
    _serialize_resource,
)
from airflow.providers.microsoft.azure.hooks.base_azure import _AZURE_CLOUD_ENVIRONMENTS
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_async_default_azure_credential,
    get_field,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.ai.projects.models import (
        AgentBlueprintReference,
        AgentDefinition,
        AgentDetails,
        AgentVersionDetails,
        DeleteAgentResponse,
        DeleteAgentVersionResponse,
    )
    from azure.core.credentials import TokenCredential
    from azure.core.credentials_async import AsyncTokenCredential
    from pydantic import JsonValue

    from airflow.sdk import Connection


DEFAULT_REQUEST_TIMEOUT = 60.0


class AzureAIAgentsHook(BaseHook):
    """
    Hook for Microsoft Foundry Hosted agents, backed by the ``azure-ai-projects`` SDK.

    Wraps the Agents operations of :class:`azure.ai.projects.AIProjectClient`:
    https://learn.microsoft.com/en-us/azure/foundry/agents/how-to/deploy-hosted-agent

    :param azure_ai_agents_conn_id: The Azure AI Agents connection id.
        Default is ``azure_ai_agents_default``.
    :param endpoint: Optional Azure AI Foundry project endpoint. If not provided, the hook uses the
        connection host or the ``endpoint`` connection extra. Default is ``None``.
    :param api_version: Foundry Agent Service API version. Default is ``v1``.
    :param timeout: Optional connection/read timeout for service requests, in seconds.
        Default is ``60.0``.
    """

    conn_name_attr = "azure_ai_agents_conn_id"
    default_conn_name = "azure_ai_agents_default"
    conn_type = "azure_ai_agents"
    hook_name = "Azure AI Foundry Hosted Agents"

    def __init__(
        self,
        azure_ai_agents_conn_id: str = default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        timeout: float | None = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        super().__init__()
        self.conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version
        self.timeout = timeout
        self._sync_client: AIProjectClient | None = None

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Azure Tenant ID"), widget=BS3TextFieldWidget()),
            "cloud_environment": StringField(
                lazy_gettext("Azure Cloud Environment"), widget=BS3TextFieldWidget()
            ),
            "endpoint": StringField(lazy_gettext("Project Endpoint"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Project Endpoint",
                "login": "Azure Client ID",
                "password": "Azure Secret",
            },
            "placeholders": {
                "host": "https://<aiservices-id>.services.ai.azure.com/api/projects/<project-name>",
                "login": "client_id (token credentials auth)",
                "password": "secret (token credentials auth)",
                "tenantId": "tenantId (token credentials auth)",
                "cloud_environment": "AzurePublicCloud (default) | AzureUSGovernment | AzureChinaCloud",
                "endpoint": "Overrides Project Endpoint from host",
            },
        }

    def get_conn(self) -> AIProjectClient:
        """Return the Azure AI Foundry project client."""
        if self._sync_client is None:
            conn = self.get_connection(self.conn_id)
            # Hosted agents are a Foundry preview feature; allow_preview makes the SDK send
            # the required Foundry-Features request header.
            self._sync_client = AIProjectClient(
                endpoint=self._get_endpoint(conn),
                credential=self._get_credential(conn),
                api_version=self.api_version,
                allow_preview=True,
                connection_timeout=self.timeout,
                read_timeout=self.timeout,
            )
        return self._sync_client

    def _get_endpoint(self, conn: Connection) -> str:
        endpoint = self.endpoint or conn.host or self._get_field(conn.extra_dejson, "endpoint")
        if not endpoint:
            raise ValueError(
                "Azure AI Foundry project endpoint must be provided by the hook, connection host, "
                "or connection extra."
            )
        return endpoint.rstrip("/")

    def _get_authority(self, extras: dict[str, Any]) -> str:
        cloud_env_name = self._get_field(extras, "cloud_environment") or "AzurePublicCloud"
        cloud_env = _AZURE_CLOUD_ENVIRONMENTS.get(
            cloud_env_name, _AZURE_CLOUD_ENVIRONMENTS["AzurePublicCloud"]
        )
        return cloud_env["authority"]

    @staticmethod
    def _should_use_client_secret_credential(conn: Connection, tenant: str | None) -> bool:
        credential_fields = (conn.login, conn.password, tenant)
        if any(credential_fields) and not all(credential_fields):
            raise ValueError(
                "Azure Client ID, Azure Secret, and Azure Tenant ID must all be provided when "
                "authenticating with a service principal."
            )
        return all(credential_fields)

    def _get_credential(self, conn: Connection) -> TokenCredential:
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        if self._should_use_client_secret_credential(conn, tenant):
            self.log.info("Getting connection using specific credentials.")
            return ClientSecretCredential(
                client_id=cast("str", conn.login),
                client_secret=cast("str", conn.password),
                tenant_id=cast("str", tenant),
                authority=self._get_authority(extras),
            )

        self.log.info("Using DefaultAzureCredential as credential.")
        return get_sync_default_azure_credential(
            managed_identity_client_id=self._get_field(extras, "managed_identity_client_id"),
            workload_identity_tenant_id=self._get_field(extras, "workload_identity_tenant_id"),
        )

    def _get_field(self, extras: dict[str, Any], field_name: str) -> str | None:
        return cast(
            "str | None",
            get_field(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                extras=extras,
                field_name=field_name,
            ),
        )

    def create_agent_version(
        self,
        agent_name: str,
        definition: dict[str, Any] | AgentDefinition,
        *,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        blueprint_reference: AgentBlueprintReference | None = None,
    ) -> AgentVersionDetails:
        """
        Create a Hosted agent version, creating the Hosted agent itself on first use.

        :param agent_name: Hosted agent name.
        :param definition: Hosted agent container definition.
        :param metadata: Optional metadata attached to the Hosted agent. Default is ``None``.
        :param description: Optional human-readable Hosted agent description. Default is ``None``.
        :param blueprint_reference: Optional managed identity blueprint reference. Default is ``None``.
        :return: Created Hosted agent version.
        """
        return self.get_conn().agents.create_version(
            agent_name=agent_name,
            definition=cast("AgentDefinition", definition),
            metadata=metadata,
            description=description,
            blueprint_reference=blueprint_reference,
        )

    def get_agent_version(self, agent_name: str, agent_version: str) -> AgentVersionDetails:
        """
        Get a Hosted agent version.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :return: Hosted agent version details.
        """
        return self.get_conn().agents.get_version(agent_name=agent_name, agent_version=agent_version)

    def get_agent(self, agent_name: str) -> AgentDetails:
        """
        Get a Hosted agent.

        :param agent_name: Hosted agent name.
        :return: Hosted agent details.
        """
        return self.get_conn().agents.get(agent_name=agent_name)

    def delete_agent(self, agent_name: str, *, force: bool = False) -> DeleteAgentResponse:
        """
        Delete a Hosted agent and all versions.

        :param agent_name: Hosted agent name.
        :param force: Whether to force deletion when the Hosted agent has active sessions.
            Default is ``False``.
        :return: Deletion response.
        """
        return self.get_conn().agents.delete(agent_name=agent_name, force=force)

    def delete_agent_version(
        self, agent_name: str, agent_version: str, *, force: bool = False
    ) -> DeleteAgentVersionResponse:
        """
        Delete one Hosted agent version.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :param force: Whether to force deletion when the Hosted agent version has active sessions.
            Default is ``False``.
        :return: Deletion response.
        """
        return self.get_conn().agents.delete_version(
            agent_name=agent_name, agent_version=agent_version, force=force
        )

    def is_agent_version_deleted(self, agent_name: str, agent_version: str) -> bool:
        """
        Return True if the Hosted agent version no longer exists or is deleted.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :return: ``True`` when the Hosted agent version no longer exists or has deleted status.
        """
        try:
            version = self.get_agent_version(agent_name=agent_name, agent_version=agent_version)
        except ResourceNotFoundError:
            return True
        return _get_version_status(version) == VERSION_DELETED_STATUS

    def is_agent_deleted(self, agent_name: str) -> bool:
        """
        Return True if the Hosted agent no longer exists.

        :param agent_name: Hosted agent name.
        :return: ``True`` when the Hosted agent no longer exists.
        """
        try:
            self.get_agent(agent_name=agent_name)
        except ResourceNotFoundError:
            return True
        return False

    def invoke_agent_responses(
        self,
        agent_name: str,
        input_data: dict[str, Any],
        *,
        user_isolation_key: str | None = None,
    ) -> dict[str, JsonValue]:
        """
        Invoke a Hosted agent through the OpenAI Responses protocol.

        :param agent_name: Hosted agent name.
        :param input_data: Request payload for the Responses protocol.
        :param user_isolation_key: Optional user isolation key for endpoint resources. Default is ``None``.
        :return: JSON-compatible Responses protocol response.
        """
        request_kwargs: dict[str, Any] = {}
        if user_isolation_key:
            request_kwargs["extra_headers"] = {"x-ms-user-isolation-key": user_isolation_key}
        with self.get_conn().get_openai_client(agent_name=agent_name) as openai_client:
            response = openai_client.responses.create(**input_data, **request_kwargs)
        return cast("dict[str, JsonValue]", response.model_dump(mode="json"))

    def invoke_agent_invocations(
        self,
        agent_name: str,
        input_data: dict[str, Any],
        *,
        agent_session_id: str | None = None,
        user_isolation_key: str | None = None,
    ) -> JsonValue:
        """
        Invoke a Hosted agent through the Invocations protocol.

        The SDK does not expose the Invocations protocol yet, so the request is sent through
        the project client pipeline, which handles authentication and preview feature headers.

        :param agent_name: Hosted agent name.
        :param input_data: Request payload for the Invocations protocol.
        :param agent_session_id: Optional Hosted agent session id. Default is ``None``.
        :param user_isolation_key: Optional user isolation key for endpoint resources. Default is ``None``.
        :return: JSON-compatible Invocations protocol payload; its shape is defined by the Hosted agent
            container.
        """
        params = {"api-version": self.api_version}
        if agent_session_id:
            params["agent_session_id"] = agent_session_id
        headers = {"x-ms-user-isolation-key": user_isolation_key} if user_isolation_key else None
        request = HttpRequest(
            "POST",
            f"/agents/{quote(agent_name, safe='')}/endpoint/protocols/invocations",
            params=params,
            headers=headers,
            json=input_data,
        )
        response = self.get_conn().send_request(request)
        response.raise_for_status()
        if not response.content:
            return None
        return _serialize_resource(response.json())


class AzureAIAgentsAsyncHook(AzureAIAgentsHook):
    """
    Async hook for Microsoft Foundry Hosted agents.

    :param azure_ai_agents_conn_id: The Azure AI Agents connection id.
        Default is ``azure_ai_agents_default``.
    :param endpoint: Optional Azure AI Foundry project endpoint override. Default is ``None``.
    :param api_version: Foundry Agent Service API version. Default is ``v1``.
    :param timeout: Optional connection/read timeout for service requests, in seconds.
        Default is ``60.0``.
    """

    def __init__(
        self,
        azure_ai_agents_conn_id: str = AzureAIAgentsHook.default_conn_name,
        endpoint: str | None = None,
        api_version: str = "v1",
        timeout: float | None = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self._async_client: AsyncAIProjectClient | None = None
        self._async_credential: AsyncTokenCredential | None = None
        super().__init__(
            azure_ai_agents_conn_id=azure_ai_agents_conn_id,
            endpoint=endpoint,
            api_version=api_version,
            timeout=timeout,
        )

    async def close(self) -> None:
        """Close the async Azure AI Foundry project client and credential."""
        try:
            if self._async_client is not None:
                await self._async_client.close()
        finally:
            self._async_client = None
            if self._async_credential is not None:
                try:
                    await self._async_credential.close()
                finally:
                    self._async_credential = None

    async def get_async_conn(self) -> AsyncAIProjectClient:
        """Return the async Azure AI Foundry project client."""
        if self._async_client is not None:
            return self._async_client

        conn = await get_async_connection(self.conn_id)
        self._async_credential = self._get_async_credential(conn)
        self._async_client = AsyncAIProjectClient(
            endpoint=self._get_endpoint(conn),
            credential=self._async_credential,
            api_version=self.api_version,
            allow_preview=True,
            connection_timeout=self.timeout,
            read_timeout=self.timeout,
        )
        return self._async_client

    def _get_async_credential(self, conn: Connection) -> AsyncTokenCredential:
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        if self._should_use_client_secret_credential(conn, tenant):
            self.log.info("Getting connection using specific credentials.")
            return AsyncClientSecretCredential(
                client_id=cast("str", conn.login),
                client_secret=cast("str", conn.password),
                tenant_id=cast("str", tenant),
                authority=self._get_authority(extras),
            )

        self.log.info("Using DefaultAzureCredential as credential.")
        return get_async_default_azure_credential(
            managed_identity_client_id=self._get_field(extras, "managed_identity_client_id"),
            workload_identity_tenant_id=self._get_field(extras, "workload_identity_tenant_id"),
        )

    async def async_get_agent_version(self, agent_name: str, agent_version: str) -> AgentVersionDetails:
        """
        Get a Hosted agent version asynchronously.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :return: Hosted agent version details.
        """
        client = await self.get_async_conn()
        return await client.agents.get_version(agent_name=agent_name, agent_version=agent_version)
