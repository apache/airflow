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
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from requests import Session

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.hooks.base_azure import _AZURE_CLOUD_ENVIRONMENTS
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from requests import Response

    from airflow.sdk import Connection


HOSTED_AGENT_FEATURE_HEADER = "HostedAgents=V1Preview"
TOKEN_SCOPE = "https://ai.azure.com/.default"
VERSION_INTERMEDIATE_STATUSES = {"creating", "deleting"}
VERSION_SUCCESS_STATUSES = {"active"}
VERSION_FAILURE_STATUSES = {"failed"}


def serialize_resource(resource: Any) -> Any:
    """Serialize an SDK or HTTP response object into XCom-safe primitives."""
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


def get_version_status(version: Any) -> str:
    """Return a normalized Hosted agent version status string."""
    status = get_resource_attr(version, "status")
    if hasattr(status, "value"):
        status = status.value
    if status is None:
        raise ValueError("Azure AI Hosted agent version did not include a status.")
    return str(status).lower()


def get_agent_version(version: Any) -> str:
    """Return the version identifier from a Hosted agent version payload."""
    agent_version = get_resource_attr(version, "version") or get_resource_attr(version, "agent_version")
    if agent_version is None:
        raise ValueError("Azure AI Hosted agent response did not include a version.")
    return str(agent_version)


class AzureAIAgentsHook(BaseHook):
    """
    Hook for Microsoft Foundry Hosted agents.

    :param azure_ai_agents_conn_id: The Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint. If not provided, the hook uses the
        connection host or the ``endpoint`` connection extra.
    :param api_version: Foundry Agent Service API version.
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
    ) -> None:
        super().__init__()
        self.conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version

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

    @cached_property
    def session(self) -> Session:
        """Return a cached requests session."""
        return Session()

    def _get_endpoint(self, conn: Connection, extras: dict[str, Any] | None = None) -> str:
        connection_extras = extras if extras is not None else conn.extra_dejson
        endpoint = self.endpoint or conn.host or self._get_field(connection_extras, "endpoint")
        if not endpoint:
            raise ValueError(
                "Azure AI Foundry project endpoint must be provided by the hook, connection host, "
                "or connection extra."
            )
        return endpoint.rstrip("/")

    def _get_credential(self, conn: Connection) -> TokenCredential:
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")
        cloud_env_name = self._get_field(extras, "cloud_environment") or "AzurePublicCloud"
        cloud_env = _AZURE_CLOUD_ENVIRONMENTS.get(
            cloud_env_name, _AZURE_CLOUD_ENVIRONMENTS["AzurePublicCloud"]
        )

        if all([conn.login, conn.password, tenant]):
            self.log.info("Getting connection using specific credentials.")
            return ClientSecretCredential(
                client_id=cast("str", conn.login),
                client_secret=cast("str", conn.password),
                tenant_id=cast("str", tenant),
                authority=cloud_env["authority"],
            )

        self.log.info("Using DefaultAzureCredential as credential.")
        managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
        workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
        return get_sync_default_azure_credential(
            managed_identity_client_id=managed_identity_client_id,
            workload_identity_tenant_id=workload_identity_tenant_id,
        )

    def _get_field(self, extras: dict[str, Any], field_name: str) -> Any:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Any:
        conn = self.get_connection(self.conn_id)
        credential = self._get_credential(conn)
        token = credential.get_token(TOKEN_SCOPE).token
        url = f"{self._get_endpoint(conn)}/{path.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Foundry-Features": HOSTED_AGENT_FEATURE_HEADER,
        }
        if extra_headers:
            headers.update(extra_headers)

        response = self.session.request(
            method=method,
            url=url,
            params={"api-version": self.api_version},
            headers=headers,
            json=json_payload,
        )
        return self._process_response(response)

    def _process_response(self, response: Response) -> Any:
        if response.status_code == 404:
            raise ResourceNotFoundError("Azure AI Hosted agent resource was not found.")
        response.raise_for_status()
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    @staticmethod
    def _quote_resource_id(resource_id: str) -> str:
        return quote(resource_id, safe="")

    def create_agent(self, agent_name: str, definition: dict[str, Any]) -> dict[str, Any]:
        """Create a Hosted agent and its first version."""
        return self._request(
            "POST",
            "agents",
            json_payload={"name": agent_name, "definition": definition},
        )

    def create_agent_version(self, agent_name: str, definition: dict[str, Any]) -> dict[str, Any]:
        """Create a new Hosted agent version."""
        return self._request(
            "POST",
            f"agents/{self._quote_resource_id(agent_name)}/versions",
            json_payload={"definition": definition},
        )

    def get_agent_version(self, agent_name: str, agent_version: str) -> dict[str, Any]:
        """Get a Hosted agent version."""
        return self._request(
            "GET",
            f"agents/{self._quote_resource_id(agent_name)}/versions/{self._quote_resource_id(agent_version)}",
        )

    def delete_agent(self, agent_name: str) -> None:
        """Delete a Hosted agent and all versions."""
        self._request("DELETE", f"agents/{self._quote_resource_id(agent_name)}")

    def delete_agent_version(self, agent_name: str, agent_version: str) -> None:
        """Delete one Hosted agent version."""
        self._request(
            "DELETE",
            f"agents/{self._quote_resource_id(agent_name)}/versions/{self._quote_resource_id(agent_version)}",
        )

    def is_agent_version_deleted(self, agent_name: str, agent_version: str) -> bool:
        """Return True if the Hosted agent version no longer exists or is deleted."""
        try:
            version = self.get_agent_version(agent_name=agent_name, agent_version=agent_version)
        except ResourceNotFoundError:
            return True
        return get_version_status(version) == "deleted"

    def is_agent_deleted(self, agent_name: str) -> bool:
        """Return True if version 1 is no longer retrievable for the Hosted agent."""
        return self.is_agent_version_deleted(agent_name=agent_name, agent_version="1")

    def invoke_agent_responses(self, agent_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
        """Invoke a Hosted agent through the OpenAI Responses protocol."""
        return self._request(
            "POST",
            f"agents/{self._quote_resource_id(agent_name)}/endpoint/protocols/openai/responses",
            json_payload=input_data,
        )

    def invoke_agent_invocations(self, agent_name: str, input_data: dict[str, Any]) -> dict[str, Any]:
        """Invoke a Hosted agent through the Invocations protocol."""
        return self._request(
            "POST",
            f"agents/{self._quote_resource_id(agent_name)}/endpoint/protocols/invocations",
            json_payload=input_data,
        )


class AzureAIAgentsAsyncHook(AzureAIAgentsHook):
    """Async hook for Microsoft Foundry Hosted agents."""

    async def async_get_agent_version(self, agent_name: str, agent_version: str) -> dict[str, Any]:
        """Get a Hosted agent version asynchronously."""
        return await asyncio.to_thread(
            self.get_agent_version,
            agent_name=agent_name,
            agent_version=agent_version,
        )

    async def async_is_agent_deleted(self, agent_name: str, agent_version: str | None = None) -> bool:
        """Return True if the Hosted agent or version is deleted."""
        if agent_version is None:
            return await asyncio.to_thread(self.is_agent_deleted, agent_name=agent_name)
        return await asyncio.to_thread(
            self.is_agent_version_deleted,
            agent_name=agent_name,
            agent_version=agent_version,
        )
