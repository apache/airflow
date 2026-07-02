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
import json
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import quote

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from pydantic import JsonValue
from requests import Session
from requests.exceptions import HTTPError

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
DEFAULT_REQUEST_TIMEOUT = 60.0
VERSION_INTERMEDIATE_STATUSES = {"creating", "deleting"}
VERSION_SUCCESS_STATUSES = {"active"}
VERSION_FAILURE_STATUSES = {"failed"}
VERSION_DELETED_STATUS = "deleted"

JsonDict = dict[str, JsonValue]


def _serialize_resource(resource: Any) -> JsonValue:
    """Serialize an SDK or HTTP response object into XCom-safe primitives."""
    if resource is None or isinstance(resource, str | int | float | bool):
        return resource
    if isinstance(resource, list | tuple):
        return [_serialize_resource(item) for item in resource]
    if isinstance(resource, dict):
        return {key: _serialize_resource(value) for key, value in resource.items()}
    if hasattr(resource, "as_dict"):
        return _serialize_resource(resource.as_dict())
    if hasattr(resource, "model_dump"):
        return _serialize_resource(resource.model_dump())
    return cast("JsonValue", resource)


def _get_resource_attr(resource: Any, attr: str) -> JsonValue:
    """Get an attribute from an SDK resource or mapping."""
    if isinstance(resource, dict):
        return cast("JsonValue", resource.get(attr))
    return cast("JsonValue", getattr(resource, attr, None))


def _get_version_status(version: Any) -> str:
    """Return a normalized Hosted agent version status string."""
    status = _get_resource_attr(version, "status")
    if hasattr(status, "value"):
        status = cast("Any", status).value
    if status is None:
        raise ValueError("Azure AI Hosted agent version did not include a status.")
    return str(status).lower()


def _get_agent_version(version: Any) -> str:
    """
    Return the version identifier from a Hosted agent version or agent payload.

    Accepts both a ``agent.version`` object (returned by POST /agents/{name}/versions and
    GET /agents/{name}/versions/{version}) and a top-level ``agent`` object (returned by
    POST /agents), extracting the version from ``versions.latest.version`` in the latter case.
    """
    agent_version = _get_resource_attr(version, "version") or _get_resource_attr(version, "agent_version")
    if agent_version is None:
        # POST /agents returns an agent object; the initial version lives under versions.latest
        versions = _get_resource_attr(version, "versions")
        latest = _get_resource_attr(versions, "latest") if versions is not None else None
        agent_version = _get_resource_attr(latest, "version") if latest is not None else None
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
    :param timeout: Optional timeout for HTTP requests, in seconds.
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
        timeout: float | tuple[float, float] | None = DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        super().__init__()
        self.conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint
        self.api_version = api_version
        self.timeout = timeout

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

    @cached_property
    def _connection(self) -> Connection:
        return self.get_connection(self.conn_id)

    @cached_property
    def _credential(self) -> TokenCredential:
        return self._get_credential(self._connection)

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

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_payload: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
        query_params: dict[str, str] | None = None,
        include_api_version: bool = True,
        timeout: float | tuple[float, float] | None = None,
    ) -> JsonValue:
        token = self._credential.get_token(TOKEN_SCOPE).token
        url = f"{self._get_endpoint(self._connection)}/{path.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Foundry-Features": HOSTED_AGENT_FEATURE_HEADER,
        }
        if extra_headers:
            headers.update(extra_headers)

        params = {"api-version": self.api_version} if include_api_version else {}
        if query_params:
            params.update(query_params)

        response = self.session.request(
            method=method,
            url=url,
            params=params or None,
            headers=headers,
            json=json_payload,
            timeout=timeout if timeout is not None else self.timeout,
        )
        return self._process_response(response)

    def _process_response(self, response: Response) -> JsonValue:
        if response.status_code == 404:
            raise ResourceNotFoundError("Azure AI Hosted agent resource was not found.")
        try:
            response.raise_for_status()
        except HTTPError as err:
            error_body = self._get_response_error_body(response)
            if error_body:
                raise HTTPError(f"{err}; response body: {error_body}", response=response) from err
            raise
        if response.status_code == 204 or not response.content:
            return None
        headers = getattr(response, "headers", None) or {}
        content_type = str(headers.get("Content-Type", "")) if hasattr(headers, "get") else ""
        if "json" in content_type:
            return cast("JsonValue", response.json())
        try:
            return cast("JsonValue", response.json())
        except ValueError:
            return response.text

    @staticmethod
    def _get_response_error_body(response: Response) -> str | None:
        response_text = getattr(response, "text", None)
        if response_text:
            return response_text
        try:
            return json.dumps(response.json())
        except ValueError:
            return None

    @staticmethod
    def _quote_resource_id(resource_id: str) -> str:
        return quote(resource_id, safe="")

    @staticmethod
    def _build_agent_payload(
        *,
        definition: dict[str, Any],
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        blueprint_reference: dict[str, Any] | None = None,
        agent_endpoint: dict[str, Any] | None = None,
        agent_card: dict[str, Any] | None = None,
    ) -> JsonDict:
        payload: dict[str, Any] = {"definition": definition}
        optional_values = {
            "metadata": metadata,
            "description": description,
            "blueprint_reference": blueprint_reference,
            "agent_endpoint": agent_endpoint,
            "agent_card": agent_card,
        }
        payload.update({key: value for key, value in optional_values.items() if value is not None})
        return payload

    def create_agent(
        self,
        agent_name: str,
        definition: dict[str, Any],
        *,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        blueprint_reference: dict[str, Any] | None = None,
        agent_endpoint: dict[str, Any] | None = None,
        agent_card: dict[str, Any] | None = None,
    ) -> JsonDict:
        """
        Create a Hosted agent and its first version.

        :param agent_name: Hosted agent name.
        :param definition: Hosted agent container definition.
        :param metadata: Optional metadata attached to the Hosted agent.
        :param description: Optional human-readable Hosted agent description.
        :param blueprint_reference: Optional managed identity blueprint reference.
        :param agent_endpoint: Optional Hosted agent endpoint configuration.
        :param agent_card: Optional agent card for the Hosted agent.
        :return: Created Hosted agent payload.
        """
        payload = self._build_agent_payload(
            definition=definition,
            metadata=metadata,
            description=description,
            blueprint_reference=blueprint_reference,
            agent_endpoint=agent_endpoint,
            agent_card=agent_card,
        )
        return cast(
            "JsonDict",
            self._request(
                "POST",
                "agents",
                json_payload={"name": agent_name, **payload},
            ),
        )

    def update_agent(
        self,
        agent_name: str,
        definition: dict[str, Any],
        *,
        metadata: dict[str, str] | None = None,
        description: str | None = None,
        blueprint_reference: dict[str, Any] | None = None,
    ) -> JsonDict:
        """
        Update a Hosted agent, creating a new version when the definition changes.

        :param agent_name: Hosted agent name.
        :param definition: Updated Hosted agent container definition.
        :param metadata: Optional metadata attached to the Hosted agent.
        :param description: Optional human-readable Hosted agent description.
        :param blueprint_reference: Optional managed identity blueprint reference.
        :return: Updated Hosted agent payload.
        """
        return cast(
            "JsonDict",
            self._request(
                "POST",
                f"agents/{self._quote_resource_id(agent_name)}",
                json_payload=self._build_agent_payload(
                    definition=definition,
                    metadata=metadata,
                    description=description,
                    blueprint_reference=blueprint_reference,
                ),
            ),
        )

    def get_agent_version(self, agent_name: str, agent_version: str) -> JsonDict:
        """
        Get a Hosted agent version.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :return: Hosted agent version payload.
        """
        return cast(
            "JsonDict",
            self._request(
                "GET",
                f"agents/{self._quote_resource_id(agent_name)}/versions/{self._quote_resource_id(agent_version)}",
            ),
        )

    def get_agent(self, agent_name: str) -> JsonDict:
        """
        Get a Hosted agent.

        :param agent_name: Hosted agent name.
        :return: Hosted agent payload.
        """
        return cast("JsonDict", self._request("GET", f"agents/{self._quote_resource_id(agent_name)}"))

    def delete_agent(self, agent_name: str, *, force: bool = False) -> JsonDict | None:
        """
        Delete a Hosted agent and all versions.

        :param agent_name: Hosted agent name.
        :param force: Whether to force deletion of child resources.
        :return: Deletion response payload, or ``None`` when the service returns no content.
        """
        query_params = {"force": "true"} if force else None
        return cast(
            "JsonDict | None",
            self._request(
                "DELETE", f"agents/{self._quote_resource_id(agent_name)}", query_params=query_params
            ),
        )

    def delete_agent_version(self, agent_name: str, agent_version: str) -> None:
        """
        Delete one Hosted agent version.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        """
        self._request(
            "DELETE",
            f"agents/{self._quote_resource_id(agent_name)}/versions/{self._quote_resource_id(agent_version)}",
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
        :return: ``True`` when the Hosted agent no longer exists or has deleted status.
        """
        try:
            agent = self.get_agent(agent_name=agent_name)
        except ResourceNotFoundError:
            return True
        return str(_get_resource_attr(agent, "status") or "").lower() == VERSION_DELETED_STATUS

    def invoke_agent_responses(
        self,
        agent_name: str,
        input_data: dict[str, Any],
        *,
        agent_version: str | None = None,
        user_isolation_key: str | None = None,
    ) -> JsonDict:
        """
        Invoke a Hosted agent through the OpenAI Responses protocol.

        :param agent_name: Hosted agent name.
        :param input_data: Request payload for the Responses protocol.
        :param agent_version: Optional Hosted agent version to invoke.
        :param user_isolation_key: Optional user isolation key for endpoint resources.
        :return: Responses protocol payload.
        """
        agent_reference = {"type": "agent_reference", "name": agent_name}
        if agent_version is not None:
            agent_reference["version"] = agent_version
        headers = {"x-ms-user-isolation-key": user_isolation_key} if user_isolation_key else None
        return cast(
            "JsonDict",
            self._request(
                "POST",
                "openai/v1/responses",
                json_payload={**input_data, "agent_reference": agent_reference},
                extra_headers=headers,
                include_api_version=False,
            ),
        )

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

        :param agent_name: Hosted agent name.
        :param input_data: Request payload for the Invocations protocol.
        :param agent_session_id: Optional Hosted agent session id.
        :param user_isolation_key: Optional user isolation key for endpoint resources.
        :return: Invocations protocol payload.
        """
        query_params = {"agent_session_id": agent_session_id} if agent_session_id else None
        headers = {"x-ms-user-isolation-key": user_isolation_key} if user_isolation_key else None
        return self._request(
            "POST",
            f"agents/{self._quote_resource_id(agent_name)}/endpoint/protocols/invocations",
            json_payload=input_data,
            extra_headers=headers,
            query_params=query_params,
        )


class AzureAIAgentsAsyncHook(AzureAIAgentsHook):
    """Async hook for Microsoft Foundry Hosted agents."""

    async def async_get_agent_version(self, agent_name: str, agent_version: str) -> JsonDict:
        """
        Get a Hosted agent version asynchronously.

        :param agent_name: Hosted agent name.
        :param agent_version: Hosted agent version.
        :return: Hosted agent version payload.
        """
        return await asyncio.to_thread(
            self.get_agent_version,
            agent_name=agent_name,
            agent_version=agent_version,
        )

    async def async_is_agent_deleted(self, agent_name: str, agent_version: str | None = None) -> bool:
        """
        Return True if the Hosted agent or version is deleted.

        :param agent_name: Hosted agent name.
        :param agent_version: Optional Hosted agent version.
        :return: ``True`` when the Hosted agent or Hosted agent version is deleted.
        """
        if agent_version is None:
            return await asyncio.to_thread(self.is_agent_deleted, agent_name=agent_name)
        return await asyncio.to_thread(
            self.is_agent_version_deleted,
            agent_name=agent_name,
            agent_version=agent_version,
        )
