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

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import cached_property
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, cast

from azure.ai.agents import AgentsClient
from azure.ai.agents.aio import AgentsClient as AsyncAgentsClient
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.identity.aio import (
    ClientSecretCredential as AsyncClientSecretCredential,
)

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.hooks.base_azure import _AZURE_CLOUD_ENVIRONMENTS
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_async_default_azure_credential,
    get_field,
    get_sync_default_azure_credential,
)
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from azure.ai.agents.models import Agent, ThreadRun
    from azure.core.credentials import TokenCredential
    from azure.core.credentials_async import AsyncTokenCredential

    from airflow.sdk import Connection


RUN_INTERMEDIATE_STATUSES = {"queued", "in_progress", "cancelling"}
RUN_FAILURE_STATUSES = {"failed", "cancelled", "expired", "requires_action"}
RUN_SUCCESS_STATUSES = {"completed"}


def build_run_payload(run_id: str, thread_id: str) -> dict[str, str]:
    """Build the fallback run payload used when the SDK run object is unavailable."""
    return {"id": run_id, "thread_id": thread_id}


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


def get_incomplete_details(run: Any) -> Any:
    """Return run incomplete details when the service reports a truncated run."""
    return get_resource_attr(run, "incomplete_details")


def build_run_failure_message(run_id: str, status: str) -> str:
    """Build the failure message for a run terminal or actionable status."""
    if status == "requires_action":
        return (
            f"Azure AI Agent run {run_id} requires tool outputs, but "
            "RunAzureAIAgentOperator does not support tool-output submission."
        )
    return f"Azure AI Agent run {run_id} finished with status {status}."


def build_incomplete_run_message(run_id: str, incomplete_details: Any) -> str:
    """Build the failure message for a completed run with incomplete output."""
    reason = get_resource_attr(incomplete_details, "reason") or incomplete_details
    return f"Azure AI Agent run {run_id} completed with incomplete output: {reason}."


class AzureAIAgentsHook(BaseHook):
    """
    Hook for Microsoft Foundry Agents Service.

    :param azure_ai_agents_conn_id: The Azure AI Agents connection id.
    :param endpoint: Optional Azure AI Foundry project endpoint. If not provided, the hook uses the
        connection host or the ``endpoint`` connection extra.
    """

    conn_name_attr = "azure_ai_agents_conn_id"
    default_conn_name = "azure_ai_agents_default"
    conn_type = "azure_ai_agents"
    hook_name = "Azure AI Foundry Agents"

    def __init__(
        self,
        azure_ai_agents_conn_id: str = default_conn_name,
        endpoint: str | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = azure_ai_agents_conn_id
        self.endpoint = endpoint

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
    def conn(self) -> AgentsClient:
        """Return a cached Azure AI Agents client."""
        return self.get_conn()

    def get_conn(self) -> AgentsClient:
        """Create an Azure AI Agents client."""
        conn = self.get_connection(self.conn_id)
        return AgentsClient(endpoint=self._get_endpoint(conn), credential=self._get_credential(conn))

    def _get_endpoint(self, conn: Connection, extras: dict[str, Any] | None = None) -> str:
        connection_extras = extras if extras is not None else conn.extra_dejson
        endpoint = self.endpoint or conn.host or self._get_field(connection_extras, "endpoint")
        if not endpoint:
            raise ValueError(
                "Azure AI Foundry project endpoint must be provided by the hook, connection host, "
                "or connection extra."
            )
        return endpoint

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

    def create_agent(self, **config: Any) -> Agent:
        """Create an agent."""
        return self.conn.create_agent(**prune_dict(config))

    def update_agent(self, agent_id: str, **config: Any) -> Agent:
        """Update an agent."""
        return self.conn.update_agent(agent_id=agent_id, **prune_dict(config))

    def run_agent(self, agent_id: str, **config: Any) -> ThreadRun:
        """Create a thread and run an agent."""
        return self.conn.create_thread_and_run(agent_id=agent_id, **prune_dict(config))

    def get_run(self, thread_id: str, run_id: str) -> ThreadRun:
        """Get an agent run."""
        return self.conn.runs.get(thread_id=thread_id, run_id=run_id)

    def delete_agent(self, agent_id: str) -> None:
        """Delete an agent."""
        self.conn.delete_agent(agent_id=agent_id)

    def get_agent(self, agent_id: str) -> Agent:
        """Get an agent."""
        return self.conn.get_agent(agent_id=agent_id)

    def is_agent_deleted(self, agent_id: str) -> bool:
        """Return True if the agent no longer exists."""
        try:
            self.get_agent(agent_id=agent_id)
        except ResourceNotFoundError:
            return True
        return False


class AzureAIAgentsAsyncHook(AzureAIAgentsHook):
    """Async hook for Microsoft Foundry Agents Service."""

    @asynccontextmanager
    async def get_async_conn(self) -> AsyncGenerator[AsyncAgentsClient, None]:
        """Create an async Azure AI Agents client."""
        conn = await get_async_connection(self.conn_id)
        extras = self._deserialize_extra(conn)
        credential = self._get_async_credential(conn, extras=extras)
        client = AsyncAgentsClient(endpoint=self._get_endpoint(conn, extras=extras), credential=credential)
        try:
            yield client
        finally:
            await client.close()
            if hasattr(credential, "close"):
                await credential.close()

    def _deserialize_extra(self, conn: Connection) -> dict[str, Any]:
        if not conn.extra:
            return {}
        try:
            return json.loads(conn.extra)
        except (JSONDecodeError, TypeError):
            self.log.exception("Failed parsing the json for conn_id %s", self.conn_id)
            return {}

    def _get_async_credential(self, conn: Connection, extras: dict[str, Any]) -> AsyncTokenCredential:
        tenant = self._get_field(extras, "tenantId")
        cloud_env_name = self._get_field(extras, "cloud_environment") or "AzurePublicCloud"
        cloud_env = _AZURE_CLOUD_ENVIRONMENTS.get(
            cloud_env_name, _AZURE_CLOUD_ENVIRONMENTS["AzurePublicCloud"]
        )

        if all([conn.login, conn.password, tenant]):
            self.log.info("Getting connection using specific credentials.")
            return AsyncClientSecretCredential(
                client_id=cast("str", conn.login),
                client_secret=cast("str", conn.password),
                tenant_id=cast("str", tenant),
                authority=cloud_env["authority"],
            )

        self.log.info("Using DefaultAzureCredential as credential.")
        managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
        workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
        return get_async_default_azure_credential(
            managed_identity_client_id=managed_identity_client_id,
            workload_identity_tenant_id=workload_identity_tenant_id,
        )

    async def async_get_run(self, thread_id: str, run_id: str) -> ThreadRun:
        """Get an agent run asynchronously."""
        async with self.get_async_conn() as client:
            return await client.runs.get(thread_id=thread_id, run_id=run_id)

    async def async_is_agent_deleted(self, agent_id: str) -> bool:
        """Return True if the agent no longer exists."""
        try:
            async with self.get_async_conn() as client:
                await client.get_agent(agent_id=agent_id)
        except ResourceNotFoundError:
            return True
        return False
