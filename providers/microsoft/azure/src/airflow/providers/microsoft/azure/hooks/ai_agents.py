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

from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from azure.ai.agents import AgentsClient
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.hooks.base_azure import _AZURE_CLOUD_ENVIRONMENTS
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    from azure.identity import DefaultAzureCredential

    from airflow.sdk import Connection


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

    def _get_endpoint(self, conn: Connection) -> str:
        endpoint = self.endpoint or conn.host or self._get_field(conn.extra_dejson, "endpoint")
        if not endpoint:
            raise ValueError(
                "Azure AI Foundry project endpoint must be provided by the hook, connection host, "
                "or connection extra."
            )
        return endpoint

    def _get_credential(self, conn: Connection) -> TokenCredential:
        tenant = self._get_field(conn.extra_dejson, "tenantId")
        cloud_env_name = self._get_field(conn.extra_dejson, "cloud_environment") or "AzurePublicCloud"
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
        managed_identity_client_id = self._get_field(conn.extra_dejson, "managed_identity_client_id")
        workload_identity_tenant_id = self._get_field(conn.extra_dejson, "workload_identity_tenant_id")
        return cast(
            "DefaultAzureCredential",
            get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            ),
        )

    def _get_field(self, extras: dict[str, Any], field_name: str) -> Any:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )

    def create_agent(self, **config) -> Any:
        """Create an agent."""
        return self.conn.create_agent(**prune_dict(config))

    def update_agent(self, agent_id: str, **config) -> Any:
        """Update an agent."""
        return self.conn.update_agent(agent_id=agent_id, **prune_dict(config))

    def run_agent(self, agent_id: str, **config) -> Any:
        """Create a thread and run an agent."""
        return self.conn.create_thread_and_run(agent_id=agent_id, **prune_dict(config))

    def get_run(self, thread_id: str, run_id: str) -> Any:
        """Get an agent run."""
        return self.conn.runs.get(thread_id=thread_id, run_id=run_id)

    def delete_agent(self, agent_id: str) -> None:
        """Delete an agent."""
        self.conn.delete_agent(agent_id=agent_id)

    def get_agent(self, agent_id: str) -> Any:
        """Get an agent."""
        return self.conn.get_agent(agent_id=agent_id)

    def is_agent_deleted(self, agent_id: str) -> bool:
        """Return True if the agent no longer exists."""
        try:
            self.get_agent(agent_id=agent_id)
        except ResourceNotFoundError:
            return True
        return False
