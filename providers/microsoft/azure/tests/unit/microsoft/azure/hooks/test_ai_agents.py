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

from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError

pytest.importorskip("azure.ai.agents")

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.ai_agents import (
    AzureAIAgentsAsyncHook,
    AzureAIAgentsHook,
    get_run_status,
)

MODULE = "airflow.providers.microsoft.azure.hooks.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_ID = "agent_123"


class TestAzureAIAgentsHook:
    def test_connection_form_widgets(self):
        pytest.importorskip("flask_appbuilder")
        widgets = AzureAIAgentsHook.get_connection_form_widgets()

        assert "tenantId" in widgets
        assert "cloud_environment" in widgets
        assert "endpoint" in widgets
        assert "managed_identity_client_id" in widgets
        assert "workload_identity_tenant_id" in widgets

    def test_ui_field_behaviour(self):
        behaviour = AzureAIAgentsHook.get_ui_field_behaviour()

        assert behaviour["hidden_fields"] == ["schema", "port"]
        assert behaviour["relabeling"]["host"] == "Project Endpoint"
        assert behaviour["relabeling"]["login"] == "Azure Client ID"
        assert behaviour["relabeling"]["password"] == "Azure Secret"

    @mock.patch(f"{MODULE}.AgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    def test_get_conn_uses_connection_endpoint_and_client_secret_credential(
        self, mock_credential_cls, mock_client_cls, create_mock_connection
    ):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_ai_agents",
                host=ENDPOINT,
                login="client-id",
                password="client-secret",
                extra={"tenantId": "tenant-id", "cloud_environment": "AzureUSGovernment"},
            )
        )
        mock_credential = mock_credential_cls.return_value
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        result = hook.get_conn()

        assert result == mock_client_cls.return_value
        mock_credential_cls.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id",
            authority="login.microsoftonline.us",
        )
        mock_client_cls.assert_called_once_with(endpoint=ENDPOINT, credential=mock_credential)

    @mock.patch(f"{MODULE}.AgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_get_conn_uses_default_credential_and_endpoint_extra(
        self, mock_default_credential, mock_client_cls, create_mock_connection
    ):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_ai_agents",
                extra={
                    "endpoint": ENDPOINT,
                    "managed_identity_client_id": "managed-identity-client-id",
                    "workload_identity_tenant_id": "workload-identity-tenant-id",
                },
            )
        )
        mock_credential = mock_default_credential.return_value
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        result = hook.get_conn()

        assert result == mock_client_cls.return_value
        mock_default_credential.assert_called_once_with(
            managed_identity_client_id="managed-identity-client-id",
            workload_identity_tenant_id="workload-identity-tenant-id",
        )
        mock_client_cls.assert_called_once_with(endpoint=ENDPOINT, credential=mock_credential)

    @mock.patch(f"{MODULE}.AgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_get_conn_hook_endpoint_overrides_connection_endpoint(
        self, mock_default_credential, mock_client_cls, create_mock_connection
    ):
        endpoint_override = "https://override.services.ai.azure.com/api/projects/project"
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT))
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID, endpoint=endpoint_override)

        hook.get_conn()

        mock_default_credential.assert_called_once_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        mock_client_cls.assert_called_once_with(
            endpoint=endpoint_override, credential=mock_default_credential.return_value
        )

    @mock.patch(f"{MODULE}.AgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_get_conn_hook_endpoint_overrides_extra_endpoint(
        self, mock_default_credential, mock_client_cls, create_mock_connection
    ):
        endpoint_override = "https://override.services.ai.azure.com/api/projects/project"
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_ai_agents",
                extra={"endpoint": ENDPOINT},
            )
        )
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID, endpoint=endpoint_override)

        hook.get_conn()

        mock_default_credential.assert_called_once_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        mock_client_cls.assert_called_once_with(
            endpoint=endpoint_override, credential=mock_default_credential.return_value
        )

    def test_get_conn_raises_when_endpoint_missing(self, create_mock_connection):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents"))
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="Azure AI Foundry project endpoint must be provided"):
            hook.get_conn()

    def test_create_agent(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["create_agent"])
        response = object()
        client.create_agent.return_value = response
        hook.__dict__["conn"] = client

        result = hook.create_agent(model="gpt-4o", name="agent", instructions=None)

        assert result == response
        client.create_agent.assert_called_once_with(model="gpt-4o", name="agent")

    def test_update_agent(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["update_agent"])
        response = object()
        client.update_agent.return_value = response
        hook.__dict__["conn"] = client

        result = hook.update_agent(agent_id=AGENT_ID, model="gpt-4o", instructions=None)

        assert result == response
        client.update_agent.assert_called_once_with(agent_id=AGENT_ID, model="gpt-4o")

    def test_run_agent(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["create_thread_and_run"])
        response = object()
        client.create_thread_and_run.return_value = response
        hook.__dict__["conn"] = client

        result = hook.run_agent(agent_id=AGENT_ID, thread={"messages": []}, metadata=None)

        assert result == response
        client.create_thread_and_run.assert_called_once_with(agent_id=AGENT_ID, thread={"messages": []})

    def test_get_run(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        runs = mock.Mock(spec_set=["get"])
        client = mock.Mock(spec_set=["runs"])
        client.runs = runs
        response = object()
        runs.get.return_value = response
        hook.__dict__["conn"] = client

        result = hook.get_run(thread_id="thread_123", run_id="run_123")

        assert result == response
        runs.get.assert_called_once_with(thread_id="thread_123", run_id="run_123")

    def test_delete_agent(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["delete_agent"])
        hook.__dict__["conn"] = client

        hook.delete_agent(agent_id=AGENT_ID)

        client.delete_agent.assert_called_once_with(agent_id=AGENT_ID)

    def test_get_agent(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["get_agent"])
        response = object()
        client.get_agent.return_value = response
        hook.__dict__["conn"] = client

        result = hook.get_agent(agent_id=AGENT_ID)

        assert result == response
        client.get_agent.assert_called_once_with(agent_id=AGENT_ID)

    def test_is_agent_deleted_when_resource_does_not_exist(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["get_agent"])
        client.get_agent.side_effect = ResourceNotFoundError("not found")
        hook.__dict__["conn"] = client

        assert hook.is_agent_deleted(agent_id=AGENT_ID) is True
        client.get_agent.assert_called_once_with(agent_id=AGENT_ID)

    def test_is_agent_deleted_when_resource_exists(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.Mock(spec_set=["get_agent"])
        hook.__dict__["conn"] = client

        assert hook.is_agent_deleted(agent_id=AGENT_ID) is False
        client.get_agent.assert_called_once_with(agent_id=AGENT_ID)

    def test_get_run_status_raises_when_status_missing(self):
        with pytest.raises(ValueError, match="did not include a status"):
            get_run_status({})


class TestAzureAIAgentsAsyncHook:
    pytestmark = pytest.mark.asyncio

    @mock.patch(f"{MODULE}.AsyncAgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.AsyncClientSecretCredential", autospec=True)
    async def test_get_async_conn_uses_connection_endpoint_and_client_secret_credential(
        self, mock_credential_cls, mock_client_cls, create_mock_connection
    ):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_ai_agents",
                host=ENDPOINT,
                login="client-id",
                password="client-secret",
                extra={"tenantId": "tenant-id", "cloud_environment": "AzureUSGovernment"},
            )
        )
        mock_credential = mock_credential_cls.return_value
        mock_client = mock_client_cls.return_value
        mock_client.close = mock.AsyncMock()
        mock_credential.close = mock.AsyncMock()
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        async with hook.get_async_conn() as client:
            result = client

        assert result == mock_client
        mock_credential_cls.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id",
            authority="login.microsoftonline.us",
        )
        mock_client_cls.assert_called_once_with(endpoint=ENDPOINT, credential=mock_credential)
        mock_client.close.assert_awaited_once()
        mock_credential.close.assert_awaited_once()

    @mock.patch(f"{MODULE}.AsyncAgentsClient", autospec=True)
    @mock.patch(f"{MODULE}.get_async_default_azure_credential", autospec=True)
    async def test_get_async_conn_uses_default_credential_and_endpoint_extra(
        self, mock_default_credential, mock_client_cls, create_mock_connection
    ):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_ai_agents",
                extra={
                    "endpoint": ENDPOINT,
                    "managed_identity_client_id": "managed-identity-client-id",
                    "workload_identity_tenant_id": "workload-identity-tenant-id",
                },
            )
        )
        mock_credential = mock_default_credential.return_value
        mock_client = mock_client_cls.return_value
        mock_client.close = mock.AsyncMock()
        mock_credential.close = mock.AsyncMock()
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        async with hook.get_async_conn() as client:
            result = client

        assert result == mock_client
        mock_default_credential.assert_called_once_with(
            managed_identity_client_id="managed-identity-client-id",
            workload_identity_tenant_id="workload-identity-tenant-id",
        )
        mock_client_cls.assert_called_once_with(endpoint=ENDPOINT, credential=mock_credential)

    @mock.patch.object(AzureAIAgentsAsyncHook, "get_async_conn")
    async def test_async_get_run(self, mock_get_async_conn):
        client = mock.AsyncMock()
        client.runs.get.return_value = mock.sentinel.run
        mock_get_async_conn.return_value.__aenter__.return_value = client
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        result = await hook.async_get_run(thread_id="thread_123", run_id="run_123")

        assert result == mock.sentinel.run
        client.runs.get.assert_awaited_once_with(thread_id="thread_123", run_id="run_123")

    @mock.patch.object(AzureAIAgentsAsyncHook, "get_async_conn")
    async def test_is_agent_deleted_when_resource_does_not_exist(self, mock_get_async_conn):
        client = mock.AsyncMock()
        client.get_agent.side_effect = ResourceNotFoundError("not found")
        mock_get_async_conn.return_value.__aenter__.return_value = client
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        assert await hook.async_is_agent_deleted(agent_id=AGENT_ID) is True
        client.get_agent.assert_awaited_once_with(agent_id=AGENT_ID)

    @mock.patch.object(AzureAIAgentsAsyncHook, "get_async_conn")
    async def test_is_agent_deleted_when_resource_exists(self, mock_get_async_conn):
        client = mock.AsyncMock()
        mock_get_async_conn.return_value.__aenter__.return_value = client
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        assert await hook.async_is_agent_deleted(agent_id=AGENT_ID) is False
        client.get_agent.assert_awaited_once_with(agent_id=AGENT_ID)
