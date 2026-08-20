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

from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.ai_agents import (
    DEFAULT_REQUEST_TIMEOUT,
    AzureAIAgentsAsyncHook,
    AzureAIAgentsHook,
)

MODULE = "airflow.providers.microsoft.azure.hooks.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_NAME = "agent-123"
DEFINITION = {
    "kind": "hosted",
    "container_configuration": {"image": "registry.azurecr.io/agent:v1"},
    "cpu": "1",
    "memory": "2Gi",
    "protocol_versions": [{"protocol": "responses", "version": "1.0.0"}],
}
METADATA = {"team": "airflow"}
DESCRIPTION = "Airflow hosted agent"
BLUEPRINT_REFERENCE = {"type": "ManagedAgentIdentityBlueprint", "blueprint_id": "blueprint-1"}


@pytest.fixture
def hook_with_mocked_client():
    hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
    hook._sync_client = mock.MagicMock()
    return hook


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

    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    def test_get_credential_uses_client_secret_credential(self, mock_credential_cls, create_mock_connection):
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
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        result = hook._get_credential(hook.get_connection(CONN_ID))

        assert result == mock_credential_cls.return_value
        mock_credential_cls.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id",
            authority="login.microsoftonline.us",
        )

    @pytest.mark.parametrize(
        ("login", "password", "tenant"),
        [
            ("client-id", None, None),
            (None, "client-secret", None),
            (None, None, "tenant-id"),
            ("client-id", "client-secret", None),
            ("client-id", None, "tenant-id"),
            (None, "client-secret", "tenant-id"),
        ],
    )
    def test_get_credential_rejects_partial_service_principal(self, login, password, tenant):
        conn = Connection(
            conn_id=CONN_ID,
            conn_type="azure_ai_agents",
            login=login,
            password=password,
            extra={"tenantId": tenant} if tenant else None,
        )
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="must all be provided"):
            hook._get_credential(conn)

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    @mock.patch(f"{MODULE}.AIProjectClient", autospec=True)
    def test_client_uses_default_credential_and_endpoint_extra(
        self, mock_client_cls, mock_default_credential, create_mock_connection
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
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        result = hook.get_conn()

        assert result == mock_client_cls.return_value
        mock_default_credential.assert_called_once_with(
            managed_identity_client_id="managed-identity-client-id",
            workload_identity_tenant_id="workload-identity-tenant-id",
        )
        mock_client_cls.assert_called_once_with(
            endpoint=ENDPOINT,
            credential=mock_default_credential.return_value,
            api_version="v1",
            allow_preview=True,
            connection_timeout=DEFAULT_REQUEST_TIMEOUT,
            read_timeout=DEFAULT_REQUEST_TIMEOUT,
        )

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    @mock.patch(f"{MODULE}.AIProjectClient", autospec=True)
    def test_client_uses_custom_api_version_and_timeout(
        self, mock_client_cls, mock_default_credential, create_mock_connection
    ):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT))
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID, api_version="v2", timeout=10)

        hook.get_conn()

        mock_client_cls.assert_called_once_with(
            endpoint=ENDPOINT,
            credential=mock_default_credential.return_value,
            api_version="v2",
            allow_preview=True,
            connection_timeout=10,
            read_timeout=10,
        )

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    @mock.patch(f"{MODULE}.AIProjectClient", autospec=True)
    def test_client_is_cached(self, mock_client_cls, mock_default_credential, create_mock_connection):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT))
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        assert hook.get_conn() is hook.get_conn()
        mock_client_cls.assert_called_once()
        mock_default_credential.assert_called_once()

    def test_get_endpoint_hook_endpoint_overrides_connection_endpoint(self, create_mock_connection):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT))
        endpoint_override = "https://override.services.ai.azure.com/api/projects/project"
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID, endpoint=endpoint_override)

        assert hook._get_endpoint(hook.get_connection(CONN_ID)) == endpoint_override

    def test_get_endpoint_raises_when_endpoint_missing(self, create_mock_connection):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents"))
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="Azure AI Foundry project endpoint must be provided"):
            hook._get_endpoint(hook.get_connection(CONN_ID))

    def test_create_agent_version(self, hook_with_mocked_client):
        hook = hook_with_mocked_client

        result = hook.create_agent_version(
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            metadata=METADATA,
            description=DESCRIPTION,
            blueprint_reference=BLUEPRINT_REFERENCE,
        )

        hook.get_conn().agents.create_version.assert_called_once_with(
            agent_name=AGENT_NAME,
            definition=DEFINITION,
            metadata=METADATA,
            description=DESCRIPTION,
            blueprint_reference=BLUEPRINT_REFERENCE,
        )
        assert result == hook.get_conn().agents.create_version.return_value

    def test_get_agent_version(self, hook_with_mocked_client):
        hook = hook_with_mocked_client

        result = hook.get_agent_version(agent_name=AGENT_NAME, agent_version="1")

        hook.get_conn().agents.get_version.assert_called_once_with(agent_name=AGENT_NAME, agent_version="1")
        assert result == hook.get_conn().agents.get_version.return_value

    def test_get_agent(self, hook_with_mocked_client):
        hook = hook_with_mocked_client

        result = hook.get_agent(agent_name=AGENT_NAME)

        hook.get_conn().agents.get.assert_called_once_with(agent_name=AGENT_NAME)
        assert result == hook.get_conn().agents.get.return_value

    @pytest.mark.parametrize("force", [False, True])
    def test_delete_agent(self, hook_with_mocked_client, force):
        hook = hook_with_mocked_client

        result = hook.delete_agent(agent_name=AGENT_NAME, force=force)

        hook.get_conn().agents.delete.assert_called_once_with(agent_name=AGENT_NAME, force=force)
        assert result == hook.get_conn().agents.delete.return_value

    @pytest.mark.parametrize("force", [False, True])
    def test_delete_agent_version(self, hook_with_mocked_client, force):
        hook = hook_with_mocked_client

        result = hook.delete_agent_version(agent_name=AGENT_NAME, agent_version="2", force=force)

        hook.get_conn().agents.delete_version.assert_called_once_with(
            agent_name=AGENT_NAME, agent_version="2", force=force
        )
        assert result == hook.get_conn().agents.delete_version.return_value

    def test_is_agent_version_deleted_when_resource_does_not_exist(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().agents.get_version.side_effect = ResourceNotFoundError("not found")

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is True

    def test_is_agent_version_deleted_when_status_is_deleted(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().agents.get_version.return_value = {"status": "deleted"}

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is True

    def test_is_agent_version_deleted_when_resource_exists(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().agents.get_version.return_value = {"status": "active"}

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is False

    def test_is_agent_deleted_when_resource_does_not_exist(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().agents.get.side_effect = ResourceNotFoundError("not found")

        assert hook.is_agent_deleted(agent_name=AGENT_NAME) is True

    def test_is_agent_deleted_when_resource_exists(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().agents.get.return_value = {"name": AGENT_NAME}

        assert hook.is_agent_deleted(agent_name=AGENT_NAME) is False

    def test_invoke_agent_responses(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        openai_client_manager = hook.get_conn().get_openai_client.return_value
        openai_client = openai_client_manager.__enter__.return_value
        openai_client.responses.create.return_value.model_dump.return_value = {"output_text": "hello"}

        result = hook.invoke_agent_responses(
            agent_name=AGENT_NAME,
            input_data={"input": "hello"},
            user_isolation_key="user-key",
        )

        assert result == {"output_text": "hello"}
        hook.get_conn().get_openai_client.assert_called_once_with(agent_name=AGENT_NAME)
        openai_client.responses.create.assert_called_once_with(
            input="hello",
            extra_headers={"x-ms-user-isolation-key": "user-key"},
        )
        openai_client_manager.__exit__.assert_called_once_with(None, None, None)

    def test_invoke_agent_responses_without_isolation_key(self, hook_with_mocked_client):
        hook = hook_with_mocked_client
        openai_client_manager = hook.get_conn().get_openai_client.return_value
        openai_client = openai_client_manager.__enter__.return_value

        hook.invoke_agent_responses(agent_name=AGENT_NAME, input_data={"input": "hello"})

        openai_client.responses.create.assert_called_once_with(input="hello")
        openai_client_manager.__exit__.assert_called_once_with(None, None, None)

    @mock.patch(f"{MODULE}.HttpRequest", autospec=True)
    def test_invoke_agent_invocations(self, mock_request_cls, hook_with_mocked_client):
        hook = hook_with_mocked_client
        response = hook.get_conn().send_request.return_value
        response.content = b'{"result": "done"}'
        response.json.return_value = {"result": "done"}

        result = hook.invoke_agent_invocations(
            agent_name=AGENT_NAME,
            input_data={"message": "hello"},
            agent_session_id="session-1",
            user_isolation_key="user-key",
        )

        assert result == {"result": "done"}
        mock_request_cls.assert_called_once_with(
            "POST",
            f"/agents/{AGENT_NAME}/endpoint/protocols/invocations",
            params={"api-version": "v1", "agent_session_id": "session-1"},
            headers={"x-ms-user-isolation-key": "user-key"},
            json={"message": "hello"},
        )
        hook.get_conn().send_request.assert_called_once_with(mock_request_cls.return_value)
        response.raise_for_status.assert_called_once_with()

    @mock.patch(f"{MODULE}.HttpRequest", autospec=True)
    def test_invoke_agent_invocations_quotes_resource_id(self, mock_request_cls, hook_with_mocked_client):
        hook = hook_with_mocked_client
        hook.get_conn().send_request.return_value.content = b""

        result = hook.invoke_agent_invocations(
            agent_name="agent/name with spaces", input_data={"message": "hello"}
        )

        assert result is None
        assert (
            mock_request_cls.call_args.args[1]
            == "/agents/agent%2Fname%20with%20spaces/endpoint/protocols/invocations"
        )
        assert mock_request_cls.call_args.kwargs["headers"] is None

    @mock.patch(f"{MODULE}.HttpRequest", autospec=True)
    def test_invoke_agent_invocations_rejects_non_json_response(
        self, mock_request_cls, hook_with_mocked_client
    ):
        hook = hook_with_mocked_client
        response = hook.get_conn().send_request.return_value
        response.content = b'{"result": "done"}'
        response.json.return_value = {"result": object()}

        with pytest.raises(TypeError, match="Cannot serialize.*object.*for XCom"):
            hook.invoke_agent_invocations(agent_name=AGENT_NAME, input_data={"message": "hello"})

        response.raise_for_status.assert_called_once_with()


class TestAzureAIAgentsAsyncHook:
    pytestmark = pytest.mark.asyncio

    @mock.patch(f"{MODULE}.get_async_default_azure_credential", autospec=True)
    @mock.patch(f"{MODULE}.AsyncAIProjectClient", autospec=True)
    @mock.patch(f"{MODULE}.get_async_connection")
    async def test_get_async_conn(self, mock_get_connection, mock_client_cls, mock_default_credential):
        mock_get_connection.return_value = Connection(
            conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT
        )
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        result = await hook.get_async_conn()

        assert result == mock_client_cls.return_value
        mock_client_cls.assert_called_once_with(
            endpoint=ENDPOINT,
            credential=mock_default_credential.return_value,
            api_version="v1",
            allow_preview=True,
            connection_timeout=DEFAULT_REQUEST_TIMEOUT,
            read_timeout=DEFAULT_REQUEST_TIMEOUT,
        )

        assert await hook.get_async_conn() is result
        mock_client_cls.assert_called_once()

    @mock.patch(f"{MODULE}.AsyncClientSecretCredential", autospec=True)
    @mock.patch(f"{MODULE}.AsyncAIProjectClient", autospec=True)
    @mock.patch(f"{MODULE}.get_async_connection")
    async def test_get_async_conn_uses_client_secret_credential(
        self, mock_get_connection, mock_client_cls, mock_credential_cls
    ):
        mock_get_connection.return_value = Connection(
            conn_id=CONN_ID,
            conn_type="azure_ai_agents",
            host=ENDPOINT,
            login="client-id",
            password="client-secret",
            extra={"tenantId": "tenant-id"},
        )
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        await hook.get_async_conn()

        mock_credential_cls.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id",
            authority="login.microsoftonline.com",
        )

    async def test_get_async_credential_rejects_partial_service_principal(self):
        conn = Connection(
            conn_id=CONN_ID,
            conn_type="azure_ai_agents",
            login="client-id",
        )
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        with pytest.raises(ValueError, match="must all be provided"):
            hook._get_async_credential(conn)

    async def test_async_get_agent_version(self):
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.MagicMock()
        client.agents.get_version = mock.AsyncMock(return_value={"version": "1"})
        hook.get_async_conn = mock.AsyncMock(return_value=client)

        result = await hook.async_get_agent_version(agent_name=AGENT_NAME, agent_version="1")

        assert result == {"version": "1"}
        client.agents.get_version.assert_awaited_once_with(agent_name=AGENT_NAME, agent_version="1")

    async def test_close(self):
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.MagicMock(spec=["close"])
        client.close = mock.AsyncMock()
        credential = mock.MagicMock(spec=["close"])
        credential.close = mock.AsyncMock()
        hook._async_client = client
        hook._async_credential = credential

        await hook.close()

        client.close.assert_awaited_once_with()
        credential.close.assert_awaited_once_with()
        assert hook._async_client is None
        assert hook._async_credential is None

    async def test_close_closes_credential_when_client_close_fails(self):
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)
        client = mock.MagicMock(spec=["close"])
        client.close = mock.AsyncMock(side_effect=RuntimeError("client close failed"))
        credential = mock.MagicMock(spec=["close"])
        credential.close = mock.AsyncMock()
        hook._async_client = client
        hook._async_credential = credential

        with pytest.raises(RuntimeError, match="client close failed"):
            await hook.close()

        credential.close.assert_awaited_once_with()
        assert hook._async_client is None
        assert hook._async_credential is None
