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
from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.ai_agents import (
    HOSTED_AGENT_FEATURE_HEADER,
    TOKEN_SCOPE,
    AzureAIAgentsAsyncHook,
    AzureAIAgentsHook,
    get_agent_version,
    get_version_status,
)

MODULE = "airflow.providers.microsoft.azure.hooks.ai_agents"
CONN_ID = "azure_ai_agents_test"
ENDPOINT = "https://test.services.ai.azure.com/api/projects/test-project"
AGENT_NAME = "agent-123"
SPECIAL_AGENT_NAME = "agent/name with spaces"
DEFINITION = {
    "kind": "hosted",
    "container_configuration": {"image": "registry.azurecr.io/agent:v1"},
    "cpu": "1",
    "memory": "2Gi",
    "protocol_versions": [{"protocol": "responses", "version": "1.0.0"}],
}


def build_response(status_code=200, payload=None):
    response = mock.Mock()
    response.status_code = status_code
    response.content = b"" if payload is None else json.dumps(payload).encode()
    response.json.return_value = payload
    response.raise_for_status = mock.Mock()
    return response


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

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_request_uses_default_credential_and_endpoint_extra(
        self, mock_default_credential, create_mock_connection
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
        mock_default_credential.return_value.get_token.return_value.token = "token"
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)
        hook.__dict__["session"] = mock.Mock()
        hook.session.request.return_value = build_response(payload={"ok": True})

        result = hook._request("GET", "agents/agent-123/versions/1")

        assert result == {"ok": True}
        mock_default_credential.assert_called_once_with(
            managed_identity_client_id="managed-identity-client-id",
            workload_identity_tenant_id="workload-identity-tenant-id",
        )

    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_request_sends_authorization_and_api_version(
        self, mock_default_credential, create_mock_connection
    ):
        create_mock_connection(Connection(conn_id=CONN_ID, conn_type="azure_ai_agents", host=ENDPOINT))
        mock_default_credential.return_value.get_token.return_value.token = "token"
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID, api_version="v2")
        hook.__dict__["session"] = mock.Mock()
        hook.session.request.return_value = build_response(payload={"ok": True})

        hook._request("POST", "agents", json_payload={"name": AGENT_NAME})

        mock_default_credential.return_value.get_token.assert_called_once_with(TOKEN_SCOPE)
        hook.session.request.assert_called_once_with(
            method="POST",
            url=f"{ENDPOINT}/agents",
            params={"api-version": "v2"},
            headers={
                "Authorization": "Bearer token",
                "Content-Type": "application/json",
                "Foundry-Features": HOSTED_AGENT_FEATURE_HEADER,
            },
            json={"name": AGENT_NAME},
        )

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

    def test_process_response_raises_resource_not_found(self):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        with pytest.raises(ResourceNotFoundError):
            hook._process_response(build_response(status_code=404, payload={"error": "not found"}))

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_create_agent(self, mock_request):
        mock_request.return_value = {"name": AGENT_NAME, "version": "1"}
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        result = hook.create_agent(agent_name=AGENT_NAME, definition=DEFINITION)

        assert result == {"name": AGENT_NAME, "version": "1"}
        mock_request.assert_called_once_with(
            hook,
            "POST",
            "agents",
            json_payload={"name": AGENT_NAME, "definition": DEFINITION},
        )

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_create_agent_version(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.create_agent_version(agent_name=AGENT_NAME, definition=DEFINITION)

        mock_request.assert_called_once_with(
            hook,
            "POST",
            f"agents/{AGENT_NAME}/versions",
            json_payload={"definition": DEFINITION},
        )

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_get_agent_version(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.get_agent_version(agent_name=AGENT_NAME, agent_version="1")

        mock_request.assert_called_once_with(hook, "GET", f"agents/{AGENT_NAME}/versions/1")

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_get_agent_version_quotes_resource_ids(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.get_agent_version(agent_name=SPECIAL_AGENT_NAME, agent_version="version/1")

        mock_request.assert_called_once_with(
            hook,
            "GET",
            "agents/agent%2Fname%20with%20spaces/versions/version%2F1",
        )

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_delete_agent(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.delete_agent(agent_name=AGENT_NAME)

        mock_request.assert_called_once_with(hook, "DELETE", f"agents/{AGENT_NAME}")

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_delete_agent_quotes_resource_id(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.delete_agent(agent_name=SPECIAL_AGENT_NAME)

        mock_request.assert_called_once_with(hook, "DELETE", "agents/agent%2Fname%20with%20spaces")

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_delete_agent_version(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.delete_agent_version(agent_name=AGENT_NAME, agent_version="2")

        mock_request.assert_called_once_with(hook, "DELETE", f"agents/{AGENT_NAME}/versions/2")

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    def test_is_agent_version_deleted_when_resource_does_not_exist(self, mock_get_version):
        mock_get_version.side_effect = ResourceNotFoundError("not found")
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is True

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    def test_is_agent_version_deleted_when_status_is_deleted(self, mock_get_version):
        mock_get_version.return_value = {"status": "deleted"}
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is True

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    def test_is_agent_version_deleted_when_resource_exists(self, mock_get_version):
        mock_get_version.return_value = {"status": "active"}
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        assert hook.is_agent_version_deleted(agent_name=AGENT_NAME, agent_version="1") is False

    @mock.patch.object(AzureAIAgentsHook, "is_agent_version_deleted", autospec=True)
    def test_is_agent_deleted_checks_first_version(self, mock_is_version_deleted):
        mock_is_version_deleted.return_value = True
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        assert hook.is_agent_deleted(agent_name=AGENT_NAME) is True
        mock_is_version_deleted.assert_called_once_with(hook, agent_name=AGENT_NAME, agent_version="1")

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_invoke_agent_responses(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.invoke_agent_responses(agent_name=AGENT_NAME, input_data={"input": "hello"})

        mock_request.assert_called_once_with(
            hook,
            "POST",
            f"agents/{AGENT_NAME}/endpoint/protocols/openai/responses",
            json_payload={"input": "hello"},
        )

    @mock.patch.object(AzureAIAgentsHook, "_request", autospec=True)
    def test_invoke_agent_invocations(self, mock_request):
        hook = AzureAIAgentsHook(azure_ai_agents_conn_id=CONN_ID)

        hook.invoke_agent_invocations(agent_name=AGENT_NAME, input_data={"message": "hello"})

        mock_request.assert_called_once_with(
            hook,
            "POST",
            f"agents/{AGENT_NAME}/endpoint/protocols/invocations",
            json_payload={"message": "hello"},
        )

    def test_get_version_status_raises_when_status_missing(self):
        with pytest.raises(ValueError, match="did not include a status"):
            get_version_status({})

    def test_get_agent_version_raises_when_version_missing(self):
        with pytest.raises(ValueError, match="did not include a version"):
            get_agent_version({})


class TestAzureAIAgentsAsyncHook:
    pytestmark = pytest.mark.asyncio

    @mock.patch.object(AzureAIAgentsHook, "get_agent_version", autospec=True)
    async def test_async_get_agent_version(self, mock_get_version):
        mock_get_version.return_value = {"version": "1"}
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        result = await hook.async_get_agent_version(agent_name=AGENT_NAME, agent_version="1")

        assert result == {"version": "1"}
        mock_get_version.assert_called_once_with(hook, agent_name=AGENT_NAME, agent_version="1")

    @mock.patch.object(AzureAIAgentsHook, "is_agent_deleted", autospec=True)
    async def test_async_is_agent_deleted(self, mock_is_deleted):
        mock_is_deleted.return_value = True
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        result = await hook.async_is_agent_deleted(agent_name=AGENT_NAME)

        assert result is True
        mock_is_deleted.assert_called_once_with(hook, agent_name=AGENT_NAME)

    @mock.patch.object(AzureAIAgentsHook, "is_agent_version_deleted", autospec=True)
    async def test_async_is_agent_version_deleted(self, mock_is_version_deleted):
        mock_is_version_deleted.return_value = True
        hook = AzureAIAgentsAsyncHook(azure_ai_agents_conn_id=CONN_ID)

        result = await hook.async_is_agent_deleted(agent_name=AGENT_NAME, agent_version="2")

        assert result is True
        mock_is_version_deleted.assert_called_once_with(hook, agent_name=AGENT_NAME, agent_version="2")
