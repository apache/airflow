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

import httpx
import pytest
from azure.core.credentials import AccessToken
from azure.core.exceptions import ClientAuthenticationError

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    TOKEN_SCOPE,
    VALID_REFRESH_TYPES,
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)

CONN_ID = "azure_analysis_services_test"
HOST = "westus.asazure.windows.net"
SERVER_NAME = "testserver"
DATABASE = "Adventure Works"
REFRESH_ID = "refresh-id"
REQUEST_TIMEOUT = 30
HEADERS = {"Authorization": "Bearer token", "Content-Type": "application/json"}
MODULE = "airflow.providers.microsoft.azure.hooks.analysis_services"


def build_response(
    *, headers: dict[str, str] | None = None, body: object | None = None, text: str = ""
) -> mock.Mock:
    """Build a response mock with the requested headers and JSON body."""
    response = mock.Mock(spec=httpx.Response)
    response.headers = headers or {}
    response.json.return_value = body
    response.text = text
    return response


def build_client(client_class: mock.MagicMock) -> mock.MagicMock:
    """Return the ``httpx.AsyncClient`` instance that the patched class hands to the hook."""
    return client_class.return_value


class TestAzureAnalysisServicesHook:
    @pytest.fixture(autouse=True)
    def setup_connection(self, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_analysis_services",
                host=HOST,
                login="client-id",
                password="client-secret",
                extra={"tenantId": "tenant-id"},
            )
        )

    @pytest.mark.parametrize("request_timeout", [0, -1])
    def test_rejects_invalid_request_timeout(self, request_timeout):
        with pytest.raises(ValueError, match="request_timeout must be greater than zero"):
            AzureAnalysisServicesHook(CONN_ID, request_timeout=request_timeout)

    def test_defines_connection_form_widget(self):
        pytest.importorskip("flask_appbuilder")
        assert set(AzureAnalysisServicesHook.get_connection_form_widgets()) == {"tenantId"}

    def test_defines_connection_ui_field_behaviour(self):
        assert AzureAnalysisServicesHook.get_ui_field_behaviour() == {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "host": "Region Endpoint",
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {"host": "westus.asazure.windows.net"},
        }

    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    def test_get_conn_creates_and_caches_credential(self, credential_class):
        hook = AzureAnalysisServicesHook(CONN_ID)

        first_credential = hook.get_conn()
        second_credential = hook.get_conn()

        credential_class.assert_called_once_with(
            tenant_id="tenant-id",
            client_id="client-id",
            client_secret="client-secret",
        )
        assert first_credential is credential_class.return_value
        assert second_credential is first_credential

    @mock.patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    def test_caches_connection(self, get_connection):
        get_connection.return_value = Connection(
            conn_id=CONN_ID,
            conn_type="azure_analysis_services",
            host=HOST,
            login="client-id",
            password="client-secret",
            extra={"tenantId": "tenant-id"},
        )
        hook = AzureAnalysisServicesHook(CONN_ID)

        first_connection = hook.connection
        second_connection = hook.connection

        get_connection.assert_called_once_with(CONN_ID)
        assert second_connection is first_connection

    @pytest.mark.parametrize(
        ("login", "password", "extra", "message"),
        [
            (None, "secret", {"tenantId": "tenant"}, "Client ID is required"),
            ("client", None, {"tenantId": "tenant"}, "Client secret is required"),
            ("client", "secret", {}, "Tenant ID is required"),
            ("client", "secret", {"tenantId": 123}, "Tenant ID is required"),
        ],
    )
    def test_get_conn_requires_service_principal_fields(
        self, create_mock_connection, login, password, extra, message
    ):
        create_mock_connection(
            Connection(
                conn_id="invalid-auth",
                conn_type="azure_analysis_services",
                host=HOST,
                login=login,
                password=password,
                extra=extra,
            )
        )

        with pytest.raises(ValueError, match=message):
            AzureAnalysisServicesHook("invalid-auth").get_conn()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    async def test_get_headers_uses_literal_token_scope(self, credential_class):
        credential_class.return_value.get_token.return_value = AccessToken("token", 0)

        headers = await AzureAnalysisServicesHook(CONN_ID)._get_headers()

        credential_class.return_value.get_token.assert_awaited_once_with(TOKEN_SCOPE)
        assert TOKEN_SCOPE == "https://*.asazure.windows.net/.default"
        assert headers == HEADERS

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    async def test_get_headers_wraps_authentication_errors(self, credential_class):
        credential_class.return_value.get_token.side_effect = ClientAuthenticationError("bad credential")

        with pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to authenticate"):
            await AzureAnalysisServicesHook(CONN_ID)._get_headers()

    @pytest.mark.parametrize(
        "host",
        [
            None,
            "",
            "https://westus.asazure.windows.net",
            "host/path",
            "attacker@evil.example",
            "@evil.example",
            "evil.example:9999",
            "host:abc",
            "evil.example:0",
        ],
    )
    def test_rejects_invalid_region_endpoint(self, host, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id="invalid-host",
                conn_type="azure_analysis_services",
                host=host,
                login="client-id",
                password="client-secret",
                extra={"tenantId": "tenant-id"},
            )
        )

        with pytest.raises(ValueError, match="valid region endpoint"):
            AzureAnalysisServicesHook("invalid-host")._get_base_url()

    def test_strips_trailing_slash_from_region_endpoint(self, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id="trailing-slash",
                conn_type="azure_analysis_services",
                host=f"{HOST}/",
                login="client-id",
                password="client-secret",
                extra={"tenantId": "tenant-id"},
            )
        )

        assert AzureAnalysisServicesHook("trailing-slash")._get_base_url() == f"https://{HOST}"

    @pytest.mark.parametrize(
        ("server_name", "database", "message"), [("", "db", "server_name"), ("s", "", "database")]
    )
    def test_rejects_empty_resource_names(self, server_name, database, message):
        with pytest.raises(ValueError, match=message):
            AzureAnalysisServicesHook(CONN_ID)._get_refreshes_url(server_name, database)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("refresh_type", sorted(VALID_REFRESH_TYPES))
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_trigger_refresh_posts_official_request_body(self, client_class, get_headers, refresh_type):
        client = build_client(client_class)
        client.post.return_value = build_response(
            headers={
                "Location": f"https://{HOST}/servers/{SERVER_NAME}/models/Adventure%20Works/refreshes/{REFRESH_ID}"
            }
        )
        hook = AzureAnalysisServicesHook(CONN_ID, request_timeout=REQUEST_TIMEOUT)

        result = await hook.trigger_refresh(SERVER_NAME, DATABASE, refresh_type)

        assert result == REFRESH_ID
        client_class.assert_called_once_with(timeout=REQUEST_TIMEOUT)
        client.post.assert_awaited_once_with(
            f"https://{HOST}/servers/{SERVER_NAME}/models/Adventure%20Works/refreshes",
            json={"Type": refresh_type},
            headers=HEADERS,
        )

    @pytest.mark.asyncio
    async def test_trigger_refresh_rejects_unknown_type(self):
        with pytest.raises(ValueError, match="Invalid refresh_type"):
            await AzureAnalysisServicesHook(CONN_ID).trigger_refresh(SERVER_NAME, DATABASE, "invalid")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "location",
        [None, "", f"https://{HOST}/servers/{SERVER_NAME}/models/{DATABASE}/refreshes/", "https://["],
    )
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_trigger_refresh_rejects_invalid_location(self, client_class, get_headers, location):
        build_client(client_class).post.return_value = build_response(
            headers={"Location": location} if location is not None else {}
        )

        with pytest.raises(AzureAnalysisServicesRefreshException, match="Location header"):
            await AzureAnalysisServicesHook(CONN_ID).trigger_refresh(SERVER_NAME, DATABASE)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_trigger_refresh_decodes_refresh_id(self, client_class, get_headers):
        build_client(client_class).post.return_value = build_response(
            headers={"Location": f"https://{HOST}/models/model/refreshes/refresh%20id"}
        )

        result = await AzureAnalysisServicesHook(CONN_ID).trigger_refresh(SERVER_NAME, DATABASE)

        assert result == "refresh id"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [httpx.ConnectError("offline"), httpx.ReadTimeout("slow")])
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_trigger_refresh_wraps_request_errors(self, client_class, get_headers, error):
        build_client(client_class).post.side_effect = error

        with pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to trigger"):
            await AzureAnalysisServicesHook(CONN_ID).trigger_refresh(SERVER_NAME, DATABASE)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_trigger_refresh_includes_truncated_http_error_response(self, client_class, get_headers):
        response_text = "A refresh is already running. " + "x" * 1000
        response = build_response(text=response_text)
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "409 Client Error: Conflict", request=mock.Mock(spec=httpx.Request), response=response
        )
        build_client(client_class).post.return_value = response

        with pytest.raises(AzureAnalysisServicesRefreshException) as error:
            await AzureAnalysisServicesHook(CONN_ID).trigger_refresh(SERVER_NAME, DATABASE)

        assert "409 Client Error: Conflict" in str(error.value)
        assert f"response body: {response_text[:1000]}" in str(error.value)
        assert response_text[:1001] not in str(error.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", sorted(AzureAnalysisServicesRefreshStatus.VALID_STATUSES))
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_get_refresh_status_returns_valid_status(self, client_class, get_headers, status):
        client = build_client(client_class)
        client.get.return_value = build_response(body={"status": status})
        hook = AzureAnalysisServicesHook(CONN_ID, request_timeout=REQUEST_TIMEOUT)

        result = await hook.get_refresh_status(SERVER_NAME, DATABASE, "refresh id")

        assert result == status
        client_class.assert_called_once_with(timeout=REQUEST_TIMEOUT)
        client.get.assert_awaited_once_with(
            f"https://{HOST}/servers/{SERVER_NAME}/models/Adventure%20Works/refreshes/refresh%20id",
            headers=HEADERS,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("body", "message"),
        [
            ([], "invalid status response"),
            ({}, "unknown status None"),
            ({"status": "unexpected"}, "unknown status 'unexpected'"),
            ({"status": 1}, "unknown status 1"),
        ],
    )
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_get_refresh_status_rejects_invalid_response(
        self, client_class, get_headers, body, message
    ):
        build_client(client_class).get.return_value = build_response(body=body)

        with pytest.raises(AzureAnalysisServicesRefreshException, match=message):
            await AzureAnalysisServicesHook(CONN_ID).get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_get_refresh_status_rejects_non_json_response(self, client_class, get_headers):
        response = build_response()
        response.json.side_effect = json.JSONDecodeError("bad json", "", 0)
        build_client(client_class).get.return_value = response

        with pytest.raises(AzureAnalysisServicesRefreshException, match="non-JSON"):
            await AzureAnalysisServicesHook(CONN_ID).get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error", [httpx.ConnectError("offline"), httpx.ReadTimeout("slow")])
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_get_refresh_status_wraps_request_errors(self, client_class, get_headers, error):
        build_client(client_class).get.side_effect = error

        with pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to get status"):
            await AzureAnalysisServicesHook(CONN_ID).get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_get_refresh_status_includes_http_error_response(self, client_class, get_headers):
        response = build_response(text='{"error":"model not found"}')
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=mock.Mock(spec=httpx.Request), response=response
        )
        build_client(client_class).get.return_value = response

        with pytest.raises(AzureAnalysisServicesRefreshException) as error:
            await AzureAnalysisServicesHook(CONN_ID).get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

        assert "500 Server Error" in str(error.value)
        assert 'response body: {"error":"model not found"}' in str(error.value)

    @pytest.mark.asyncio
    @mock.patch.object(AzureAnalysisServicesHook, "_get_headers", autospec=True, return_value=HEADERS)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_reuses_single_async_client_across_calls(self, client_class, get_headers):
        client = build_client(client_class)
        client.get.return_value = build_response(
            body={"status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS}
        )
        hook = AzureAnalysisServicesHook(CONN_ID, request_timeout=REQUEST_TIMEOUT)

        await hook.get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)
        await hook.get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

        client_class.assert_called_once_with(timeout=REQUEST_TIMEOUT)
        assert client.get.await_count == 2

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_aclose_closes_client_and_credential(self, client_class, credential_class):
        hook = AzureAnalysisServicesHook(CONN_ID)
        client = hook.client
        credential = hook.get_conn()

        await hook.aclose()

        client.aclose.assert_awaited_once_with()
        credential.close.assert_awaited_once_with()
        assert hook._client is None
        assert hook._credential is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_aclose_closes_credential_even_if_client_fails(self, client_class, credential_class):
        hook = AzureAnalysisServicesHook(CONN_ID)
        hook.client.aclose.side_effect = RuntimeError("transport already gone")
        credential = hook.get_conn()

        with pytest.raises(RuntimeError, match="transport already gone"):
            await hook.aclose()

        credential.close.assert_awaited_once_with()
        assert hook._client is None
        assert hook._credential is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    @mock.patch(f"{MODULE}.httpx.AsyncClient", autospec=True)
    async def test_aclose_is_idempotent(self, client_class, credential_class):
        hook = AzureAnalysisServicesHook(CONN_ID)
        client = hook.client
        credential = hook.get_conn()

        await hook.aclose()
        await hook.aclose()

        client.aclose.assert_awaited_once_with()
        credential.close.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_aclose_is_noop_when_nothing_created(self):
        await AzureAnalysisServicesHook(CONN_ID).aclose()
