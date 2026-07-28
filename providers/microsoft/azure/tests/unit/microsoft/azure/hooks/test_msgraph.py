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
import inspect
from contextlib import AbstractAsyncContextManager
from json import JSONDecodeError
from os.path import dirname
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from httpx import AsyncClient, Response
from httpx._utils import URLPattern
from kiota_abstractions.request_information import RequestInformation
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from kiota_serialization_json.json_parse_node import JsonParseNode
from kiota_serialization_text.text_parse_node import TextParseNode
from msgraph_core import APIVersion, NationalClouds
from opentelemetry.trace import Span

from airflow.exceptions import AirflowBadRequest, AirflowConfigException, AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException
from airflow.providers.microsoft.azure.hooks.msgraph import (
    CachedAsyncTokenCredential,
    DefaultResponseHandler,
    KiotaRequestAdapterHook,
    execute_callable,
)

from tests_common.test_utils.file_loading import load_file_from_resources, load_json_from_resources
from tests_common.test_utils.providers import get_provider_min_airflow_version
from unit.microsoft.azure.test_utils import (
    get_airflow_connection,
    mock_authentication_provider,
    mock_connection,
    mock_json_response,
    mock_response,
    mock_token_credentials,
    patch_hook,
    patch_hook_and_request_adapter,
)


class TestCachedAsyncTokenCredential:
    @pytest.mark.parametrize(
        ("closed", "expected"),
        (
            pytest.param(None, False),
            pytest.param(False, False),
            pytest.param(True, True),
        ),
    )
    def test_closed(self, closed: bool | None, expected: bool):
        actual = CachedAsyncTokenCredential(credential=mock_token_credentials(closed=closed))

        assert actual.closed == expected

    @pytest.mark.asyncio
    async def test_close(self):
        credential = mock_token_credentials()
        actual = CachedAsyncTokenCredential(credential=credential)

        await actual.close()
        credential.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_token(self):
        credential = mock_token_credentials()
        actual = CachedAsyncTokenCredential(credential=credential)

        await actual.get_token()
        credential.get_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_token_info(self):
        credential = mock_token_credentials()
        actual = CachedAsyncTokenCredential(credential=credential)

        await actual.get_token_info()
        credential.get_token_info.assert_called_once()


class TestKiotaRequestAdapterHook:
    def test_get_conn(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            with pytest.warns(
                DeprecationWarning,
                match="get_conn is deprecated, please use the async get_async_conn method!",
            ):
                actual = hook.get_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual.base_url == "https://graph.microsoft.com/v1.0/"

    @pytest.mark.asyncio
    async def test_get_async_conn(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual.base_url == "https://graph.microsoft.com/v1.0/"

    @pytest.mark.asyncio
    async def test_get_async_conn_with_custom_base_url(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                host="api.fabric.microsoft.com",
                api_version="v1",
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual.base_url == "https://api.fabric.microsoft.com/v1/"

    @pytest.mark.asyncio
    async def test_get_async_conn_with_proxies_as_string(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                host="api.fabric.microsoft.com",
                api_version="v1",
                proxies="{'http': 'http://proxy:80', 'https': 'https://proxy:80'}",
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual._http_client._mounts.get(URLPattern("http://"))
            assert actual._http_client._mounts.get(URLPattern("https://"))

    @pytest.mark.asyncio
    async def test_get_async_conn_with_proxies_as_invalid_string(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                host="api.fabric.microsoft.com",
                api_version="v1",
                proxies='["http://proxy:80", "https://proxy:80"]',
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            with pytest.raises(AirflowConfigException):
                await hook.get_async_conn()

    @pytest.mark.asyncio
    async def test_get_async_conn_with_proxies_as_json(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                host="api.fabric.microsoft.com",
                api_version="v1",
                proxies='{"http": "http://proxy:80", "https": "https://proxy:80"}',
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual._http_client._mounts.get(URLPattern("http://"))
            assert actual._http_client._mounts.get(URLPattern("https://"))

    def test_scopes_when_default(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            assert hook.scopes == [KiotaRequestAdapterHook.DEFAULT_SCOPE]

    def test_scopes_when_passed_as_string(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(
                conn_id="msgraph_api", scopes="https://microsoft.sharepoint.com/.default"
            )

            assert hook.scopes == ["https://microsoft.sharepoint.com/.default"]

    def test_scopes_when_passed_as_list(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(
                conn_id="msgraph_api", scopes=["https://microsoft.sharepoint.com/.default"]
            )

            assert hook.scopes == ["https://microsoft.sharepoint.com/.default"]

    def test_api_version(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", api_version=APIVersion.v1.value)

            assert hook.api_version == APIVersion.v1.value

    def test_api_version_when_none_is_explicitly_passed_as_api_version(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", api_version=None)

            assert not hook.api_version

    def test_get_api_version_when_empty_config_dict(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert actual == APIVersion.v1.value

    def test_get_api_version_when_api_version_in_config_dict(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert actual == APIVersion.beta.value

    def test_get_api_version_when_custom_api_version_in_config_dict(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", api_version="v1")
            actual = hook.get_api_version({})

            assert actual == "v1"

    def test_get_host_when_connection_has_scheme_and_host(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            connection = mock_connection(schema="https", host="graph.microsoft.de")
            actual = hook.get_host(connection)

            assert actual == NationalClouds.Germany.value

    def test_get_host_when_connection_has_no_scheme_or_host(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            connection = mock_connection()
            actual = hook.get_host(connection)

            assert actual == NationalClouds.Global.value

    def test_get_host_when_connection_has_no_scheme_or_host_but_hook_overrides_host(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(
                conn_id="msgraph_api", host="wabi-north-europe-o-primary-redirect.analysis.windows.net"
            )
            connection = mock_connection(schema="https", host=NationalClouds.Global.value)
            actual = hook.get_host(connection)

            assert actual == "https://wabi-north-europe-o-primary-redirect.analysis.windows.net"

    def test_execute_callable(self):
        response = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")

        url, query_parameters = execute_callable(
            KiotaRequestAdapterHook.default_pagination,
            response=response,
        )

        assert url == response["@odata.nextLink"]
        assert not query_parameters

    def test_execute_callable_with_additional_parameters(self):
        response = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")

        url, query_parameters = execute_callable(
            KiotaRequestAdapterHook.default_pagination,
            response=response,
            url="users",
            query_parameters={},
            data=None,
        )

        assert url == response["@odata.nextLink"]
        assert query_parameters == {}

    def test_execute_callable_when_required_parameter_is_missing(self):
        with pytest.raises(TypeError):
            execute_callable(KiotaRequestAdapterHook.default_pagination)

    @pytest.mark.asyncio
    async def test_tenant_id(self):
        with patch_hook():
            with patch(
                "airflow.providers.microsoft.azure.hooks.msgraph.ClientSecretCredential",
                autospec=True,
            ) as mock_credential_cls:
                hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
                await hook.get_async_conn()

                mock_credential_cls.assert_called_once()
                assert mock_credential_cls.call_args.kwargs.get("tenant_id") == "tenant-id"

    @pytest.mark.asyncio
    async def test_azure_tenant_id(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                azure_tenant_id="azure-tenant-id",
            )
        ):
            with patch(
                "airflow.providers.microsoft.azure.hooks.msgraph.ClientSecretCredential",
                autospec=True,
            ) as mock_credential_cls:
                hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
                await hook.get_async_conn()

                mock_credential_cls.assert_called_once()
                assert mock_credential_cls.call_args.kwargs.get("tenant_id") == "azure-tenant-id"

    @pytest.mark.asyncio
    async def test_proxies(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                proxies={"http": "http://proxy:80", "https": "https://proxy:80"},
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            with pytest.warns(AirflowProviderDeprecationWarning):
                actual = hook.get_conn()

            assert actual._http_client._mounts

    @pytest.mark.asyncio
    async def test_proxies_override_with_empty_dict(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                proxies={"http": "http://proxy:80", "https": "https://proxy:80"},
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", proxies={})

            with pytest.warns(AirflowProviderDeprecationWarning):
                actual = hook.get_conn()

            assert not actual._http_client._mounts

    def test_encoded_query_parameters(self):
        actual = KiotaRequestAdapterHook.encoded_query_parameters(
            query_parameters={"$expand": "reports,users,datasets,dataflows,dashboards", "$top": 5000},
        )

        assert actual == {"%24expand": "reports,users,datasets,dataflows,dashboards", "%24top": 5000}

    @pytest.mark.asyncio
    async def test_request_information_with_custom_host(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                host="api.fabric.microsoft.com",
                api_version="v1",
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            request_info = hook.request_information(url="myorg/admin/apps", query_parameters={"$top": 5000})
            request_adapter = await hook.get_async_conn()
            request_adapter.set_base_url_for_request_information(request_info)

            assert isinstance(request_info, RequestInformation)
            assert isinstance(request_adapter, HttpxRequestAdapter)
            assert request_info.url == "https://api.fabric.microsoft.com/v1/myorg/admin/apps?%24top=5000"

    @pytest.mark.asyncio
    async def test_throw_failed_responses_with_text_plain_content_type(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            response = Mock(spec=Response)
            response.headers = {"content-type": "text/plain"}
            response.status_code = 429
            response.content = b"TenantThrottleThresholdExceeded"
            response.is_success = False
            span = Mock(spec=Span)

            conn = await hook.get_async_conn()
            actual = await conn.get_root_parse_node(response, span, span)

            assert isinstance(actual, TextParseNode)
            assert actual.get_str_value() == "TenantThrottleThresholdExceeded"

    @pytest.mark.asyncio
    async def test_throw_failed_responses_with_application_json_content_type(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            response = Mock(spec=Response)
            response.headers = {"content-type": "application/json"}
            response.status_code = 429
            response.content = b'{"error": {"code": "TenantThrottleThresholdExceeded"}}'
            response.is_success = False
            span = Mock(spec=Span)

            conn = await hook.get_async_conn()
            actual = await conn.get_root_parse_node(response, span, span)

            assert isinstance(actual, JsonParseNode)
            error_code = actual.get_child_node("error").get_child_node("code").get_str_value()
            assert error_code == "TenantThrottleThresholdExceeded"

    @pytest.mark.asyncio
    async def test_run(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with patch_hook_and_request_adapter(response):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            actual = await hook.run(url="users")

            assert isinstance(actual, dict)
            assert actual == users

    @pytest.mark.asyncio
    async def test_paginated_run(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        next_users = load_json_from_resources(dirname(__file__), "..", "resources", "next_users.json")
        response = mock_json_response(200, users, next_users)

        with patch_hook_and_request_adapter(response):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            actual = await hook.paginated_run(url="users")

            assert isinstance(actual, list)
            assert actual == [users, next_users]

    @pytest.mark.asyncio
    async def test_paginated_run_refuses_cross_host_next_link(self):
        first_page = {
            "@odata.nextLink": "https://attacker.example/v1.0/users?$skiptoken=steal",
            "value": [{"id": "1"}],
        }
        response = mock_json_response(200, first_page)

        with patch_hook_and_request_adapter(response) as mocks:
            mock_get_http_response = mocks[-1]
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            with pytest.raises(ValueError, match="attacker.example"):
                await hook.paginated_run(url="users")

            # The off-host pagination link is refused before it is fetched, so the bearer
            # token is never sent to the attacker host.
            assert mock_get_http_response.call_count == 1

    @pytest.mark.asyncio
    async def test_build_request_adapter_masks_secrets(self):
        """Test that sensitive data is masked when building request adapter."""
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                password="my_secret_password",
                proxies={"http": "http://user:pass@proxy:3128"},
            )
        ):
            with patch("airflow.providers.microsoft.azure.hooks.msgraph.redact") as mock_redact:
                mock_redact.side_effect = lambda x, name=None: "***" if x else x

                hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
                await hook.get_async_conn()

                assert mock_redact.call_count >= 3
                mock_redact.assert_any_call({"http": "http://user:pass@proxy:3128"}, name="proxies")
                mock_redact.assert_any_call("my_secret_password", name="client_secret")

    def test_msal_returns_none_when_authority_matches_no_proxy(self):
        hook = KiotaRequestAdapterHook(conn_id="msgraph")

        proxies = {"http": "http://proxy", "no": "*.example.com"}
        authority = "api.example.com"

        result = hook.to_msal_proxies(authority, proxies)

        assert result is None

    def test_msal_returns_proxies_when_authority_does_not_match_no_proxy(self):
        hook = KiotaRequestAdapterHook(conn_id="msgraph")

        proxies = {"http": "http://proxy", "no": "*.example.com"}
        authority = "api.other.com"

        result = hook.to_msal_proxies(authority, proxies)

        assert result == proxies

    def test_msal_returns_proxies_when_no_authority_no_proxy_key(self):
        hook = KiotaRequestAdapterHook(conn_id="msgraph")

        proxies = {"no": "*example.com"}
        authority = None

        result = hook.to_msal_proxies(authority, proxies)

        assert result == proxies

    def test_msal_returns_proxies_when_no_authority_with_proxy_key(self):
        hook = KiotaRequestAdapterHook(conn_id="msgraph")

        proxies = {"http": "http://proxy"}
        authority = None

        result = hook.to_msal_proxies(authority, proxies)

        assert result == proxies

    def test_get_credentials_returns_async_client_secret_credential(self):
        """get_credentials must return an async context manager (azure.identity.aio credential)."""
        hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
        config = {"tenant_id": "tenant-id"}

        credentials = hook.get_credentials(
            login="client_id",
            password="client_secret",
            config=config,
            authority=None,
            verify=True,
            proxies=None,
        )

        assert isinstance(credentials, AbstractAsyncContextManager)

    def test_get_credentials_returns_async_certificate_credential(self):
        """get_credentials must return an async context manager when certificate_data is set."""
        import datetime

        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
        cert = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
            .sign(private_key, hashes.SHA256())
        )
        pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ) + cert.public_bytes(serialization.Encoding.PEM)

        hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
        config = {
            "tenant_id": "tenant-id",
            "certificate_data": pem.decode(),
        }

        credentials = hook.get_credentials(
            login="client_id",
            password=None,
            config=config,
            authority=None,
            verify=True,
            proxies=None,
        )

        assert isinstance(credentials, AbstractAsyncContextManager)

    @pytest.mark.asyncio
    async def test_get_async_conn_uses_async_credentials(self):
        """get_async_conn must build a request adapter backed by async credentials."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            request_adapter = await hook.get_async_conn()

            adapter: HttpxRequestAdapter = cast("HttpxRequestAdapter", request_adapter)
            # Reach into the auth provider chain to retrieve the underlying credential object.
            access_token_provider = adapter._authentication_provider.access_token_provider
            credentials = access_token_provider._credentials

            assert isinstance(credentials, AbstractAsyncContextManager)

    @pytest.mark.asyncio
    async def test_get_async_conn_rebuilds_adapter_when_http_client_is_closed(self):
        """get_async_conn evicts and rebuilds the adapter when the cached HTTP client is already closed."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            stale_adapter = Mock(spec=HttpxRequestAdapter)
            stale_adapter._http_client = Mock(spec=AsyncClient, is_closed=True)
            hook.cached_request_adapters[hook.conn_id] = (hook.api_version, stale_adapter)

            fresh_adapter = Mock(spec=HttpxRequestAdapter)
            fresh_adapter._http_client = Mock(is_closed=False)
            fresh_adapter.base_url = "https://graph.microsoft.com/v1.0"

            with patch.object(hook, "_build_request_adapter", return_value=("v1.0", fresh_adapter)):
                result = await hook.get_async_conn()

            assert result is fresh_adapter
            assert hook.cached_request_adapters[hook.conn_id] == ("v1.0", fresh_adapter)

    @pytest.mark.asyncio
    async def test_get_async_conn_rebuilds_adapter_when_credentials_session_is_closed(self):
        """get_async_conn evicts and rebuilds the adapter when the cached HTTP client is already closed."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            stale_adapter = Mock(spec=HttpxRequestAdapter)
            stale_adapter._http_client = Mock(spec=AsyncClient, is_closed=False)
            stale_adapter._authentication_provider = mock_authentication_provider(closed=True)
            hook.cached_request_adapters[hook.conn_id] = (hook.api_version, stale_adapter)

            fresh_adapter = Mock(spec=HttpxRequestAdapter)
            fresh_adapter._http_client = Mock(is_closed=False)
            fresh_adapter.base_url = "https://graph.microsoft.com/v1.0"

            with patch.object(hook, "_build_request_adapter", return_value=("v1.0", fresh_adapter)):
                result = await hook.get_async_conn()

            assert result is fresh_adapter
            assert hook.cached_request_adapters[hook.conn_id] == ("v1.0", fresh_adapter)

    @pytest.mark.asyncio
    async def test_get_async_conn_does_not_rebuild_adapter_when_transport_never_opened(self):
        """get_async_conn must keep the cached adapter when transport has never been opened (session is None)."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            adapter = Mock(spec=HttpxRequestAdapter)
            adapter._http_client = Mock(spec=AsyncClient, is_closed=False)
            adapter._authentication_provider = mock_authentication_provider()
            adapter.base_url = "https://graph.microsoft.com/v1.0"
            hook.cached_request_adapters[hook.conn_id] = (hook.api_version, adapter)

            result = await hook.get_async_conn()

            assert result is adapter

    @pytest.mark.asyncio
    async def test_send_request_invalidates_cache_and_raises_on_any_error(self):
        """send_request evicts the cached adapter and re-raises on any request error."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            adapter = Mock(spec=HttpxRequestAdapter)
            adapter._http_client = Mock(spec=AsyncClient, is_closed=False)
            adapter._authentication_provider = mock_authentication_provider(closed=False)
            adapter.base_url = "https://graph.microsoft.com/v1.0"
            adapter.send_no_response_content_async = AsyncMock(side_effect=RuntimeError("some error"))
            hook.cached_request_adapters[hook.conn_id] = (hook.api_version, adapter)

            with pytest.raises(RuntimeError, match="some error"):
                await hook.run(url="users")

            adapter.send_no_response_content_async.assert_called_once()
            assert hook.conn_id not in hook.cached_request_adapters

    def test_allowed_hosts_is_empty_list_when_not_configured(self):
        """An unset allowed_hosts/authority must yield []."""
        actual = KiotaRequestAdapterHook.get_allowed_hosts(None, {})

        assert actual == []

    def test_allowed_hosts_from_config(self):
        """A configured allowed_hosts string must be split into a list."""
        actual = KiotaRequestAdapterHook.get_allowed_hosts(
            None, {"allowed_hosts": "api.powerbi.com,login.microsoftonline.com"}
        )

        assert actual == ["api.powerbi.com", "login.microsoftonline.com"]


class TestKiotaRequestAdapterHookProtocol:
    """Test protocol handling in KiotaRequestAdapterHook."""

    def test_init_with_https_protocol(self):
        """Test that URL with https protocol is preserved."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host="https://api.powerbi.com")
            assert hook.host == "https://api.powerbi.com"

    def test_init_with_http_protocol(self):
        """Test that URL with http protocol is preserved."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host="http://api.powerbi.com")
            assert hook.host == "http://api.powerbi.com"

    def test_init_without_protocol(self):
        """Test that URL without protocol gets https added."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host="api.powerbi.com")
            assert hook.host == "https://api.powerbi.com"

    def test_init_with_none_host(self):
        """Test that None host remains None."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host=None)
            assert hook.host is None

    def test_init_with_empty_host(self):
        """Test that empty string host becomes None."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host="")
            assert hook.host is None

    def test_get_host_with_protocol_in_host_parameter(self):
        """Test get_host returns self.host when it already has protocol."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host="https://api.powerbi.com")
            connection = mock_connection(schema="https", host="graph.microsoft.com")
            actual = hook.get_host(connection)
            assert actual == "https://api.powerbi.com"

    def test_get_host_without_host_parameter_uses_connection(self):
        """Test get_host builds URL from connection when self.host is None."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host=None)
            connection = mock_connection(schema="https", host="graph.microsoft.com")
            actual = hook.get_host(connection)
            assert actual == "https://graph.microsoft.com"

    def test_get_host_fallback_to_default_when_no_connection_info(self):
        """Test get_host returns default when no host info available."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host=None)
            connection = mock_connection(schema=None, host=None)
            actual = hook.get_host(connection)
            assert actual == NationalClouds.Global.value

    def test_get_host_with_none_schema_uses_https_fallback(self):
        """Test get_host uses https fallback when connection.schema is None but host exists."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api", host=None)
            hook.host = "api.powerbi.com"
            connection = mock_connection(schema=None, host="dummy.com")
            actual = hook.get_host(connection)
            assert actual == "https://api.powerbi.com"

    def test_ensure_protocol_warns_when_adding_protocol(self):
        """Test that _ensure_protocol logs warning when adding protocol."""
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            with patch.object(hook.log, "warning") as mock_warning:
                result = hook._ensure_protocol("api.powerbi.com")

                assert result == "https://api.powerbi.com"
                mock_warning.assert_called_once()
                assert "missing protocol prefix" in mock_warning.call_args[0][0].lower()


class TestResponseHandler:
    def test_default_response_handler_when_json(self):
        users = load_json_from_resources(dirname(__file__), "..", "resources", "users.json")
        response = mock_json_response(200, users)

        actual = asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

        assert isinstance(actual, dict)
        assert actual == users

    def test_default_response_handler_when_not_json(self):
        response = mock_json_response(200, JSONDecodeError("", "", 0))

        actual = asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

        assert actual == {}

    def test_default_response_handler_when_content(self):
        users = load_file_from_resources(dirname(__file__), "..", "resources", "users.json").encode()
        response = mock_response(200, users)

        actual = asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

        assert isinstance(actual, bytes)
        assert actual == users

    def test_default_response_handler_when_unicode_content(self):
        dummy = load_file_from_resources(
            dirname(__file__), "..", "resources", "dummy.pdf", mode="rb", encoding=None
        )
        response = mock_response(200, dummy)
        response.json.side_effect = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

        actual = asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

        assert isinstance(actual, bytes)
        assert actual == dummy

    def test_default_response_handler_when_no_content_but_headers(self):
        response = mock_response(200, headers={"RequestId": "ffb6096e-d409-4826-aaeb-b5d4b165dc4d"})

        actual = asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

        assert isinstance(actual, dict)
        assert actual["requestid"] == "ffb6096e-d409-4826-aaeb-b5d4b165dc4d"

    def test_handle_response_async_when_bad_request(self):
        response = mock_json_response(400, {})

        with pytest.raises(AirflowBadRequest):
            asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

    def test_handle_response_async_when_not_found(self):
        response = mock_json_response(404, {})

        with pytest.raises(AirflowNotFoundException):
            asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

    def test_handle_response_async_when_internal_server_error(self):
        response = mock_json_response(500, {})

        with pytest.raises(AirflowException):
            asyncio.run(DefaultResponseHandler().handle_response_async(response, None))

    # TODO: Elad: review this after merging the bump 2.10 PR
    # We should not have specific provider test block the release
    @pytest.mark.xfail(reason="TODO: Remove")
    def test_when_provider_min_airflow_version_is_2_10_or_higher_remove_obsolete_code(self):
        """
        Once this test starts failing due to the fact that the minimum Airflow version is now 2.10.0 or higher
        for this provider, you should remove the obsolete code in the get_proxies method of the
        KiotaRequestAdapterHook and remove this test.  This test was added to make sure to not forget to
        remove the fallback code for backward compatibility with Airflow 2.9.x which isn't need anymore once
        this provider depends on Airflow 2.10.0 or higher.
        """
        min_airflow_version = get_provider_min_airflow_version("apache-airflow-providers-microsoft-azure")

        # Check if the current Airflow version is 2.10.0 or higher
        if min_airflow_version[0] >= 3 or (min_airflow_version[0] >= 2 and min_airflow_version[1] >= 10):
            method_source = inspect.getsource(KiotaRequestAdapterHook.get_proxies)
            raise AirflowProviderDeprecationWarning(
                f"Check TODO's to remove obsolete code in get_proxies method:\n\r\n\r\t\t\t{method_source}"
            )
