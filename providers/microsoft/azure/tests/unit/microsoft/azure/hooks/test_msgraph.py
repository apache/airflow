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
from json import JSONDecodeError
from os.path import dirname
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest
from httpx import Response
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
    DefaultResponseHandler,
    KiotaRequestAdapterHook,
)

from tests_common.test_utils.file_loading import load_file_from_resources, load_json_from_resources
from tests_common.test_utils.providers import get_provider_min_airflow_version
from unit.microsoft.azure.test_utils import (
    get_airflow_connection,
    mock_connection,
    mock_json_response,
    mock_response,
    patch_hook,
)

if TYPE_CHECKING:
    from azure.identity._internal.msal_credentials import MsalCredential
    from kiota_abstractions.authentication import BaseBearerTokenAuthenticationProvider
    from kiota_abstractions.request_adapter import RequestAdapter
    from kiota_authentication_azure.azure_identity_access_token_provider import (
        AzureIdentityAccessTokenProvider,
    )


class TestKiotaRequestAdapterHook:
    @staticmethod
    def assert_tenant_id(request_adapter: RequestAdapter, expected_tenant_id: str):
        adapter: HttpxRequestAdapter = cast("HttpxRequestAdapter", request_adapter)
        auth_provider: BaseBearerTokenAuthenticationProvider = cast(
            "BaseBearerTokenAuthenticationProvider",
            adapter._authentication_provider,
        )
        access_token_provider: AzureIdentityAccessTokenProvider = cast(
            "AzureIdentityAccessTokenProvider",
            auth_provider.access_token_provider,
        )
        credentials: MsalCredential = cast("MsalCredential", access_token_provider._credentials)
        tenant_id = credentials._tenant_id
        assert tenant_id == expected_tenant_id

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

    @pytest.mark.asyncio
    async def test_tenant_id(self):
        with patch_hook():
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            self.assert_tenant_id(actual, "tenant-id")

    @pytest.mark.asyncio
    async def test_azure_tenant_id(self):
        with patch_hook(
            side_effect=lambda conn_id: get_airflow_connection(
                conn_id=conn_id,
                azure_tenant_id="azure-tenant-id",
            )
        ):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = await hook.get_async_conn()

            self.assert_tenant_id(actual, "azure-tenant-id")

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
