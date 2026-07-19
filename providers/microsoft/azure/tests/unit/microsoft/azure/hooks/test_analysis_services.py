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

from unittest.mock import MagicMock, patch

import pytest
import requests

from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "testmodel"
REFRESH_ID = "11bf290a-346b-48b7-8973-c5df149337ff"
REGION = "eastus.asazure.windows.net"
BASE_URL = f"https://{REGION}"
MODULE = "airflow.providers.microsoft.azure.hooks.analysis_services"


@pytest.fixture(autouse=True)
def setup_connections(create_mock_connections):
    create_mock_connections(
        Connection(
            conn_id=CONN_ID,
            conn_type="azure_analysis_services",
            host=REGION,
            login="clientId",
            password="clientSecret",
            extra={"tenantId": "tenantId"},
        )
    )


@pytest.fixture
def hook():
    return AzureAnalysisServicesHook(azure_analysis_services_conn_id=CONN_ID)


class TestAzureAnalysisServicesHook:
    def test_get_conn_client_secret(self, hook):
        with patch(f"{MODULE}.ClientSecretCredential") as mock_cred:
            hook.get_conn()
            mock_cred.assert_called_once_with(
                client_id="clientId",
                client_secret="clientSecret",
                tenant_id="tenantId",
            )

    def test_get_conn_requires_tenant_with_client_secret(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="no_tenant_conn",
                conn_type="azure_analysis_services",
                host=REGION,
                login="clientId",
                password="clientSecret",
            )
        )
        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id="no_tenant_conn")
        with pytest.raises(ValueError, match="Tenant ID is required"):
            hook.get_conn()

    def test_get_conn_default_credential(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="default_cred_conn",
                conn_type="azure_analysis_services",
                host=REGION,
            )
        )
        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id="default_cred_conn")
        with patch(f"{MODULE}.get_sync_default_azure_credential") as mock_cred:
            hook.get_conn()
            mock_cred.assert_called_once()

    def test_get_base_url_strips_trailing_slash(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="trailing_slash_conn",
                conn_type="azure_analysis_services",
                host=f"{REGION}/",
                login="clientId",
                password="clientSecret",
                extra={"tenantId": "tenantId"},
            )
        )
        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id="trailing_slash_conn")
        assert hook._get_base_url() == BASE_URL

    def test_get_base_url_requires_host(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="no_host_conn",
                conn_type="azure_analysis_services",
            )
        )
        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id="no_host_conn")
        with pytest.raises(ValueError, match="region endpoint"):
            hook._get_base_url()

    def test_trigger_refresh_returns_refresh_id(self, hook):
        mock_response = MagicMock()
        mock_response.headers = {
            "Location": f"{BASE_URL}/servers/{SERVER_NAME}/models/{DATABASE}/refreshes/{REFRESH_ID}"
        }
        mock_response.raise_for_status = MagicMock()

        with (
            patch(f"{MODULE}.requests.post", return_value=mock_response) as mock_post,
            patch.object(hook, "_get_headers", return_value={"Authorization": "Bearer token"}),
        ):
            result = hook.trigger_refresh(SERVER_NAME, DATABASE)

        assert result == REFRESH_ID
        mock_post.assert_called_once_with(
            f"{BASE_URL}/servers/{SERVER_NAME}/models/{DATABASE}/refreshes",
            json={"type": "full"},
            headers={"Authorization": "Bearer token"},
        )

    def test_trigger_refresh_custom_type(self, hook):
        mock_response = MagicMock()
        mock_response.headers = {
            "Location": f"{BASE_URL}/servers/{SERVER_NAME}/models/{DATABASE}/refreshes/{REFRESH_ID}"
        }
        mock_response.raise_for_status = MagicMock()

        with (
            patch(f"{MODULE}.requests.post", return_value=mock_response) as mock_post,
            patch.object(hook, "_get_headers", return_value={}),
        ):
            hook.trigger_refresh(SERVER_NAME, DATABASE, refresh_type="clearValues")

        _, kwargs = mock_post.call_args
        assert kwargs["json"] == {"type": "clearValues"}

    def test_trigger_refresh_raises_on_invalid_type(self, hook):
        with pytest.raises(ValueError, match="Invalid refresh_type"):
            hook.trigger_refresh(SERVER_NAME, DATABASE, refresh_type="bogus")

    def test_trigger_refresh_raises_on_http_error(self, hook):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")

        with (
            patch(f"{MODULE}.requests.post", return_value=mock_response),
            patch.object(hook, "_get_headers", return_value={}),
            pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to trigger refresh"),
        ):
            hook.trigger_refresh(SERVER_NAME, DATABASE)

    def test_trigger_refresh_raises_when_no_location(self, hook):
        mock_response = MagicMock()
        mock_response.headers = {"Location": ""}
        mock_response.raise_for_status = MagicMock()

        with (
            patch(f"{MODULE}.requests.post", return_value=mock_response),
            patch.object(hook, "_get_headers", return_value={}),
            pytest.raises(AzureAnalysisServicesRefreshException, match="refresh ID"),
        ):
            hook.trigger_refresh(SERVER_NAME, DATABASE)

    @pytest.mark.parametrize(
        ("api_status", "expected"),
        [
            ("succeeded", AzureAnalysisServicesRefreshStatus.SUCCEEDED),
            ("inProgress", AzureAnalysisServicesRefreshStatus.IN_PROGRESS),
            ("notStarted", AzureAnalysisServicesRefreshStatus.NOT_STARTED),
            ("timedOut", AzureAnalysisServicesRefreshStatus.TIMED_OUT),
        ],
    )
    def test_get_refresh_status(self, hook, api_status, expected):
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": api_status}
        mock_response.raise_for_status = MagicMock()

        with (
            patch(f"{MODULE}.requests.get", return_value=mock_response) as mock_get,
            patch.object(hook, "_get_headers", return_value={"Authorization": "Bearer token"}),
        ):
            status = hook.get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

        assert status == expected
        mock_get.assert_called_once_with(
            f"{BASE_URL}/servers/{SERVER_NAME}/models/{DATABASE}/refreshes/{REFRESH_ID}",
            headers={"Authorization": "Bearer token"},
        )

    def test_get_refresh_status_raises_on_http_error(self, hook):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with (
            patch(f"{MODULE}.requests.get", return_value=mock_response),
            patch.object(hook, "_get_headers", return_value={}),
            pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to get refresh status"),
        ):
            hook.get_refresh_status(SERVER_NAME, DATABASE, REFRESH_ID)

    @pytest.mark.parametrize(
        ("statuses", "expected_result"),
        [
            ([AzureAnalysisServicesRefreshStatus.SUCCEEDED], True),
            ([AzureAnalysisServicesRefreshStatus.FAILED], False),
            ([AzureAnalysisServicesRefreshStatus.CANCELLED], False),
            ([AzureAnalysisServicesRefreshStatus.TIMED_OUT], False),
            (
                [
                    AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
                    AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                ],
                True,
            ),
        ],
    )
    def test_wait_for_refresh(self, hook, statuses, expected_result):
        with patch.object(hook, "get_refresh_status", side_effect=statuses), patch("time.sleep"):
            result = hook.wait_for_refresh(SERVER_NAME, DATABASE, REFRESH_ID, check_interval=1)

        assert result == expected_result

    def test_wait_for_refresh_timeout(self, hook):
        with (
            patch.object(
                hook, "get_refresh_status", return_value=AzureAnalysisServicesRefreshStatus.IN_PROGRESS
            ),
            patch("time.sleep"),
            patch("time.monotonic", side_effect=[0, 0, 9999]),
        ):
            with pytest.raises(AzureAnalysisServicesRefreshException, match="Timeout"):
                hook.wait_for_refresh(SERVER_NAME, DATABASE, REFRESH_ID, check_interval=1, timeout=1)
