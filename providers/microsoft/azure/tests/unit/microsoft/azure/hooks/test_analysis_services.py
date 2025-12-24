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

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)

# Test constants
TEST_CONN_ID = "test_conn"
TEST_CLIENT_ID = "test_client_id"
TEST_CLIENT_SECRET = "test_secret"
TEST_TENANT_ID = "test_tenant_id"
TEST_REGION = "westus2"
TEST_SERVER = "testserver"
TEST_MODEL = "testmodel"
TEST_REFRESH_ID = "refresh123"
TEST_ACCESS_TOKEN = "fake_access_token"


class TestAzureAnalysisServicesHook:
    """Test cases for AzureAnalysisServicesHook."""

    @pytest.fixture
    def mock_connection_with_secret(self):
        """Create a mock connection with client secret authentication."""
        return Connection(
            conn_id=TEST_CONN_ID,
            conn_type="azure_analysis_services",
            login=TEST_CLIENT_ID,
            password=TEST_CLIENT_SECRET,
            extra=f'{{"tenantId": "{TEST_TENANT_ID}"}}',
        )

    @pytest.fixture
    def mock_connection_default_credential(self):
        """Create a mock connection using DefaultAzureCredential."""
        return Connection(
            conn_id=TEST_CONN_ID,
            conn_type="azure_analysis_services",
            extra=f'{{"region": "{TEST_REGION}"}}',
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_connection"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.ClientSecretCredential")
    def test_get_credential_with_client_secret(
        self, mock_credential_class, mock_get_connection, mock_connection_with_secret
    ):
        """Test authentication with client secret and verify caching."""
        mock_get_connection.return_value = mock_connection_with_secret
        mock_credential_instance = mock.Mock()
        mock_credential_class.return_value = mock_credential_instance

        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id=TEST_CONN_ID)
        credential = hook.get_credential()

        # Verify credential was created correctly
        mock_credential_class.assert_called_once_with(
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            tenant_id=TEST_TENANT_ID,
        )
        assert credential is mock_credential_instance

        # Verify caching works
        credential2 = hook.get_credential()
        assert credential2 is credential
        assert mock_credential_class.call_count == 1  # Still only called once

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_connection"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.get_sync_default_azure_credential")
    def test_get_credential_with_default_credential(
        self, mock_default_cred, mock_get_connection, mock_connection_default_credential
    ):
        """Test authentication with DefaultAzureCredential."""
        mock_get_connection.return_value = mock_connection_default_credential
        mock_credential_instance = mock.Mock()
        mock_default_cred.return_value = mock_credential_instance

        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id=TEST_CONN_ID)
        credential = hook.get_credential()

        mock_default_cred.assert_called_once_with(
            managed_identity_client_id=None,
            workload_identity_tenant_id=None,
        )
        assert credential is mock_credential_instance

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_connection"
    )
    def test_get_credential_missing_tenant_id(self, mock_get_connection):
        """Test that missing tenant ID raises ValueError when using client secret."""
        conn = Connection(
            conn_id=TEST_CONN_ID,
            conn_type="azure_analysis_services",
            login=TEST_CLIENT_ID,
            password=TEST_CLIENT_SECRET,
            extra="{}",
        )
        mock_get_connection.return_value = conn

        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id=TEST_CONN_ID)
        with pytest.raises(ValueError, match="Tenant ID is required"):
            hook.get_credential()

    def test_build_api_url_without_refresh_id(self):
        """Test building API URL for triggering refresh."""
        hook = AzureAnalysisServicesHook()
        url = hook._build_api_url(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )
        expected = (
            f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes"
        )
        assert url == expected

    def test_build_api_url_with_refresh_id(self):
        """Test building API URL for checking status."""
        hook = AzureAnalysisServicesHook()
        url = hook._build_api_url(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )
        expected = f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes/{TEST_REFRESH_ID}"
        assert url == expected

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.post")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_refresh_model_success(self, mock_get_token, mock_post):
        """Test successful model refresh with default parameters."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 202
        location_url = f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes/{TEST_REFRESH_ID}"
        mock_response.headers = {"Location": location_url}
        mock_post.return_value = mock_response

        hook = AzureAnalysisServicesHook()
        refresh_id = hook.refresh_model(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )

        assert refresh_id == TEST_REFRESH_ID

        # Verify POST call
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args[1]

        # Check URL
        expected_url = (
            f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes"
        )
        assert mock_post.call_args[0][0] == expected_url

        # Check headers
        assert call_kwargs["headers"]["Authorization"] == f"Bearer {TEST_ACCESS_TOKEN}"
        assert call_kwargs["headers"]["Content-Type"] == "application/json"

        # Check request body uses correct defaults
        body = call_kwargs["json"]
        assert body["Type"] == "full"
        assert body["CommitMode"] == "transactional"
        assert body["Objects"] == []
        assert "MaxParallelism" not in body
        assert "RetryCount" not in body

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.post")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_refresh_model_with_custom_options(self, mock_get_token, mock_post):
        """Test refresh with custom parameters."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 202
        location_url = f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes/custom456"
        mock_response.headers = {"Location": location_url}
        mock_post.return_value = mock_response

        custom_objects = [{"table": "Sales"}]
        hook = AzureAnalysisServicesHook()
        refresh_id = hook.refresh_model(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_type="dataOnly",
            commit_mode="partialBatch",
            max_parallelism=4,
            retry_count=2,
            objects=custom_objects,
        )

        assert refresh_id == "custom456"

        body = mock_post.call_args[1]["json"]
        assert body["Type"] == "dataOnly"
        assert body["CommitMode"] == "partialBatch"
        assert body["MaxParallelism"] == 4
        assert body["RetryCount"] == 2
        assert body["Objects"] == custom_objects

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.post")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_refresh_model_missing_location_header(self, mock_get_token, mock_post):
        """Test that missing Location header raises AirflowException."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 202
        mock_response.headers = {}
        mock_post.return_value = mock_response

        hook = AzureAnalysisServicesHook()
        with pytest.raises(AirflowException, match="no Location header"):
            hook.refresh_model(
                server_name=TEST_SERVER,
                model_name=TEST_MODEL,
                region=TEST_REGION,
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.post")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_refresh_model_api_error(self, mock_get_token, mock_post):
        """Test that API error (non-202) raises AirflowException."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 400
        mock_response.text = "Invalid request"
        mock_post.return_value = mock_response

        hook = AzureAnalysisServicesHook()
        with pytest.raises(AirflowException, match="Failed to start refresh operation"):
            hook.refresh_model(
                server_name=TEST_SERVER,
                model_name=TEST_MODEL,
                region=TEST_REGION,
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.get")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_get_refresh_status_success(self, mock_get_token, mock_get):
        """Test getting refresh status successfully."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 200
        expected_status = {
            "status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
            "startTime": "2025-01-01T00:00:00Z",
            "type": "full",
        }
        mock_response.json.return_value = expected_status
        mock_get.return_value = mock_response

        hook = AzureAnalysisServicesHook()
        status = hook.get_refresh_status(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        assert status["status"] == AzureAnalysisServicesRefreshStatus.IN_PROGRESS
        assert status["startTime"] == "2025-01-01T00:00:00Z"
        assert status["type"] == "full"

        # Verify GET call
        mock_get.assert_called_once()
        expected_url = f"https://{TEST_REGION}.asazure.windows.net/servers/{TEST_SERVER}/models/{TEST_MODEL}/refreshes/{TEST_REFRESH_ID}"
        assert mock_get.call_args[0][0] == expected_url
        assert mock_get.call_args[1]["headers"]["Authorization"] == f"Bearer {TEST_ACCESS_TOKEN}"

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.requests.get")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook._get_access_token"
    )
    def test_get_refresh_status_api_error(self, mock_get_token, mock_get):
        """Test that API error when getting status raises AirflowException."""
        mock_get_token.return_value = TEST_ACCESS_TOKEN
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_response.text = "Not found"
        mock_get.return_value = mock_response

        hook = AzureAnalysisServicesHook()
        with pytest.raises(AirflowException, match="Failed to get refresh status"):
            hook.get_refresh_status(
                server_name=TEST_SERVER,
                model_name=TEST_MODEL,
                region=TEST_REGION,
                refresh_id="nonexistent",
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.time.sleep")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_refresh_status"
    )
    def test_wait_for_refresh_completion_success(self, mock_get_status, mock_sleep):
        """Test waiting for successful completion."""
        mock_get_status.side_effect = [
            {"status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS},
            {"status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS},
            {
                "status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                "endTime": "2025-01-01T00:05:00Z",
            },
        ]

        hook = AzureAnalysisServicesHook()
        result = hook.wait_for_refresh_completion(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
            check_interval=1,
        )

        assert result["status"] == AzureAnalysisServicesRefreshStatus.SUCCEEDED
        assert mock_get_status.call_count == 3
        assert mock_sleep.call_count == 2

    @pytest.mark.parametrize(
        "failed_status",
        [
            AzureAnalysisServicesRefreshStatus.FAILED,
            AzureAnalysisServicesRefreshStatus.CANCELLED,
        ],
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.time.sleep")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_refresh_status"
    )
    def test_wait_for_refresh_completion_failure(self, mock_get_status, mock_sleep, failed_status):
        """Test waiting for operation that fails or is cancelled."""
        mock_get_status.return_value = {
            "status": failed_status,
            "error": "Operation failed",
        }

        hook = AzureAnalysisServicesHook()
        with pytest.raises(AzureAnalysisServicesRefreshException, match="failed with status"):
            hook.wait_for_refresh_completion(
                server_name=TEST_SERVER,
                model_name=TEST_MODEL,
                region=TEST_REGION,
                refresh_id=TEST_REFRESH_ID,
                check_interval=1,
            )

    @mock.patch("airflow.providers.microsoft.azure.hooks.analysis_services.time.sleep")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.analysis_services.AzureAnalysisServicesHook.get_refresh_status"
    )
    def test_wait_for_refresh_completion_timeout(self, mock_get_status, mock_sleep):
        """Test timeout while waiting for completion."""
        mock_get_status.return_value = {"status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS}

        hook = AzureAnalysisServicesHook()
        with pytest.raises(AirflowException, match="did not complete within"):
            hook.wait_for_refresh_completion(
                server_name=TEST_SERVER,
                model_name=TEST_MODEL,
                region=TEST_REGION,
                refresh_id=TEST_REFRESH_ID,
                check_interval=1,
                timeout=2,
            )


class TestAzureAnalysisServicesRefreshStatus:
    """Test the status constants class."""

    def test_status_constants_values(self):
        """Test that status constants have correct values."""
        assert AzureAnalysisServicesRefreshStatus.IN_PROGRESS == "inProgress"
        assert AzureAnalysisServicesRefreshStatus.SUCCEEDED == "succeeded"
        assert AzureAnalysisServicesRefreshStatus.FAILED == "failed"
        assert AzureAnalysisServicesRefreshStatus.CANCELLED == "cancelled"

    def test_terminal_statuses_set(self):
        """Test terminal statuses set contains correct statuses."""
        terminal = AzureAnalysisServicesRefreshStatus.TERMINAL_STATUSES
        assert AzureAnalysisServicesRefreshStatus.SUCCEEDED in terminal
        assert AzureAnalysisServicesRefreshStatus.FAILED in terminal
        assert AzureAnalysisServicesRefreshStatus.CANCELLED in terminal
        assert AzureAnalysisServicesRefreshStatus.IN_PROGRESS not in terminal
        assert len(terminal) == 3

    def test_failure_statuses_set(self):
        """Test failure statuses set contains correct statuses."""
        failures = AzureAnalysisServicesRefreshStatus.FAILURE_STATES
        assert AzureAnalysisServicesRefreshStatus.FAILED in failures
        assert AzureAnalysisServicesRefreshStatus.CANCELLED in failures
        assert AzureAnalysisServicesRefreshStatus.SUCCEEDED not in failures
        assert len(failures) == 2
