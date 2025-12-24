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

from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.sensors.analysis_services import (
    AzureAnalysisServicesRefreshSensor,
)

# Test constants
TEST_CONN_ID = "test_conn"
TEST_SERVER = "testserver"
TEST_MODEL = "testmodel"
TEST_REGION = "westus2"
TEST_REFRESH_ID = "refresh123"
TEST_TASK_ID = "test_task"


class TestAzureAnalysisServicesRefreshSensor:
    """Test cases for AzureAnalysisServicesRefreshSensor."""

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_poke_succeeded(self, mock_hook_class):
        """Test poke returns True when refresh succeeds."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
            "endTime": "2025-01-01T00:10:00Z",
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
            azure_analysis_services_conn_id=TEST_CONN_ID,
        )

        result = sensor.poke(context={})

        assert result is True
        mock_hook_instance.get_refresh_status.assert_called_once_with(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_poke_in_progress(self, mock_hook_class):
        """Test poke returns False when refresh is in progress."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
            "startTime": "2025-01-01T00:00:00Z",
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        result = sensor.poke(context={})

        assert result is False

    @pytest.mark.parametrize(
        "failed_status",
        [
            AzureAnalysisServicesRefreshStatus.FAILED,
            AzureAnalysisServicesRefreshStatus.CANCELLED,
        ],
    )
    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_poke_failed_states(self, mock_hook_class, failed_status):
        """Test poke raises exception when refresh fails or is cancelled."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": failed_status,
            "error": "Operation failed",
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        with pytest.raises(AzureAnalysisServicesRefreshException, match="failed with status"):
            sensor.poke(context={})

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_poke_unexpected_status(self, mock_hook_class):
        """Test poke handles unexpected status by returning False."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": "unknown_status",
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        result = sensor.poke(context={})

        assert result is False

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_hook_is_cached(self, mock_hook_class):
        """Test that hook property is cached and reused."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        # Access hook multiple times
        hook1 = sensor.hook
        hook2 = sensor.hook

        # Verify hook was only created once (cached)
        assert hook1 is hook2
        mock_hook_class.assert_called_once()

    def test_template_fields(self):
        """Test that template fields are properly defined."""
        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        assert "server_name" in sensor.template_fields
        assert "model_name" in sensor.template_fields
        assert "region" in sensor.template_fields
        assert "refresh_id" in sensor.template_fields

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_default_connection_id(self, mock_hook_class):
        """Test that default connection ID is used when not specified."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
        )

        sensor.poke(context={})

        # Verify default connection ID was used
        mock_hook_class.assert_called_once_with(
            azure_analysis_services_conn_id="azure_analysis_services_default"
        )

    @mock.patch("airflow.providers.microsoft.azure.sensors.analysis_services.AzureAnalysisServicesHook")
    def test_poke_with_custom_connection_id(self, mock_hook_class):
        """Test sensor with custom connection ID."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.get_refresh_status.return_value = {
            "status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
        }

        sensor = AzureAnalysisServicesRefreshSensor(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_id=TEST_REFRESH_ID,
            azure_analysis_services_conn_id=TEST_CONN_ID,
        )

        result = sensor.poke(context={})

        assert result is True
        mock_hook_class.assert_called_once_with(azure_analysis_services_conn_id=TEST_CONN_ID)
