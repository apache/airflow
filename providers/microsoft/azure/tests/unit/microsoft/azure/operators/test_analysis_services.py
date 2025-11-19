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

from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)

# Test constants
TEST_CONN_ID = "test_conn"
TEST_SERVER = "testserver"
TEST_MODEL = "testmodel"
TEST_REGION = "westus2"
TEST_REFRESH_ID = "refresh123"
TEST_TASK_ID = "test_task"


class TestAzureAnalysisServicesRefreshOperator:
    """Test cases for AzureAnalysisServicesRefreshOperator."""

    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_execute_with_default_parameters(self, mock_hook_class):
        """Test operator execution with default parameters."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            azure_analysis_services_conn_id=TEST_CONN_ID,
        )

        result = operator.execute(context={})

        # Verify hook was created with correct connection ID
        mock_hook_class.assert_called_once_with(azure_analysis_services_conn_id=TEST_CONN_ID)

        # Verify refresh_model was called with correct parameters
        mock_hook_instance.refresh_model.assert_called_once_with(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_type="full",
            commit_mode="transactional",
            max_parallelism=None,
            retry_count=None,
            objects=None,
        )

        # Verify return value
        assert result == TEST_REFRESH_ID

    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_execute_with_custom_parameters(self, mock_hook_class):
        """Test operator execution with all custom parameters."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        custom_objects = [{"table": "Sales", "partition": "Current"}]

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_type="dataOnly",
            commit_mode="partialBatch",
            max_parallelism=4,
            retry_count=2,
            objects=custom_objects,
            azure_analysis_services_conn_id=TEST_CONN_ID,
        )

        result = operator.execute(context={})

        # Verify refresh_model was called with custom parameters
        mock_hook_instance.refresh_model.assert_called_once_with(
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_type="dataOnly",
            commit_mode="partialBatch",
            max_parallelism=4,
            retry_count=2,
            objects=custom_objects,
        )

        assert result == TEST_REFRESH_ID

    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_hook_is_cached(self, mock_hook_class):
        """Test that hook property is cached and reused."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )

        # Access hook multiple times
        hook1 = operator.hook
        hook2 = operator.hook

        # Verify hook was only created once (cached)
        assert hook1 is hook2
        mock_hook_class.assert_called_once()

    def test_template_fields(self):
        """Test that template fields are properly defined."""
        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )

        assert "server_name" in operator.template_fields
        assert "model_name" in operator.template_fields
        assert "region" in operator.template_fields
        assert "refresh_type" in operator.template_fields

    def test_template_fields_renderers(self):
        """Test that template field renderers are properly defined."""
        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )

        assert operator.template_fields_renderers.get("objects") == "json"

    @pytest.mark.parametrize(
        "refresh_type",
        ["full", "automatic", "dataOnly", "calculate", "clearValues"],
    )
    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_refresh_types(self, mock_hook_class, refresh_type):
        """Test operator with different refresh types."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            refresh_type=refresh_type,
        )

        result = operator.execute(context={})

        # Verify correct refresh type was passed
        call_kwargs = mock_hook_instance.refresh_model.call_args[1]
        assert call_kwargs["refresh_type"] == refresh_type
        assert result == TEST_REFRESH_ID

    @pytest.mark.parametrize(
        "commit_mode",
        ["transactional", "partialBatch"],
    )
    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_commit_modes(self, mock_hook_class, commit_mode):
        """Test operator with different commit modes."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
            commit_mode=commit_mode,
        )

        result = operator.execute(context={})

        # Verify correct commit mode was passed
        call_kwargs = mock_hook_instance.refresh_model.call_args[1]
        assert call_kwargs["commit_mode"] == commit_mode
        assert result == TEST_REFRESH_ID

    @mock.patch("airflow.providers.microsoft.azure.operators.analysis_services.AzureAnalysisServicesHook")
    def test_default_connection_id(self, mock_hook_class):
        """Test that default connection ID is used when not specified."""
        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance.refresh_model.return_value = TEST_REFRESH_ID

        operator = AzureAnalysisServicesRefreshOperator(
            task_id=TEST_TASK_ID,
            server_name=TEST_SERVER,
            model_name=TEST_MODEL,
            region=TEST_REGION,
        )

        operator.execute(context={})

        # Verify default connection ID was used
        mock_hook_class.assert_called_once_with(
            azure_analysis_services_conn_id="azure_analysis_services_default"
        )
