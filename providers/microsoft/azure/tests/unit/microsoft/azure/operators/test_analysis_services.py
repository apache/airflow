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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "testmodel"
REFRESH_ID = "11bf290a-346b-48b7-8973-c5df149337ff"
MODULE = "airflow.providers.microsoft.azure.operators.analysis_services"


@pytest.fixture
def operator():
    return AzureAnalysisServicesRefreshOperator(
        task_id="test_refresh",
        server_name=SERVER_NAME,
        database=DATABASE,
        azure_analysis_services_conn_id=CONN_ID,
    )


class TestAzureAnalysisServicesRefreshOperator:
    def test_execute_triggers_refresh_and_returns_id(self, operator):
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            mock_hook.wait_for_refresh.return_value = True
            result = operator.execute(context)

        assert result == REFRESH_ID
        mock_hook.trigger_refresh.assert_called_once_with(SERVER_NAME, DATABASE, "full")
        mock_hook.wait_for_refresh.assert_called_once()
        context["ti"].xcom_push.assert_called_once_with(key="refresh_id", value=REFRESH_ID)

    def test_execute_no_wait(self):
        operator = AzureAnalysisServicesRefreshOperator(
            task_id="test_refresh_no_wait",
            server_name=SERVER_NAME,
            database=DATABASE,
            azure_analysis_services_conn_id=CONN_ID,
            wait_for_termination=False,
        )
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            result = operator.execute(context)

        assert result == REFRESH_ID
        mock_hook.wait_for_refresh.assert_not_called()

    def test_execute_raises_on_failed_refresh(self, operator):
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            mock_hook.wait_for_refresh.return_value = False
            with pytest.raises(AzureAnalysisServicesRefreshException, match="failed or was cancelled"):
                operator.execute(context)

    def test_execute_deferrable_defers_when_not_terminal(self, operator):
        operator.deferrable = True
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook, patch.object(operator, "defer") as mock_defer:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.IN_PROGRESS
            operator.execute(context)

        mock_defer.assert_called_once()

    def test_execute_deferrable_succeeds_immediately(self, operator):
        operator.deferrable = True
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook, patch.object(operator, "defer") as mock_defer:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED
            result = operator.execute(context)

        mock_defer.assert_not_called()
        assert result == REFRESH_ID

    def test_execute_deferrable_raises_on_immediate_failure(self, operator):
        operator.deferrable = True
        context = {"ti": MagicMock()}

        with patch.object(operator, "hook") as mock_hook:
            mock_hook.trigger_refresh.return_value = REFRESH_ID
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.FAILED
            with pytest.raises(AzureAnalysisServicesRefreshException):
                operator.execute(context)

    def test_execute_complete_success(self, operator):
        context = MagicMock()
        event = {"status": "success", "message": "Refresh completed.", "refresh_id": REFRESH_ID}
        result = operator.execute_complete(context, event)
        assert result == REFRESH_ID

    def test_execute_complete_raises_on_error(self, operator):
        context = MagicMock()
        event = {"status": "error", "message": "Refresh failed.", "refresh_id": REFRESH_ID}
        with pytest.raises(AirflowException, match="Refresh failed."):
            operator.execute_complete(context, event)
