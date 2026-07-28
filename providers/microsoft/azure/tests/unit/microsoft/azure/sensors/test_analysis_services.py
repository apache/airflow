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
from airflow.providers.microsoft.azure.sensors.analysis_services import AzureAnalysisServicesSensor

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "testmodel"
REFRESH_ID = "11bf290a-346b-48b7-8973-c5df149337ff"


@pytest.fixture
def sensor():
    return AzureAnalysisServicesSensor(
        task_id="test_sensor",
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_id=REFRESH_ID,
        azure_analysis_services_conn_id=CONN_ID,
    )


class TestAzureAnalysisServicesSensor:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (AzureAnalysisServicesRefreshStatus.SUCCEEDED, True),
            (AzureAnalysisServicesRefreshStatus.IN_PROGRESS, False),
            (AzureAnalysisServicesRefreshStatus.NOT_STARTED, False),
        ],
    )
    def test_poke_returns_correct_value(self, sensor, status, expected):
        with patch.object(sensor, "hook") as mock_hook:
            mock_hook.get_refresh_status.return_value = status
            result = sensor.poke(context=MagicMock())
        assert result == expected

    @pytest.mark.parametrize(
        "status",
        [
            AzureAnalysisServicesRefreshStatus.FAILED,
            AzureAnalysisServicesRefreshStatus.CANCELLED,
            AzureAnalysisServicesRefreshStatus.TIMED_OUT,
        ],
    )
    def test_poke_raises_on_failure_status(self, sensor, status):
        with patch.object(sensor, "hook") as mock_hook:
            mock_hook.get_refresh_status.return_value = status
            with pytest.raises(AzureAnalysisServicesRefreshException, match=REFRESH_ID):
                sensor.poke(context=MagicMock())

    def test_execute_deferrable_defers_when_not_succeeded(self, sensor):
        sensor.deferrable = True

        with patch.object(sensor, "hook") as mock_hook, patch.object(sensor, "defer") as mock_defer:
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.IN_PROGRESS
            sensor.execute(context=MagicMock())

        mock_defer.assert_called_once()

    def test_execute_deferrable_skips_defer_when_already_succeeded(self, sensor):
        sensor.deferrable = True

        with patch.object(sensor, "hook") as mock_hook, patch.object(sensor, "defer") as mock_defer:
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED
            sensor.execute(context=MagicMock())

        mock_defer.assert_not_called()

    def test_execute_complete_success(self, sensor):
        event = {"status": "success", "message": "Refresh completed.", "refresh_id": REFRESH_ID}
        sensor.execute_complete(context=MagicMock(), event=event)

    def test_execute_complete_raises_on_error(self, sensor):
        event = {"status": "error", "message": "Refresh failed.", "refresh_id": REFRESH_ID}
        with pytest.raises(AirflowException, match="Refresh failed."):
            sensor.execute_complete(context=MagicMock(), event=event)
