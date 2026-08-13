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

from airflow.providers.microsoft.azure.hooks.analysis_services import AzureAnalysisServicesRefreshStatus
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)
from airflow.triggers.base import TriggerEvent

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "testmodel"
REFRESH_ID = "11bf290a-346b-48b7-8973-c5df149337ff"
POKE_INTERVAL = 5
MODULE = "airflow.providers.microsoft.azure.triggers.analysis_services"


class TestAzureAnalysisServicesRefreshTrigger:
    TRIGGER = AzureAnalysisServicesRefreshTrigger(
        conn_id=CONN_ID,
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_id=REFRESH_ID,
        poke_interval=POKE_INTERVAL,
    )

    def test_serialization(self):
        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == f"{MODULE}.AzureAnalysisServicesRefreshTrigger"
        assert kwargs == {
            "conn_id": CONN_ID,
            "server_name": SERVER_NAME,
            "database": DATABASE,
            "refresh_id": REFRESH_ID,
            "poke_interval": POKE_INTERVAL,
        }

    @pytest.mark.asyncio
    async def test_run_yields_success_on_succeeded(self):
        with patch(f"{MODULE}.AzureAnalysisServicesHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED
            mock_hook_cls.return_value = mock_hook

            events = [e async for e in self.TRIGGER.run()]

        assert len(events) == 1
        assert events[0] == TriggerEvent(
            {
                "status": "success",
                "message": f"Refresh {REFRESH_ID} completed successfully.",
                "refresh_id": REFRESH_ID,
            }
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [
            AzureAnalysisServicesRefreshStatus.FAILED,
            AzureAnalysisServicesRefreshStatus.CANCELLED,
            AzureAnalysisServicesRefreshStatus.TIMED_OUT,
        ],
    )
    async def test_run_yields_error_on_failure_status(self, status):
        with patch(f"{MODULE}.AzureAnalysisServicesHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook.get_refresh_status.return_value = status
            mock_hook_cls.return_value = mock_hook

            events = [e async for e in self.TRIGGER.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert REFRESH_ID in events[0].payload["message"]

    @pytest.mark.asyncio
    async def test_run_polls_until_terminal_state(self):
        statuses = [
            AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
            AzureAnalysisServicesRefreshStatus.IN_PROGRESS,
            AzureAnalysisServicesRefreshStatus.SUCCEEDED,
        ]
        with patch(f"{MODULE}.AzureAnalysisServicesHook") as mock_hook_cls, patch(f"{MODULE}.asyncio.sleep"):
            mock_hook = MagicMock()
            mock_hook.get_refresh_status.side_effect = statuses
            mock_hook_cls.return_value = mock_hook

            events = [e async for e in self.TRIGGER.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert mock_hook.get_refresh_status.call_count == 3

    @pytest.mark.asyncio
    async def test_run_yields_error_on_exception(self):
        with patch(f"{MODULE}.AzureAnalysisServicesHook") as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook.get_refresh_status.side_effect = Exception("Connection error")
            mock_hook_cls.return_value = mock_hook

            events = [e async for e in self.TRIGGER.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "Connection error" in events[0].payload["message"]
