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

from datetime import timedelta
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.sensors.analysis_services import AzureAnalysisServicesSensor
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

from tests_common.test_utils.operators.run_deferrable import execute_operator

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "adventureworks"
REFRESH_ID = "refresh-id"
POKE_INTERVAL = 5
TIMEOUT = 120
REQUEST_TIMEOUT = 30


def build_sensor(*, request_timeout: float = REQUEST_TIMEOUT) -> AzureAnalysisServicesSensor:
    """Build a sensor with standard test arguments."""
    return AzureAnalysisServicesSensor(
        task_id="wait_for_refresh",
        server_name=SERVER_NAME,
        database=DATABASE,
        refresh_id=REFRESH_ID,
        azure_analysis_services_conn_id=CONN_ID,
        poke_interval=POKE_INTERVAL,
        timeout=TIMEOUT,
        request_timeout=request_timeout,
    )


class TestAzureAnalysisServicesSensor:
    def test_rejects_invalid_request_timeout(self):
        with pytest.raises(ValueError, match="request_timeout must be greater than zero"):
            build_sensor(request_timeout=0)

    def test_defines_template_fields(self):
        assert AzureAnalysisServicesSensor.template_fields == (
            "azure_analysis_services_conn_id",
            "server_name",
            "database",
            "refresh_id",
        )

    def test_execute_defers_with_provided_refresh_id(self):
        with pytest.raises(TaskDeferred) as deferred:
            build_sensor().execute(context={})

        trigger = deferred.value.trigger
        assert isinstance(trigger, AzureAnalysisServicesRefreshTrigger)
        assert trigger.refresh_id == REFRESH_ID
        assert trigger.poke_interval == POKE_INTERVAL
        assert trigger.request_timeout == REQUEST_TIMEOUT
        assert deferred.value.method_name == "execute_complete"

    def test_execute_passes_sensor_timeout_to_defer(self):
        with pytest.raises(TaskDeferred) as deferred:
            build_sensor().execute(context={})

        assert deferred.value.timeout == timedelta(seconds=TIMEOUT)

    @mock.patch.object(AzureAnalysisServicesHook, "get_refresh_status", autospec=True)
    def test_execute_sensor_full_lifecycle(self, get_refresh_status):
        get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        result, events = execute_operator(build_sensor())

        assert result is None
        get_refresh_status.assert_awaited_once_with(
            mock.ANY,
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_id=REFRESH_ID,
        )
        assert [event.payload for event in events] == [
            {
                "status": "success",
                "refresh_status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                "message": f"Refresh {REFRESH_ID} completed successfully",
                "refresh_id": REFRESH_ID,
            }
        ]

    @pytest.mark.parametrize(
        "event",
        [
            None,
            {"status": "success", "refresh_status": AzureAnalysisServicesRefreshStatus.FAILED},
            {"status": "error", "message": "refresh failed", "refresh_id": REFRESH_ID},
        ],
    )
    def test_execute_complete_rejects_malformed_event(self, event):
        with pytest.raises(AzureAnalysisServicesRefreshException):
            build_sensor().execute_complete(context={}, event=event)
