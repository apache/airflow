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
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.sensors.analysis_services import AzureAnalysisServicesSensor
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "adventureworks"
REFRESH_ID = "refresh-id"
POKE_INTERVAL = 5
TIMEOUT = 120
REQUEST_TIMEOUT = 30
MODULE = "airflow.providers.microsoft.azure.sensors.analysis_services"


def build_sensor(
    *,
    request_timeout: float = REQUEST_TIMEOUT,
    deferrable: bool = False,
) -> AzureAnalysisServicesSensor:
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
        deferrable=deferrable,
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

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_builds_cached_hook_with_request_timeout(self, hook_class):
        sensor = build_sensor()

        first_hook = sensor.hook
        second_hook = sensor.hook

        hook_class.assert_called_once_with(
            azure_analysis_services_conn_id=CONN_ID,
            request_timeout=REQUEST_TIMEOUT,
        )
        assert first_hook is hook_class.return_value
        assert second_hook is first_hook

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (AzureAnalysisServicesRefreshStatus.NOT_STARTED, False),
            (AzureAnalysisServicesRefreshStatus.IN_PROGRESS, False),
            (AzureAnalysisServicesRefreshStatus.SUCCEEDED, True),
        ],
    )
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_poke_returns_completion_state(self, hook_class, status, expected):
        hook_class.return_value.get_refresh_status.return_value = status

        result = build_sensor().poke(context={})

        assert result is expected
        hook_class.return_value.get_refresh_status.assert_called_once_with(
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_id=REFRESH_ID,
        )

    @pytest.mark.parametrize("status", sorted(AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES))
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_poke_raises_for_failure_status(self, hook_class, status):
        hook_class.return_value.get_refresh_status.return_value = status

        with pytest.raises(AzureAnalysisServicesRefreshException, match=f"status {status}"):
            build_sensor().poke(context={})

    @mock.patch(f"{MODULE}.BaseSensorOperator.execute", autospec=True)
    def test_execute_uses_base_sensor_in_synchronous_mode(self, base_execute):
        sensor = build_sensor(deferrable=False)
        context = {}

        sensor.execute(context=context)

        base_execute.assert_called_once_with(sensor, context=context)

    @mock.patch.object(AzureAnalysisServicesSensor, "poke", autospec=True, return_value=True)
    def test_execute_returns_when_deferrable_refresh_already_succeeded(self, poke):
        sensor = build_sensor(deferrable=True)
        context = {}

        sensor.execute(context=context)

        poke.assert_called_once_with(sensor, context=context)

    @mock.patch.object(AzureAnalysisServicesSensor, "poke", autospec=True, return_value=False)
    def test_execute_defers_with_sensor_timeout(self, poke):
        sensor = build_sensor(deferrable=True)

        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute(context={})

        assert deferred.value.method_name == "execute_complete"
        assert deferred.value.timeout == timedelta(seconds=TIMEOUT)
        trigger = deferred.value.trigger
        assert isinstance(trigger, AzureAnalysisServicesRefreshTrigger)
        assert trigger.conn_id == CONN_ID
        assert trigger.server_name == SERVER_NAME
        assert trigger.database == DATABASE
        assert trigger.refresh_id == REFRESH_ID
        assert trigger.poke_interval == POKE_INTERVAL
        assert trigger.request_timeout == REQUEST_TIMEOUT

    @mock.patch(f"{MODULE}.validate_refresh_event", autospec=True, return_value=REFRESH_ID)
    def test_execute_complete_validates_event(self, validate_event):
        event = {"status": "success"}

        result = build_sensor().execute_complete(context={}, event=event)

        validate_event.assert_called_once_with(event)
        assert result is None

    def test_execute_complete_rejects_malformed_event(self):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="valid event"):
            build_sensor().execute_complete(context={}, event=None)
