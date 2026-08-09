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
from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "adventureworks"
REFRESH_ID = "refresh-id"
CHECK_INTERVAL = 5
TIMEOUT = 120
REQUEST_TIMEOUT = 30
MODULE = "airflow.providers.microsoft.azure.operators.analysis_services"


def build_operator(
    *,
    wait_for_termination: bool = True,
    check_interval: float = CHECK_INTERVAL,
    timeout: float = TIMEOUT,
    request_timeout: float = REQUEST_TIMEOUT,
    deferrable: bool = False,
) -> AzureAnalysisServicesRefreshOperator:
    """Build an operator with standard test arguments."""
    return AzureAnalysisServicesRefreshOperator(
        task_id="refresh_model",
        server_name=SERVER_NAME,
        database=DATABASE,
        azure_analysis_services_conn_id=CONN_ID,
        refresh_type="calculate",
        wait_for_termination=wait_for_termination,
        check_interval=check_interval,
        timeout=timeout,
        request_timeout=request_timeout,
        deferrable=deferrable,
    )


class TestAzureAnalysisServicesRefreshOperator:
    @pytest.mark.parametrize(
        ("check_interval", "timeout", "request_timeout", "message"),
        [
            (0, TIMEOUT, REQUEST_TIMEOUT, "check_interval"),
            (CHECK_INTERVAL, 0, REQUEST_TIMEOUT, "timeout"),
            (CHECK_INTERVAL, TIMEOUT, 0, "request_timeout"),
        ],
    )
    def test_rejects_invalid_polling_arguments(self, check_interval, timeout, request_timeout, message):
        with pytest.raises(ValueError, match=message):
            build_operator(
                check_interval=check_interval,
                timeout=timeout,
                request_timeout=request_timeout,
            )

    def test_defines_template_fields(self):
        assert AzureAnalysisServicesRefreshOperator.template_fields == (
            "azure_analysis_services_conn_id",
            "server_name",
            "database",
            "refresh_type",
        )

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_builds_cached_hook_with_request_timeout(self, hook_class):
        operator = build_operator()

        first_hook = operator.hook
        second_hook = operator.hook

        hook_class.assert_called_once_with(
            azure_analysis_services_conn_id=CONN_ID,
            request_timeout=REQUEST_TIMEOUT,
        )
        assert first_hook is hook_class.return_value
        assert second_hook is first_hook

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_returns_refresh_id_without_waiting(self, hook_class):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID
        operator = build_operator(wait_for_termination=False)

        result = operator.execute(context={})

        assert result == REFRESH_ID
        hook_class.return_value.trigger_refresh.assert_called_once_with(
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_type="calculate",
        )
        hook_class.return_value.get_refresh_status.assert_not_called()
        hook_class.return_value.wait_for_refresh.assert_not_called()

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_waits_synchronously_for_success(self, hook_class):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID

        result = build_operator().execute(context={})

        assert result == REFRESH_ID
        hook_class.return_value.wait_for_refresh.assert_called_once_with(
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_id=REFRESH_ID,
            check_interval=CHECK_INTERVAL,
            timeout=TIMEOUT,
        )

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_propagates_synchronous_refresh_failure(self, hook_class):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID
        hook_class.return_value.wait_for_refresh.side_effect = AzureAnalysisServicesRefreshException(
            f"Azure Analysis Services refresh {REFRESH_ID} finished with status failed"
        )

        with pytest.raises(AzureAnalysisServicesRefreshException, match="status failed"):
            build_operator().execute(context={})

    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_returns_when_deferrable_refresh_already_succeeded(self, hook_class):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID
        hook_class.return_value.get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        result = build_operator(deferrable=True).execute(context={})

        assert result == REFRESH_ID
        hook_class.return_value.wait_for_refresh.assert_not_called()

    @pytest.mark.parametrize("status", sorted(AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES))
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_raises_when_deferrable_refresh_already_failed(self, hook_class, status):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID
        hook_class.return_value.get_refresh_status.return_value = status

        with pytest.raises(AzureAnalysisServicesRefreshException, match=f"status {status}"):
            build_operator(deferrable=True).execute(context={})

    @pytest.mark.parametrize(
        "status",
        [AzureAnalysisServicesRefreshStatus.NOT_STARTED, AzureAnalysisServicesRefreshStatus.IN_PROGRESS],
    )
    @mock.patch(f"{MODULE}.AzureAnalysisServicesHook", autospec=True)
    def test_defers_non_terminal_refresh_with_overall_timeout(self, hook_class, status):
        hook_class.return_value.trigger_refresh.return_value = REFRESH_ID
        hook_class.return_value.get_refresh_status.return_value = status
        operator = build_operator(deferrable=True)

        with pytest.raises(TaskDeferred) as deferred:
            operator.execute(context={})

        assert deferred.value.method_name == "execute_complete"
        assert deferred.value.timeout == timedelta(seconds=TIMEOUT)
        trigger = deferred.value.trigger
        assert isinstance(trigger, AzureAnalysisServicesRefreshTrigger)
        assert trigger.conn_id == CONN_ID
        assert trigger.server_name == SERVER_NAME
        assert trigger.database == DATABASE
        assert trigger.refresh_id == REFRESH_ID
        assert trigger.poke_interval == CHECK_INTERVAL
        assert trigger.request_timeout == REQUEST_TIMEOUT

    @mock.patch(f"{MODULE}.validate_refresh_event", autospec=True, return_value=REFRESH_ID)
    def test_execute_complete_validates_event(self, validate_event):
        event = {"status": "success"}

        result = build_operator().execute_complete(context={}, event=event)

        validate_event.assert_called_once_with(event)
        assert result == REFRESH_ID

    def test_execute_complete_rejects_malformed_event(self):
        with pytest.raises(AzureAnalysisServicesRefreshException, match="valid event"):
            build_operator().execute_complete(context={}, event=None)
