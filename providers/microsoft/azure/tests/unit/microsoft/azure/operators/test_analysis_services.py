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
    RefreshType,
)
from airflow.providers.microsoft.azure.operators.analysis_services import (
    AzureAnalysisServicesRefreshOperator,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

from tests_common.test_utils.operators.run_deferrable import execute_operator

CONN_ID = "azure_analysis_services_test"
SERVER_NAME = "testserver"
DATABASE = "adventureworks"
REFRESH_ID = "refresh-id"
REFRESH_TYPE: RefreshType = "calculate"
CHECK_INTERVAL = 5
TIMEOUT = 120
REQUEST_TIMEOUT = 30


def build_operator(
    *,
    wait_for_termination: bool = True,
    check_interval: float = CHECK_INTERVAL,
    timeout: float = TIMEOUT,
    request_timeout: float = REQUEST_TIMEOUT,
) -> AzureAnalysisServicesRefreshOperator:
    """Build an operator with standard test arguments."""
    return AzureAnalysisServicesRefreshOperator(
        task_id="refresh_model",
        server_name=SERVER_NAME,
        database=DATABASE,
        azure_analysis_services_conn_id=CONN_ID,
        refresh_type=REFRESH_TYPE,
        wait_for_termination=wait_for_termination,
        check_interval=check_interval,
        timeout=timeout,
        request_timeout=request_timeout,
    )


def build_context() -> dict:
    """Build a context whose task instance records XCom pushes."""
    return {"ti": mock.MagicMock()}


def success_event(refresh_status: str | None) -> dict:
    return {
        "status": "success",
        "refresh_status": refresh_status,
        "message": "ok",
        "refresh_id": REFRESH_ID,
    }


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

    def test_execute_defers_without_refresh_id(self):
        with pytest.raises(TaskDeferred) as deferred:
            build_operator().execute(context=build_context())

        trigger = deferred.value.trigger
        assert isinstance(trigger, AzureAnalysisServicesRefreshTrigger)
        assert trigger.refresh_id is None
        assert trigger.refresh_type == REFRESH_TYPE
        assert trigger.poke_interval == CHECK_INTERVAL
        assert trigger.request_timeout == REQUEST_TIMEOUT
        assert deferred.value.method_name == "handle_refresh"
        # The timeout covers waiting for the refresh, so it only starts on the second deferral.
        assert deferred.value.timeout is None

    @mock.patch.object(AzureAnalysisServicesHook, "get_refresh_status", autospec=True)
    @mock.patch.object(AzureAnalysisServicesHook, "trigger_refresh", autospec=True)
    def test_execute_operator_full_lifecycle(self, trigger_refresh, get_refresh_status):
        trigger_refresh.return_value = REFRESH_ID
        get_refresh_status.return_value = AzureAnalysisServicesRefreshStatus.SUCCEEDED

        result, events = execute_operator(build_operator())

        assert result == REFRESH_ID
        trigger_refresh.assert_awaited_once_with(
            mock.ANY,
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_type=REFRESH_TYPE,
        )
        get_refresh_status.assert_awaited_once_with(
            mock.ANY,
            server_name=SERVER_NAME,
            database=DATABASE,
            refresh_id=REFRESH_ID,
        )
        assert [event.payload for event in events] == [
            {
                "status": "success",
                "refresh_status": None,
                "message": f"Refresh {REFRESH_ID} has been triggered",
                "refresh_id": REFRESH_ID,
            },
            {
                "status": "success",
                "refresh_status": AzureAnalysisServicesRefreshStatus.SUCCEEDED,
                "message": f"Refresh {REFRESH_ID} completed successfully",
                "refresh_id": REFRESH_ID,
            },
        ]

    def test_handle_refresh_defers_again_with_refresh_id(self):
        operator = build_operator()

        with pytest.raises(TaskDeferred) as deferred:
            operator.handle_refresh(context=build_context(), event=success_event(None))

        trigger = deferred.value.trigger
        # The serialised refresh ID is what lets polling survive a triggerer restart.
        assert trigger.refresh_id == REFRESH_ID
        assert trigger.serialize()[1]["refresh_id"] == REFRESH_ID
        assert deferred.value.method_name == "execute_complete"

    def test_handle_refresh_passes_timeout_to_polling_defer(self):
        with pytest.raises(TaskDeferred) as deferred:
            build_operator().handle_refresh(context=build_context(), event=success_event(None))

        assert deferred.value.timeout == timedelta(seconds=TIMEOUT)

    def test_handle_refresh_pushes_refresh_id_to_xcom(self):
        operator = build_operator(wait_for_termination=False)
        context = build_context()

        operator.handle_refresh(context=context, event=success_event(None))

        context["ti"].xcom_push.assert_called_once_with(key="refresh_model.refresh_id", value=REFRESH_ID)

    def test_handle_refresh_returns_without_waiting(self):
        result = build_operator(wait_for_termination=False).handle_refresh(
            context=build_context(), event=success_event(None)
        )

        assert result == REFRESH_ID

    def test_handle_refresh_raises_on_error_event(self):
        event = {
            "status": "error",
            "refresh_status": None,
            "message": "Failed to trigger an Azure Analysis Services model refresh",
            "refresh_id": None,
        }

        with pytest.raises(AzureAnalysisServicesRefreshException, match="Failed to trigger"):
            build_operator().handle_refresh(context=build_context(), event=event)

    def test_execute_complete_returns_refresh_id(self):
        result = build_operator().execute_complete(
            context=build_context(),
            event=success_event(AzureAnalysisServicesRefreshStatus.SUCCEEDED),
        )

        assert result == REFRESH_ID

    @pytest.mark.parametrize(
        "event",
        [None, {"status": "success", "refresh_status": AzureAnalysisServicesRefreshStatus.FAILED}],
    )
    def test_execute_complete_rejects_malformed_event(self, event):
        with pytest.raises(AzureAnalysisServicesRefreshException):
            build_operator().execute_complete(context=build_context(), event=event)
