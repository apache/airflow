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
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator
from airflow.providers.microsoft.azure.triggers.powerbi import PowerBITrigger

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
TASK_ID = "run_powerbi_operator"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"
CONFIG = {
    "task_id": TASK_ID,
    "conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
    "group_id": GROUP_ID,
    "dataset_id": DATASET_ID,
    "check_interval": 1,
    "timeout": 3,
}
NEW_REFRESH_REQUEST_ID = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

# Sample responses from PowerBI API
COMPLETED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
    # serviceExceptionJson is not present when status is not "Failed"
}

FAILED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.FAILED,
    PowerBIDatasetRefreshFields.ERROR.value: '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
}

IN_PROGRESS_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: NEW_REFRESH_REQUEST_ID,
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,  # endtime is not available.
}


@pytest.fixture
def mock_powerbi_hook():
    hook = PowerBIHook()
    return hook


# Test cases: refresh_details returns None, Terminal Status, In-progress Status
_get_latest_refresh_details_args = [
    COMPLETED_REFRESH_DETAILS,
    FAILED_REFRESH_DETAILS,
    IN_PROGRESS_REFRESH_DETAILS,
]


@pytest.mark.parametrize(
    argnames=("latest_refresh_details"),
    argvalues=_get_latest_refresh_details_args,
    ids=[
        (
            f"latest_refresh_status_{argval[PowerBIDatasetRefreshFields.STATUS.value]}_no_wait_for_termination"
            if argval is not None
            else "latest_refresh_status_None_no_wait_for_termination"
        )
        for argval in _get_latest_refresh_details_args
    ],
)
def test_execute_no_wait_for_termination(mock_powerbi_hook, latest_refresh_details):
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=False,
        **CONFIG,
    )
    operator.hook = mock_powerbi_hook
    context = {"ti": MagicMock()}

    mock_powerbi_hook.trigger_dataset_refresh = AsyncMock(return_value=NEW_REFRESH_REQUEST_ID)
    mock_powerbi_hook.get_refresh_details_by_refresh_id = AsyncMock(return_value=latest_refresh_details)

    operator.execute(context)

    assert mock_powerbi_hook.get_refresh_details_by_refresh_id.called
    assert context["ti"].xcom_push.call_count == 3
    assert context["ti"].xcom_push.call_args_list == [
        call(
            key="powerbi_dataset_refresh_id",
            value="5e2d9921-e91b-491f-b7e1-e7d8db49194c",
            execution_date=None,
        ),
        call(
            key="powerbi_dataset_refresh_status",
            value=latest_refresh_details.get("status"),
            execution_date=None,
        ),
        call(
            key="powerbi_dataset_refresh_error",
            value=latest_refresh_details.get("error", "None"),
            execution_date=None,
        ),
    ]


def test_execute_wait_for_termination_with_Deferrable(mock_powerbi_hook):
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=True,
        **CONFIG,
    )
    operator.hook = mock_powerbi_hook
    context = {"ti": MagicMock()}

    # Magic mock the hook methods
    mock_powerbi_hook.trigger_dataset_refresh = AsyncMock(return_value=NEW_REFRESH_REQUEST_ID)

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert mock_powerbi_hook.trigger_dataset_refresh.called
    assert isinstance(exc.value.trigger, PowerBITrigger), "Trigger is not a PowerBITriiger"

    assert context["ti"].xcom_push.call_count == 1
    assert context["ti"].xcom_push.call_args_list == [
        call(key="powerbi_dataset_refresh_id", value=NEW_REFRESH_REQUEST_ID, execution_date=None),
    ]


def test_powerbi_operator_async_execute_complete_success():
    """Assert that execute_complete log success message"""
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=True,
        **CONFIG,
    )
    context = {"ti": MagicMock()}
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(
            context=context,
            event={"status": "success", "message": "success", "dataset_refresh_id": "1234"},
        )
    mock_log_info.assert_called_with("success")
    assert context["ti"].xcom_push.call_count == 1


def test_powerbi_operator_async_execute_complete_fail():
    """Assert that execute_complete raise exception on error"""
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=True,
        **CONFIG,
    )
    context = {"ti": MagicMock()}
    with pytest.raises(AirflowException):
        operator.execute_complete(
            context=context,
            event={"status": "error", "message": "error", "dataset_refresh_id": "1234"},
        )
