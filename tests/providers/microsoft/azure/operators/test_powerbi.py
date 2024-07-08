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

from unittest.mock import MagicMock, call

import pytest

from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.operators.powerbi import PowerBIDatasetRefreshOperator

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
TASK_ID = "run_powerbi_operator"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"
CONFIG = {
    "task_id": TASK_ID,
    "powerbi_conn_id": DEFAULT_CONNECTION_CLIENT_SECRET,
    "group_id": GROUP_ID,
    "dataset_id": DATASET_ID,
    "check_interval": 1,
    "timeout": 3,
}

# Sample responses from PowerBI API
COMPLETED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
    # serviceExceptionJson is not present when status is not "Failed"
}

FAILED_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: "11bf290a-346b-48b7-8973-c5df149337ff",
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.FAILED,
    PowerBIDatasetRefreshFields.ERROR.value: '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
}

IN_PROGRESS_REFRESH_DETAILS = {
    PowerBIDatasetRefreshFields.REQUEST_ID.value: "6b6536c1-cfcb-4148-9c21-402c3f5241e4",
    PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,  # endtime is not available.
}


@pytest.fixture
def mock_powerbi_hook():
    hook = PowerBIHook()
    return hook


# Test cases: refresh_details returns None, Terminal Status, In-progress Status
_get_latest_refresh_details_args = [
    (None),
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
        force_refresh=False,
        **CONFIG,
    )
    operator.hook = mock_powerbi_hook
    context = {"ti": MagicMock()}
    new_refresh_request_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
    mock_powerbi_hook.get_latest_refresh_details = MagicMock(return_value=latest_refresh_details)
    mock_powerbi_hook.trigger_dataset_refresh = MagicMock(return_value=new_refresh_request_id)
    mock_powerbi_hook.get_refresh_details_by_request_id = MagicMock(
        return_value={
            PowerBIDatasetRefreshFields.REQUEST_ID.value: new_refresh_request_id,
            PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
            # serviceExceptionJson is not present when status is not "Failed"
        }
    )
    mock_powerbi_hook.wait_for_dataset_refresh_status = MagicMock(return_value=True)
    operator.execute(context)

    if (
        latest_refresh_details is None
        or latest_refresh_details[PowerBIDatasetRefreshFields.STATUS.value]
        in PowerBIDatasetRefreshStatus.TERMINAL_STATUSES
    ):
        assert mock_powerbi_hook.get_latest_refresh_details.called
        assert mock_powerbi_hook.trigger_dataset_refresh.called
    else:
        assert not mock_powerbi_hook.trigger_dataset_refresh.called

    assert not mock_powerbi_hook.wait_for_dataset_refresh_status.called
    assert mock_powerbi_hook.get_refresh_details_by_request_id.called
    assert context["ti"].xcom_push.call_count == 4
    assert context["ti"].xcom_push.call_args_list == [
        call(key="powerbi_dataset_refresh_id", value=new_refresh_request_id),
        call(key="powerbi_dataset_refresh_status", value=PowerBIDatasetRefreshStatus.COMPLETED),
        call(key="powerbi_dataset_refresh_end_time", value="2024-04-15T20:14:08.1458221Z"),
        call(key="powerbi_dataset_refresh_error", value="None"),
    ]


_get_wait_for_status_args = [(True), (False)]


@pytest.mark.parametrize(
    argnames=("wait_for_status_return_value"),
    argvalues=_get_wait_for_status_args,
    ids=[f"wait_for_status_return_value_{argval}" for argval in _get_wait_for_status_args],
)
def test_execute_wait_for_termination_preexisting_refresh_going_on(
    mock_powerbi_hook, wait_for_status_return_value
):
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=True,
        force_refresh=True,
        **CONFIG,
    )
    preexisting_refresh_request_id = "6b6536c1-cfcb-4148-9c21-402c3f5241e4"
    new_refresh_request_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
    operator.hook = mock_powerbi_hook
    context = {"ti": MagicMock()}
    mock_powerbi_hook.get_latest_refresh_details = MagicMock(
        return_value={
            PowerBIDatasetRefreshFields.REQUEST_ID.value: preexisting_refresh_request_id,
            PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,  # endtime is not available.
        }
    )
    mock_powerbi_hook.trigger_dataset_refresh = MagicMock(return_value=new_refresh_request_id)
    mock_powerbi_hook.get_refresh_details_by_request_id = MagicMock(
        return_value={
            PowerBIDatasetRefreshFields.REQUEST_ID.value: new_refresh_request_id,
            PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
            # serviceExceptionJson is not present when status is not "Failed"
        }
    )
    mock_powerbi_hook.wait_for_dataset_refresh_status = MagicMock(return_value=wait_for_status_return_value)

    if wait_for_status_return_value is False:
        with pytest.raises(PowerBIDatasetRefreshException):
            operator.execute(context)
        assert not mock_powerbi_hook.trigger_dataset_refresh.called
    else:
        operator.execute(context)
        assert mock_powerbi_hook.trigger_dataset_refresh.called
        assert mock_powerbi_hook.get_refresh_details_by_request_id.called
        assert mock_powerbi_hook.wait_for_dataset_refresh_status.call_count == 2
        assert context["ti"].xcom_push.call_count == 4
        assert context["ti"].xcom_push.call_args_list == [
            call(key="powerbi_dataset_refresh_id", value=new_refresh_request_id),
            call(key="powerbi_dataset_refresh_status", value=PowerBIDatasetRefreshStatus.COMPLETED),
            call(key="powerbi_dataset_refresh_end_time", value="2024-04-15T20:14:08.1458221Z"),
            call(key="powerbi_dataset_refresh_error", value="None"),
        ]


_get_wait_for_status_and_latest_refresh_details_args = [
    (True, None),
    (False, None),
    (True, COMPLETED_REFRESH_DETAILS),
    (False, COMPLETED_REFRESH_DETAILS),
    (True, FAILED_REFRESH_DETAILS),
    (False, FAILED_REFRESH_DETAILS),
]


@pytest.mark.parametrize(
    argnames=("wait_for_status_return_value", "latest_refresh_details"),
    argvalues=_get_wait_for_status_and_latest_refresh_details_args,
    ids=[
        (
            f"wait_for_status_detail_{argval[1][PowerBIDatasetRefreshFields.STATUS.value]}_return_value_{argval[0]}"
            if argval[1] is not None
            else f"wait_for_status_detail_None_return_value_{argval[0]}"
        )
        for argval in _get_wait_for_status_and_latest_refresh_details_args
    ],
)
def test_execute_wait_for_termination_no_preexisting_refresh(
    mock_powerbi_hook, wait_for_status_return_value, latest_refresh_details
):
    operator = PowerBIDatasetRefreshOperator(
        wait_for_termination=True,
        force_refresh=True,
        **CONFIG,
    )
    operator.hook = mock_powerbi_hook
    context = {"ti": MagicMock()}
    new_refresh_request_id = "11bf290a-346b-48b7-8973-c5df149337ff"

    # Magic mock the hook methods
    mock_powerbi_hook.get_latest_refresh_details = MagicMock(return_value=latest_refresh_details)
    mock_powerbi_hook.trigger_dataset_refresh = MagicMock(return_value=new_refresh_request_id)
    mock_powerbi_hook.get_refresh_details_by_request_id = MagicMock(
        return_value={
            PowerBIDatasetRefreshFields.REQUEST_ID.value: new_refresh_request_id,
            PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
            # serviceExceptionJson is not present when status is not "Failed"
        }
    )
    mock_powerbi_hook.wait_for_dataset_refresh_status = MagicMock(return_value=wait_for_status_return_value)

    # Act and assert
    if wait_for_status_return_value is False:
        with pytest.raises(PowerBIDatasetRefreshException):
            operator.execute(context)
    else:
        operator.execute(context)
        assert mock_powerbi_hook.trigger_dataset_refresh.called
        assert mock_powerbi_hook.get_refresh_details_by_request_id.called
        mock_powerbi_hook.wait_for_dataset_refresh_status.assert_called_once_with(
            request_id=new_refresh_request_id,
            dataset_id=DATASET_ID,
            group_id=GROUP_ID,
            expected_status=PowerBIDatasetRefreshStatus.COMPLETED,
        )
        assert context["ti"].xcom_push.call_count == 4
        assert context["ti"].xcom_push.call_args_list == [
            call(
                key="powerbi_dataset_refresh_id",
                value=new_refresh_request_id,
            ),
            call(key="powerbi_dataset_refresh_status", value=PowerBIDatasetRefreshStatus.COMPLETED),
            call(key="powerbi_dataset_refresh_end_time", value="2024-04-15T20:14:08.1458221Z"),
            call(key="powerbi_dataset_refresh_error", value="None"),
        ]
