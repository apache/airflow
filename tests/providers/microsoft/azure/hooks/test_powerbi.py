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
from unittest.mock import MagicMock

import pytest

from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
MODULE = "airflow.providers.microsoft.azure.hooks.powerbi"
CLIENT_ID = "client_id"
CLIENT_SECRET = "client_secret"
TENANT_ID = "tenant_id"
BASE_URL = "https://api.powerbi.com"
API_VERSION = "v1.0"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"

API_RAW_RESPONSE = {
    "value": [
        # Completed refresh
        {
            "requestId": "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
            "status": "Completed",
            "endTime": "2024-04-15T20:14:08.1458221Z",
            # serviceExceptionJson is not present when status is not "Failed"
        },
        # In-progress refresh
        {
            "requestId": "6b6536c1-cfcb-4148-9c21-402c3f5241e4",
            "status": "Unknown",  # endtime is not available.
        },
        # Failed refresh
        {
            "requestId": "11bf290a-346b-48b7-8973-c5df149337ff",
            "status": "Failed",
            "endTime": "2024-04-15T20:14:08.1458221Z",
            "serviceExceptionJson": '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
        },
    ]
}

FORMATTED_RESPONSE = [
    # Completed refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
        PowerBIDatasetRefreshFields.ERROR.value: "None",
    },
    # In-progress refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "6b6536c1-cfcb-4148-9c21-402c3f5241e4",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,
        PowerBIDatasetRefreshFields.ERROR.value: "None",
    },
    # Failed refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "11bf290a-346b-48b7-8973-c5df149337ff",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.FAILED,
        PowerBIDatasetRefreshFields.ERROR.value: '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
    },
]


@pytest.fixture
def powerbi_hook():
    client = PowerBIHook(powerbi_conn_id=DEFAULT_CONNECTION_CLIENT_SECRET)
    return client


@pytest.fixture
def get_token(powerbi_hook):
    powerbi_hook._get_token = MagicMock(return_value="access_token")
    return powerbi_hook._get_token()


def test_get_token_with_missing_credentials(powerbi_hook):
    # Mock the get_connection method to return a connection with missing credentials
    powerbi_hook.get_connection = MagicMock(
        return_value=Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="powerbi",
            login=None,
            password=None,
            extra={
                "tenant_id": TENANT_ID,
            },
        )
    )

    with pytest.raises(ValueError):
        powerbi_hook._get_token()


def test_get_token_with_missing_tenant_id(powerbi_hook):
    # Mock the get_connection method to return a connection with missing tenant ID
    powerbi_hook.get_connection = MagicMock(
        return_value=Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="powerbi",
            login=CLIENT_ID,
            password=CLIENT_SECRET,
            extra={},
        )
    )

    with pytest.raises(ValueError):
        powerbi_hook._get_token()


@mock.patch(f"{MODULE}.ClientSecretCredential")
def test_get_token_with_valid_credentials(mock_credential, powerbi_hook):
    # Mock the get_connection method to return a connection with valid credentials
    powerbi_hook.get_connection = MagicMock(
        return_value=Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="powerbi",
            login=CLIENT_ID,
            password=CLIENT_SECRET,
            extra={
                "tenant_id": TENANT_ID,
            },
        )
    )

    token = powerbi_hook._get_token()
    mock_credential.assert_called()

    assert token is not None


def test_refresh_dataset(powerbi_hook, requests_mock, get_token):
    request_id = "request_id"

    # Mock the request in _send_request method to return a successful response
    requests_mock.post(
        f"{BASE_URL}/{API_VERSION}/myorg/groups/{GROUP_ID}/datasets/{DATASET_ID}/refreshes",
        status_code=202,
        headers={"Authorization": f"Bearer {get_token}", "RequestId": request_id},
    )

    result = powerbi_hook.refresh_dataset(dataset_id=DATASET_ID, group_id=GROUP_ID)

    assert result == request_id


def test_get_refresh_history_success(powerbi_hook, requests_mock, get_token):
    url = f"{BASE_URL}/{API_VERSION}/myorg/groups/{GROUP_ID}/datasets/{DATASET_ID}/refreshes"

    requests_mock.get(
        url, json=API_RAW_RESPONSE, headers={"Authorization": f"Bearer {get_token}"}, status_code=200
    )

    result = powerbi_hook.get_refresh_history(DATASET_ID, GROUP_ID)

    assert len(result) == 3
    assert result == FORMATTED_RESPONSE


def test_get_latest_refresh_details_with_no_history(powerbi_hook):
    # Mock the get_refresh_history method to return an empty list
    powerbi_hook.get_refresh_history = MagicMock(return_value=[])

    result = powerbi_hook.get_latest_refresh_details(dataset_id=DATASET_ID, group_id=GROUP_ID)

    assert result is None


def test_get_latest_refresh_details_with_history(powerbi_hook):
    # Mock the get_refresh_history method to return a list with refresh details
    refresh_history = FORMATTED_RESPONSE
    powerbi_hook.get_refresh_history = MagicMock(return_value=refresh_history)

    result = powerbi_hook.get_latest_refresh_details(dataset_id=DATASET_ID, group_id=GROUP_ID)

    assert result == FORMATTED_RESPONSE[0]


def test_get_refresh_details_by_request_id(powerbi_hook):
    # Mock the get_refresh_history method to return a list of refresh histories
    refresh_histories = FORMATTED_RESPONSE
    powerbi_hook.get_refresh_history = MagicMock(return_value=refresh_histories)

    # Call the function with a valid request ID
    request_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
    result = powerbi_hook.get_refresh_details_by_request_id(
        dataset_id=DATASET_ID, group_id=GROUP_ID, request_id=request_id
    )

    # Assert that the correct refresh details are returned
    assert result == {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
        PowerBIDatasetRefreshFields.STATUS.value: "Completed",
        PowerBIDatasetRefreshFields.END_TIME.value: "2024-04-15T20:14:08.1458221Z",
        PowerBIDatasetRefreshFields.ERROR.value: "None",
    }

    # Call the function with an invalid request ID
    invalid_request_id = "invalid_request_id"
    with pytest.raises(PowerBIDatasetRefreshException):
        powerbi_hook.get_refresh_details_by_request_id(
            dataset_id=DATASET_ID, group_id=GROUP_ID, request_id=invalid_request_id
        )


_wait_for_dataset_refresh_status_test_args = [
    (PowerBIDatasetRefreshStatus.COMPLETED, PowerBIDatasetRefreshStatus.COMPLETED, True),
    (PowerBIDatasetRefreshStatus.FAILED, PowerBIDatasetRefreshStatus.COMPLETED, False),
    (PowerBIDatasetRefreshStatus.IN_PROGRESS, PowerBIDatasetRefreshStatus.COMPLETED, "timeout"),
]


@pytest.mark.parametrize(
    argnames=("dataset_refresh_status", "expected_status", "expected_result"),
    argvalues=_wait_for_dataset_refresh_status_test_args,
    ids=[
        f"refresh_status_{argval[0]}_expected_{argval[1]}"
        for argval in _wait_for_dataset_refresh_status_test_args
    ],
)
def test_wait_for_dataset_refresh_status(
    powerbi_hook, dataset_refresh_status, expected_status, expected_result
):
    config = {
        "dataset_id": DATASET_ID,
        "group_id": GROUP_ID,
        "request_id": "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
        "timeout": 3,
        "check_interval": 1,
        "expected_status": expected_status,
    }

    # Mock the get_refresh_details_by_request_id method to return a dataset refresh details
    dataset_refresh_details = {PowerBIDatasetRefreshFields.STATUS.value: dataset_refresh_status}
    powerbi_hook.get_refresh_details_by_request_id = MagicMock(return_value=dataset_refresh_details)

    if expected_result != "timeout":
        assert powerbi_hook.wait_for_dataset_refresh_status(**config) == expected_result
    else:
        with pytest.raises(PowerBIDatasetRefreshException):
            powerbi_hook.wait_for_dataset_refresh_status(**config)


def test_trigger_dataset_refresh(powerbi_hook):
    # Mock the refresh_dataset method to return a request ID
    powerbi_hook.refresh_dataset = MagicMock(return_value="request_id")

    # Assert trigger_dataset_refresh raises an exception.
    response = powerbi_hook.trigger_dataset_refresh(dataset_id=DATASET_ID, group_id=GROUP_ID)

    assert response == "request_id"
