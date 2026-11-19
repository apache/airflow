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

from os.path import dirname
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.msgraph import KiotaRequestAdapterHook
from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshFields,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)

from tests_common.test_utils.file_loading import load_json_from_resources

FORMATTED_RESPONSE = [
    # Completed refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.COMPLETED,
        PowerBIDatasetRefreshFields.ERROR.value: "None",
        PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
    },
    # In-progress refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "6b6536c1-cfcb-4148-9c21-402c3f5241e4",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.IN_PROGRESS,
        PowerBIDatasetRefreshFields.ERROR.value: "None",
        PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
    },
    # Failed refresh
    {
        PowerBIDatasetRefreshFields.REQUEST_ID.value: "11bf290a-346b-48b7-8973-c5df149337ff",
        PowerBIDatasetRefreshFields.STATUS.value: PowerBIDatasetRefreshStatus.FAILED,
        PowerBIDatasetRefreshFields.ERROR.value: '{"errorCode":"ModelRefreshFailed_CredentialsNotSpecified"}',
        PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
    },
]

DEFAULT_CONNECTION_CLIENT_SECRET = "powerbi_conn_id"
GROUP_ID = "group_id"
DATASET_ID = "dataset_id"


class TestPowerBIHook:
    @pytest.mark.asyncio
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_completed_enhanced_refresh_observation(self, mock_run, powerbi_hook):
        fixture = load_json_from_resources(
            dirname(__file__), "..", "resources", "powerbi_completed_refresh.json"
        )
        history = fixture["history"]
        execution = fixture["execution"]
        refresh_id = history["value"][0]["requestId"]
        mock_run.side_effect = [history, execution]

        result = await powerbi_hook.get_refresh_details_by_refresh_id(DATASET_ID, GROUP_ID, refresh_id)

        assert result == {
            "request_id": refresh_id,
            "status": "Completed",
            "error": "None",
            "refresh_type": "ViaEnhancedApi",
        }
        assert mock_run.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("message", "expected_error"),
        [
            ({"type": "Error", "code": "SyntheticMissingText"}, "None"),
            ({"type": "Error", "message": None}, "None"),
            ({"type": "Error", "message": ""}, "None"),
            ({"type": "Warning", "message": "Synthetic warning"}, "None"),
            ({"message": "Synthetic untyped error"}, "Synthetic untyped error"),
            ({"type": "Error", "message": "Synthetic error"}, "Synthetic error"),
        ],
    )
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_enhanced_refresh_optional_message_text(
        self, mock_run, powerbi_hook, message, expected_error
    ):
        mock_run.side_effect = [
            {"value": [{"requestId": "1234", "refreshType": "ViaEnhancedApi", "status": "Unknown"}]},
            {"status": "Unknown", "extendedStatus": "Failed", "messages": [message]},
        ]

        result = await powerbi_hook.get_refresh_details_by_refresh_id(DATASET_ID, GROUP_ID, "1234")

        assert result["status"] == "Failed"
        assert result["error"] == expected_error

    @pytest.mark.asyncio
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_enhanced_refresh_execution_request_error(self, mock_run, powerbi_hook):
        error = AirflowException("Synthetic request failure")
        mock_run.side_effect = [
            {"value": [{"requestId": "1234", "refreshType": "ViaEnhancedApi", "status": "Unknown"}]},
            error,
        ]

        with pytest.raises(
            PowerBIDatasetRefreshException, match="Failed to retrieve refresh execution details"
        ) as exc:
            await powerbi_hook.get_refresh_details_by_refresh_id(DATASET_ID, GROUP_ID, "1234")

        assert exc.value.__cause__ is error

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("extended_status", "status", "expected_status"),
        [
            ("Failed", "Unknown", "Failed"),
            ("Completed", "Unknown", "Completed"),
            ("TimedOut", "Unknown", "TimedOut"),
            ("Cancelled", "Unknown", "Cancelled"),
            ("Disabled", "Unknown", "Disabled"),
            ("InProgress", "Failed", "In Progress"),
            ("NotStarted", "Unknown", "In Progress"),
            (None, "Completed", "Completed"),
            (None, "InProgress", "In Progress"),
            (None, "NotStarted", "In Progress"),
            ("", "Failed", "Failed"),
            ("Unknown", "Failed", "Failed"),
            ("Unknown", "Unknown", "In Progress"),
            ("FutureStatus", "Unknown", "In Progress"),
        ],
    )
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_enhanced_refresh_execution_status(
        self, mock_run, powerbi_hook, extended_status, status, expected_status
    ):
        mock_run.side_effect = [
            {
                "value": [
                    {"requestId": "other-refresh", "refreshType": "ViaEnhancedApi", "status": "Completed"},
                    {"requestId": "submitted-refresh", "refreshType": "ViaEnhancedApi", "status": "Unknown"},
                ]
            },
            {"status": status, "extendedStatus": extended_status},
        ]

        result = await powerbi_hook.get_refresh_details_by_refresh_id(
            dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id="submitted-refresh"
        )

        assert result == {
            "request_id": "submitted-refresh",
            "status": expected_status,
            "error": "None",
            "refresh_type": "ViaEnhancedApi",
        }
        assert mock_run.call_count == 2
        mock_run.assert_called_with(
            powerbi_hook,
            url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{refresh_id}",
            path_parameters={
                "group_id": GROUP_ID,
                "dataset_id": DATASET_ID,
                "refresh_id": "submitted-refresh",
            },
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("null_fields", "expected_errors"),
        [
            ((), ["Synthetic engine failure", "SyntheticRefreshError", "Synthetic attempt failure"]),
            (("messages",), ["SyntheticRefreshError", "Synthetic attempt failure"]),
            (("refreshAttempts",), ["Synthetic engine failure", "SyntheticRefreshError"]),
            (("messages", "refreshAttempts"), ["SyntheticRefreshError"]),
        ],
    )
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_enhanced_refresh_execution_errors(
        self, mock_run, powerbi_hook, null_fields, expected_errors
    ):
        execution = {
            "status": "Unknown",
            "extendedStatus": "Failed",
            "messages": [{"type": "Error", "message": "Synthetic engine failure"}],
            "serviceExceptionJson": '{"errorCode":"SyntheticRefreshError"}',
            "refreshAttempts": [
                {"serviceExceptionJson": '{"errorDescription":"Synthetic attempt failure"}'},
                {"attemptId": 2},
            ],
        }
        for field in null_fields:
            execution[field] = None
        mock_run.side_effect = [
            {"value": [{"requestId": "1234", "refreshType": "ViaEnhancedApi", "status": "Unknown"}]},
            execution,
        ]

        result = await powerbi_hook.get_refresh_details_by_refresh_id(DATASET_ID, GROUP_ID, "1234")

        assert result["status"] == "Failed"
        errors = {
            "Synthetic engine failure": "Synthetic engine failure",
            "SyntheticRefreshError": '{"errorCode":"SyntheticRefreshError"}',
            "Synthetic attempt failure": '{"errorDescription":"Synthetic attempt failure"}',
        }
        assert result["error"] == "; ".join(errors[error] for error in expected_errors)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["Unknown", "Completed", "Failed", "Disabled"])
    @pytest.mark.parametrize("refresh_type", ["ViaApi", "OnDemand", "WebModeling", None])
    @mock.patch.object(KiotaRequestAdapterHook, "run", autospec=True)
    async def test_standard_refresh_uses_only_history(self, mock_run, powerbi_hook, status, refresh_type):
        mock_run.return_value = {
            "value": [{"requestId": "1234", "refreshType": refresh_type, "status": status}]
        }

        result = await powerbi_hook.get_refresh_details_by_refresh_id(DATASET_ID, GROUP_ID, "1234")

        assert result == {
            "request_id": "1234",
            "status": "In Progress" if status == "Unknown" else status,
            "error": "None",
            "refresh_type": str(refresh_type),
        }
        mock_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_refresh_history(self, powerbi_hook):
        response_data = {"value": [{"requestId": "1234", "status": "Completed", "serviceExceptionJson": ""}]}

        with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
            mock_run.return_value = response_data
            result = await powerbi_hook.get_refresh_history(DATASET_ID, GROUP_ID)

            expected = [{"request_id": "1234", "status": "Completed", "error": "", "refresh_type": "None"}]
            assert result == expected

    @pytest.mark.asyncio
    async def test_get_refresh_history_airflow_exception(self, powerbi_hook):
        """Test handling of AirflowException in get_refresh_history."""

        with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
            mock_run.side_effect = AirflowException("Test exception")

            with pytest.raises(PowerBIDatasetRefreshException, match="Failed to retrieve refresh history"):
                await powerbi_hook.get_refresh_history(DATASET_ID, GROUP_ID)

    @pytest.mark.parametrize(
        ("input_data", "expected_output"),
        [
            (
                {"requestId": "1234", "status": "Completed", "serviceExceptionJson": ""},
                {
                    PowerBIDatasetRefreshFields.REQUEST_ID.value: "1234",
                    PowerBIDatasetRefreshFields.STATUS.value: "Completed",
                    PowerBIDatasetRefreshFields.ERROR.value: "",
                    PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
                },
            ),
            (
                {"requestId": "5678", "status": "Unknown", "serviceExceptionJson": "Some error"},
                {
                    PowerBIDatasetRefreshFields.REQUEST_ID.value: "5678",
                    PowerBIDatasetRefreshFields.STATUS.value: "In Progress",
                    PowerBIDatasetRefreshFields.ERROR.value: "Some error",
                    PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
                },
            ),
            (
                {"requestId": None, "status": None, "serviceExceptionJson": None},
                {
                    PowerBIDatasetRefreshFields.REQUEST_ID.value: "None",
                    PowerBIDatasetRefreshFields.STATUS.value: "None",
                    PowerBIDatasetRefreshFields.ERROR.value: "None",
                    PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
                },
            ),
            (
                {},  # Empty input dictionary
                {
                    PowerBIDatasetRefreshFields.REQUEST_ID.value: "None",
                    PowerBIDatasetRefreshFields.STATUS.value: "None",
                    PowerBIDatasetRefreshFields.ERROR.value: "None",
                    PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
                },
            ),
        ],
    )
    def test_raw_to_refresh_details(self, input_data, expected_output):
        """Test raw_to_refresh_details method."""
        result = PowerBIHook.raw_to_refresh_details(input_data)
        assert result == expected_output

    @pytest.mark.asyncio
    async def test_get_refresh_details_by_refresh_id(self, powerbi_hook):
        # Mock the get_refresh_history method to return a list of refresh histories
        refresh_histories = FORMATTED_RESPONSE
        powerbi_hook.get_refresh_history = mock.AsyncMock(return_value=refresh_histories)

        # Call the function with a valid request ID
        refresh_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
        result = await powerbi_hook.get_refresh_details_by_refresh_id(
            dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=refresh_id
        )

        # Assert that the correct refresh details are returned
        assert result == {
            PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
            PowerBIDatasetRefreshFields.STATUS.value: "Completed",
            PowerBIDatasetRefreshFields.ERROR.value: "None",
            PowerBIDatasetRefreshFields.REFRESH_TYPE.value: "None",
        }

        # Call the function with an invalid request ID
        invalid_request_id = "invalid_request_id"
        with pytest.raises(PowerBIDatasetRefreshException):
            await powerbi_hook.get_refresh_details_by_refresh_id(
                dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=invalid_request_id
            )

    @pytest.mark.asyncio
    async def test_get_refresh_details_by_refresh_id_empty_history(self, powerbi_hook):
        """Test exception when refresh history is empty."""
        # Mock the get_refresh_history method to return an empty list
        powerbi_hook.get_refresh_history = mock.AsyncMock(return_value=[])

        # Call the function with a request ID
        refresh_id = "any_request_id"
        with pytest.raises(
            PowerBIDatasetRefreshException,
            match=f"Unable to fetch the details of dataset refresh with Request Id: {refresh_id}",
        ):
            await powerbi_hook.get_refresh_details_by_refresh_id(
                dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=refresh_id
            )

    @pytest.mark.asyncio
    async def test_get_refresh_details_by_refresh_id_not_found(self, powerbi_hook):
        """Test exception when the refresh ID is not found in the refresh history."""
        # Mock the get_refresh_history method to return a list of refresh histories without the specified ID
        powerbi_hook.get_refresh_history = mock.AsyncMock(return_value=FORMATTED_RESPONSE)

        # Call the function with an invalid request ID
        invalid_request_id = "invalid_request_id"
        with pytest.raises(
            PowerBIDatasetRefreshException,
            match=f"Unable to fetch the details of dataset refresh with Request Id: {invalid_request_id}",
        ):
            await powerbi_hook.get_refresh_details_by_refresh_id(
                dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=invalid_request_id
            )

    @pytest.mark.asyncio
    async def test_trigger_dataset_refresh_success(self, powerbi_hook):
        response_data = {"requestid": "5e2d9921-e91b-491f-b7e1-e7d8db49194c"}

        with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
            mock_run.return_value = response_data
            result = await powerbi_hook.trigger_dataset_refresh(dataset_id=DATASET_ID, group_id=GROUP_ID)

        assert result == "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

    @pytest.mark.asyncio
    async def test_trigger_dataset_refresh_failure(self, powerbi_hook):
        """Test failure to trigger dataset refresh due to AirflowException."""
        with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
            mock_run.side_effect = AirflowException("Test exception")

            with pytest.raises(PowerBIDatasetRefreshException, match="Failed to trigger dataset refresh."):
                await powerbi_hook.trigger_dataset_refresh(dataset_id=DATASET_ID, group_id=GROUP_ID)

    @pytest.mark.asyncio
    async def test_cancel_dataset_refresh(self, powerbi_hook):
        dataset_refresh_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

        with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
            await powerbi_hook.cancel_dataset_refresh(DATASET_ID, GROUP_ID, dataset_refresh_id)

        mock_run.assert_called_once_with(
            url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{dataset_refresh_id}",
            response_type=None,
            path_parameters={
                "group_id": GROUP_ID,
                "dataset_id": DATASET_ID,
                "dataset_refresh_id": dataset_refresh_id,
            },
            method="DELETE",
        )
