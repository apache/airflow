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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.aas import (
    AasModelRefreshState,
    AasHook,
)

FORMATTED_RESPONSE = [
    # Completed refresh
    {
        "operation_id": "5e2d9921-e91b-491f-b7e1-e7d8db49194a",
        "state": AasModelRefreshState.COMPLETED,
    },
    # In-progress refresh
    {
        "operation_id": "5e2d9921-e91b-491f-b7e1-e7d8db49194b",
        "state": AasModelRefreshState.IN_PROGRESS,
    },
    # Failed refresh
    {
        "operation_id": "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
        "state": AasModelRefreshState.FAILED,
    },
]

DEFAULT_CONNECTION_CLIENT_SECRET = "aas_conn_id"
SERVER_LOCATION = "northeurope"
SERVER_NAME = "server1"
DATABASE_NAME = "db1"


class TestAasHook:
    @pytest.fixture
    def aas_hook():
        return AasHook()

    def test_get_refresh_history(aas_hook):
        # Define the mock response data
        mock_response_data = [
            {"refreshId": "1", "status": AasModelRefreshState.SUCCEEDED},
            {"refreshId": "2", "status": AasModelRefreshState.IN_PROGRESS},
        ]
        
        # Create a mock for the _api_call method
        with mock.patch.object(AasHook, '_api_call', return_value=mock.Mock(json=mock.Mock(return_value=mock_response_data))) as mock_api_call:
            # Call the get_refresh_history method
            result = aas_hook.get_refresh_history(SERVER_NAME, SERVER_LOCATION, DATABASE_NAME)
            
            # Assert that the result is as expected
            expected_result = [
                {"operation_id": "1", "state": AasModelRefreshState.SUCCEEDED},
                {"operation_id": "2", "state": AasModelRefreshState.IN_PROGRESS},
            ]
            assert result == expected_result

            # Assert that the _api_call method was called with the correct URL
            mock_api_call.assert_called_once_with(
                url=f"https://{SERVER_LOCATION}.asazure.windows.net/servers/{SERVER_NAME}/models/{DATABASE_NAME}/refreshes",
            )
    
    # @pytest.mark.asyncio
    # async def test_get_refresh_history(self, aas_hook):
    #     response_data = {"value": [{"requestId": "1234", "status": "Completed", "serviceExceptionJson": ""}]}

    #     with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
    #         mock_run.return_value = response_data
    #         result = await aas_hook.get_refresh_history(DATASET_ID, GROUP_ID)

    #         expected = [{"request_id": "1234", "status": "Completed", "error": ""}]
    #         assert result == expected

    # @pytest.mark.asyncio
    # async def test_get_refresh_history_airflow_exception(self, aas_hook):
    #     """Test handling of AirflowException in get_refresh_history."""

    #     with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
    #         mock_run.side_effect = AirflowException("Test exception")

    #         with pytest.raises(PowerBIDatasetRefreshException, match="Failed to retrieve refresh history"):
    #             await aas_hook.get_refresh_history(DATASET_ID, GROUP_ID)

    # @pytest.mark.parametrize(
    #     "input_data, expected_output",
    #     [
    #         (
    #             {"requestId": "1234", "status": "Completed", "serviceExceptionJson": ""},
    #             {
    #                 PowerBIDatasetRefreshFields.REQUEST_ID.value: "1234",
    #                 PowerBIDatasetRefreshFields.STATUS.value: "Completed",
    #                 PowerBIDatasetRefreshFields.ERROR.value: "",
    #             },
    #         ),
    #         (
    #             {"requestId": "5678", "status": "Unknown", "serviceExceptionJson": "Some error"},
    #             {
    #                 PowerBIDatasetRefreshFields.REQUEST_ID.value: "5678",
    #                 PowerBIDatasetRefreshFields.STATUS.value: "In Progress",
    #                 PowerBIDatasetRefreshFields.ERROR.value: "Some error",
    #             },
    #         ),
    #         (
    #             {"requestId": None, "status": None, "serviceExceptionJson": None},
    #             {
    #                 PowerBIDatasetRefreshFields.REQUEST_ID.value: "None",
    #                 PowerBIDatasetRefreshFields.STATUS.value: "None",
    #                 PowerBIDatasetRefreshFields.ERROR.value: "None",
    #             },
    #         ),
    #         (
    #             {},  # Empty input dictionary
    #             {
    #                 PowerBIDatasetRefreshFields.REQUEST_ID.value: "None",
    #                 PowerBIDatasetRefreshFields.STATUS.value: "None",
    #                 PowerBIDatasetRefreshFields.ERROR.value: "None",
    #             },
    #         ),
    #     ],
    # )
    # def test_raw_to_refresh_details(self, input_data, expected_output):
    #     """Test raw_to_refresh_details method."""
    #     result = PowerBIHook.raw_to_refresh_details(input_data)
    #     assert result == expected_output

    # @pytest.mark.asyncio
    # async def test_get_refresh_details_by_refresh_id(self, aas_hook):
    #     # Mock the get_refresh_history method to return a list of refresh histories
    #     refresh_histories = FORMATTED_RESPONSE
    #     aas_hook.get_refresh_history = mock.AsyncMock(return_value=refresh_histories)

    #     # Call the function with a valid request ID
    #     refresh_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"
    #     result = await aas_hook.get_refresh_details_by_refresh_id(
    #         dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=refresh_id
    #     )

    #     # Assert that the correct refresh details are returned
    #     assert result == {
    #         PowerBIDatasetRefreshFields.REQUEST_ID.value: "5e2d9921-e91b-491f-b7e1-e7d8db49194c",
    #         PowerBIDatasetRefreshFields.STATUS.value: "Completed",
    #         PowerBIDatasetRefreshFields.ERROR.value: "None",
    #     }

    #     # Call the function with an invalid request ID
    #     invalid_request_id = "invalid_request_id"
    #     with pytest.raises(PowerBIDatasetRefreshException):
    #         await aas_hook.get_refresh_details_by_refresh_id(
    #             dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=invalid_request_id
    #         )

    # @pytest.mark.asyncio
    # async def test_get_refresh_details_by_refresh_id_empty_history(self, aas_hook):
    #     """Test exception when refresh history is empty."""
    #     # Mock the get_refresh_history method to return an empty list
    #     aas_hook.get_refresh_history = mock.AsyncMock(return_value=[])

    #     # Call the function with a request ID
    #     refresh_id = "any_request_id"
    #     with pytest.raises(
    #         PowerBIDatasetRefreshException,
    #         match=f"Unable to fetch the details of dataset refresh with Request Id: {refresh_id}",
    #     ):
    #         await aas_hook.get_refresh_details_by_refresh_id(
    #             dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=refresh_id
    #         )

    # @pytest.mark.asyncio
    # async def test_get_refresh_details_by_refresh_id_not_found(self, aas_hook):
    #     """Test exception when the refresh ID is not found in the refresh history."""
    #     # Mock the get_refresh_history method to return a list of refresh histories without the specified ID
    #     aas_hook.get_refresh_history = mock.AsyncMock(return_value=FORMATTED_RESPONSE)

    #     # Call the function with an invalid request ID
    #     invalid_request_id = "invalid_request_id"
    #     with pytest.raises(
    #         PowerBIDatasetRefreshException,
    #         match=f"Unable to fetch the details of dataset refresh with Request Id: {invalid_request_id}",
    #     ):
    #         await aas_hook.get_refresh_details_by_refresh_id(
    #             dataset_id=DATASET_ID, group_id=GROUP_ID, refresh_id=invalid_request_id
    #         )

    # @pytest.mark.asyncio
    # async def test_trigger_dataset_refresh_success(self, aas_hook):
    #     response_data = {"requestid": "5e2d9921-e91b-491f-b7e1-e7d8db49194c"}

    #     with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
    #         mock_run.return_value = response_data
    #         result = await aas_hook.trigger_dataset_refresh(dataset_id=DATASET_ID, group_id=GROUP_ID)

    #     assert result == "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

    # @pytest.mark.asyncio
    # async def test_trigger_dataset_refresh_failure(self, aas_hook):
    #     """Test failure to trigger dataset refresh due to AirflowException."""
    #     with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
    #         mock_run.side_effect = AirflowException("Test exception")

    #         with pytest.raises(PowerBIDatasetRefreshException, match="Failed to trigger dataset refresh."):
    #             await aas_hook.trigger_dataset_refresh(dataset_id=DATASET_ID, group_id=GROUP_ID)

    # @pytest.mark.asyncio
    # async def test_cancel_dataset_refresh(self, aas_hook):
    #     dataset_refresh_id = "5e2d9921-e91b-491f-b7e1-e7d8db49194c"

    #     with mock.patch.object(KiotaRequestAdapterHook, "run", new_callable=mock.AsyncMock) as mock_run:
    #         await aas_hook.cancel_dataset_refresh(DATASET_ID, GROUP_ID, dataset_refresh_id)

    #     mock_run.assert_called_once_with(
    #         url="myorg/groups/{group_id}/datasets/{dataset_id}/refreshes/{dataset_refresh_id}",
    #         response_type=None,
    #         path_parameters={
    #             "group_id": GROUP_ID,
    #             "dataset_id": DATASET_ID,
    #             "dataset_refresh_id": dataset_refresh_id,
    #         },
    #         method="DELETE",
    #     )
