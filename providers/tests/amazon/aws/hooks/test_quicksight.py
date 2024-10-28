#
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
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook

DEFAULT_AWS_ACCOUNT_ID = "123456789012"
MOCK_DATA = {
    "DataSetId": "DemoDataSet",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionType": "INCREMENTAL_REFRESH",
}
MOCK_CREATE_INGESTION_RESPONSE = {
    "Status": 201,
    "Arn": "arn:aws:quicksight:us-east-1:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionStatus": "INITIALIZED",
    "RequestId": "fc1f7eea-1327-41d6-9af7-c12f097ed343",
}
MOCK_DESCRIBE_INGESTION_SUCCESS = {
    "Status": 200,
    "Ingestion": {
        "Arn": "arn:aws:quicksight:region:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
        "IngestionId": "DemoDataSet_Ingestion",
        "IngestionStatus": "COMPLETED",
        "ErrorInfo": {},
        "RowInfo": {"RowsIngested": 228, "RowsDropped": 0, "TotalRowsInDataset": 228},
        "CreatedTime": 1646589017.05,
        "IngestionTimeInSeconds": 17,
        "IngestionSizeInBytes": 27921,
        "RequestSource": "MANUAL",
        "RequestType": "FULL_REFRESH",
    },
    "RequestId": "DemoDataSet_Ingestion_Request_ID",
}
MOCK_DESCRIBE_INGESTION_FAILURE = {
    "Status": 403,
    "Ingestion": {
        "Arn": "arn:aws:quicksight:region:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
        "IngestionId": "DemoDataSet_Ingestion",
        "IngestionStatus": "Failed",
        "ErrorInfo": {},
        "RowInfo": {"RowsIngested": 228, "RowsDropped": 0, "TotalRowsInDataset": 228},
        "CreatedTime": 1646589017.05,
        "IngestionTimeInSeconds": 17,
        "IngestionSizeInBytes": 27921,
        "RequestSource": "MANUAL",
        "RequestType": "FULL_REFRESH",
    },
    "RequestId": "DemoDataSet_Ingestion_Request_ID",
}
ACCOUNT_TEST_CASES = [
    pytest.param(None, DEFAULT_AWS_ACCOUNT_ID, id="default-account-id"),
    pytest.param("777777777777", "777777777777", id="custom-account-id"),
]


@pytest.fixture
def mocked_account_id():
    with mock.patch.object(
        QuickSightHook, "account_id", new_callable=mock.PropertyMock
    ) as m:
        m.return_value = DEFAULT_AWS_ACCOUNT_ID
        yield m


@pytest.fixture
def mocked_client():
    with mock.patch.object(QuickSightHook, "conn") as m:
        yield m


class TestQuicksight:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = QuickSightHook(aws_conn_id="aws_default", region_name="us-east-1")
        assert hook.conn is not None

    @pytest.mark.parametrize(
        "response, expected_status",
        [
            pytest.param(MOCK_DESCRIBE_INGESTION_SUCCESS, "COMPLETED", id="completed"),
            pytest.param(MOCK_DESCRIBE_INGESTION_FAILURE, "Failed", id="failed"),
        ],
    )
    @pytest.mark.parametrize("aws_account_id, expected_account_id", ACCOUNT_TEST_CASES)
    def test_get_job_status(
        self,
        response,
        expected_status,
        aws_account_id,
        expected_account_id,
        mocked_account_id,
        mocked_client,
    ):
        """Test get job status."""
        mocked_client.describe_ingestion.return_value = response

        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        assert (
            hook.get_status(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                aws_account_id=aws_account_id,
            )
            == expected_status
        )
        mocked_client.describe_ingestion.assert_called_with(
            AwsAccountId=expected_account_id,
            DataSetId="DemoDataSet",
            IngestionId="DemoDataSet_Ingestion",
        )

    @pytest.mark.parametrize(
        "exception, error_match",
        [
            pytest.param(KeyError("Foo"), "Could not get status", id="key-error"),
            pytest.param(
                ClientError(error_response={}, operation_name="fake"),
                "AWS request failed",
                id="botocore-client",
            ),
        ],
    )
    def test_get_job_status_exception(
        self, exception, error_match, mocked_client, mocked_account_id
    ):
        mocked_client.describe_ingestion.side_effect = exception

        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        with pytest.raises(AirflowException, match=error_match):
            assert hook.get_status(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                aws_account_id=None,
            )

    @pytest.mark.parametrize(
        "error_info",
        [
            pytest.param({"foo": "bar"}, id="error-info-exists"),
            pytest.param(None, id="error-info-not-exists"),
        ],
    )
    @pytest.mark.parametrize("aws_account_id, expected_account_id", ACCOUNT_TEST_CASES)
    def test_get_error_info(
        self,
        error_info,
        aws_account_id,
        expected_account_id,
        mocked_client,
        mocked_account_id,
    ):
        mocked_response = {"Ingestion": {}}
        if error_info:
            mocked_response["Ingestion"]["ErrorInfo"] = error_info
        mocked_client.describe_ingestion.return_value = mocked_response

        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        assert (
            hook.get_error_info(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                aws_account_id=None,
            )
            == error_info
        )

    @mock.patch.object(QuickSightHook, "get_status", return_value="FAILED")
    @mock.patch.object(QuickSightHook, "get_error_info")
    @pytest.mark.parametrize("aws_account_id, expected_account_id", ACCOUNT_TEST_CASES)
    def test_wait_for_state_failure(
        self,
        mocked_get_error_info,
        mocked_get_status,
        aws_account_id,
        expected_account_id,
        mocked_client,
        mocked_account_id,
    ):
        mocked_get_error_info.return_value = "Something Bad Happen"
        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        with pytest.raises(AirflowException, match="Error info: Something Bad Happen"):
            hook.wait_for_state(
                aws_account_id,
                "data_set_id",
                "ingestion_id",
                target_state={"COMPLETED"},
                check_interval=0,
            )
        mocked_get_status.assert_called_with(
            expected_account_id, "data_set_id", "ingestion_id"
        )
        mocked_get_error_info.assert_called_with(
            expected_account_id, "data_set_id", "ingestion_id"
        )

    @mock.patch.object(QuickSightHook, "get_status", return_value="CANCELLED")
    def test_wait_for_state_canceled(self, _):
        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        with pytest.raises(
            AirflowException, match="The Amazon QuickSight SPICE ingestion cancelled"
        ):
            hook.wait_for_state(
                "aws_account_id",
                "data_set_id",
                "ingestion_id",
                target_state={"COMPLETED"},
                check_interval=0,
            )

    @mock.patch.object(QuickSightHook, "get_status")
    def test_wait_for_state_completed(self, mocked_get_status):
        mocked_get_status.side_effect = ["INITIALIZED", "QUEUED", "RUNNING", "COMPLETED"]
        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        assert (
            hook.wait_for_state(
                "aws_account_id",
                "data_set_id",
                "ingestion_id",
                target_state={"COMPLETED"},
                check_interval=0,
            )
            == "COMPLETED"
        )
        assert mocked_get_status.call_count == 4

    @pytest.mark.parametrize(
        "wait_for_completion",
        [pytest.param(True, id="wait"), pytest.param(False, id="no-wait")],
    )
    @pytest.mark.parametrize("aws_account_id, expected_account_id", ACCOUNT_TEST_CASES)
    def test_create_ingestion(
        self,
        wait_for_completion,
        aws_account_id,
        expected_account_id,
        mocked_account_id,
        mocked_client,
    ):
        mocked_client.create_ingestion.return_value = MOCK_CREATE_INGESTION_RESPONSE

        hook = QuickSightHook(aws_conn_id=None, region_name="us-east-1")
        with mock.patch.object(QuickSightHook, "wait_for_state") as mocked_wait_for_state:
            assert (
                hook.create_ingestion(
                    data_set_id="DemoDataSet",
                    ingestion_id="DemoDataSet_Ingestion",
                    ingestion_type="INCREMENTAL_REFRESH",
                    aws_account_id=aws_account_id,
                    wait_for_completion=wait_for_completion,
                    check_interval=0,
                )
                == MOCK_CREATE_INGESTION_RESPONSE
            )
            if wait_for_completion:
                mocked_wait_for_state.assert_called_once_with(
                    aws_account_id=expected_account_id,
                    data_set_id="DemoDataSet",
                    ingestion_id="DemoDataSet_Ingestion",
                    target_state={"COMPLETED"},
                    check_interval=0,
                )
            else:
                mocked_wait_for_state.assert_not_called()

        mocked_client.create_ingestion.assert_called_with(
            AwsAccountId=expected_account_id, **MOCK_DATA
        )

    def test_create_ingestion_exception(self, mocked_account_id, mocked_client, caplog):
        mocked_client.create_ingestion.side_effect = ValueError("Fake Error")
        hook = QuickSightHook(aws_conn_id=None)
        with pytest.raises(ValueError, match="Fake Error"):
            hook.create_ingestion(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                ingestion_type="INCREMENTAL_REFRESH",
            )
        assert "create_ingestion API, error: Fake Error" in caplog.text
