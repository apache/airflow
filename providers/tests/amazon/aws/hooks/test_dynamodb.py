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

import uuid
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

TEST_IMPORT_ARN = "arn:aws:dynamodb:us-east-1:255683865591:table/test-table/import/01662190284205-aa94decf"


class TestDynamoDBHook:
    @mock_aws
    def test_get_conn_returns_a_boto3_connection(self):
        hook = DynamoDBHook(aws_conn_id="aws_default")
        conn = hook.get_conn()
        assert conn is not None
        assert conn.__class__.__name__ == "dynamodb.ServiceResource"

    @mock_aws
    def test_get_client_from_dynamodb_ressource(self):
        hook = DynamoDBHook(aws_conn_id="aws_default")
        client = hook.client
        assert client.__class__.__name__ == "DynamoDB"

    @mock_aws
    def test_insert_batch_items_dynamodb_table(self):
        hook = DynamoDBHook(
            aws_conn_id="aws_default",
            table_name="test_airflow",
            table_keys=["id"],
            region_name="us-east-1",
        )

        # this table needs to be created in production
        hook.get_conn().create_table(
            TableName="test_airflow",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        table = hook.get_conn().Table("test_airflow")

        items = [{"id": str(uuid.uuid4()), "name": "airflow"} for _ in range(10)]

        hook.write_batch_data(items)

        table.meta.client.get_waiter("table_exists").wait(TableName="test_airflow")
        assert table.item_count == 10

    @mock.patch("pathlib.Path.exists", return_value=True)
    def test_waiter_path_generated_from_resource_type(self, _):
        hook = DynamoDBHook(aws_conn_id="aws_default")
        path = hook.waiter_path
        assert path.as_uri().endswith(
            "/airflow/providers/amazon/aws/waiters/dynamodb.json"
        )

    @pytest.mark.parametrize(
        "response, status, error",
        [
            pytest.param(
                {"ImportTableDescription": {"ImportStatus": "COMPLETED"}},
                "COMPLETED",
                False,
                id="complete",
            ),
            pytest.param(
                {
                    "ImportTableDescription": {
                        "ImportStatus": "CANCELLING",
                        "FailureCode": "Failure1",
                        "FailureMessage": "Message",
                    }
                },
                "CANCELLING",
                True,
                id="cancel",
            ),
            pytest.param(
                {"ImportTableDescription": {"ImportStatus": "IN_PROGRESS"}},
                "IN_PROGRESS",
                False,
                id="progress",
            ),
        ],
    )
    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_get_s3_import_status(self, mock_make_api_call, response, status, error):
        mock_make_api_call.return_value = response
        hook = DynamoDBHook(aws_conn_id="aws_default")
        sta, code, msg = hook.get_import_status(import_arn=TEST_IMPORT_ARN)
        mock_make_api_call.assert_called_once_with(
            "DescribeImport", {"ImportArn": TEST_IMPORT_ARN}
        )
        assert sta == status
        if error:
            assert code == "Failure1"
            assert msg == "Message"
        else:
            assert code is None
            assert msg is None

    @pytest.mark.parametrize(
        "effect, error",
        [
            pytest.param(
                ClientError(
                    error_response={
                        "Error": {"Message": "Error message", "Code": "GeneralException"}
                    },
                    operation_name="UnitTest",
                ),
                ClientError,
                id="general-exception",
            ),
            pytest.param(
                ClientError(
                    error_response={
                        "Error": {
                            "Message": "Error message",
                            "Code": "ImportNotFoundException",
                        }
                    },
                    operation_name="UnitTest",
                ),
                AirflowException,
                id="not-found-exception",
            ),
        ],
    )
    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_get_s3_import_status_with_error(self, mock_make_api_call, effect, error):
        mock_make_api_call.side_effect = effect
        hook = DynamoDBHook(aws_conn_id="aws_default")
        with pytest.raises(error):
            hook.get_import_status(import_arn=TEST_IMPORT_ARN)

    def test_hook_has_import_waiters(self):
        hook = DynamoDBHook(aws_conn_id="aws_default")
        waiter = hook.get_waiter("import_table")
        assert waiter is not None
