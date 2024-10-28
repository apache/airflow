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
from botocore.exceptions import ClientError, WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.transfers.s3_to_dynamodb import S3ToDynamoDBOperator

TASK_ID = "transfer_1"
BUCKET = "test-bucket"
S3_KEY_PREFIX = "test/test_data"
S3_KEY = "test/test_data_file_1.csv"
S3_CONN_ID = "aws_default"
DYNAMODB_TABLE_NAME = "test-table"
DYNAMODB_ATTRIBUTES = [
    {"AttributeName": "attribute_a", "AttributeType": "S"},
    {"AttributeName": "attribute_b", "AttributeType": "I"},
]
DYNAMODB_KEY_SCHEMA = [
    {"AttributeName": "attribute_a", "KeyType": "HASH"},
]

DYNAMODB_PROV_THROUGHPUT = {"ReadCapacityUnits": 123, "WriteCapacityUnits": 123}
SUCCESS_S3_RESPONSE = {
    "ImportTableDescription": {
        "ImportArn": "arn:aws:dynamodb:import",
        "ImportStatus": "IN_PROGRESS",
        "TableArn": "arn:aws:dynamodb:table",
        "TableId": "test-table",
        "ClientToken": "client",
    }
}
FAILURE_S3_RESPONSE = {
    "ImportTableDescription": {
        "ImportArn": "arn:aws:dynamodb:import",
        "ImportStatus": "FAILED",
        "TableArn": "arn:aws:dynamodb:table",
        "TableId": "test-table",
        "ClientToken": "client",
        "FailureCode": "300",
        "FailureMessage": "invalid csv format",
    }
}
IMPORT_TABLE_RESPONSE = {
    "S3BucketSource": {"S3Bucket": "test-bucket", "S3KeyPrefix": "test/test_data"},
    "InputFormat": "DYNAMODB_JSON",
    "TableCreationParameters": {
        "TableName": "test-table",
        "AttributeDefinitions": [
            {"AttributeName": "attribute_a", "AttributeType": "S"},
            {"AttributeName": "attribute_b", "AttributeType": "I"},
        ],
        "KeySchema": [{"AttributeName": "attribute_a", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "ProvisionedThroughput": {"ReadCapacityUnits": 123, "WriteCapacityUnits": 123},
    },
}


@pytest.fixture
def new_table_op():
    return S3ToDynamoDBOperator(
        task_id=TASK_ID,
        s3_key=S3_KEY_PREFIX,
        s3_bucket=BUCKET,
        dynamodb_table_name=DYNAMODB_TABLE_NAME,
        dynamodb_attributes=DYNAMODB_ATTRIBUTES,
        dynamodb_key_schema=DYNAMODB_KEY_SCHEMA,
        aws_conn_id=S3_CONN_ID,
        import_table_creation_kwargs={"ProvisionedThroughput": DYNAMODB_PROV_THROUGHPUT},
    )


@pytest.fixture
def exist_table_op():
    return S3ToDynamoDBOperator(
        task_id=TASK_ID,
        s3_key=S3_KEY,
        dynamodb_key_schema=DYNAMODB_KEY_SCHEMA,
        s3_bucket=BUCKET,
        dynamodb_table_name=DYNAMODB_TABLE_NAME,
        use_existing_table=True,
        aws_conn_id=S3_CONN_ID,
    )


class TestS3ToDynamoDBOperator:
    @mock.patch.object(DynamoDBHook, "get_waiter")
    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_s3_to_dynamodb_new_table_wait_for_completion(self, mock_make_api_call, mock_wait, new_table_op):
        mock_make_api_call.return_value = SUCCESS_S3_RESPONSE

        res = new_table_op.execute(None)

        mock_make_api_call.assert_called_once_with("ImportTable", IMPORT_TABLE_RESPONSE)
        mock_wait.assert_called_once_with("import_table")
        mock_wait.return_value.wait.assert_called_once_with(
            ImportArn="arn:aws:dynamodb:import", WaiterConfig={"Delay": 30, "MaxAttempts": 240}
        )
        assert res == "arn:aws:dynamodb:import"

    @pytest.mark.parametrize(
        "delete_on_error",
        [
            pytest.param(
                True,
                id="delete-on-error",
            ),
            pytest.param(
                False,
                id="no-delete-on-error",
            ),
        ],
    )
    @mock.patch("airflow.providers.amazon.aws.transfers.s3_to_dynamodb.DynamoDBHook")
    def test_s3_to_dynamodb_new_table_delete_on_error(self, mock_hook, new_table_op, delete_on_error):
        mock_wait = mock.Mock()
        mock_wait.side_effect = WaiterError(name="NetworkError", reason="unit test error", last_response={})
        mock_hook.return_value.get_waiter.return_value.wait = mock_wait
        new_table_op.delete_on_error = delete_on_error
        mock_hook.return_value.get_import_status.return_value = "FAILED", "400", "General error"

        with pytest.raises(AirflowException):
            new_table_op.execute(None)

        if delete_on_error:
            mock_hook.return_value.client.delete_table.assert_called_once_with(TableName="test-table")
        else:
            mock_hook.return_value.client.delete_table.assert_not_called()

    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_s3_to_dynamodb_new_table_no_wait(self, mock_make_api_call):
        mock_make_api_call.return_value = SUCCESS_S3_RESPONSE
        op = S3ToDynamoDBOperator(
            task_id=TASK_ID,
            s3_key=S3_KEY_PREFIX,
            s3_bucket=BUCKET,
            dynamodb_table_name=DYNAMODB_TABLE_NAME,
            dynamodb_attributes=DYNAMODB_ATTRIBUTES,
            dynamodb_key_schema=DYNAMODB_KEY_SCHEMA,
            aws_conn_id=S3_CONN_ID,
            import_table_creation_kwargs={"ProvisionedThroughput": DYNAMODB_PROV_THROUGHPUT},
            wait_for_completion=False,
        )
        res = op.execute(None)

        mock_make_api_call.assert_called_once_with("ImportTable", IMPORT_TABLE_RESPONSE)
        assert res == "arn:aws:dynamodb:import"

    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_s3_to_dynamodb_new_table_client_error(self, mock_make_api_call, new_table_op):
        mock_make_api_call.side_effect = ClientError(
            error_response={"Error": {"Message": "Error message", "Code": "GeneralException"}},
            operation_name="UnitTest",
        )
        with pytest.raises(AirflowException) as excinfo:
            new_table_op.execute(None)
        assert "S3 load into DynamoDB table failed with error" in str(
            excinfo.value
        ), "Exception message not passed correctly"

    @mock.patch("botocore.client.BaseClient._make_api_call")
    def test_s3_to_dynamodb_new_table_job_startup_error(self, mock_make_api_call, new_table_op):
        mock_make_api_call.return_value = FAILURE_S3_RESPONSE
        exp_err_msg = "S3 into Dynamodb job creation failed. Code: 300. Failure: invalid csv format"
        with pytest.raises(AirflowException) as excinfo:
            new_table_op.execute(None)
        assert str(excinfo.value) == exp_err_msg, "Exception message not passed correctly"

    @mock.patch(
        "airflow.providers.amazon.aws.transfers.s3_to_dynamodb.S3ToDynamoDBOperator._load_into_new_table"
    )
    @mock.patch.object(DynamoDBHook, "get_conn")
    def test_s3_to_dynamodb_existing_table(self, mock_get_conn, new_table_load_mock, exist_table_op):
        response = [
            {
                "Items": [
                    {"Date": {"N": "54675846"}, "Message": {"S": "Message1"}, "_id": {"S": "1"}},
                    {"Date": {"N": "54675847"}, "Message": {"S": "Message2"}, "_id": {"S": "2"}},
                    {"Date": {"N": "54675857"}, "Message": {"S": "Message3"}, "_id": {"S": "4"}},
                ]
            }
        ]
        batch_writer_calls = [mock.call(Item=item) for item in response[0]["Items"]]
        mock_paginator = mock.Mock()
        mock_paginator.paginate.return_value = response

        mock_conn = mock.MagicMock()
        mock_client = mock.Mock()
        mock_put_item = mock.Mock()

        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.meta.client = mock_client
        mock_conn.Table.return_value.batch_writer.return_value.__enter__.return_value.put_item = mock_put_item
        mock_conn.Table.return_value.table_arn = "arn:aws:dynamodb"
        mock_get_conn.return_value = mock_conn

        res = exist_table_op.execute(None)

        new_table_load_mock.assert_called_once_with(
            table_name=exist_table_op.tmp_table_name, delete_on_error=False
        )
        mock_client.get_paginator.assert_called_once_with("scan")
        mock_client.get_paginator.return_value.paginate.assert_called_once_with(
            TableName=exist_table_op.tmp_table_name,
            Select="ALL_ATTRIBUTES",
            ReturnConsumedCapacity="NONE",
            ConsistentRead=True,
        )
        mock_conn.Table.assert_called_with("test-table")
        mock_conn.Table.return_value.batch_writer.assert_called_once_with(overwrite_by_pkeys=["attribute_a"])
        mock_put_item.assert_has_calls(batch_writer_calls)
        mock_client.delete_table.assert_called_once_with(TableName=exist_table_op.tmp_table_name)
        assert res == "arn:aws:dynamodb"

        # Test case: exception during pagination should prevent return but still delete temp table
        mock_paginator.paginate.side_effect = Exception("Pagination error")
        with self.assertLogs(exist_table_op.log, level="INFO") as log:
            res = None
            try:
                res = exist_table_op.execute(None)
            except Exception as e:
                self.assertIsNone(res)  # No return value on exception
                assert "Pagination error" in str(e)
                mock_client.delete_table.assert_called_with(TableName=exist_table_op.tmp_table_name)
                assert "Deleting tmp DynamoDB table" in log.output[-1]

        # Test case: exception during batch writing should prevent return but still delete temp table
        mock_paginator.paginate.side_effect = None  # Reset paginator to work normally
        mock_put_item.side_effect = Exception("Batch write error")
        with self.assertLogs(exist_table_op.log, level="INFO") as log:
            res = None
            try:
                res = exist_table_op.execute(None)
            except Exception as e:
                self.assertIsNone(res)  # No return value on exception
                assert "Batch write error" in str(e)
                mock_client.delete_table.assert_called_with(TableName=exist_table_op.tmp_table_name)
                assert "Deleting tmp DynamoDB table" in log.output[-1]

        # Reset mock side effects after each test
        mock_put_item.side_effect = None
        mock_client.delete_table.side_effect = None
