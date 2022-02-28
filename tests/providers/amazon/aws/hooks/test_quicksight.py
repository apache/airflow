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

from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook

MOCK_DATA = {
    "DataSetId": "DemoDataSet",
    "IngestionId": "DemoDataSet_Ingestion",
    "AwsAccountId": "123456789012",
    "IngestionType": "INCREMENTAL_REFRESH",
}

MOCK_RESPONSE = {
    "Status": 201,
    "Arn": "arn:aws:quicksight:us-east-1:123456789012:dataset/DemoDataSet/ingestion/DemoDataSet3_Ingestion",
    "IngestionId": "DemoDataSet_Ingestion",
    "IngestionStatus": "INITIALIZED",
    "RequestId": "fc1f7eea-1327-41d6-9af7-c12f097ed343",
}


class TestQuicksight:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = QuickSightHook(aws_conn_id="aws_default", region_name="us-east-1")
        assert hook.conn is not None

    @mock.patch.object(QuickSightHook, "get_conn")
    def test_create_ingestion(self, mock_conn):
        mock_conn.return_value.create_ingestion.return_value = MOCK_RESPONSE
        quicksight_hook = QuickSightHook(aws_conn_id="aws_default", region_name="us-east-1")
        result = quicksight_hook.create_ingestion(
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
            aws_account_id="123456789012",
            ingestion_type="INCREMENTAL_REFRESH",
        )
        expected_call_params = MOCK_DATA
        mock_conn.return_value.create_ingestion.assert_called_with(**expected_call_params)
        assert result == MOCK_RESPONSE

    def test_create_ingestion_exception(self):
        hook = QuickSightHook(aws_conn_id="aws_default")
        with pytest.raises(ClientError) as raised_exception:
            hook.create_ingestion(
                data_set_id="DemoDataSet",
                ingestion_id="DemoDataSet_Ingestion",
                aws_account_id="123456789012",
                ingestion_type="INCREMENTAL_REFRESH",
            )
        ex = raised_exception.value
        assert ex.operation_name == "CreateIngestion"
        assert ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404
