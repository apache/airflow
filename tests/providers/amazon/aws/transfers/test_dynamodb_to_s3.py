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

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator, JSONEncoder


class JSONEncoderTest:
    def test_jsonencoder_with_decimal(self):
        """Test JSONEncoder correctly encodes and decodes decimal values."""

        for i in ["102938.3043847474", 1.010001, 10, "100", "1E-128", 1e-128]:
            org = Decimal(i)
            encoded = json.dumps(org, cls=JSONEncoder)
            decoded = json.loads(encoded, parse_float=Decimal)
            self.assertAlmostEqual(decoded, org)


class DynamodbToS3Test:
    def setup_method(self):
        self.output_queue = []

    def mock_upload_file(self, Filename, Bucket, Key):
        with open(Filename) as f:
            lines = f.readlines()
            for line in lines:
                self.output_queue.append(json.loads(line))

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_success(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_success_with_decimal(self, mock_aws_dynamodb_hook, mock_s3_hook):
        a = Decimal(10.028)
        b = Decimal("10.048")
        responses = [
            {
                "Items": [{"a": a}, {"b": b}],
            }
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": float(a)}, {"b": float(b)}] == self.output_queue

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_with_different_aws_conn_id(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        aws_conn_id = "test-conn-id"
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
            aws_conn_id=aws_conn_id,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

        mock_s3_hook.assert_called_with(aws_conn_id=aws_conn_id)
        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id=aws_conn_id)
