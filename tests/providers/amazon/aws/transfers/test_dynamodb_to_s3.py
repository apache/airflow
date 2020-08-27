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
#
import json
import unittest
from unittest.mock import MagicMock, patch

from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import DynamoDBToS3Operator


class DynamodbToS3Test(unittest.TestCase):
    def setUp(self):
        self.output_queue = []

    def mock_upload_file(self, Filename, Bucket, Key):  # pylint: disable=unused-argument,invalid-name
        with open(Filename) as f:
            lines = f.readlines()
            for line in lines:
                self.output_queue.append(json.loads(line))

    @patch('airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook')
    @patch('airflow.providers.amazon.aws.transfers.dynamodb_to_s3.AwsDynamoDBHook')
    def test_dynamodb_to_s3_success(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {'Items': [{'a': 1}, {'b': 2}], 'LastEvaluatedKey': '123',},
            {'Items': [{'c': 3}],},
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id='dynamodb_to_s3',
            dynamodb_table_name='airflow_rocks',
            s3_bucket_name='airflow-bucket',
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})

        self.assertEqual([{'a': 1}, {'b': 2}, {'c': 3}], self.output_queue)
