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

import unittest
import uuid

from airflow.providers.amazon.aws.hooks.kinesis import AwsFirehoseHook

try:
    from moto import mock_kinesis
except ImportError:
    mock_kinesis = None


class TestAwsFirehoseHook(unittest.TestCase):

    @unittest.skipIf(mock_kinesis is None, 'mock_kinesis package not present')
    @mock_kinesis
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsFirehoseHook(aws_conn_id='aws_default',
                               delivery_stream="test_airflow", region_name="us-east-1")
        self.assertIsNotNone(hook.get_conn())

    @unittest.skipIf(mock_kinesis is None, 'mock_kinesis package not present')
    @mock_kinesis
    def test_insert_batch_records_kinesis_firehose(self):
        hook = AwsFirehoseHook(aws_conn_id='aws_default',
                               delivery_stream="test_airflow", region_name="us-east-1")

        response = hook.get_conn().create_delivery_stream(
            DeliveryStreamName="test_airflow",
            S3DestinationConfiguration={
                'RoleARN': 'arn:aws:iam::123456789012:role/firehose_delivery_role',
                'BucketARN': 'arn:aws:s3:::kinesis-test',
                'Prefix': 'airflow/',
                'BufferingHints': {
                    'SizeInMBs': 123,
                    'IntervalInSeconds': 124
                },
                'CompressionFormat': 'UNCOMPRESSED',
            }
        )

        stream_arn = response['DeliveryStreamARN']
        self.assertEqual(
            stream_arn, "arn:aws:firehose:us-east-1:123456789012:deliverystream/test_airflow")

        records = [{"Data": str(uuid.uuid4())}
                   for _ in range(100)]

        response = hook.put_records(records)

        self.assertEqual(response['FailedPutCount'], 0)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
