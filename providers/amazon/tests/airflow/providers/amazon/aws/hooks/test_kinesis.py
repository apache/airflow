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

import boto3
from moto import mock_firehose, mock_s3

from airflow.providers.amazon.aws.hooks.kinesis import FirehoseHook


@mock_firehose
class TestFirehoseHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = FirehoseHook(
            aws_conn_id="aws_default", delivery_stream="test_airflow", region_name="us-east-1"
        )
        assert hook.get_conn() is not None

    @mock_s3
    def test_insert_batch_records_kinesis_firehose(self):
        boto3.client("s3").create_bucket(Bucket="kinesis-test")
        hook = FirehoseHook(
            aws_conn_id="aws_default", delivery_stream="test_airflow", region_name="us-east-1"
        )

        response = hook.get_conn().create_delivery_stream(
            DeliveryStreamName="test_airflow",
            S3DestinationConfiguration={
                "RoleARN": "arn:aws:iam::123456789012:role/firehose_delivery_role",
                "BucketARN": "arn:aws:s3:::kinesis-test",
                "Prefix": "airflow/",
                "BufferingHints": {"SizeInMBs": 123, "IntervalInSeconds": 124},
                "CompressionFormat": "UNCOMPRESSED",
            },
        )

        stream_arn = response["DeliveryStreamARN"]
        assert stream_arn == "arn:aws:firehose:us-east-1:123456789012:deliverystream/test_airflow"

        records = [{"Data": str(uuid.uuid4())} for _ in range(100)]

        response = hook.put_records(records)

        assert response["FailedPutCount"] == 0
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
