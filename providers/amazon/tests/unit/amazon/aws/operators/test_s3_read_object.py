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

import boto3
import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ReadObjectOperator
from airflow.providers.common.compat.openlineage.facet import Dataset

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

BUCKET_NAME = "test-read-bucket"
S3_KEY = "test-key.txt"
CONTENT = "hello world from s3"


class TestS3ReadObjectOperator:
    def setup_method(self):
        self.operator = S3ReadObjectOperator(
            task_id="test-s3-read-object",
            s3_bucket=BUCKET_NAME,
            s3_key=S3_KEY,
        )

    @mock_aws
    def test_execute_reads_object(self):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=BUCKET_NAME)
        conn.put_object(Bucket=BUCKET_NAME, Key=S3_KEY, Body=CONTENT.encode("utf-8"))

        result = self.operator.execute({})
        assert result == CONTENT

    @mock_aws
    @mock.patch.object(S3Hook, "read_key")
    def test_execute_calls_read_key(self, mock_read_key):
        mock_read_key.return_value = CONTENT
        result = self.operator.execute({})
        mock_read_key.assert_called_once_with(key=S3_KEY, bucket_name=BUCKET_NAME)
        assert result == CONTENT

    @mock_aws
    def test_execute_with_s3_url(self):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=BUCKET_NAME)
        conn.put_object(Bucket=BUCKET_NAME, Key=S3_KEY, Body=b"url-content")

        op = S3ReadObjectOperator(
            task_id="test-s3-url",
            s3_key=f"s3://{BUCKET_NAME}/{S3_KEY}",
        )
        result = op.execute({})
        assert result == "url-content"

    @mock_aws
    def test_execute_empty_object(self):
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=BUCKET_NAME)
        conn.put_object(Bucket=BUCKET_NAME, Key=S3_KEY, Body=b"")

        result = self.operator.execute({})
        assert result == ""

    @pytest.mark.parametrize(
        ("bucket", "key"),
        (("bucket", "file.txt"), (None, "s3://bucket/file.txt")),
    )
    def test_get_openlineage_facets_on_start(self, bucket, key):
        expected_input = Dataset(
            namespace="s3://bucket",
            name="file.txt",
        )
        op = S3ReadObjectOperator(task_id="test", s3_bucket=bucket, s3_key=key)
        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.outputs) == 0
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0] == expected_input

    def test_template_fields(self):
        validate_template_fields(self.operator)
