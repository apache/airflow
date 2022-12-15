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

import os
from unittest import mock

from moto import mock_s3

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator

BUCKET_NAME = os.environ.get("BUCKET_NAME", "test-airflow-bucket")
TASK_ID = os.environ.get("TASK_ID", "test-s3-operator")


class TestS3CreateBucketOperator:
    def setup_method(self):
        self.create_bucket_operator = S3CreateBucketOperator(
            task_id=TASK_ID,
            bucket_name=BUCKET_NAME,
        )

    @mock_s3
    @mock.patch.object(S3Hook, "create_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, mock_create_bucket):
        mock_check_for_bucket.return_value = True
        # execute s3 bucket create operator
        self.create_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_create_bucket.assert_not_called()

    @mock_s3
    @mock.patch.object(S3Hook, "create_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, mock_create_bucket):
        mock_check_for_bucket.return_value = False
        # execute s3 bucket create operator
        self.create_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_create_bucket.assert_called_once_with(bucket_name=BUCKET_NAME, region_name=None)


class TestS3DeleteBucketOperator:
    def setup_method(self):
        self.delete_bucket_operator = S3DeleteBucketOperator(
            task_id=TASK_ID,
            bucket_name=BUCKET_NAME,
        )

    @mock_s3
    @mock.patch.object(S3Hook, "delete_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_bucket_exist(self, mock_check_for_bucket, mock_delete_bucket):
        mock_check_for_bucket.return_value = True
        # execute s3 bucket delete operator
        self.delete_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_delete_bucket.assert_called_once_with(bucket_name=BUCKET_NAME, force_delete=False)

    @mock_s3
    @mock.patch.object(S3Hook, "delete_bucket")
    @mock.patch.object(S3Hook, "check_for_bucket")
    def test_execute_if_not_bucket_exist(self, mock_check_for_bucket, mock_delete_bucket):
        mock_check_for_bucket.return_value = False
        # execute s3 bucket delete operator
        self.delete_bucket_operator.execute({})
        mock_check_for_bucket.assert_called_once_with(BUCKET_NAME)
        mock_delete_bucket.assert_not_called()
