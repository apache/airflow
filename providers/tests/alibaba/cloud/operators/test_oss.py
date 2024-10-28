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

from airflow.providers.alibaba.cloud.operators.oss import (
    OSSCreateBucketOperator,
    OSSDeleteBatchObjectOperator,
    OSSDeleteBucketOperator,
    OSSDeleteObjectOperator,
    OSSDownloadObjectOperator,
    OSSUploadObjectOperator,
)

MOCK_TASK_ID = "test-oss-operator"
MOCK_REGION = "mock_region"
MOCK_BUCKET = "mock_bucket_name"
MOCK_OSS_CONN_ID = "mock_oss_conn_default"
MOCK_KEY = "mock_key"
MOCK_KEYS = ["mock_key1", "mock_key_2", "mock_key3"]
MOCK_CONTENT = "mock_content"


class TestOSSCreateBucketOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSCreateBucketOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.create_bucket.assert_called_once_with(
            bucket_name=MOCK_BUCKET
        )


class TestOSSDeleteBucketOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSDeleteBucketOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.delete_bucket.assert_called_once_with(
            bucket_name=MOCK_BUCKET
        )


class TestOSSUploadObjectOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSUploadObjectOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
            key=MOCK_KEY,
            file=MOCK_CONTENT,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.upload_local_file.assert_called_once_with(
            bucket_name=MOCK_BUCKET, key=MOCK_KEY, file=MOCK_CONTENT
        )


class TestOSSDownloadObjectOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSDownloadObjectOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
            key=MOCK_KEY,
            file=MOCK_CONTENT,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.download_file.assert_called_once_with(
            bucket_name=MOCK_BUCKET, key=MOCK_KEY, local_file=MOCK_CONTENT
        )


class TestOSSDeleteBatchObjectOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSDeleteBatchObjectOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
            keys=MOCK_KEYS,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.delete_objects.assert_called_once_with(
            bucket_name=MOCK_BUCKET, key=MOCK_KEYS
        )


class TestOSSDeleteObjectOperator:
    @mock.patch("airflow.providers.alibaba.cloud.operators.oss.OSSHook")
    def test_execute(self, mock_hook):
        operator = OSSDeleteObjectOperator(
            task_id=MOCK_TASK_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            oss_conn_id=MOCK_OSS_CONN_ID,
            key=MOCK_KEY,
        )
        operator.execute(None)
        mock_hook.assert_called_once_with(
            oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION
        )
        mock_hook.return_value.delete_object.assert_called_once_with(
            bucket_name=MOCK_BUCKET, key=MOCK_KEY
        )
