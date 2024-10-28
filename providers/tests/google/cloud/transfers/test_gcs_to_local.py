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
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.models.xcom import MAX_XCOM_SIZE
from airflow.providers.google.cloud.transfers.gcs_to_local import (
    GCSToLocalFilesystemOperator,
)

TASK_ID = "test-gcs-operator"
TEST_BUCKET = "test-bucket"
TEST_PROJECT = "test-project"
DELIMITER = ".csv"
PREFIX = "TEST"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
TEST_OBJECT = "dir1/test-object"
LOCAL_FILE_PATH = "/home/airflow/gcp/test-object"
XCOM_KEY = "some_xkom_key"
FILE_CONTENT_STR = "some file content"
FILE_CONTENT_BYTES_UTF8 = b"some file content"
FILE_CONTENT_BYTES_UTF16 = b"\xff\xfes\x00o\x00m\x00e\x00 \x00f\x00i\x00l\x00e\x00 \x00c\x00o\x00n\x00t\x00e\x00n\x00t\x00"


class TestGoogleCloudStorageDownloadOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_local.GCSHook")
    def test_execute(self, mock_hook):
        operator = GCSToLocalFilesystemOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            object_name=TEST_OBJECT,
            filename=LOCAL_FILE_PATH,
        )

        operator.execute(None)
        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT, filename=LOCAL_FILE_PATH
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_local.GCSHook")
    def test_size_lt_max_xcom_size(self, mock_hook):
        operator = GCSToLocalFilesystemOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            object_name=TEST_OBJECT,
            store_to_xcom_key=XCOM_KEY,
        )
        context = {"ti": MagicMock()}
        mock_hook.return_value.download.return_value = FILE_CONTENT_BYTES_UTF8
        mock_hook.return_value.get_size.return_value = MAX_XCOM_SIZE - 1

        operator.execute(context=context)
        mock_hook.return_value.get_size.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT
        )
        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT
        )
        context["ti"].xcom_push.assert_called_once_with(
            key=XCOM_KEY, value=FILE_CONTENT_STR
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_local.GCSHook")
    def test_size_gt_max_xcom_size(self, mock_hook):
        operator = GCSToLocalFilesystemOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            object_name=TEST_OBJECT,
            store_to_xcom_key=XCOM_KEY,
        )
        context = {"ti": MagicMock()}
        mock_hook.return_value.download.return_value = FILE_CONTENT_BYTES_UTF8
        mock_hook.return_value.get_size.return_value = MAX_XCOM_SIZE + 1

        with pytest.raises(AirflowException, match="file is too large"):
            operator.execute(context=context)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_local.GCSHook")
    def test_xcom_encoding(self, mock_hook):
        operator = GCSToLocalFilesystemOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            object_name=TEST_OBJECT,
            store_to_xcom_key=XCOM_KEY,
            file_encoding="utf-16",
        )
        context = {"ti": MagicMock()}
        mock_hook.return_value.download.return_value = FILE_CONTENT_BYTES_UTF16
        mock_hook.return_value.get_size.return_value = MAX_XCOM_SIZE - 1

        operator.execute(context=context)
        mock_hook.return_value.get_size.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT
        )
        mock_hook.return_value.download.assert_called_once_with(
            bucket_name=TEST_BUCKET, object_name=TEST_OBJECT
        )
        context["ti"].xcom_push.assert_called_once_with(
            key=XCOM_KEY, value=FILE_CONTENT_STR
        )
