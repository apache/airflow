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

from io import RawIOBase
from unittest import mock

from moto import mock_aws

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.azure_blob_to_s3 import (
    AzureBlobStorageToS3Operator,
)

TASK_ID = "test-gcs-list-operator"
CONTAINER_NAME = "test-container"
DELIMITER = ".csv"
PREFIX = "TEST"
S3_BUCKET = "s3://bucket/"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
S3_ACL_POLICY = "private-read"


def _create_test_bucket():
    hook = S3Hook(aws_conn_id="airflow_gcs_test")
    # We're mocking all actual AWS calls and don't need a connection.
    # This avoids an Airflow warning about connection cannot be found.
    hook.get_connection = lambda _: None
    bucket = hook.get_bucket("bucket")
    bucket.create()
    return hook, bucket


@mock_aws
class TestAzureBlobToS3Operator:
    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    def test_operator_all_file_upload(self, mock_hook):
        """
        Destination bucket has no file (of interest) common with origin bucket i.e
        Azure - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        S3 - []
        """
        mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            replace=False,
        )

        hook, _ = _create_test_bucket()
        uploaded_files = operator.execute(None)

        assert sorted(MOCK_FILES) == sorted(uploaded_files)
        assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))

    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    def test_operator_incremental_file_upload_without_replace(self, mock_hook):
        """
        Destination bucket has subset of files common with origin bucket i.e
        Azure - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        S3 - ["TEST1.csv"]
        """
        mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES
        get_file = mock_hook.return_value.get_file

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            # without replace
            replace=False,
        )

        hook, bucket = _create_test_bucket()
        # uploading only first file
        bucket.put_object(Key=MOCK_FILES[0], Body=b"testing")

        uploaded_files = operator.execute(None)

        assert sorted(MOCK_FILES[1:]) == sorted(uploaded_files)
        assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))
        assert get_file.call_count == len(MOCK_FILES[1:])

    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    def test_operator_incremental_file_upload_with_replace(self, mock_hook):
        """
        Destination bucket has subset of files common with origin bucket i.e
        Azure - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        S3 - ["TEST1.csv"]
        """
        mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES
        get_file = mock_hook.return_value.get_file

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            # with replace
            replace=True,
        )

        hook, bucket = _create_test_bucket()
        # uploading only first file
        bucket.put_object(Key=MOCK_FILES[0], Body=b"testing")

        uploaded_files = operator.execute(None)

        assert sorted(MOCK_FILES) == sorted(uploaded_files)
        assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))
        assert get_file.call_count == len(MOCK_FILES)

    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    def test_operator_no_file_upload_without_replace(self, mock_hook):
        """
        Destination bucket has all the files common with origin bucket i.e
        Azure - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        S3 - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        """
        mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES
        get_file = mock_hook.return_value.get_file

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            replace=False,
        )

        hook, bucket = _create_test_bucket()
        # uploading all the files
        for mock_file in MOCK_FILES:
            bucket.put_object(Key=mock_file, Body=b"testing")

        uploaded_files = operator.execute(None)

        assert sorted([]) == sorted(uploaded_files)
        assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))
        assert get_file.call_count == 0

    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    def test_operator_no_file_upload_with_replace(self, mock_hook):
        """
        Destination bucket has all the files common with origin bucket i.e
        Azure - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        S3 - ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
        """
        mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES
        get_file = mock_hook.return_value.get_file

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            replace=True,
        )

        hook, bucket = _create_test_bucket()
        # uploading all the files
        for mock_file in MOCK_FILES:
            bucket.put_object(Key=mock_file, Body=b"testing")

        uploaded_files = operator.execute(None)

        assert sorted(MOCK_FILES) == sorted(uploaded_files)
        assert sorted(MOCK_FILES) == sorted(hook.list_keys("bucket", delimiter="/"))
        # this ensures that upload happened
        assert get_file.call_count == len(MOCK_FILES)

    @mock.patch("tempfile.NamedTemporaryFile")
    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.WasbHook")
    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.S3Hook")
    def test_operator_should_pass_dest_s3_extra_args_and_s3_acl_policy(
        self, s3_hook_mock, wasb_hook_mock, mock_tempfile
    ):
        wasb_blob_name = "test_file"
        s3_acl_policy = "test policy"
        s3_extra_args = {"ContentLanguage": "value"}

        wasb_hook_mock.return_value.get_blobs_list_recursive.return_value = [
            wasb_blob_name
        ]
        wasb_hook_mock.return_value.download.return_value = RawIOBase()
        mock_tempfile.return_value.__enter__.return_value.name = "test_temp_file"

        # with current S3_BUCKET url, parse_s3_url would complain
        s3_hook_mock.parse_s3_url.return_value = ("bucket", wasb_blob_name)
        mock_load_files = s3_hook_mock.return_value.load_file

        operator = AzureBlobStorageToS3Operator(
            task_id=TASK_ID,
            container_name=CONTAINER_NAME,
            dest_s3_key=S3_BUCKET,
            replace=False,
            dest_s3_extra_args=s3_extra_args,
            s3_acl_policy=s3_acl_policy,
        )

        operator.execute(None)
        s3_hook_mock.assert_called_once_with(
            aws_conn_id="aws_default", extra_args=s3_extra_args, verify=None
        )
        mock_load_files.assert_called_once_with(
            filename="test_temp_file",
            key=f"{S3_BUCKET}{wasb_blob_name}",
            replace=False,
            acl_policy=s3_acl_policy,
        )
