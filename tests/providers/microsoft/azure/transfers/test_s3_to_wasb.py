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

from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator

TASK_ID = "test-s3-to-azure-blob-operator"
AWS_CONN_ID = "test-conn-id"
S3_BUCKET = "s3://test-bucket/"
CONTAINER_NAME = "test-container"
DELIMITER = ".csv"
PREFIX = "TEST"
TEMPFILE_NAME = "test-tempfile"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]

# Here are some of the tests that need to be run (for the execute() function)
# 1. Prefix with no existing files, without replace [DONE]
# 2. Prefix with existing files, without replace [DONE]
# 3. Prefix with existing files, with replace [DONE]
# 4. Two keys without existing files, without replace [DONE]
# 5. Two keys with existing files, without replace
# 6. Two keys with existing files, with replace
# 7. S3 key with Azure prefix, without existing files, without replace
# 8. S3 key with Azure prefix, with existing files, without replace
# 9. S3 key with Azure prefix, with existing files, with replace

# Other tests that need to be run
# - Test __init__
# - Test get_files_to_move


@mock_aws
class TestS3ToAzureBlobStorageOperator:
    def test__init__(self):
        # Create a mock operator with a single set of parameters that are used to test the __init__()
        # constructor. Not every parameter needs to be provided, as this code will also be used to test the
        # default parameters that are configured
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            aws_conn_id="test-conn-id",
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
            replace=True,
            delimiter=DELIMITER
        )

        # ... is None is used to validate if a value is None, while not ... is used to evaluate if a value
        # is False
        assert operator.task_id == TASK_ID
        assert operator.aws_conn_id == AWS_CONN_ID
        assert operator.wasb_conn_id == "wasb_default"
        assert operator.s3_bucket == S3_BUCKET
        assert operator.container_name == CONTAINER_NAME
        assert operator.s3_prefix == PREFIX
        assert operator.s3_key is None
        assert operator.blob_prefix == PREFIX
        assert operator.blob_name is None
        assert operator.delimiter == DELIMITER
        assert not operator.create_container  # Should be false (match default value in constructor)
        assert operator.replace
        assert not operator.s3_verify  # Should be false (match default value in constructor)
        assert operator.s3_extra_args == {}
        assert operator.wasb_extra_args == {}

    # There are a number of very similar tests that use the same mocking, and for the most part, the same
    # logic
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_execute_prefix_without_replace_empty_destination(
        self, tempfile_mock, wasb_mock_hook, s3_mock_hook
    ):
        # TODO: Ideally, I'd like to rewrite this to use the get_files_to_move() method, rather than execute
        # Set the list files that the S3Hook should return, as well as the list of files that are returned
        # when the get_blobs_list_recursive method is called using the WasbHook. In this scenario, the
        # destination is empty, meaning that the full list should be returned
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []

        s3_mock_hook.return_value.download_file.return_value = RawIOBase(b"test file contents")
        tempfile_mock.return_value.__enter__.return_value.name = TEMPFILE_NAME

        # For testing, we're using a few default values. The most notable are the s3_conn_id and the
        # wasb_conn_id
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        # When the execute() method is called in the operator, it first creates two hooks (S3Hook and
        # WasbHook). The return values from any calls that use these hooks are mocked above, allowing the
        # tests to run in a test setting
        uploaded_files = operator.execute(None)

        assert sorted(uploaded_files) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_execute_prefix_without_replace_populated_destination(
        self, tempfile_mock, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES[1:]

        s3_mock_hook.return_value.download_file.return_value = RawIOBase(b"test file contents")
        tempfile_mock.return_value.__enter__.return_value.name = TEMPFILE_NAME

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.execute(None)

        assert sorted(uploaded_files) == sorted([MOCK_FILES[0]])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_execute_prefix_with_replace_populated_destination(
        self, tempfile_mock, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES[1:]

        s3_mock_hook.return_value.download_file.return_value = RawIOBase(b"test file contents")
        tempfile_mock.return_value.__enter__.return_value.name = TEMPFILE_NAME

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
            replace=True,
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.execute(None)

        # Since the replace parameter is being set to True, all the files that are present in the S3 bucket
        # will be moved to the Azure Blob, even though there are existing files in the Azure Blob
        assert sorted(uploaded_files) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_execute_key_without_replace_empty_destination(
        self, tempfile_mock, wasb_mock_hook, s3_mock_hook
    ):
        # Different than above, able to remove the mocking of the list_keys method for the S3 hook (since a
        # single key is being passed, rather than a prefix). Here, there is no file present in the container,
        # so the file can be moved
        wasb_mock_hook.return_value.check_for_blob.return_value = False

        s3_mock_hook.return_value.download_file.return_value = RawIOBase(b"test file contents")
        tempfile_mock.return_value.__enter__.return_value.name = TEMPFILE_NAME

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_name="TEST/TEST1.csv",
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.execute(None)

        # Only the file name should be returned, rather than the entire blob name
        assert sorted(uploaded_files) == sorted(["TEST1.csv"])


# Test helpers
