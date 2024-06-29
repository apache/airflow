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

import pytest
from moto import mock_aws

from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator

TASK_ID = "test-s3-to-azure-blob-operator"
AWS_CONN_ID = "test-conn-id"
S3_BUCKET = "test-bucket"
CONTAINER_NAME = "test-container"
PREFIX = "TEST"
TEMPFILE_NAME = "test-tempfile"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]

# Here are some of the tests that need to be run (for the get_files_to_move() function)
# 1. Prefix with no existing files, without replace                           [DONE]
# 2. Prefix with existing files, without replace                              [DONE]
# 3. Prefix with existing files, with replace                                 [DONE]
# 4. Two keys without existing files, without replace                         [DONE]
# 5. Two keys with existing files, without replace                            [DONE]
# 6. Two keys with existing files, with replace                               [DONE]
# 7. S3 key with Azure prefix, without existing files, without replace        [DONE]
# 8. S3 key with Azure prefix, with existing files, without replace           [DONE]
# 9. S3 key with Azure prefix, with existing files, with replace           [SKIPPED]

# Other tests that need to be run
# - Test __init__                                                             [DONE]
# - Test execute()                                                            [DONE]


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
        assert not operator.create_container  # Should be false (match default value in constructor)
        assert operator.replace
        assert not operator.s3_verify  # Should be false (match default value in constructor)
        assert operator.s3_extra_args == {}
        assert operator.wasb_extra_args == {}

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test__execute__prefix_without_replace_empty_destination(
        self, tempfile_mock, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return, along with an empty list of files in the Azure
        # Blob storage container. This scenario was picked for testing, as it's most likely the most common
        # setting the operator will be used in
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []

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
        assert sorted(uploaded_files) == sorted(MOCK_FILES)

        # Using the default connection ID, along with the default value of verify (for the S3 hook)
        s3_mock_hook.assert_called_once_with(aws_conn_id="aws_default", verify=False)
        wasb_mock_hook.assert_called_once_with(wasb_conn_id="wasb_default")

    # There are a number of very similar tests that use the same mocking, and for the most part, the same
    # logic. These tests are used to validate the records being returned by the get_files_to_move() method,
    # which heavily drives the successful execution of the operator
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move__prefix_without_replace_empty_destination(self, wasb_mock_hook, s3_mock_hook):
        # Set the list files that the S3Hook should return, as well as the list of files that are returned
        # when the get_blobs_list_recursive method is called using the WasbHook. In this scenario, the
        # destination is empty, meaning that the full list should be returned
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []

        # For testing, we're using a few default values. The most notable are the s3_conn_id and the
        # wasb_conn_id
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        uploaded_files = operator.get_files_to_move()
        assert sorted(uploaded_files) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move___prefix_without_replace_populated_destination(
        self, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES[1:]

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.get_files_to_move()
        assert sorted(uploaded_files) == sorted([MOCK_FILES[0]])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move__prefix_with_replace_populated_destination(
        self, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = MOCK_FILES[1:]

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
            replace=True,
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.get_files_to_move()

        # Since the replace parameter is being set to True, all the files that are present in the S3 bucket
        # will be moved to the Azure Blob, even though there are existing files in the Azure Blob
        assert sorted(uploaded_files) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_file_to_move__key_without_replace_empty_destination(self, wasb_mock_hook):
        # Different than above, able to remove the mocking of the list_keys method for the S3 hook (since a
        # single key is being passed, rather than a prefix). Here, there is no file present in the container,
        # so the file SHOULD be moved
        wasb_mock_hook.return_value.check_for_blob.return_value = False
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_name="TEST/TEST1.csv",
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.get_files_to_move()

        # Only the file name should be returned, rather than the entire blob name
        assert sorted(uploaded_files) == sorted(["TEST1.csv"])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_file_to_move__key_without_replace_populated_destination(self, wasb_mock_hook):
        # Different than above, able to remove the mocking of the list_keys method for the S3 hook (since a
        # single key is being passed, rather than a prefix). Here, there IS a file present in the container,
        # so the file should NOT be moved
        wasb_mock_hook.return_value.check_for_blob.return_value = True
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_name="TEST/TEST1.csv",
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.get_files_to_move()

        # Only the file name should be returned, rather than the entire blob name
        assert sorted(uploaded_files) == sorted([])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_file_to_move__key_with_replace_populated_destination(self, wasb_mock_hook):
        # Different than above, able to remove the mocking of the list_keys method for the S3 hook (since a
        # single key is being passed, rather than a prefix). Here, there IS a file present in the container,
        # and the value of replace is True, so the file SHOULD be moved
        wasb_mock_hook.return_value.check_for_blob.return_value = True  # Denoting presence of file in WASB
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_name="TEST/TEST1.csv",
            replace=True,
        )
        # Placing an empty "context" object here (using None)
        uploaded_files = operator.get_files_to_move()

        # Only the file name should be returned, rather than the entire blob name
        assert sorted(uploaded_files) == sorted(["TEST1.csv"])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move__s3_key_blob_prefix_without_replace_empty_destination(self, wasb_mock_hook):
        # A single S3 key is being used to move to a file to a container using a prefix. The files being
        # returned should take the same name as the file key that was passed to s3_key
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        uploaded_files = operator.get_files_to_move()
        assert sorted(uploaded_files) == sorted(["TEST1.csv"])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move__s3_key_blob_prefix_without_replace_populated_destination(
        self, wasb_mock_hook
    ):
        # A single S3 key is being used to move to a file to a container using a prefix. Since replace is
        # set to False, and the name of the file is present in the container, no file should be returned
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = ["TEST1.csv"]
        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )
        uploaded_files = operator.get_files_to_move()
        assert sorted(uploaded_files) == sorted([])

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__get_files_to_move__s3_prefix_blob_name_without_replace_empty_destination(
        self, wasb_mock_hook, s3_mock_hook
    ):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix=PREFIX,
            container_name=CONTAINER_NAME,
            blob_name="TEST/TEST1.csv",
        )

        # This should throw an exception, since more than a single S3 object is attempted to move to a single
        # Azure blob
        with pytest.raises(Exception):
            operator.get_files_to_move()

    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.S3Hook")
    @mock.patch("airflow.providers.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test__move_file(self, wasb_mock_hook, s3_mock_hook):
        # Only a single S3 key is provided, and there are no blobs in the container. This means that this file
        # should be moved, and the move_file method will be executed
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []
        s3_mock_hook.return_value.download_file.return_value = RawIOBase(b"test file contents")

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_key="TEST/TEST1.csv",
            container_name=CONTAINER_NAME,
            blob_prefix=PREFIX,
        )

        # Call the move_file method
        operator.move_file("TEST1.csv")

        # Test that the s3_hook has been called once (to create the client), and the wasb_hook has been called
        # to load the file to WASB
        operator.s3_hook.get_conn.assert_called_once()
        operator.wasb_hook.load_file.assert_called_once_with(
            file_path=mock.ANY,
            container_name=CONTAINER_NAME,
            blob_name=f"{PREFIX}/TEST1.csv",
            create_container=False,
        )

    def test__create_key(self):
        # There are three tests that will be run:
        # 1. Test will a full path
        # 2. Test with a prefix and a file name
        # 3. Test with no full path, and a missing file name
        assert S3ToAzureBlobStorageOperator._create_key("TEST/TEST1.csv", None, None) == "TEST/TEST1.csv"
        assert S3ToAzureBlobStorageOperator._create_key(None, "TEST", "TEST1.csv") == "TEST/TEST1.csv"
        with pytest.raises(Exception):
            S3ToAzureBlobStorageOperator._create_key(None, "TEST", None)
