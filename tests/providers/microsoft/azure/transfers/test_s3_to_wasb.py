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
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator

from tests.test_utils.azure_system_helpers import WASB_CONNECTION_ID

TASK_ID = "test-s3-to-azure-blob-operator"
CONTAINER_NAME = "test-container"
DELIMITER = ".csv"
PREFIX = "TEST"
S3_BUCKET = "s3://test-bucket/"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
S3_ACL_POLICY = "private-read"


@mock_aws
class TestS3ToAzureBlobStorageOperator:

    @mock.patch("airflow.providers.amazon.aws.transfers.azure_blob_to_s3.S3Hook")
    @mock.patch("airflow.providers.amazon.microsoft.azure.transfers.s3_to_wasb.WasbHook")
    def test_operators_prefix(self, s3_mock_hook, wasb_mock_hook):
        # Set the list files that the S3Hook should return
        s3_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        wasb_mock_hook.return_value.get_blobs_list_recursive.return_value = []

        operator = S3ToAzureBlobStorageOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            s3_prefix="TEST",
            container_name=CONTAINER_NAME,
            blob_prefix="TEST"
        )
        # Need to create a container here
        uploaded_files = operator.execute(None)

        assert sorted(uploaded_files) == sorted(MOCK_FILES)


# Test helpers
def _create_container():
    hook = WasbHook(WASB_CONNECTION_ID)
    return hook
