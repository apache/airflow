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

import mock

from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

TASK_ID = "test-gcs-operator"
TEST_BUCKET = "test-bucket"
TEST_PROJECT = "test-project"
DELIMITER = ".csv"
PREFIX = "TEST"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
TEST_OBJECT = "dir1/test-object"
LOCAL_FILE_PATH = "/home/airflow/gcp/test-object"


class TestGoogleCloudStorageDownloadOperator(unittest.TestCase):
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
