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

from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor

TEST_FOLDER_ID = os.environ.get("GCP_GDRIVE_FOLDER_ID", "abcd1234")
TEST_FILE_NAME = os.environ.get("GCP_GDRIVE_DRIVE_ID", "gdrive_to_gcs_file.txt")
TEST_DRIVE_ID = os.environ.get("GCP_GDRIVE_TO_GCS_FILE_NAME", "abcd1234")
TEST_GCP_CONN_ID = "google_cloud_default"
TEST_DELEGATE_TO = "TEST_DELEGATE_TO"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestGoogleDriveFileSensor:
    @mock.patch("airflow.providers.google.suite.sensors.drive.GoogleDriveHook")
    def test_should_pass_argument_to_hook(self, mock_hook):
        task = GoogleDriveFileExistenceSensor(
            task_id="task-id",
            folder_id=TEST_FOLDER_ID,
            file_name=TEST_FILE_NAME,
            drive_id=TEST_DRIVE_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

        mock_hook.return_value.exists.return_value = True

        result = task.poke(mock.MagicMock())
        assert result

        mock_hook.assert_called_once_with(
            delegate_to=TEST_DELEGATE_TO,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.exists.assert_called_once_with(
            folder_id=TEST_FOLDER_ID, file_name=TEST_FILE_NAME, drive_id=TEST_DRIVE_ID
        )
