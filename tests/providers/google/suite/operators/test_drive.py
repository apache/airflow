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

from unittest import mock

from airflow.providers.google.suite.operators.drive import GoogleDriveUploadOperator

GCP_CONN_ID = "test"
DRIVE_FOLDER = "test_folder"
LOCAL_PATHS = ["test1", "test2"]
REMOTE_FILE_IDS = ["rtest1", "rtest2"]


class TestGoogleDriveUpload:
    @mock.patch("airflow.providers.google.suite.operators.drive.GoogleDriveHook")
    @mock.patch("airflow.providers.google.suite.operators.drive.GoogleDriveUploadOperator.xcom_push")
    def test_execute(self, mock_xcom, mock_hook):
        context = {}
        mock_hook.return_value.upload_file.return_value = REMOTE_FILE_IDS
        op = GoogleDriveUploadOperator(
            task_id="test_task", local_paths=LOCAL_PATHS, drive_folder=DRIVE_FOLDER, gcp_conn_id=GCP_CONN_ID
        )
        op.execute(context)

        calls = [
            mock.call(
                local_location="test1",
                remote_location="test_folder/test1",
                chunk_size=100 * 1024 * 1024,
                resumable=False,
            ),
            mock.call(
                local_location="test2",
                remote_location="test_folder/test2",
                chunk_size=100 * 1024 * 1024,
                resumable=False,
            ),
        ]
        mock_hook.return_value.upload_file.assert_has_calls(calls)

        xcom_calls = [
            mock.call(context, "remote_file_ids", REMOTE_FILE_IDS),
        ]
        mock_xcom.has_calls(xcom_calls)
