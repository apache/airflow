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

from pathlib import Path
from unittest import mock

from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator

GCP_CONN_ID = "test"
DRIVE_FOLDER = Path("test_folder")
LOCAL_PATHS = [Path("test1"), Path("test2")]
REMOTE_FILE_IDS = ["rtest1", "rtest2"]


class TestLocalFilesystemToGoogleDriveOperator:
    @mock.patch("airflow.providers.google.suite.transfers.local_to_drive.GoogleDriveHook")
    def test_execute(self, mock_hook):
        context = {}
        mock_hook.return_value.upload_file.return_value = REMOTE_FILE_IDS
        op = LocalFilesystemToGoogleDriveOperator(
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
