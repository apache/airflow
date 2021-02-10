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
#
import unittest
from unittest import mock

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from tests.providers.google.cloud.utils.base_gcp_mock import GCP_CONNECTION_WITH_PROJECT_ID


class TestGoogleDriveHook(unittest.TestCase):
    def setUp(self):
        self.patcher_get_connection = mock.patch(
            "airflow.hooks.base.BaseHook.get_connection", return_value=GCP_CONNECTION_WITH_PROJECT_ID
        )
        self.patcher_get_connection.start()
        self.gdrive_hook = GoogleDriveHook(gcp_conn_id="test")

    def tearDown(self) -> None:
        self.patcher_get_connection.stop()

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook._authorize",
        return_value="AUTHORIZE",
    )
    @mock.patch("airflow.providers.google.suite.hooks.drive.build")
    def test_get_conn(self, mock_discovery_build, mock_authorize):
        self.gdrive_hook.get_conn()
        mock_discovery_build.assert_called_once_with("drive", "v3", cache_discovery=False, http="AUTHORIZE")

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_ensure_folders_exists_when_no_folder_exists(self, mock_get_conn):
        mock_get_conn.return_value.files.return_value.list.return_value.execute.return_value = {"files": []}
        mock_get_conn.return_value.files.return_value.create.return_value.execute.side_effect = [
            {"id": "ID_1"},
            {"id": "ID_2"},
            {"id": "ID_3"},
            {"id": "ID_4"},
        ]

        result_value = self.gdrive_hook._ensure_folders_exists("AAA/BBB/CCC/DDD")

        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .files()
                .create(
                    body={
                        "name": "AAA",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["root"],
                    },
                    fields="id",
                ),
                mock.call()
                .files()
                .create(
                    body={
                        "name": "BBB",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["ID_1"],
                    },
                    fields="id",
                ),
                mock.call()
                .files()
                .create(
                    body={
                        "name": "CCC",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["ID_2"],
                    },
                    fields="id",
                ),
                mock.call()
                .files()
                .create(
                    body={
                        "name": "DDD",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["ID_3"],
                    },
                    fields="id",
                ),
            ],
            any_order=True,
        )

        assert "ID_4" == result_value

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_ensure_folders_exists_when_some_folders_exists(self, mock_get_conn):
        mock_get_conn.return_value.files.return_value.list.return_value.execute.side_effect = [
            {"files": [{"id": "ID_1"}]},
            {"files": [{"id": "ID_2"}]},
            {"files": []},
        ]
        mock_get_conn.return_value.files.return_value.create.return_value.execute.side_effect = [
            {"id": "ID_3"},
            {"id": "ID_4"},
        ]

        result_value = self.gdrive_hook._ensure_folders_exists("AAA/BBB/CCC/DDD")

        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .files()
                .create(
                    body={
                        "name": "CCC",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["ID_2"],
                    },
                    fields="id",
                ),
                mock.call()
                .files()
                .create(
                    body={
                        "name": "DDD",
                        "mimeType": "application/vnd.google-apps.folder",
                        "parents": ["ID_3"],
                    },
                    fields="id",
                ),
            ],
            any_order=True,
        )

        assert "ID_4" == result_value

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_ensure_folders_exists_when_all_folders_exists(self, mock_get_conn):
        mock_get_conn.return_value.files.return_value.list.return_value.execute.side_effect = [
            {"files": [{"id": "ID_1"}]},
            {"files": [{"id": "ID_2"}]},
            {"files": [{"id": "ID_3"}]},
            {"files": [{"id": "ID_4"}]},
        ]

        result_value = self.gdrive_hook._ensure_folders_exists("AAA/BBB/CCC/DDD")

        mock_get_conn.return_value.files.return_value.create.assert_not_called()
        assert "ID_4" == result_value

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_file_id")
    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_exists_when_file_exists(self, mock_get_conn, mock_method):
        folder_id = "abxy1z"
        drive_id = "abc123"
        file_name = "abc123.csv"

        result_value = self.gdrive_hook.exists(folder_id=folder_id, file_name=file_name, drive_id=drive_id)
        mock_method.assert_called_once_with(folder_id=folder_id, file_name=file_name, drive_id=drive_id)
        self.assertEqual(True, result_value)

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_file_id")
    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_exists_when_file_not_exists(self, mock_get_conn, mock_method):
        folder_id = "abxy1z"
        drive_id = "abc123"
        file_name = "abc123.csv"

        self.gdrive_hook.exists(folder_id=folder_id, file_name=file_name, drive_id=drive_id)
        mock_method.assert_called_once_with(folder_id=folder_id, file_name=file_name, drive_id=drive_id)

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_get_media_request(self, mock_get_conn):
        file_id = "1eC-Ahi4t57pHcLbW3C_xHB3-YrTQLQBa"

        self.gdrive_hook.get_media_request(file_id)
        mock_get_conn.return_value.files.return_value.get_media.assert_called_once_with(fileId=file_id)

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_get_file_id_when_one_file_exists(self, mock_get_conn):
        folder_id = "abxy1z"
        drive_id = "abc123"
        file_name = "abc123.csv"

        mock_get_conn.return_value.files.return_value.list.return_value.execute.side_effect = [
            {"files": [{"id": "ID_1", "mimeType": "text/plain"}]}
        ]

        result_value = self.gdrive_hook.get_file_id(folder_id, file_name, drive_id)
        self.assertEqual({"id": "ID_1", "mime_type": "text/plain"}, result_value)

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_get_file_id_when_multiple_files_exists(self, mock_get_conn):
        folder_id = "abxy1z"
        drive_id = "abc123"
        file_name = "abc123.csv"

        mock_get_conn.return_value.files.return_value.list.return_value.execute.side_effect = [
            {"files": [{"id": "ID_1", "mimeType": "text/plain"}, {"id": "ID_2", "mimeType": "text/plain"}]}
        ]

        result_value = self.gdrive_hook.get_file_id(folder_id, file_name, drive_id)
        self.assertEqual({"id": "ID_1", "mime_type": "text/plain"}, result_value)

    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    def test_get_file_id_when_no_file_exists(self, mock_get_conn):
        folder_id = "abxy1z"
        drive_id = "abc123"
        file_name = "abc123.csv"

        mock_get_conn.return_value.files.return_value.list.return_value.execute.side_effect = [{"files": []}]

        result_value = self.gdrive_hook.get_file_id(folder_id, file_name, drive_id)
        self.assertEqual({}, result_value)

    @mock.patch("airflow.providers.google.suite.hooks.drive.MediaFileUpload")
    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook._ensure_folders_exists")
    def test_upload_file_to_root_directory(
        self, mock_ensure_folders_exists, mock_get_conn, mock_media_file_upload
    ):
        mock_get_conn.return_value.files.return_value.create.return_value.execute.return_value = {
            "id": "FILE_ID"
        }

        return_value = self.gdrive_hook.upload_file("local_path", "remote_path")

        mock_ensure_folders_exists.assert_not_called()
        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .files()
                .create(
                    body={"name": "remote_path", "parents": ["root"]},
                    fields="id",
                    media_body=mock_media_file_upload.return_value,
                )
            ]
        )
        assert return_value == "FILE_ID"

    @mock.patch("airflow.providers.google.suite.hooks.drive.MediaFileUpload")
    @mock.patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
    @mock.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook._ensure_folders_exists",
        return_value="PARENT_ID",
    )
    def test_upload_file_to_subdirectory(
        self, mock_ensure_folders_exists, mock_get_conn, mock_media_file_upload
    ):
        mock_get_conn.return_value.files.return_value.create.return_value.execute.return_value = {
            "id": "FILE_ID"
        }

        return_value = self.gdrive_hook.upload_file("local_path", "AA/BB/CC/remote_path")

        mock_ensure_folders_exists.assert_called_once_with("AA/BB/CC")
        mock_get_conn.assert_has_calls(
            [
                mock.call()
                .files()
                .create(
                    body={"name": "remote_path", "parents": ["PARENT_ID"]},
                    fields="id",
                    media_body=mock_media_file_upload.return_value,
                )
            ]
        )
        assert return_value == "FILE_ID"
