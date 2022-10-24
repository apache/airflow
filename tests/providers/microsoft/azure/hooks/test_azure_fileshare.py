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
"""
This module contains integration with Azure File Share.

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `wasb` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection `azure_fileshare_default` for an example).
"""
from __future__ import annotations

import json
import unittest
from unittest import mock

import pytest
from azure.storage.file import Directory, File

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook
from airflow.utils import db


class TestAzureFileshareHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id="azure_fileshare_test_key",
                conn_type="azure_file_share",
                login="login",
                password="key",
            )
        )
        db.merge_conn(
            Connection(
                conn_id="azure_fileshare_extras",
                conn_type="azure_fileshare",
                login="login",
                extra=json.dumps(
                    {
                        "extra__azure_fileshare__sas_token": "token",
                        "extra__azure_fileshare__protocol": "http",
                    }
                ),
            )
        )
        db.merge_conn(
            # Neither password nor sas_token present
            Connection(
                conn_id="azure_fileshare_missing_credentials",
                conn_type="azure_fileshare",
                login="login",
            )
        )
        db.merge_conn(
            Connection(
                conn_id="azure_fileshare_extras_deprecated",
                conn_type="azure_fileshare",
                login="login",
                extra=json.dumps(
                    {
                        "sas_token": "token",
                    }
                ),
            )
        )
        db.merge_conn(
            Connection(
                conn_id="azure_fileshare_extras_deprecated_empty_wasb_extra",
                conn_type="azure_fileshare",
                login="login",
                password="password",
                extra=json.dumps(
                    {
                        "extra__azure_fileshare__shared_access_key": "",
                    }
                ),
            )
        )

        db.merge_conn(
            Connection(
                conn_id="azure_fileshare_extras_wrong",
                conn_type="azure_fileshare",
                login="login",
                extra=json.dumps(
                    {
                        "wrong_key": "token",
                    }
                ),
            )
        )

    def test_key_and_connection(self):
        from azure.storage.file import FileService

        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_test_key")
        assert hook.conn_id == "azure_fileshare_test_key"
        assert hook._conn is None
        print(hook.get_conn())
        assert isinstance(hook.get_conn(), FileService)

    def test_sas_token(self):
        from azure.storage.file import FileService

        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        assert hook.conn_id == "azure_fileshare_extras"
        assert isinstance(hook.get_conn(), FileService)

    def test_deprecated_sas_token(self):
        from azure.storage.file import FileService

        with pytest.warns(DeprecationWarning):
            hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras_deprecated")
            assert hook.conn_id == "azure_fileshare_extras_deprecated"
            assert isinstance(hook.get_conn(), FileService)

    def test_deprecated_wasb_connection(self):
        from azure.storage.file import FileService

        with pytest.warns(DeprecationWarning):
            hook = AzureFileShareHook(
                azure_fileshare_conn_id="azure_fileshare_extras_deprecated_empty_wasb_extra"
            )
            assert hook.conn_id == "azure_fileshare_extras_deprecated_empty_wasb_extra"
            assert isinstance(hook.get_conn(), FileService)

    def test_wrong_extras(self):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras_wrong")
        assert hook.conn_id == "azure_fileshare_extras_wrong"
        with pytest.raises(TypeError, match=".*wrong_key.*"):
            hook.get_conn()

    def test_missing_credentials(self):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_missing_credentials")
        assert hook.conn_id == "azure_fileshare_missing_credentials"
        with pytest.raises(ValueError, match=".*account_key or sas_token.*"):
            hook.get_conn()

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_check_for_file(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        assert hook.check_for_file("share", "directory", "file", timeout=3)
        mock_instance.exists.assert_called_once_with("share", "directory", "file", timeout=3)

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_check_for_directory(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        assert hook.check_for_directory("share", "directory", timeout=3)
        mock_instance.exists.assert_called_once_with("share", "directory", timeout=3)

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_load_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.load_file("path", "share", "directory", "file", max_connections=1)
        mock_instance.create_file_from_path.assert_called_once_with(
            "share", "directory", "file", "path", max_connections=1
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_load_string(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.load_string("big string", "share", "directory", "file", timeout=1)
        mock_instance.create_file_from_text.assert_called_once_with(
            "share", "directory", "file", "big string", timeout=1
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_load_stream(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.load_stream("stream", "share", "directory", "file", 42, timeout=1)
        mock_instance.create_file_from_stream.assert_called_once_with(
            "share", "directory", "file", "stream", 42, timeout=1
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_list_directories_and_files(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.list_directories_and_files("share", "directory", timeout=1)
        mock_instance.list_directories_and_files.assert_called_once_with("share", "directory", timeout=1)

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_list_files(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.list_directories_and_files.return_value = [
            File("file1"),
            File("file2"),
            Directory("dir1"),
            Directory("dir2"),
        ]
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        files = hook.list_files("share", "directory", timeout=1)
        assert files == ["file1", "file2"]
        mock_instance.list_directories_and_files.assert_called_once_with("share", "directory", timeout=1)

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_create_directory(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.create_directory("share", "directory", timeout=1)
        mock_instance.create_directory.assert_called_once_with("share", "directory", timeout=1)

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_get_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.get_file("path", "share", "directory", "file", max_connections=1)
        mock_instance.get_file_to_path.assert_called_once_with(
            "share", "directory", "file", "path", max_connections=1
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_get_file_to_stream(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.get_file_to_stream("stream", "share", "directory", "file", max_connections=1)
        mock_instance.get_file_to_stream.assert_called_once_with(
            "share", "directory", "file", "stream", max_connections=1
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_create_share(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.create_share("my_share")
        mock_instance.create_share.assert_called_once_with("my_share")

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_delete_share(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.delete_share("my_share")
        mock_instance.delete_share.assert_called_once_with("my_share")

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_connection_success(self, mock_service):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.get_conn().list_shares.return_value = ["test_container"]
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure File Share."

    @mock.patch("airflow.providers.microsoft.azure.hooks.fileshare.FileService", autospec=True)
    def test_connection_failure(self, mock_service):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.get_conn().list_shares.side_effect = Exception("Test Connection Failure")
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Test Connection Failure"
