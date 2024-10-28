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

from io import StringIO
from unittest import mock

import pytest
from azure.storage.fileshare import (
    DirectoryProperties,
    FileProperties,
    ShareDirectoryClient,
    ShareFileClient,
    ShareServiceClient,
)

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.fileshare import AzureFileShareHook

MODULE = "airflow.providers.microsoft.azure.hooks.fileshare"


class TestAzureFileshareHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="azure_fileshare_test_key",
                conn_type="azure_file_share",
                login="login",
                password="key",
            ),
            Connection(
                conn_id="azure_fileshare_extras",
                conn_type="azure_fileshare",
                login="login",
                extra={"sas_token": "token"},
            ),
            # Neither password nor sas_token present
            Connection(
                conn_id="azure_fileshare_missing_credentials",
                conn_type="azure_fileshare",
                login="login",
            ),
            Connection(
                conn_id="azure_fileshare_extras_wrong",
                conn_type="azure_fileshare",
                login="login",
                extra={"wrong_key": "token"},
            ),
            Connection(
                conn_id="azure_default",
                conn_type="azure",
                login="app_id",
                password="secret",
                extra={"tenantId": "t_id", "subscriptionId": "s_id"},
            ),
        )

    def test_key_and_connection(self):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_test_key")
        assert hook._conn_id == "azure_fileshare_test_key"
        share_client = hook.share_service_client
        assert isinstance(share_client, ShareServiceClient)

    def test_azure_default_share_service_client(self):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_default")
        assert hook._conn_id == "azure_default"
        share_client = hook.share_service_client
        assert isinstance(share_client, ShareServiceClient)

    def test_azure_default_share_directory_client(self):
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_default",
            share_name="share",
            directory_path="directory",
        )
        assert hook._conn_id == "azure_default"
        share_client = hook.share_directory_client
        assert isinstance(share_client, ShareDirectoryClient)

    def test_azure_default_share_file_client(self):
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_default", share_name="share", file_path="file"
        )
        assert hook._conn_id == "azure_default"
        share_client = hook.share_file_client
        assert isinstance(share_client, ShareFileClient)

    def test_sas_token(self):
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        assert hook._conn_id == "azure_fileshare_extras"
        share_client = hook.share_service_client
        assert isinstance(share_client, ShareServiceClient)

    @mock.patch(f"{MODULE}.ShareDirectoryClient", autospec=True)
    def test_check_for_directory(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.exists.return_value = True
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            directory_path="directory",
        )
        assert hook.check_for_directory()
        mock_instance.exists.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareFileClient", autospec=True)
    def test_load_data(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            file_path="file",
        )
        hook.load_data("big string")
        mock_instance.upload_file.assert_called_once_with("big string")

    @mock.patch(f"{MODULE}.ShareDirectoryClient", autospec=True)
    def test_list_directories_and_files(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            directory_path="directory",
        )
        hook.list_directories_and_files()
        mock_instance.list_directories_and_files.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareDirectoryClient", autospec=True)
    def test_list_files(self, mock_service):
        mock_instance = mock_service.return_value
        mock_instance.list_directories_and_files.return_value = [
            FileProperties(name="file1"),
            FileProperties(name="file2"),
            DirectoryProperties(name="dir1"),
            DirectoryProperties(name="dir2"),
        ]
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            directory_path="directory",
        )
        files = hook.list_files()
        assert files == ["file1", "file2"]
        mock_instance.list_directories_and_files.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareDirectoryClient", autospec=True)
    def test_create_directory(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            directory_path="directory",
        )
        hook.create_directory()
        mock_instance.create_directory.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareFileClient", autospec=True)
    def test_get_file(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            file_path="file",
        )
        hook.get_file("path")
        mock_instance.download_file.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareFileClient", autospec=True)
    def test_get_file_to_stream(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(
            azure_fileshare_conn_id="azure_fileshare_extras",
            share_name="share",
            file_path="file",
        )
        data = StringIO("stream")
        hook.get_file_to_stream(stream=data)
        mock_instance.download_file.assert_called_once_with()

    @mock.patch(f"{MODULE}.ShareServiceClient", autospec=True)
    def test_create_share(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.create_share(share_name="my_share")
        mock_instance.create_share.assert_called_once_with("my_share")

    @mock.patch(f"{MODULE}.ShareServiceClient", autospec=True)
    def test_delete_share(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        hook.delete_share("my_share")
        mock_instance.delete_share.assert_called_once_with("my_share")

    @mock.patch(f"{MODULE}.ShareServiceClient", autospec=True)
    def test_connection_success(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        mock_instance.list_shares.return_value = ["test_container"]
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure File Share."

    @mock.patch(f"{MODULE}.ShareServiceClient", autospec=True)
    def test_connection_failure(self, mock_service):
        mock_instance = mock_service.return_value
        hook = AzureFileShareHook(azure_fileshare_conn_id="azure_fileshare_extras")
        mock_instance.list_shares.side_effect = Exception("Test Connection Failure")
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Test Connection Failure"
