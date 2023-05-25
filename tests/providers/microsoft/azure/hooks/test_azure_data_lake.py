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

import json
from unittest import mock

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook, AzureDataLakeStorageV2Hook
from airflow.utils import db
from tests.test_utils.providers import get_provider_min_airflow_version


class TestAzureDataLakeHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="adl_test_key",
                conn_type="azure_data_lake",
                login="client_id",
                password="client secret",
                extra=json.dumps({"tenant": "tenant", "account_name": "accountname"}),
            )
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_conn(self, mock_lib):
        from azure.datalake.store import core

        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        assert hook._conn is None
        assert hook.conn_id == "adl_test_key"
        assert isinstance(hook.get_conn(), core.AzureDLFileSystem)
        assert mock_lib.auth.called

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.core.AzureDLFileSystem", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_check_for_blob(self, mock_lib, mock_filesystem):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.check_for_file("file_path")
        mock_filesystem.glob.called

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.multithread.ADLUploader", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_upload_file(self, mock_lib, mock_uploader):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.upload_file(
            local_path="tests/hooks/test_adl_hook.py",
            remote_path="/test_adl_hook.py",
            nthreads=64,
            overwrite=True,
            buffersize=4194304,
            blocksize=4194304,
        )
        mock_uploader.assert_called_once_with(
            hook.get_conn(),
            lpath="tests/hooks/test_adl_hook.py",
            rpath="/test_adl_hook.py",
            nthreads=64,
            overwrite=True,
            buffersize=4194304,
            blocksize=4194304,
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.multithread.ADLDownloader", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_download_file(self, mock_lib, mock_downloader):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.download_file(
            local_path="test_adl_hook.py",
            remote_path="/test_adl_hook.py",
            nthreads=64,
            overwrite=True,
            buffersize=4194304,
            blocksize=4194304,
        )
        mock_downloader.assert_called_once_with(
            hook.get_conn(),
            lpath="test_adl_hook.py",
            rpath="/test_adl_hook.py",
            nthreads=64,
            overwrite=True,
            buffersize=4194304,
            blocksize=4194304,
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.core.AzureDLFileSystem", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_list_glob(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/*")
        mock_fs.return_value.glob.assert_called_once_with("file_path/*")

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.core.AzureDLFileSystem", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_list_walk(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/some_folder/")
        mock_fs.return_value.walk.assert_called_once_with("file_path/some_folder/")

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.core.AzureDLFileSystem", autospec=True)
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.lib", autospec=True)
    def test_remove(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.remove("filepath", True)
        mock_fs.return_value.remove.assert_called_once_with("filepath", recursive=True)

    def test_get_ui_field_behaviour_placeholders(self):
        """
        Check that ensure_prefixes decorator working properly

        Note: remove this test and the _ensure_prefixes decorator after min airflow version >= 2.5.0
        """
        assert list(AzureDataLakeHook.get_ui_field_behaviour()["placeholders"].keys()) == [
            "login",
            "password",
            "extra__azure_data_lake__tenant",
            "extra__azure_data_lake__account_name",
        ]
        if get_provider_min_airflow_version("apache-airflow-providers-microsoft-azure") >= (2, 5):
            raise Exception(
                "You must now remove `_ensure_prefixes` from azure utils."
                " The functionality is now taken care of by providers manager."
            )


class TestAzureDataLakeStorageV2Hook:
    def setup_class(self) -> None:
        self.conn_id: str = "adls_conn_id"
        self.file_system_name = "test_file_system"
        self.directory_name = "test_directory"
        self.file_name = "test_file_name"

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.create_file_system("test_file_system")
        expected_calls = [mock.call().create_file_system(file_system=self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.FileSystemClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_get_file_system(self, mock_conn, mock_file_system):
        mock_conn.return_value.get_file_system_client.return_value = mock_file_system
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.get_file_system(self.file_system_name)
        assert result == mock_file_system

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.DataLakeDirectoryClient")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_file_system"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.create_directory.return_value = mock_directory_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.create_directory(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.DataLakeDirectoryClient")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_file_system"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_get_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.get_directory_client.return_value = mock_directory_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.get_directory_client(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.DataLakeFileClient")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_file_system"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_file(self, mock_conn, mock_get_file_system, mock_file_client):
        mock_get_file_system.return_value.create_file.return_value = mock_file_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.create_file(self.file_system_name, self.file_name)
        assert result == mock_file_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_delete_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.delete_file_system(self.file_system_name)
        expected_calls = [mock.call().delete_file_system(self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.DataLakeDirectoryClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_delete_directory(self, mock_conn, mock_directory_client):
        mock_conn.return_value.get_directory_client.return_value = mock_directory_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.delete_directory(self.file_system_name, self.directory_name)
        expected_calls = [
            mock.call()
            .get_file_system_client(self.file_system_name)
            .get_directory_client(self.directory_name)
            .delete_directory()
        ]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_list_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.list_file_system(prefix="prefix")
        mock_conn.return_value.list_file_systems.assert_called_once_with(
            name_starts_with="prefix", include_metadata=False
        )

    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_file_system"
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_lake.AzureDataLakeStorageV2Hook.get_conn")
    def test_list_files_directory(self, mock_conn, mock_get_file_system):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.list_files_directory(self.file_system_name, self.directory_name)
        mock_get_file_system.return_value.get_paths.assert_called_once_with(self.directory_name)
