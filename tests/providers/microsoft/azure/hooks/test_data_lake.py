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

from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from azure.core.pipeline.policies._universal import ProxyPolicy
from azure.storage.filedatalake._models import FileSystemProperties

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

if TYPE_CHECKING:
    from azure.storage.filedatalake import DataLakeServiceClient

MODULE = "airflow.providers.microsoft.azure.hooks.data_lake"


@pytest.fixture
def connection_without_tenant(create_mock_connections):
    create_mock_connections(
        Connection(
            conn_id="adl_test_key_without_tenant",
            conn_type="azure_data_lake",
            login="client_id",
            password="client secret",
            extra={"account_name": "accountname"},
        )
    )


@pytest.fixture
def connection(create_mock_connections):
    create_mock_connections(
        Connection(
            conn_id="adl_test_key",
            conn_type="azure_data_lake",
            login="client_id",
            password="client secret",
            extra={"tenant": "tenant", "account_name": "accountname"},
        )
    )


class TestAzureDataLakeHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_mock_connections):
        create_mock_connections(
            Connection(
                conn_id="adl_test_key",
                conn_type="azure_data_lake",
                login="client_id",
                password="client secret",
                extra={"tenant": "tenant", "account_name": "accountname"},
            )
        )

    @mock.patch(f"{MODULE}.lib", autospec=True)
    def test_conn(self, mock_lib):
        from azure.datalake.store import core

        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        assert hook._conn is None
        assert hook.conn_id == "adl_test_key"
        assert isinstance(hook.get_conn(), core.AzureDLFileSystem)
        assert mock_lib.auth.called

    @pytest.mark.usefixtures("connection_without_tenant")
    @mock.patch(f"{MODULE}.lib")
    @mock.patch(f"{MODULE}.AzureIdentityCredentialAdapter")
    def test_fallback_to_azure_identity_credential_adppter_when_tenant_is_not_provided(
        self,
        mock_azure_identity_credential_adapter,
        mock_datalake_store_lib,
    ):
        from azure.datalake.store import core

        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key_without_tenant")
        assert hook._conn is None
        assert hook.conn_id == "adl_test_key_without_tenant"
        assert isinstance(hook.get_conn(), core.AzureDLFileSystem)
        mock_azure_identity_credential_adapter.assert_called()
        args = mock_azure_identity_credential_adapter.call_args
        assert args.kwargs["managed_identity_client_id"] is None
        assert args.kwargs["workload_identity_tenant_id"] is None
        mock_datalake_store_lib.auth.assert_not_called()

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
    def test_check_for_blob(self, mock_lib, mock_filesystem):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        mocked_glob = mock_filesystem.return_value.glob
        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.check_for_file("file_path")
        mocked_glob.assert_called()

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.multithread.ADLUploader", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
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

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.multithread.ADLDownloader", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
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

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
    def test_list_glob(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/*")
        mock_fs.return_value.glob.assert_called_once_with("file_path/*")

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
    def test_list_walk(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/some_folder/")
        mock_fs.return_value.walk.assert_called_once_with("file_path/some_folder/")

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.lib", autospec=True)
    def test_remove(self, mock_lib, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.remove("filepath", True)
        mock_fs.return_value.remove.assert_called_once_with("filepath", recursive=True)


class TestAzureDataLakeStorageV2Hook:
    def setup_class(self) -> None:
        self.conn_id: str = "adls_conn_id1"
        self.file_system_name = "test_file_system"
        self.directory_name = "test_directory"
        self.file_name = "test_file_name"

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.create_file_system("test_file_system")
        expected_calls = [mock.call().create_file_system(file_system=self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.FileSystemClient")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_get_file_system(self, mock_conn, mock_file_system):
        mock_conn.return_value.get_file_system_client.return_value = mock_file_system
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.get_file_system(self.file_system_name)
        assert result == mock_file_system

    @mock.patch(f"{MODULE}.DataLakeDirectoryClient")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_file_system")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.create_directory.return_value = mock_directory_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.create_directory(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch(f"{MODULE}.DataLakeDirectoryClient")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_file_system")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_get_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.get_directory_client.return_value = mock_directory_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.get_directory_client(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch(f"{MODULE}.DataLakeFileClient")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_file_system")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_create_file(self, mock_conn, mock_get_file_system, mock_file_client):
        mock_get_file_system.return_value.create_file.return_value = mock_file_client
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        result = hook.create_file(self.file_system_name, self.file_name)
        assert result == mock_file_client

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_delete_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.delete_file_system(self.file_system_name)
        expected_calls = [mock.call().delete_file_system(self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.DataLakeDirectoryClient")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
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

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_list_file_system(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.list_file_system(prefix="prefix")
        mock_conn.return_value.list_file_systems.assert_called_once_with(
            name_starts_with="prefix", include_metadata=False
        )

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_file_system")
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_list_files_directory(self, mock_conn, mock_get_file_system):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.list_files_directory(self.file_system_name, self.directory_name)
        mock_get_file_system.return_value.get_paths.assert_called_once_with(self.directory_name)

    @pytest.mark.parametrize(
        argnames="list_file_systems_result",
        argvalues=[iter([FileSystemProperties]), iter([])],
    )
    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_connection_success(self, mock_conn, list_file_systems_result):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.get_conn().list_file_systems.return_value = list_file_systems_result
        status, msg = hook.test_connection()

        assert status is True
        assert msg == "Successfully connected to ADLS Gen2 Storage."

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn")
    def test_connection_failure(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        hook.get_conn().list_file_systems = PropertyMock(side_effect=Exception("Authentication failed."))
        status, msg = hook.test_connection()

        assert status is False
        assert msg == "Authentication failed."

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_connection")
    def test_proxies_passed_to_credentials(self, mock_conn):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
        mock_conn.return_value = Connection(
            conn_id=self.conn_id,
            login="client_id",
            password="secret",
            extra={
                "tenant_id": "tenant-id",
                "proxies": {"https": "https://proxy:80"},
                "account_url": "https://onelake.dfs.fabric.microsoft.com",
            },
        )
        conn: DataLakeServiceClient = hook.get_conn()

        assert conn is not None
        assert conn.primary_endpoint == "https://onelake.dfs.fabric.microsoft.com/"
        assert conn.primary_hostname == "onelake.dfs.fabric.microsoft.com"
        assert conn.scheme == "https"
        assert conn.url == "https://onelake.dfs.fabric.microsoft.com/"
        assert conn.credential._client_id == "client_id"
        assert conn.credential._client_credential == "secret"
        assert self.find_policy(conn, ProxyPolicy) is not None
        assert self.find_policy(conn, ProxyPolicy).proxies["https"] == "https://proxy:80"

    def find_policy(self, conn, policy_type):
        policies = conn.credential._client._pipeline._impl_policies
        return next(
            map(
                lambda policy: policy._policy,
                filter(lambda policy: isinstance(policy._policy, policy_type), policies),
            )
        )
