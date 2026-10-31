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
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential
from azure.storage.filedatalake import FileSystemClient
from azure.storage.filedatalake._models import FileSystemProperties
from azure.storage.filedatalake.aio import (
    DataLakeFileClient as AsyncDataLakeFileClient,
    DataLakeServiceClient as AsyncDataLakeServiceClient,
    FileSystemClient as AsyncFileSystemClient,
)

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

    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.ClientSecretCredential", autospec=True)
    def test_conn_with_tenant_uses_client_secret_credential(self, mock_credential, mock_filesystem):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")

        assert hook.get_conn() is mock_filesystem.return_value
        mock_credential.assert_called_once_with(
            tenant_id="tenant", client_id="client_id", client_secret="client secret"
        )
        mock_filesystem.assert_called_once_with(mock_credential.return_value, store_name="accountname")

    @pytest.mark.usefixtures("connection_without_tenant")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential", autospec=True)
    def test_conn_without_tenant_uses_default_azure_credential(self, mock_credential, mock_filesystem):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key_without_tenant")

        assert hook.get_conn() is mock_filesystem.return_value
        mock_credential.assert_called_once_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        mock_filesystem.assert_called_once_with(mock_credential.return_value, store_name="accountname")

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    def test_check_for_blob(self, mock_filesystem):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        mocked_glob = mock_filesystem.return_value.glob
        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.check_for_file("file_path")
        mocked_glob.assert_called()

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.multithread.ADLUploader", autospec=True)
    def test_upload_file(self, mock_uploader):
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
    def test_download_file(self, mock_downloader):
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
    def test_list_glob(self, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/*")
        mock_fs.return_value.glob.assert_called_once_with("file_path/*")

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    def test_list_walk(self, mock_fs):
        from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook

        hook = AzureDataLakeHook(azure_data_lake_conn_id="adl_test_key")
        hook.list("file_path/some_folder/")
        mock_fs.return_value.walk.assert_called_once_with("file_path/some_folder/")

    @pytest.mark.usefixtures("connection")
    @mock.patch(f"{MODULE}.core.AzureDLFileSystem", autospec=True)
    def test_remove(self, mock_fs):
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

    @mock.patch(f"{MODULE}.AzureDataLakeStorageV2Hook.get_file_system")
    def test_load_bytes(self, mock_get_file_system):
        mock_get_file_system.return_value = mock.create_autospec(FileSystemClient, instance=True)
        hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)

        hook.load_bytes(self.file_system_name, "dir/file.jsonl", b"data", overwrite=True)

        mock_get_file_system.assert_called_once_with(self.file_system_name)
        file_system = mock_get_file_system.return_value
        file_system.get_file_client.assert_called_once_with("dir/file.jsonl")
        file_system.get_file_client.return_value.upload_data.assert_called_once_with(b"data", overwrite=True)

    @staticmethod
    def async_service_client():
        """An async DataLakeServiceClient mock whose file client records the upload."""
        client = mock.create_autospec(AsyncDataLakeServiceClient, instance=True)
        client.__aenter__.return_value = client
        file_system = mock.create_autospec(AsyncFileSystemClient, instance=True)
        file_system.get_file_client.return_value = mock.create_autospec(
            AsyncDataLakeFileClient, instance=True
        )
        client.get_file_system_client.return_value = file_system
        return client

    async def run_aload_bytes(self, connection: Connection):
        """Run aload_bytes() against ``connection``; return the client class and credential class mocks."""
        client = self.async_service_client()
        client_cls = mock.create_autospec(AsyncDataLakeServiceClient, return_value=client)
        client_cls.from_connection_string.return_value = client
        credential_cls = mock.create_autospec(AsyncClientSecretCredential)
        client.credential = credential_cls.return_value

        async def get_connection(conn_id):
            return connection

        async def get_extra_dejson(conn):
            return conn.extra_dejson

        with (
            mock.patch(f"{MODULE}.get_async_connection", side_effect=get_connection),
            mock.patch(f"{MODULE}.get_async_extra_dejson", side_effect=get_extra_dejson),
            mock.patch(f"{MODULE}.AsyncDataLakeServiceClient", new=client_cls),
            mock.patch(f"{MODULE}.AsyncClientSecretCredential", new=credential_cls),
            mock.patch(
                f"{MODULE}.AzureDataLakeStorageV2Hook.get_conn", side_effect=AssertionError("sync client")
            ),
        ):
            hook = AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id)
            await hook.aload_bytes(self.file_system_name, "dir/file.jsonl", b"data", overwrite=True)
        return client_cls, credential_cls, client

    @pytest.mark.asyncio
    async def test_aload_bytes_with_a_service_principal(self):
        connection = Connection(
            conn_id=self.conn_id,
            conn_type="adls",
            login="client_id",
            password="secret",
            extra={
                "tenant_id": "tenant-id",
                "proxies": {"https": "https://proxy:80"},
                "account_url": "https://onelake.dfs.fabric.microsoft.com",
            },
        )

        client_cls, credential_cls, client = await self.run_aload_bytes(connection)

        credential_cls.assert_called_once_with(
            tenant_id="tenant-id",
            client_id="client_id",
            client_secret="secret",
            proxies={"https": "https://proxy:80"},
        )
        assert client_cls.call_args.kwargs["account_url"] == "https://onelake.dfs.fabric.microsoft.com"
        assert client_cls.call_args.kwargs["credential"] is credential_cls.return_value
        client.get_file_system_client.assert_called_once_with(self.file_system_name)
        file_client = client.get_file_system_client.return_value.get_file_client.return_value
        client.get_file_system_client.return_value.get_file_client.assert_called_once_with("dir/file.jsonl")
        file_client.upload_data.assert_awaited_once_with(b"data", overwrite=True)
        client.__aexit__.assert_awaited_once()
        credential_cls.return_value.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aload_bytes_with_a_connection_string(self):
        connection = Connection(
            conn_id=self.conn_id,
            conn_type="adls",
            extra={"connection_string": "DefaultEndpointsProtocol=https;AccountName=account"},
        )

        client_cls, credential_cls, _ = await self.run_aload_bytes(connection)

        client_cls.from_connection_string.assert_called_once()
        assert client_cls.from_connection_string.call_args.args[0].startswith("DefaultEndpointsProtocol")
        credential_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_aload_bytes_with_an_account_key(self):
        connection = Connection(conn_id=self.conn_id, conn_type="adls", host="account", password="key")

        client_cls, credential_cls, _ = await self.run_aload_bytes(connection)

        assert client_cls.call_args.kwargs["account_url"] == "https://account.dfs.core.windows.net"
        assert client_cls.call_args.kwargs["credential"] == "key"
        credential_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_async_conn_with_a_service_principal(self):
        connection = Connection(
            conn_id=self.conn_id,
            conn_type="adls",
            login="client_id",
            password="secret",
            extra={
                "tenant_id": "tenant-id",
                "proxies": {"https": "https://proxy:80"},
                "account_url": "https://onelake.dfs.fabric.microsoft.com",
            },
        )

        async def get_connection(conn_id):
            return connection

        async def get_extra_dejson(conn):
            return conn.extra_dejson

        with (
            mock.patch(f"{MODULE}.get_async_connection", side_effect=get_connection),
            mock.patch(f"{MODULE}.get_async_extra_dejson", side_effect=get_extra_dejson),
        ):
            client = await AzureDataLakeStorageV2Hook(adls_conn_id=self.conn_id).get_async_conn()

        async with client:
            assert isinstance(client, AsyncDataLakeServiceClient)
            assert client.url == "https://onelake.dfs.fabric.microsoft.com/"
            assert isinstance(client.credential, AsyncClientSecretCredential)
            assert client.credential._client_id == "client_id"
        await client.credential.close()

    def find_policy(self, conn, policy_type):
        policies = conn.credential._client._pipeline._impl_policies
        return next(
            map(
                lambda policy: policy._policy,
                filter(lambda policy: isinstance(policy._policy, policy_type), policies),
            )
        )
