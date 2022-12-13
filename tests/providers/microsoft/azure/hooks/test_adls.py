from __future__ import annotations

import json
from unittest import mock

import pytest
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.adls import AzureDataLakeStorageGen2


class TestAzureDataLakeStorageGen2:
    def setup_class(self) -> None:
        self.conn_id: str = "azure_data_lake_storage_v2"
        self.file_system_name = "test_file_system"
        self.directory_name = "test_directory"
        self.file_name = "test_file_name"

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_create_file_system(self, mock_conn):
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        hook.create_file_system("test_file_system")
        expected_calls = [mock.call().create_file_system(file_system=self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.FileSystemClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_get_file_system(self, mock_conn, mock_file_system):
        mock_conn.return_value.get_file_system_client.return_value = mock_file_system
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        result = hook.get_file_system(self.file_system_name)
        assert result == mock_file_system

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.DataLakeDirectoryClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_file_system")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_create_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.create_directory.return_value = mock_directory_client
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        result = hook.create_directory(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.DataLakeDirectoryClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_file_system")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_get_directory(self, mock_conn, mock_get_file_system, mock_directory_client):
        mock_get_file_system.return_value.get_directory_client.return_value = mock_directory_client
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        result = hook.get_directory_client(self.file_system_name, self.directory_name)
        assert result == mock_directory_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.DataLakeFileClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_file_system")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_create_file(self,mock_conn, mock_get_file_system, mock_file_client):
        mock_get_file_system.return_value.create_file.return_value = mock_file_client
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        result = hook.create_file(self.file_system_name, self.file_name)
        assert result == mock_file_client

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_delete_file_system(self, mock_conn):
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        hook.delete_file_system(self.file_system_name)
        expected_calls = [mock.call().delete_file_system(self.file_system_name)]
        mock_conn.assert_has_calls(expected_calls)

    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.DataLakeDirectoryClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.adls.AzureDataLakeStorageGen2.get_conn")
    def test_delete_directory(self, mock_conn, mock_directory_client):
        mock_conn.return_value.get_directory_client.return_value = mock_directory_client
        hook = AzureDataLakeStorageGen2(adls_v2_conn_id=self.conn_id)
        hook.delete_directory(self.file_system_name, self.directory_name)
        expected_calls = [mock.call().get_file_system_client(
            self.file_system_name).get_directory_client(self.directory_name).delete_directory()]
        mock_conn.assert_has_calls(expected_calls)


