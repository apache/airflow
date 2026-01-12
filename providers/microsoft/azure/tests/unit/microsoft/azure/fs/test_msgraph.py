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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.microsoft.azure.fs.msgraph import get_fs


@pytest.fixture
def mock_connection():
    return Connection(
        conn_id="msgraph_default",
        conn_type="msgraph",
        login="test_client_id",
        password="test_client_secret",
        host="test_tenant_id",
        extra={"drive_id": "test_drive_id"},
    )


@pytest.fixture
def mock_connection_minimal():
    return Connection(
        conn_id="msgraph_minimal",
        conn_type="msgraph",
        login="test_client_id",
        password="test_client_secret",
        host="test_tenant_id",
    )


class TestMSGraphFS:
    @patch("airflow.providers.microsoft.azure.fs.msgraph.BaseHook.get_connection")
    @patch("msgraphfs.MSGDriveFS")
    def test_get_fs_with_drive_id(self, mock_msgdrivefs, mock_get_connection, mock_connection):
        mock_get_connection.return_value = mock_connection
        mock_fs_instance = MagicMock()
        mock_msgdrivefs.return_value = mock_fs_instance

        result = get_fs("msgraph_default")

        mock_msgdrivefs.assert_called_once_with(
            drive_id="test_drive_id",
            oauth2_client_params={
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
                "tenant_id": "test_tenant_id",
            },
        )
        assert result == mock_fs_instance

    @patch("msgraphfs.MSGDriveFS")
    def test_get_fs_no_connection(self, mock_msgdrivefs):
        mock_fs_instance = MagicMock()
        mock_msgdrivefs.return_value = mock_fs_instance

        result = get_fs(None)

        mock_msgdrivefs.assert_called_once_with({})
        assert result == mock_fs_instance

    @patch("airflow.providers.microsoft.azure.fs.msgraph.BaseHook.get_connection")
    @patch("msgraphfs.MSGDriveFS")
    def test_get_fs_with_extra_oauth_params(self, mock_msgdrivefs, mock_get_connection):
        connection = Connection(
            conn_id="msgraph_extra",
            conn_type="msgraph",
            login="test_client_id",
            password="test_client_secret",
            host="test_tenant_id",
            extra={
                "drive_id": "test_drive_id",
                "scope": "https://graph.microsoft.com/.default",
                "token_endpoint": "https://login.microsoftonline.com/test/oauth2/v2.0/token",
                "redirect_uri": "http://localhost:8080/callback",
            },
        )
        mock_get_connection.return_value = connection
        mock_fs_instance = MagicMock()
        mock_msgdrivefs.return_value = mock_fs_instance

        result = get_fs("msgraph_extra")

        expected_oauth2_params = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "tenant_id": "test_tenant_id",
            "scope": "https://graph.microsoft.com/.default",
            "token_endpoint": "https://login.microsoftonline.com/test/oauth2/v2.0/token",
            "redirect_uri": "http://localhost:8080/callback",
        }
        mock_msgdrivefs.assert_called_once_with(
            drive_id="test_drive_id", oauth2_client_params=expected_oauth2_params
        )
        assert result == mock_fs_instance

    @patch("airflow.providers.microsoft.azure.fs.msgraph.BaseHook.get_connection")
    @patch("msgraphfs.MSGDriveFS")
    def test_get_fs_with_storage_options(self, mock_msgdrivefs, mock_get_connection, mock_connection_minimal):
        mock_get_connection.return_value = mock_connection_minimal
        mock_fs_instance = MagicMock()
        mock_msgdrivefs.return_value = mock_fs_instance

        storage_options = {"drive_id": "storage_drive_id", "scope": "custom.scope"}
        result = get_fs("msgraph_minimal", storage_options=storage_options)

        expected_oauth2_params = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "tenant_id": "test_tenant_id",
            "scope": "custom.scope",
        }
        mock_msgdrivefs.assert_called_once_with(
            drive_id="storage_drive_id", oauth2_client_params=expected_oauth2_params
        )
        assert result == mock_fs_instance

    @patch("airflow.providers.microsoft.azure.fs.msgraph.BaseHook.get_connection")
    @patch("msgraphfs.MSGDriveFS")
    def test_get_fs_incomplete_credentials(self, mock_msgdrivefs, mock_get_connection):
        # Connection with missing client_secret
        connection = Connection(
            conn_id="msgraph_incomplete",
            conn_type="msgraph",
            login="test_client_id",
            host="test_tenant_id",
        )
        mock_get_connection.return_value = connection
        mock_fs_instance = MagicMock()
        mock_msgdrivefs.return_value = mock_fs_instance

        result = get_fs("msgraph_incomplete")

        # Should return default filesystem when credentials are incomplete
        mock_msgdrivefs.assert_called_once_with(drive_id=None, oauth2_client_params={})
        assert result == mock_fs_instance
