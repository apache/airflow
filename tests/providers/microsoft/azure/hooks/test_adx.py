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

from unittest import mock

import pytest
from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.adx import AzureDataExplorerHook

ADX_TEST_CONN_ID = "adx_test_connection_id"


class TestAzureDataExplorerHook:
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="missing_method",
                conn_type="azure_data_explorer",
                login="client_id",
                password="client secret",
                host="https://help.kusto.windows.net",
                extra={},
            )
        ],
        indirect=True,
    )
    def test_conn_missing_method(self, mocked_connection):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)
        error_pattern = "is missing: `auth_method`"
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.get_conn()
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.connection

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="unknown_method",
                conn_type="azure_data_explorer",
                login="client_id",
                password="client secret",
                host="https://help.kusto.windows.net",
                extra={"auth_method": "AAD_OTHER"},
            ),
        ],
        indirect=True,
    )
    def test_conn_unknown_method(self, mocked_connection):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)
        error_pattern = "Unknown authentication method: AAD_OTHER"
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.get_conn()
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.connection

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="missing_cluster",
                conn_type="azure_data_explorer",
                login="client_id",
                password="client secret",
                extra={},
            ),
        ],
        indirect=True,
    )
    def test_conn_missing_cluster(self, mocked_connection):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)
        error_pattern = "Host connection option is required"
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.get_conn()
        with pytest.raises(AirflowException, match=error_pattern):
            assert hook.connection

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="method_aad_creds",
                conn_type="azure_data_explorer",
                login="client_id",
                password="client secret",
                host="https://help.kusto.windows.net",
                extra={"tenant": "tenant", "auth_method": "AAD_CREDS"},
            )
        ],
        indirect=True,
    )
    @mock.patch.object(KustoClient, "__init__")
    def test_conn_method_aad_creds(self, mock_init, mocked_connection):
        mock_init.return_value = None
        AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id).get_conn()
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_user_password_authentication(
                "https://help.kusto.windows.net", "client_id", "client secret", "tenant"
            )
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="method_token_creds",
                conn_type="azure_data_explorer",
                host="https://help.kusto.windows.net",
                extra={
                    "auth_method": "AZURE_TOKEN_CRED",
                },
            ),
        ],
        indirect=True,
    )
    @mock.patch("azure.identity._credentials.environment.ClientSecretCredential")
    def test_conn_method_token_creds(self, mock1, mocked_connection, monkeypatch):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)

        monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
        monkeypatch.setenv("AZURE_CLIENT_ID", "client")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")

        assert hook.connection._kcsb.data_source == "https://help.kusto.windows.net"
        mock1.assert_called_once_with(
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",
            authority="https://login.microsoftonline.com",
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="method_aad_app",
                conn_type="azure_data_explorer",
                login="app_id",
                password="app key",
                host="https://help.kusto.windows.net",
                extra={
                    "tenant": "tenant",
                    "auth_method": "AAD_APP",
                },
            )
        ],
        indirect=True,
    )
    @mock.patch.object(KustoClient, "__init__")
    def test_conn_method_aad_app(self, mock_init, mocked_connection):
        mock_init.return_value = None
        AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id).get_conn()
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_application_key_authentication(
                "https://help.kusto.windows.net", "app_id", "app key", "tenant"
            )
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="method_aad_app",
                conn_type="azure_data_explorer",
                login="app_id",
                password="app key",
                host="https://help.kusto.windows.net",
                extra={
                    "tenant": "tenant",
                    "auth_method": "AAD_APP",
                },
            )
        ],
        indirect=True,
    )
    @mock.patch.object(KustoClient, "__init__")
    def test_conn_method_aad_app_cert(self, mock_init, mocked_connection):
        mock_init.return_value = None
        AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id).get_conn()
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                "https://help.kusto.windows.net", "client_id", "PEM", "thumbprint", "tenant"
            )
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=ADX_TEST_CONN_ID,
                conn_type="azure_data_explorer",
                host="https://help.kusto.windows.net",
                extra={"auth_method": "AAD_DEVICE"},
            )
        ],
        indirect=True,
    )
    @mock.patch.object(KustoClient, "__init__")
    def test_conn_method_aad_device(self, mock_init, mocked_connection):
        mock_init.return_value = None
        AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id).get_conn()
        assert mock_init.called_with(
            KustoConnectionStringBuilder.with_aad_device_authentication("https://help.kusto.windows.net")
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id=ADX_TEST_CONN_ID,
                conn_type="azure_data_explorer",
                host="https://help.kusto.windows.net",
                extra={"auth_method": "AAD_DEVICE"},
            )
        ],
        indirect=True,
    )
    @mock.patch.object(KustoClient, "execute")
    def test_run_query(self, mock_execute, mocked_connection):
        mock_execute.return_value = None
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=ADX_TEST_CONN_ID)
        hook.run_query("Database", "Logs | schema", options={"option1": "option_value"})
        properties = ClientRequestProperties()
        properties.set_option("option1", "option_value")
        assert mock_execute.called_with("Database", "Logs | schema", properties=properties)

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            pytest.param(
                "a://usr:pw@host?extra__azure_data_explorer__tenant=my-tenant"
                "&extra__azure_data_explorer__auth_method=AAD_APP",
                id="prefix",
            ),
            pytest.param("a://usr:pw@host?tenant=my-tenant&auth_method=AAD_APP", id="no-prefix"),
        ],
        indirect=True,
    )
    def test_backcompat_prefix_works(self, mocked_connection):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)
        assert hook.connection._kcsb.data_source == "host"
        assert hook.connection._kcsb.application_client_id == "usr"
        assert hook.connection._kcsb.application_key == "pw"
        assert hook.connection._kcsb.authority_id == "my-tenant"

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            (
                "a://usr:pw@host?tenant=my-tenant&auth_method=AAD_APP"
                "&extra__azure_data_explorer__auth_method=AAD_APP"
            )
        ],
        indirect=True,
    )
    def test_backcompat_prefix_both_causes_warning(self, mocked_connection):
        hook = AzureDataExplorerHook(azure_data_explorer_conn_id=mocked_connection.conn_id)
        assert hook.connection._kcsb.data_source == "host"
        assert hook.connection._kcsb.application_client_id == "usr"
        assert hook.connection._kcsb.application_key == "pw"
        assert hook.connection._kcsb.authority_id == "my-tenant"
