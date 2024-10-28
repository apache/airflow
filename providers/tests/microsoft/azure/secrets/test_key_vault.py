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
from azure.core.exceptions import ResourceNotFoundError

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.microsoft.azure.secrets.key_vault import AzureKeyVaultBackend

KEY_VAULT_MODULE = "airflow.providers.microsoft.azure.secrets.key_vault"


class TestAzureKeyVaultBackend:
    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.get_conn_value")
    def test_get_connection(self, mock_get_value):
        mock_get_value.return_value = "scheme://user:pass@host:100"
        conn = AzureKeyVaultBackend().get_connection("fake_conn")
        assert conn.host == "host"

    @mock.patch(f"{KEY_VAULT_MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{KEY_VAULT_MODULE}.SecretClient")
    def test_get_conn_uri(self, mock_secret_client, mock_azure_cred):
        mock_cred = mock.Mock()
        mock_sec_client = mock.Mock()
        mock_azure_cred.return_value = mock_cred
        mock_secret_client.return_value = mock_sec_client

        mock_sec_client.get_secret.return_value = mock.Mock(
            value="postgresql://airflow:airflow@host:5432/airflow"
        )

        backend = AzureKeyVaultBackend(
            vault_url="https://example-akv-resource-name.vault.azure.net/"
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Method `.*get_conn_uri` is deprecated",
        ):
            returned_uri = backend.get_conn_uri(conn_id="hi")
        mock_secret_client.assert_called_once_with(
            credential=mock_cred,
            vault_url="https://example-akv-resource-name.vault.azure.net/",
        )
        assert returned_uri == "postgresql://airflow:airflow@host:5432/airflow"

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.client")
    def test_get_conn_uri_non_existent_key(self, mock_client):
        """
        Test that if the key with connection ID is not present,
        AzureKeyVaultBackend.get_connection should return None
        """
        conn_id = "test_mysql"
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend(
            vault_url="https://example-akv-resource-name.vault.azure.net/"
        )

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Method `.*get_conn_uri` is deprecated",
        ):
            assert backend.get_conn_uri(conn_id=conn_id) is None
        assert backend.get_connection(conn_id=conn_id) is None

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.client")
    def test_get_variable(self, mock_client):
        mock_client.get_secret.return_value = mock.Mock(value="world")
        backend = AzureKeyVaultBackend()
        returned_uri = backend.get_variable("hello")
        mock_client.get_secret.assert_called_with(name="airflow-variables-hello")
        assert "world" == returned_uri

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.client")
    def test_get_variable_non_existent_key(self, mock_client):
        """
        Test that if Variable key is not present,
        AzureKeyVaultBackend.get_variables should return None
        """
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend()
        assert backend.get_variable("test_mysql") is None

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.client")
    def test_get_secret_value_not_found(self, mock_client):
        """
        Test that if a non-existent secret returns None
        """
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend()
        assert (
            backend._get_secret(
                path_prefix=backend.connections_prefix, secret_id="test_non_existent"
            )
            is None
        )

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend.client")
    def test_get_secret_value(self, mock_client):
        """
        Test that get_secret returns the secret value
        """
        mock_client.get_secret.return_value = mock.Mock(value="super-secret")
        backend = AzureKeyVaultBackend()
        secret_val = backend._get_secret("af-secrets", "test_mysql_password")
        mock_client.get_secret.assert_called_with(name="af-secrets-test-mysql-password")
        assert secret_val == "super-secret"

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend._get_secret")
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Connections prefix is None,
        AzureKeyVaultBackend.get_connection should return None
        AzureKeyVaultBackend._get_secret should not be called
        """
        kwargs = {"connections_prefix": None}

        backend = AzureKeyVaultBackend(**kwargs)
        assert backend.get_connection("test_mysql") is None
        mock_get_secret.assert_not_called()

        mock_get_secret.reset_mock()
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Method `.*get_conn_uri` is deprecated",
        ):
            assert backend.get_conn_uri("test_mysql") is None
            mock_get_secret.assert_not_called()

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend._get_secret")
    def test_variable_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variables prefix is None,
        AzureKeyVaultBackend.get_variables should return None
        AzureKeyVaultBackend._get_secret should not be called
        """
        kwargs = {"variables_prefix": None}

        backend = AzureKeyVaultBackend(**kwargs)
        assert backend.get_variable("hello") is None
        mock_get_secret.assert_not_called()

    @mock.patch(f"{KEY_VAULT_MODULE}.AzureKeyVaultBackend._get_secret")
    def test_config_prefix_none_value(self, mock_get_secret):
        """
        Test that if Config prefix is None,
        AzureKeyVaultBackend.get_config should return None
        AzureKeyVaultBackend._get_secret should not be called
        """
        kwargs = {"config_prefix": None}

        backend = AzureKeyVaultBackend(**kwargs)
        assert backend.get_config("test_mysql") is None
        mock_get_secret.assert_not_called()

    @mock.patch(f"{KEY_VAULT_MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{KEY_VAULT_MODULE}.ClientSecretCredential")
    @mock.patch(f"{KEY_VAULT_MODULE}.SecretClient")
    def test_client_authenticate_with_default_azure_credential(
        self, mock_client, mock_client_secret_credential, mock_defaul_azure_credential
    ):
        """
        Test that if AzureKeyValueBackend is authenticated with DefaultAzureCredential
        tenant_id, client_id and client_secret are not provided

        """
        backend = AzureKeyVaultBackend(
            vault_url="https://example-akv-resource-name.vault.azure.net/"
        )
        backend.client
        assert not mock_client_secret_credential.called
        mock_defaul_azure_credential.assert_called_once()

    @mock.patch(f"{KEY_VAULT_MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{KEY_VAULT_MODULE}.ClientSecretCredential")
    @mock.patch(f"{KEY_VAULT_MODULE}.SecretClient")
    def test_client_authenticate_with_default_azure_credential_and_customized_configuration(
        self, mock_client, mock_client_secret_credential, mock_defaul_azure_credential
    ):
        backend = AzureKeyVaultBackend(
            vault_url="https://example-akv-resource-name.vault.azure.net/",
            managed_identity_client_id="managed_identity_client_id",
            workload_identity_tenant_id="workload_identity_tenant_id",
        )
        backend.client
        assert not mock_client_secret_credential.called
        mock_defaul_azure_credential.assert_called_once_with(
            managed_identity_client_id="managed_identity_client_id",
            workload_identity_tenant_id="workload_identity_tenant_id",
        )

    @mock.patch(f"{KEY_VAULT_MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{KEY_VAULT_MODULE}.ClientSecretCredential")
    @mock.patch(f"{KEY_VAULT_MODULE}.SecretClient")
    def test_client_authenticate_with_client_secret_credential(
        self, mock_client, mock_client_secret_credential, mock_defaul_azure_credential
    ):
        backend = AzureKeyVaultBackend(
            vault_url="https://example-akv-resource-name.vault.azure.net/",
            tenant_id="tenant_id",
            client_id="client_id",
            client_secret="client_secret",
        )
        backend.client
        assert not mock_defaul_azure_credential.called
        mock_client_secret_credential.assert_called_once()
