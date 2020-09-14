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

from unittest import TestCase, mock

from azure.core.exceptions import ResourceNotFoundError

from airflow.providers.microsoft.azure.secrets.azure_key_vault import AzureKeyVaultBackend


class TestAzureKeyVaultBackend(TestCase):
    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.get_conn_uri')
    def test_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = 'scheme://user:pass@host:100'
        conn_list = AzureKeyVaultBackend().get_connections('fake_conn')
        conn = conn_list[0]
        self.assertEqual(conn.host, 'host')

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.DefaultAzureCredential')
    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_conn_uri(self, mock_secret_client, mock_azure_cred):
        mock_cred = mock.Mock()
        mock_sec_client = mock.Mock()
        mock_azure_cred.return_value = mock_cred
        mock_secret_client.return_value = mock_sec_client

        mock_sec_client.get_secret.return_value = mock.Mock(
            value='postgresql://airflow:airflow@host:5432/airflow'
        )

        backend = AzureKeyVaultBackend(vault_url="https://example-akv-resource-name.vault.azure.net/")
        returned_uri = backend.get_conn_uri(conn_id='hi')
        mock_secret_client.assert_called_once_with(
            credential=mock_cred, vault_url='https://example-akv-resource-name.vault.azure.net/'
        )
        self.assertEqual(returned_uri, 'postgresql://airflow:airflow@host:5432/airflow')

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.client')
    def test_get_conn_uri_non_existent_key(self, mock_client):
        """
        Test that if the key with connection ID is not present,
        AzureKeyVaultBackend.get_connections should return None
        """
        conn_id = 'test_mysql'
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend(vault_url="https://example-akv-resource-name.vault.azure.net/")

        self.assertIsNone(backend.get_conn_uri(conn_id=conn_id))
        self.assertEqual([], backend.get_connections(conn_id=conn_id))

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.client')
    def test_get_variable(self, mock_client):
        mock_client.get_secret.return_value = mock.Mock(value='world')
        backend = AzureKeyVaultBackend()
        returned_uri = backend.get_variable('hello')
        mock_client.get_secret.assert_called_with(name='airflow-variables-hello')
        self.assertEqual('world', returned_uri)

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.client')
    def test_get_variable_non_existent_key(self, mock_client):
        """
        Test that if Variable key is not present,
        AzureKeyVaultBackend.get_variables should return None
        """
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend()
        self.assertIsNone(backend.get_variable('test_mysql'))

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.client')
    def test_get_secret_value_not_found(self, mock_client):
        """
        Test that if a non-existent secret returns None
        """
        mock_client.get_secret.side_effect = ResourceNotFoundError
        backend = AzureKeyVaultBackend()
        self.assertIsNone(
            backend._get_secret(path_prefix=backend.connections_prefix, secret_id='test_non_existent')
        )

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend.client')
    def test_get_secret_value(self, mock_client):
        """
        Test that get_secret returns the secret value
        """
        mock_client.get_secret.return_value = mock.Mock(value='super-secret')
        backend = AzureKeyVaultBackend()
        secret_val = backend._get_secret('af-secrets', 'test_mysql_password')
        mock_client.get_secret.assert_called_with(name='af-secrets-test-mysql-password')
        self.assertEqual(secret_val, 'super-secret')
