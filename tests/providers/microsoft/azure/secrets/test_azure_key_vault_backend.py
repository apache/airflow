# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest import TestCase, mock

from airflow.providers.microsoft.azure.secrets.azure_key_vault import AzureKeyVaultBackend

from azure.core.exceptions import ResourceNotFoundError


class TestAzureKeyVaultBackend(TestCase):
    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault'
                'AzureKeyVaultBackend.get_conn_uri')
    def test_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = 'scheme://user:pass@host:100'
        conn_list = AzureKeyVaultBackend().get_connections('fake_conn')
        conn = conn_list[0]
        self.assertEqual(conn.host, 'host')

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_conn_uri(self, client_mock):
        secret_id = 'airflow-connections-test-postgres'
        client_mock.get_secret(secret_id).return_value = 'postgresql://airflow:airflow@host:5432/airflow'
        backend = AzureKeyVaultBackend()
        returned_uri = backend.get_conn_uri(conn_id='test_postgres')
        self.assertEqual('postgresql://airflow:airflow@host:5432/airflow', returned_uri)

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_conn_uri_non_existent_key(self, client_mock):
        """
        Test that if the key with connection ID is not present,
        AzureKeyVaultBackend.get_connections should return None
        """
        conn_id = 'test_mysql'
        client_mock.get_secret('airflow-connections-test-postgres').return_value = \
            'postgresql://airflow:airflow@host:5432/airflow'
        backend = AzureKeyVaultBackend()

        self.assertIsNone(backend.get_conn_uri(conn_id=conn_id))
        self.assertEqual([], backend.get_connections(conn_id=conn_id))

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_variable(self, client_mock):
        client_mock.get_secret('airflow-variables-hello').return_value = 'world'
        backend = AzureKeyVaultBackend()

        returned_uri = backend.get_variable('hello')
        self.assertEqual('world', returned_uri)

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_variable_non_existent_key(self, client_mock):
        """
        Test that if Variable key is not present,
        AzureKeyVaultBackend.get_variables should return None
        """
        client_mock.get_secret('airflow-variables-hello').return_value = 'world'
        backend = AzureKeyVaultBackend()
        self.assertIsNone(backend.get_variable('test_mysql'))

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_secret_value_not_found(self, client_mock):
        """
        Test that if a non-existent secret returns None
        """
        client_mock.get_secret_value('test_non_existent').side_effect = ResourceNotFoundError()
        backend = AzureKeyVaultBackend()
        self.assertIsNone(backend.get_secret_value('test_non_existent'))

    @mock.patch('airflow.providers.microsoft.azure.secrets.azure_key_vault.SecretClient')
    def test_get_secret_value(self, client_mock):
        """
        Test that get_secret returns the secret value
        """
        client_mock.get_secret_value('test_mysql_password').return_value = mock.Mock(value='super-secret')
        backend = AzureKeyVaultBackend()
        self.assertEqual(backend.get_secret_value('test_mysql_password'), 'super-secret')
