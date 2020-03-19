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

from hvac.exceptions import InvalidPath, VaultError

from airflow.providers.hashicorp.secrets.vault import VaultSecrets


class TestVaultSecrets(TestCase):

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_conn_uri(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '94011e25-f8dc-ec29-221b-1f9c1d9ad2ae',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {
                'data': {'conn_uri': 'postgresql://airflow:airflow@host:5432/airflow'},
                'metadata': {'created_time': '2020-03-16T21:01:43.331126Z',
                             'deletion_time': '',
                             'destroyed': False,
                             'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        kwargs = {
            "connections_path": "connections",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        }

        test_client = VaultSecrets(**kwargs)
        returned_uri = test_client.get_conn_uri(conn_id="test_postgres")
        self.assertEqual('postgresql://airflow:airflow@host:5432/airflow', returned_uri)

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_conn_uri_engine_version_1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v1.read_secret.return_value = {
            'request_id': '182d0673-618c-9889-4cba-4e1f4cfe4b4b',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 2764800,
            'data': {'conn_uri': 'postgresql://airflow:airflow@host:5432/airflow'},
            'wrap_info': None,
            'warnings': None,
            'auth': None}

        kwargs = {
            "connections_path": "connections",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 1
        }

        test_client = VaultSecrets(**kwargs)
        returned_uri = test_client.get_conn_uri(conn_id="test_postgres")
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point='airflow', path='connections/test_postgres')
        self.assertEqual('postgresql://airflow:airflow@host:5432/airflow', returned_uri)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_MYSQL': 'mysql://airflow:airflow@host:5432/airflow',
    })
    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_conn_uri_non_existent_key(self, mock_hvac):
        """
        Test that if the key with connection ID is not present in Vault, VaultClient.get_connections
        should return None
        """
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()

        kwargs = {
            "connections_path": "connections",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        }

        test_client = VaultSecrets(**kwargs)
        self.assertIsNone(test_client.get_conn_uri(conn_id="test_mysql"))
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='airflow', path='connections/test_mysql')
        self.assertEqual([], test_client.get_connections(conn_id="test_mysql"))

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_auth_failure_raises_error(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.is_authenticated.return_value = False

        kwargs = {
            "connections_path": "connections",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "test_wrong_token"
        }

        with self.assertRaisesRegex(VaultError, "Vault Authentication Error!"):
            VaultSecrets(**kwargs).get_connections(conn_id='test')

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_empty_token_raises_error(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client

        kwargs = {
            "connections_path": "connections",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
        }

        with self.assertRaisesRegex(VaultError, "token cannot be None for auth_type='token'"):
            VaultSecrets(**kwargs).get_connections(conn_id='test')
