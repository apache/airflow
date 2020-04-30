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

from airflow.providers.hashicorp.secrets.vault import VaultBackend


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

        test_client = VaultBackend(**kwargs)
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

        test_client = VaultBackend(**kwargs)
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

        test_client = VaultBackend(**kwargs)
        self.assertIsNone(test_client.get_conn_uri(conn_id="test_mysql"))
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='airflow', path='connections/test_mysql')
        self.assertEqual([], test_client.get_connections(conn_id="test_mysql"))

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_variable_value(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'request_id': '2d48a2ad-6bcb-e5b6-429d-da35fdf31f56',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 0,
            'data': {'data': {'value': 'world'},
                     'metadata': {'created_time': '2020-03-28T02:10:54.301784Z',
                                  'deletion_time': '',
                                  'destroyed': False,
                                  'version': 1}},
            'wrap_info': None,
            'warnings': None,
            'auth': None
        }

        kwargs = {
            "variables_path": "variables",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        }

        test_client = VaultBackend(**kwargs)
        returned_uri = test_client.get_variable("hello")
        self.assertEqual('world', returned_uri)

    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_variable_value_engine_version_1(self, mock_hvac):
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        mock_client.secrets.kv.v1.read_secret.return_value = {
            'request_id': '182d0673-618c-9889-4cba-4e1f4cfe4b4b',
            'lease_id': '',
            'renewable': False,
            'lease_duration': 2764800,
            'data': {'value': 'world'},
            'wrap_info': None,
            'warnings': None,
            'auth': None}

        kwargs = {
            "variables_path": "variables",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS",
            "kv_engine_version": 1
        }

        test_client = VaultBackend(**kwargs)
        returned_uri = test_client.get_variable("hello")
        mock_client.secrets.kv.v1.read_secret.assert_called_once_with(
            mount_point='airflow', path='variables/hello')
        self.assertEqual('world', returned_uri)

    @mock.patch.dict('os.environ', {
        'AIRFLOW_VAR_HELLO': 'world',
    })
    @mock.patch("airflow.providers.hashicorp.secrets.vault.hvac")
    def test_get_variable_value_non_existent_key(self, mock_hvac):
        """
        Test that if the key with connection ID is not present in Vault, VaultClient.get_connections
        should return None
        """
        mock_client = mock.MagicMock()
        mock_hvac.Client.return_value = mock_client
        # Response does not contain the requested key
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath()

        kwargs = {
            "variables_path": "variables",
            "mount_point": "airflow",
            "auth_type": "token",
            "url": "http://127.0.0.1:8200",
            "token": "s.7AU0I51yv1Q1lxOIg1F3ZRAS"
        }

        test_client = VaultBackend(**kwargs)
        self.assertIsNone(test_client.get_variable("hello"))
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point='airflow', path='variables/hello')
        self.assertIsNone(test_client.get_variable("hello"))

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
            VaultBackend(**kwargs).get_connections(conn_id='test')

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
            VaultBackend(**kwargs).get_connections(conn_id='test')

    def test_auth_type_kubernetes_without_role_raises_error(self):
        kwargs = {
            "auth_type": "kubernetes",
            "url": "http://127.0.0.1:8200",
        }

        with self.assertRaisesRegex(VaultError, "kubernetes_role cannot be None for auth_type='kubernetes'"):
            VaultBackend(**kwargs).get_connections(conn_id='test')

    def test_auth_type_kubernetes_with_unreadable_jwt_raises_error(self):
        path = "/var/tmp/this_does_not_exist/334e918ef11987d3ef2f9553458ea09f"
        kwargs = {
            "auth_type": "kubernetes",
            "kubernetes_role": "default",
            "kubernetes_jwt_path": path,
            "url": "http://127.0.0.1:8200",
        }

        with self.assertRaisesRegex(FileNotFoundError, path):
            VaultBackend(**kwargs).get_connections(conn_id='test')
