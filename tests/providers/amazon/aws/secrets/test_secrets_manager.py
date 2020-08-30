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

from moto import mock_secretsmanager

from airflow.providers.amazon.aws.secrets.secrets_manager import SecretsManagerBackend


class TestSecretsManagerBackend(TestCase):
    @mock.patch("airflow.providers.amazon.aws.secrets.secrets_manager." "SecretsManagerBackend.get_conn_uri")
    def test_aws_secrets_manager_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = "scheme://user:pass@host:100"
        conn_list = SecretsManagerBackend().get_connections("fake_conn")
        conn = conn_list[0]
        assert conn.host == 'host'

    @mock_secretsmanager
    def test_get_conn_uri(self):
        param = {
            'SecretId': 'airflow/connections/test_postgres',
            'SecretString': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.put_secret_value(**param)

        returned_uri = secrets_manager_backend.get_conn_uri(conn_id="test_postgres")
        self.assertEqual('postgresql://airflow:airflow@host:5432/airflow', returned_uri)

    @mock_secretsmanager
    def test_get_conn_uri_non_existent_key(self):
        """
        Test that if the key with connection ID is not present,
        SecretsManagerBackend.get_connections should return None
        """
        conn_id = "test_mysql"
        param = {
            'SecretId': 'airflow/connections/test_postgres',
            'SecretString': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.put_secret_value(**param)

        self.assertIsNone(secrets_manager_backend.get_conn_uri(conn_id=conn_id))
        self.assertEqual([], secrets_manager_backend.get_connections(conn_id=conn_id))

    @mock_secretsmanager
    def test_get_variable(self):
        param = {'SecretId': 'airflow/variables/hello', 'SecretString': 'world'}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.put_secret_value(**param)

        returned_uri = secrets_manager_backend.get_variable('hello')
        self.assertEqual('world', returned_uri)

    @mock_secretsmanager
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        param = {'SecretId': 'airflow/variables/hello', 'SecretString': 'world'}

        secrets_manager_backend = SecretsManagerBackend()
        secrets_manager_backend.client.put_secret_value(**param)

        self.assertIsNone(secrets_manager_backend.get_variable("test_mysql"))
