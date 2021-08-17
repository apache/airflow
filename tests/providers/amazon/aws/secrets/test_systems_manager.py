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

from moto import mock_ssm

from airflow.configuration import initialize_secrets_backends
from airflow.providers.amazon.aws.secrets.systems_manager import SystemsManagerParameterStoreBackend
from tests.test_utils.config import conf_vars


class TestSsmSecrets(TestCase):
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_conn_uri"
    )
    def test_aws_ssm_get_connections(self, mock_get_uri):
        mock_get_uri.return_value = "scheme://user:pass@host:100"
        conn_list = SystemsManagerParameterStoreBackend().get_connections("fake_conn")
        conn = conn_list[0]
        assert conn.host == 'host'

    @mock_ssm
    def test_get_conn_uri(self):
        param = {
            'Name': '/airflow/connections/test_postgres',
            'Type': 'String',
            'Value': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_uri = ssm_backend.get_conn_uri(conn_id="test_postgres")
        assert 'postgresql://airflow:airflow@host:5432/airflow' == returned_uri

    @mock_ssm
    def test_get_conn_uri_non_existent_key(self):
        """
        Test that if the key with connection ID is not present in SSM,
        SystemsManagerParameterStoreBackend.get_connections should return None
        """
        conn_id = "test_mysql"
        param = {
            'Name': '/airflow/connections/test_postgres',
            'Type': 'String',
            'Value': 'postgresql://airflow:airflow@host:5432/airflow',
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        assert ssm_backend.get_conn_uri(conn_id=conn_id) is None
        assert [] == ssm_backend.get_connections(conn_id=conn_id)

    @mock_ssm
    def test_get_variable(self):
        param = {'Name': '/airflow/variables/hello', 'Type': 'String', 'Value': 'world'}

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_uri = ssm_backend.get_variable('hello')
        assert 'world' == returned_uri

    @mock_ssm
    def test_get_config(self):
        param = {
            'Name': '/airflow/config/sql_alchemy_conn',
            'Type': 'String',
            'Value': 'sqlite:///Users/test_user/airflow.db',
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_uri = ssm_backend.get_config('sql_alchemy_conn')
        assert 'sqlite:///Users/test_user/airflow.db' == returned_uri

    @mock_ssm
    def test_get_variable_secret_string(self):
        param = {'Name': '/airflow/variables/hello', 'Type': 'SecureString', 'Value': 'world'}
        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)
        returned_uri = ssm_backend.get_variable('hello')
        assert 'world' == returned_uri

    @mock_ssm
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        param = {'Name': '/airflow/variables/hello', 'Type': 'String', 'Value': 'world'}

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        assert ssm_backend.get_variable("test_mysql") is None

    @conf_vars(
        {
            ('secrets', 'backend'): 'airflow.providers.amazon.aws.secrets.systems_manager.'
            'SystemsManagerParameterStoreBackend',
            ('secrets', 'backend_kwargs'): '{"use_ssl": false}',
        }
    )
    @mock.patch("airflow.providers.amazon.aws.secrets.systems_manager.boto3.Session.client")
    def test_passing_client_kwargs(self, mock_ssm_client):
        backends = initialize_secrets_backends()
        systems_manager = [
            backend
            for backend in backends
            if backend.__class__.__name__ == 'SystemsManagerParameterStoreBackend'
        ][0]

        systems_manager.client
        mock_ssm_client.assert_called_once_with('ssm', use_ssl=False)

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend._get_secret"
    )
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_conn_uri should return None,
        SystemsManagerParameterStoreBackend._get_secret should not be called
        """
        kwargs = {'connections_prefix': None}

        ssm_backend = SystemsManagerParameterStoreBackend(**kwargs)

        assert ssm_backend.get_conn_uri("test_mysql") is None
        mock_get_secret.assert_not_called()

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend._get_secret"
    )
    def test_variable_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_variables should return None,
        SystemsManagerParameterStoreBackend._get_secret should not be called
        """
        kwargs = {'variables_prefix': None}

        ssm_backend = SystemsManagerParameterStoreBackend(**kwargs)

        assert ssm_backend.get_variable("hello") is None
        mock_get_secret.assert_not_called()

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend._get_secret"
    )
    def test_config_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_config should return None,
        SystemsManagerParameterStoreBackend._get_secret should not be called
        """
        kwargs = {'config_prefix': None}

        ssm_backend = SystemsManagerParameterStoreBackend(**kwargs)

        assert ssm_backend.get_config("config") is None
        mock_get_secret.assert_not_called()
