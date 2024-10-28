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

import json
from unittest import mock

import pytest
from moto import mock_aws

from airflow.configuration import initialize_secrets_backends
from airflow.providers.amazon.aws.secrets.systems_manager import (
    SystemsManagerParameterStoreBackend,
)

from tests_common.test_utils.config import conf_vars

URI_CONNECTION = pytest.param(
    "postgres://my-login:my-pass@my-host:5432/my-schema?param1=val1&param2=val2",
    id="uri-connection",
)
JSON_CONNECTION = pytest.param(
    json.dumps(
        {
            "conn_type": "postgres",
            "login": "my-login",
            "password": "my-pass",
            "host": "my-host",
            "port": 5432,
            "schema": "my-schema",
            "extra": {"param1": "val1", "param2": "val2"},
        }
    ),
    id="json-connection",
)


class TestSsmSecrets:
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_conn_value"
    )
    def test_aws_ssm_get_connection(self, mock_get_value):
        mock_get_value.return_value = "scheme://user:pass@host:100"
        conn = SystemsManagerParameterStoreBackend().get_connection("fake_conn")
        assert conn.host == "host"

    @mock_aws
    @pytest.mark.parametrize("ssm_value", [JSON_CONNECTION, URI_CONNECTION])
    def test_get_conn_value(self, ssm_value):
        param = {
            "Name": "/airflow/connections/test_postgres",
            "Type": "String",
            "Value": ssm_value,
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_conn_value = ssm_backend.get_conn_value(conn_id="test_postgres")
        assert ssm_value == returned_conn_value

        test_conn = ssm_backend.get_connection(conn_id="test_postgres")
        assert test_conn.conn_id == "test_postgres"
        assert test_conn.conn_type == "postgres"
        assert test_conn.login == "my-login"
        assert test_conn.password == "my-pass"
        assert test_conn.host == "my-host"
        assert test_conn.port == 5432
        assert test_conn.schema == "my-schema"
        assert test_conn.extra_dejson == {"param1": "val1", "param2": "val2"}

    @mock_aws
    def test_get_conn_value_non_existent_key(self):
        """
        Test that if the key with connection ID is not present in SSM,
        SystemsManagerParameterStoreBackend.get_connection should return None
        """
        conn_id = "test_mysql"
        param = {
            "Name": "/airflow/connections/test_postgres",
            "Type": "String",
            "Value": "postgresql://airflow:airflow@host:5432/airflow",
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        assert ssm_backend.get_conn_value(conn_id=conn_id) is None
        assert ssm_backend.get_connection(conn_id=conn_id) is None

    @mock_aws
    def test_get_variable(self):
        param = {"Name": "/airflow/variables/hello", "Type": "String", "Value": "world"}

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_uri = ssm_backend.get_variable("hello")
        assert "world" == returned_uri

    @mock_aws
    def test_get_config(self):
        param = {
            "Name": "/airflow/config/sql_alchemy_conn",
            "Type": "String",
            "Value": "sqlite:///Users/test_user/airflow.db",
        }

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        returned_uri = ssm_backend.get_config("sql_alchemy_conn")
        assert "sqlite:///Users/test_user/airflow.db" == returned_uri

    @mock_aws
    def test_get_variable_secret_string(self):
        param = {
            "Name": "/airflow/variables/hello",
            "Type": "SecureString",
            "Value": "world",
        }
        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)
        returned_uri = ssm_backend.get_variable("hello")
        assert "world" == returned_uri

    @mock_aws
    def test_get_variable_non_existent_key(self):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_variables should return None
        """
        param = {"Name": "/airflow/variables/hello", "Type": "String", "Value": "world"}

        ssm_backend = SystemsManagerParameterStoreBackend()
        ssm_backend.client.put_parameter(**param)

        assert ssm_backend.get_variable("test_mysql") is None

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.providers.amazon.aws.secrets.systems_manager."
            "SystemsManagerParameterStoreBackend",
            (
                "secrets",
                "backend_kwargs",
            ): '{"use_ssl": false, "role_arn": "arn:aws:iam::222222222222:role/awesome-role"}',
        }
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.SessionFactory")
    def test_passing_client_kwargs(self, mock_session_factory):
        backends = initialize_secrets_backends()
        systems_manager = next(
            backend
            for backend in backends
            if backend.__class__.__name__ == "SystemsManagerParameterStoreBackend"
        )

        # Mock SessionFactory, session and client
        mock_session_factory_instance = mock_session_factory.return_value
        mock_ssm_client = mock.MagicMock(return_value="mock-ssm-client")
        mock_session = mock.MagicMock()
        mock_session.client = mock_ssm_client
        mock_create_session = mock.MagicMock(return_value=mock_session)
        mock_session_factory_instance.create_session = mock_create_session

        systems_manager.client
        assert mock_session_factory.call_count == 1
        mock_session_factory_call_kwargs = mock_session_factory.call_args.kwargs
        assert "conn" in mock_session_factory_call_kwargs
        conn_wrapper = mock_session_factory_call_kwargs["conn"]

        assert conn_wrapper.conn_id == "SystemsManagerParameterStoreBackend__connection"
        assert conn_wrapper.role_arn == "arn:aws:iam::222222222222:role/awesome-role"

        mock_ssm_client.assert_called_once_with(service_name="ssm", use_ssl=False)

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend._get_secret"
    )
    def test_connection_prefix_none_value(self, mock_get_secret):
        """
        Test that if Variable key is not present in SSM,
        SystemsManagerParameterStoreBackend.get_conn_value should return None,
        SystemsManagerParameterStoreBackend._get_secret should not be called
        """
        kwargs = {"connections_prefix": None}

        ssm_backend = SystemsManagerParameterStoreBackend(**kwargs)

        assert ssm_backend.get_conn_value("test_mysql") is None
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
        kwargs = {"variables_prefix": None}

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
        kwargs = {"config_prefix": None}

        ssm_backend = SystemsManagerParameterStoreBackend(**kwargs)

        assert ssm_backend.get_config("config") is None
        mock_get_secret.assert_not_called()

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "connection_id, connections_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_connection_lookup_pattern(
        self, mock_client, connection_id, connections_lookup_pattern, num_client_calls
    ):
        """
        Test that if Connection ID is looked up in AWS Parameter Store
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"connections_lookup_pattern": connections_lookup_pattern}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_conn_value(connection_id)
        assert mock_client().get_parameter.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "variable_key, variables_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_variable_lookup_pattern(
        self, mock_client, variable_key, variables_lookup_pattern, num_client_calls
    ):
        """
        Test that if Variable key is looked up in AWS Parameter Store
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"variables_lookup_pattern": variables_lookup_pattern}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_variable(variable_key)
        assert mock_client().get_parameter.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        "config_key, config_lookup_pattern, num_client_calls",
        [
            ("test", "test", 1),
            ("test", ".*", 1),
            ("test", "T.*", 1),
            ("test", "dummy-pattern", 0),
            ("test", None, 1),
        ],
    )
    def test_config_lookup_pattern(
        self, mock_client, config_key, config_lookup_pattern, num_client_calls
    ):
        """
        Test that if Variable key is looked up in AWS Parameter Store
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"config_lookup_pattern": config_lookup_pattern}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_config(config_key)
        assert mock_client().get_parameter.call_count == num_client_calls

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    def test_connection_prefix_with_no_leading_slash(self, mock_client):
        """
        Test that if Connection ID is looked up in AWS Parameter Store with the added leading "/"
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"connections_prefix": "airflow/connections"}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_conn_value("test_mysql")
        mock_client().get_parameter.assert_called_with(
            Name="/airflow/connections/test_mysql", WithDecryption=True
        )

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    def test_variable_prefix_with_no_leading_slash(self, mock_client):
        """
        Test that if Variable key is looked up in AWS Parameter Store with the added leading "/"
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"variables_prefix": "airflow/variables"}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_variable("hello")
        mock_client().get_parameter.assert_called_with(
            Name="/airflow/variables/hello", WithDecryption=True
        )

    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend.client",
        new_callable=mock.PropertyMock,
    )
    def test_config_prefix_with_no_leading_slash(self, mock_client):
        """
        Test that if Config key is looked up in AWS Parameter Store with the added leading "/"
        """
        mock_client().get_parameter.return_value = {
            "Parameter": {
                "Value": None,
            },
        }
        kwargs = {"config_prefix": "airflow/config"}

        secrets_manager_backend = SystemsManagerParameterStoreBackend(**kwargs)
        secrets_manager_backend.get_config("config")
        mock_client().get_parameter.assert_called_with(
            Name="/airflow/config/config", WithDecryption=True
        )
