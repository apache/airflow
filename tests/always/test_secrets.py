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

from airflow.configuration import ensure_secrets_loaded, initialize_secrets_backends
from airflow.models import Connection, Variable
from airflow.secrets.cache import SecretCache

from dev.tests_common.test_utils.config import conf_vars
from dev.tests_common.test_utils.db import clear_db_variables


class TestConnectionsFromSecrets:
    def setup_method(self) -> None:
        SecretCache.reset()

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connection")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    def test_get_connection_second_try(self, mock_env_get, mock_meta_get):
        mock_env_get.side_effect = [None]  # return None
        Connection.get_connection_from_secrets("fake_conn_id")
        mock_meta_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connection")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    def test_get_connection_first_try(self, mock_env_get, mock_meta_get):
        mock_env_get.return_value = Connection("something")  # returns something
        Connection.get_connection_from_secrets("fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_meta_get.assert_not_called()

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend",
            ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
        }
    )
    def test_initialize_secrets_backends(self):
        backends = initialize_secrets_backends()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        assert 3 == len(backends)
        assert "SystemsManagerParameterStoreBackend" in backend_classes

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend",
            ("secrets", "backend_kwargs"): '{"use_ssl": false}',
        }
    )
    def test_backends_kwargs(self):
        backends = initialize_secrets_backends()
        systems_manager = next(
            backend
            for backend in backends
            if backend.__class__.__name__ == "SystemsManagerParameterStoreBackend"
        )
        assert systems_manager.kwargs == {}
        assert systems_manager.use_ssl is False

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend",
            ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
        }
    )
    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_CONN_TEST_MYSQL": "mysql://airflow:airflow@host:5432/airflow",
        },
    )
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_connection"
    )
    def test_backend_fallback_to_env_var(self, mock_get_connection):
        mock_get_connection.return_value = None

        backends = ensure_secrets_loaded()
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "SystemsManagerParameterStoreBackend" in backend_classes

        conn = Connection.get_connection_from_secrets(conn_id="test_mysql")

        # Assert that SystemsManagerParameterStoreBackend.get_conn_uri was called
        mock_get_connection.assert_called_once_with(conn_id="test_mysql")

        assert "mysql://airflow:airflow@host:5432/airflow" == conn.get_uri()


@pytest.mark.db_test
class TestVariableFromSecrets:
    def setup_method(self) -> None:
        clear_db_variables()
        SecretCache.reset()

    def teardown_method(self) -> None:
        clear_db_variables()

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_second_try(self, mock_env_get, mock_meta_get):
        """
        Test if Variable is not present in Environment Variable, it then looks for it in
        Metastore DB
        """
        mock_env_get.return_value = None
        mock_meta_get.return_value = "val"

        Variable.get_variable_from_secrets("fake_var_key")

        mock_meta_get.assert_called_once_with(key="fake_var_key")
        mock_env_get.assert_called_once_with(key="fake_var_key")

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_first_try(self, mock_env_get, mock_meta_get):
        """
        Test if Variable is present in Environment Variable, it does not look for it in
        Metastore DB
        """
        mock_env_get.return_value = "something"
        Variable.get_variable_from_secrets("fake_var_key")
        mock_env_get.assert_called_once_with(key="fake_var_key")
        mock_meta_get.assert_not_called()

    def test_backend_fallback_to_default_var(self):
        """
        Test if a default_var is defined and no backend has the Variable,
        the value returned is default_var
        """
        variable_value = Variable.get(key="test_var", default_var="new")
        assert "new" == variable_value

    @conf_vars(
        {
            (
                "secrets",
                "backend",
            ): "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend",
            ("secrets", "backend_kwargs"): '{"variables_prefix": "/airflow", "profile_name": null}',
        }
    )
    @mock.patch.dict(
        "os.environ",
        {
            "AIRFLOW_VAR_MYVAR": "a_venv_value",
        },
    )
    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_variable"
    )
    def test_backend_variable_order(self, mock_secret_get, mock_meta_get):
        backends = ensure_secrets_loaded()
        backend_classes = [backend.__class__.__name__ for backend in backends]
        assert "SystemsManagerParameterStoreBackend" in backend_classes

        mock_secret_get.return_value = None
        mock_meta_get.return_value = None

        assert "a_venv_value" == Variable.get(key="MYVAR")
        mock_secret_get.assert_called_with(key="MYVAR")
        mock_meta_get.assert_not_called()

        mock_secret_get.return_value = None
        mock_meta_get.return_value = "a_metastore_value"
        assert "a_metastore_value" == Variable.get(key="not_myvar")
        mock_meta_get.assert_called_once_with(key="not_myvar")

        mock_secret_get.return_value = "a_secret_value"
        assert "a_secret_value" == Variable.get(key="not_myvar")
