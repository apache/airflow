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
#
import json
import unittest
from unittest import mock

import pytest

from airflow.configuration import ensure_secrets_loaded, initialize_secrets_backends
from airflow.exceptions import AirflowConfigException, AirflowNotFoundException
from airflow.models import Connection, Variable
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_variables

SSM_SB = "airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend"
LOCAL_SB = "airflow.secrets.local_filesystem.LocalFilesystemBackend"
ENV_SB = "airflow.secrets.environment_variables.EnvironmentVariablesBackend"
METASTORE_DB_SB = "airflow.secrets.metastore.MetastoreBackend"


class TestConnectionsFromSecrets(unittest.TestCase):
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
        mock_env_get.side_effect = ["something"]  # returns something
        Connection.get_connection_from_secrets("fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_meta_get.not_called()

    @conf_vars(
        {
            ("secrets", "backend"): SSM_SB,
            ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
        }
    )
    def test_initialize_secrets_backends(self):
        backends = initialize_secrets_backends()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        assert 3 == len(backends)
        assert 'SystemsManagerParameterStoreBackend' in backend_classes

    @conf_vars(
        {
            ("secrets", "backend"): SSM_SB,
            ("secrets", "backend_kwargs"): '{"use_ssl": false}',
        }
    )
    def test_backends_kwargs(self):
        backends = initialize_secrets_backends()
        systems_manager = [
            backend
            for backend in backends
            if backend.__class__.__name__ == 'SystemsManagerParameterStoreBackend'
        ][0]

        assert systems_manager.kwargs == {'use_ssl': False}

    @conf_vars(
        {
            ("secrets", "backend"): SSM_SB,
            ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
        }
    )
    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_CONN_TEST_MYSQL': 'mysql://airflow:airflow@host:5432/airflow',
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
        assert 'SystemsManagerParameterStoreBackend' in backend_classes

        conn = Connection.get_connection_from_secrets(conn_id="test_mysql")

        # Assert that SystemsManagerParameterStoreBackend.get_conn_uri was called
        mock_get_connection.assert_called_once_with(conn_id='test_mysql')

        assert 'mysql://airflow:airflow@host:5432/airflow' == conn.get_uri()


class TestVariableFromSecrets(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_variables()

    def tearDown(self) -> None:
        clear_db_variables()

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_second_try(self, mock_env_get, mock_meta_get):
        """
        Test if Variable is not present in Environment Variable, it then looks for it in
        Metastore DB
        """
        mock_env_get.return_value = None
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
            ("secrets", "backend"): SSM_SB,
            ("secrets", "backend_kwargs"): '{"variables_prefix": "/airflow", "profile_name": null}',
        }
    )
    @mock.patch.dict(
        'os.environ',
        {
            'AIRFLOW_VAR_MYVAR': 'a_venv_value',
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
        assert 'SystemsManagerParameterStoreBackend' in backend_classes

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


class TestSecretsBackendsConfig(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_variables()

    def tearDown(self) -> None:
        clear_db_variables()

    def test_default_secrets_backend(self):
        """
        Test expected default order of backends which compatible with previous versions of Apache Airflow
        """
        backends = ensure_secrets_loaded()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        assert backend_classes == ["EnvironmentVariablesBackend", "MetastoreBackend"]

    @conf_vars({("secrets", "backends_config"): "[]"})
    def test_empty_secrets_backend_config(self):
        """Test fallback if [secrets] 'backend_config' is empty list"""
        backends = ensure_secrets_loaded()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        assert backend_classes == ["EnvironmentVariablesBackend", "MetastoreBackend"]

    def test_custom_backend_order(self):
        secrets_backend_config = [
            {"backend": LOCAL_SB},
            {"backend": SSM_SB},
            {"backend": ENV_SB},
            {"backend": METASTORE_DB_SB},
        ]

        with conf_vars({("secrets", "backends_config"): json.dumps(secrets_backend_config)}):
            backends = ensure_secrets_loaded()
            backend_classes = [backend.__class__.__name__ for backend in backends]

            assert backend_classes == [
                "LocalFilesystemBackend",
                "SystemsManagerParameterStoreBackend",
                "EnvironmentVariablesBackend",
                "MetastoreBackend",
            ]

    def test_multiple_same_backends(self):
        secrets_backend_config = [
            {"backend": SSM_SB, "backend_kwargs": {"use_ssl": False}},
            {"backend": ENV_SB},
            {"backend": METASTORE_DB_SB},
            {"backend": SSM_SB, "backend_kwargs": {"use_ssl": True}},
        ]

        with conf_vars({("secrets", "backends_config"): json.dumps(secrets_backend_config)}):
            backends = ensure_secrets_loaded()
            backend_classes = [backend.__class__.__name__ for backend in backends]

            assert backend_classes == [
                "SystemsManagerParameterStoreBackend",
                "EnvironmentVariablesBackend",
                "MetastoreBackend",
                "SystemsManagerParameterStoreBackend",
            ]

            assert backends[0] is not backends[-1]
            assert backends[0].kwargs == {'use_ssl': False}
            assert backends[-1].kwargs == {'use_ssl': True}

    @conf_vars({("secrets", "backends_config"): '"INVALID"'})
    def test_invalid_backend_config_type(self):
        error_match = r"\[secrets\] 'backends_config' expected list of backends.*"
        with pytest.raises(AirflowConfigException, match=error_match):
            ensure_secrets_loaded()

    def test_invalid_backend_config(self):
        secrets_backend_config = [
            {"backend": LOCAL_SB, "backend_kwargs": {"use_ssl": False}},
            {"backend": ENV_SB},
            {"backend": METASTORE_DB_SB},
        ]

        with conf_vars({("secrets", "backends_config"): json.dumps(secrets_backend_config)}):
            error_match = r"Cannot read config: {'backend': 'airflow.secrets.local_filesystem.*"
            with pytest.raises(AirflowConfigException, match=error_match):
                ensure_secrets_loaded()

    @mock.patch("airflow.secrets.local_filesystem.LocalFilesystemBackend.get_variable")
    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_variable"
    )
    def test_backend_config_variable_order(self, mock_ssm_get, mock_env_get, mock_meta_get, mock_local_get):
        secrets_backend_config = [
            {"backend": LOCAL_SB},
            {"backend": METASTORE_DB_SB},
            {"backend": SSM_SB},
            {"backend": ENV_SB},
        ]

        with conf_vars({("secrets", "backends_config"): json.dumps(secrets_backend_config)}):
            ensure_secrets_loaded()

            # Test Variable exists in first backend.
            mock_local_get.return_value = "local"
            assert "local" == Variable.get(key="test_var_1")
            mock_local_get.assert_called_once_with(key="test_var_1")
            mock_meta_get.assert_not_called()
            mock_ssm_get.assert_not_called()
            mock_env_get.assert_not_called()

            # Test Variable exists in second backend.
            mock_local_get.return_value = None
            mock_meta_get.return_value = "meta"
            assert "meta" == Variable.get(key="test_var_2")
            mock_meta_get.assert_called_once_with(key="test_var_2")
            mock_ssm_get.assert_not_called()
            mock_env_get.assert_not_called()

            # Test Variable exists in third backend.
            mock_meta_get.return_value = None
            mock_ssm_get.return_value = "ssm"
            assert "ssm" == Variable.get(key="test_var_3")
            mock_ssm_get.assert_called_once_with(key="test_var_3")
            mock_env_get.assert_not_called()

            # Test Variable exists in forth backend.
            mock_ssm_get.return_value = None
            mock_env_get.return_value = "env"
            assert "env" == Variable.get(key="test_var_4")
            mock_env_get.assert_called_once_with(key="test_var_4")

            # Test Variable not exists in any backends.
            mock_env_get.return_value = None
            with pytest.raises(KeyError, match="Variable test_var_5 does not exist"):
                Variable.get(key="test_var_5")

    @mock.patch("airflow.secrets.local_filesystem.LocalFilesystemBackend.get_connection")
    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connection")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    @mock.patch(
        "airflow.providers.amazon.aws.secrets.systems_manager."
        "SystemsManagerParameterStoreBackend.get_connection"
    )
    def test_backend_config_conn_order(self, mock_ssm_get, mock_env_get, mock_meta_get, mock_local_get):
        secrets_backend_config = [
            {"backend": LOCAL_SB},
            {"backend": METASTORE_DB_SB},
            {"backend": SSM_SB},
            {"backend": ENV_SB},
        ]

        with conf_vars({("secrets", "backends_config"): json.dumps(secrets_backend_config)}):
            ensure_secrets_loaded()

            # Test Connection exists in first backend.
            mock_local_conn = mock.MagicMock()
            mock_local_get.return_value = mock_local_conn
            assert mock_local_conn == Connection.get_connection_from_secrets("test_conn_1")
            mock_local_get.assert_called_once_with(conn_id="test_conn_1")
            mock_meta_get.assert_not_called()
            mock_ssm_get.assert_not_called()
            mock_env_get.assert_not_called()

            # Test Connection exists in second backend.
            mock_meta_conn = mock.MagicMock()
            mock_local_get.return_value = None
            mock_meta_get.return_value = mock_meta_conn
            assert mock_meta_conn == Connection.get_connection_from_secrets("test_conn_2")
            mock_meta_get.assert_called_once_with(conn_id="test_conn_2")
            mock_ssm_get.assert_not_called()
            mock_env_get.assert_not_called()

            # Test Connection exists in third backend.
            mock_ssm_conn = mock.MagicMock()
            mock_meta_get.return_value = None
            mock_ssm_get.return_value = mock_ssm_conn
            assert mock_ssm_conn == Connection.get_connection_from_secrets("test_conn_3")
            mock_ssm_get.assert_called_once_with(conn_id="test_conn_3")
            mock_env_get.assert_not_called()

            # Test Connection exists in fourth backend.
            mock_env_conn = mock.MagicMock()
            mock_ssm_get.return_value = None
            mock_env_get.return_value = mock_env_conn
            assert mock_env_conn == Connection.get_connection_from_secrets("test_conn_4")
            mock_env_get.assert_called_once_with(conn_id="test_conn_4")

            # Test Connection not exists in any backends.
            mock_env_get.return_value = None
            with pytest.raises(AirflowNotFoundException, match="The conn_id `test_conn_5` isn't defined"):
                Connection.get_connection_from_secrets("test_conn_5")
