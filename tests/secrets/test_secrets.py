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
import unittest
from tests.compat import mock

from airflow.models import Variable
from airflow.secrets import ensure_secrets_loaded, get_connections, get_variable, initialize_secrets_backends
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_variables


class TestConnectionsFromSecrets(unittest.TestCase):
    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connections")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connections")
    def test_get_connections_second_try(self, mock_env_get, mock_meta_get):
        mock_env_get.side_effect = [[]]  # return empty list
        get_connections("fake_conn_id")
        mock_meta_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connections")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connections")
    def test_get_connections_first_try(self, mock_env_get, mock_meta_get):
        mock_env_get.side_effect = [["something"]]  # returns nonempty list
        get_connections("fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_meta_get.not_called()

    @conf_vars({
        ("secrets", "backend"):
            "airflow.contrib.secrets.aws_systems_manager.SystemsManagerParameterStoreBackend",
        ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
    })
    def test_initialize_secrets_backends(self):
        backends = initialize_secrets_backends()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        self.assertEqual(3, len(backends))
        self.assertIn('SystemsManagerParameterStoreBackend', backend_classes)

    @conf_vars({
        ("secrets", "backend"):
            "airflow.contrib.secrets.aws_systems_manager.SystemsManagerParameterStoreBackend",
        ("secrets", "backend_kwargs"): '{"use_ssl": false}',
    })
    def test_backends_kwargs(self):
        backends = initialize_secrets_backends()
        systems_manager = [
            backend for backend in backends
            if backend.__class__.__name__ == 'SystemsManagerParameterStoreBackend'
        ][0]

        self.assertEqual(systems_manager.kwargs, {'use_ssl': False})

    @conf_vars({
        ("secrets", "backend"):
            "airflow.contrib.secrets.aws_systems_manager.SystemsManagerParameterStoreBackend",
        ("secrets", "backend_kwargs"): '{"connections_prefix": "/airflow", "profile_name": null}',
    })
    @mock.patch.dict('os.environ', {
        'AIRFLOW_CONN_TEST_MYSQL': 'mysql://airflow:airflow@host:5432/airflow',
    })
    @mock.patch("airflow.contrib.secrets.aws_systems_manager."
                "SystemsManagerParameterStoreBackend.get_conn_uri")
    def test_backend_fallback_to_env_var(self, mock_get_uri):
        mock_get_uri.return_value = None

        backends = ensure_secrets_loaded()
        backend_classes = [backend.__class__.__name__ for backend in backends]
        self.assertIn('SystemsManagerParameterStoreBackend', backend_classes)

        uri = get_connections(conn_id="test_mysql")

        # Assert that SystemsManagerParameterStoreBackend.get_conn_uri was called
        mock_get_uri.assert_called_once_with(conn_id='test_mysql')

        self.assertEqual('mysql://airflow:airflow@host:5432/airflow', uri[0].get_uri())


class TestVariableFromSecrets(unittest.TestCase):

    def setUp(self):
        clear_db_variables()

    def tearDown(self):
        clear_db_variables()

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_second_try(self, mock_env_get, mock_meta_get):
        """
        Test if Variable is not present in Environment Variable, it then looks for it in
        Metastore DB
        """
        mock_env_get.return_value = None
        get_variable("fake_var_key")
        mock_meta_get.assert_called_once_with(key="fake_var_key")
        mock_env_get.assert_called_once_with(key="fake_var_key")

    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_variable")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_variable")
    def test_get_variable_first_try(self, mock_env_get, mock_meta_get):
        """
        Test if Variable is present in Environment Variable, it does not look for it in
        Metastore DB
        """
        mock_env_get.return_value = [["something"]]  # returns nonempty list
        get_variable("fake_var_key")
        mock_env_get.assert_called_once_with(key="fake_var_key")
        mock_meta_get.not_called()

    def test_backend_fallback_to_default_var(self):
        """
        Test if a default_var is defined and no backend has the Variable,
        the value returned is default_var
        """
        variable_value = Variable.get(key="test_var", default_var="new")
        self.assertEqual("new", variable_value)


if __name__ == "__main__":
    unittest.main()
