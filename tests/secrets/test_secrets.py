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
from unittest import mock

from airflow.secrets import get_connections, initialize_secrets_backends
from tests.test_utils.config import conf_vars


class TestSecrets(unittest.TestCase):
    @mock.patch("airflow.secrets.metastore.MetastoreSecretsBackend.get_connections")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesSecretsBackend.get_connections")
    def test_get_connections_second_try(self, mock_env_get, mock_meta_get):
        mock_env_get.side_effect = [[]]  # return empty list
        get_connections("fake_conn_id")
        mock_meta_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")

    @mock.patch("airflow.secrets.metastore.MetastoreSecretsBackend.get_connections")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesSecretsBackend.get_connections")
    def test_get_connections_first_try(self, mock_env_get, mock_meta_get):
        mock_env_get.side_effect = [["something"]]  # returns nonempty list
        get_connections("fake_conn_id")
        mock_env_get.assert_called_once_with(conn_id="fake_conn_id")
        mock_meta_get.not_called()

    @conf_vars({
        ("secrets", "backend"): "airflow.providers.amazon.aws.secrets.ssm.AwsSsmSecretsBackend",
        ("secrets", "backend_kwargs"): '{"prefix": "/airflow", "profile_name": null}',
    })
    def test_initialize_secrets_backends(self):
        backends = initialize_secrets_backends()
        backend_classes = [backend.__class__.__name__ for backend in backends]

        self.assertEqual(3, len(backends))
        self.assertIn('AwsSsmSecretsBackend', backend_classes)


if __name__ == "__main__":
    unittest.main()
