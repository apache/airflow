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

from unittest import TestCase

from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse
from parameterized import parameterized

from airflow.contrib.secrets.gcp_secrets_manager import CloudSecretsManagerBackend
from airflow.exceptions import AirflowException
from airflow.models import Connection
from tests.compat import mock

CREDENTIALS = 'test-creds'
KEY_FILE = 'test-file.json'
PROJECT_ID = 'test-project-id'
CONNECTIONS_PREFIX = "test-connections"
VARIABLES_PREFIX = "test-variables"
SEP = '-'
CONN_ID = 'test-postgres'
CONN_URI = 'postgresql://airflow:airflow@host:5432/airflow'
VAR_KEY = 'hello'
VAR_VALUE = 'world'

MODULE_NAME = "airflow.contrib.secrets.gcp_secrets_manager"


class TestCloudSecretsManagerBackend(TestCase):
    def test_default_valid_and_sep(self):
        backend = CloudSecretsManagerBackend()
        self.assertTrue(backend._is_valid_prefix_and_sep())

    @parameterized.expand([
        ("colon:", "not:valid", ":"),
        ("slash/", "not/valid", "/"),
        ("space_with_char", "a b", ""),
        ("space_only", "", " ")
    ])
    def test_raise_exception_with_invalid_prefix_sep(self, _, prefix, sep):
        with self.assertRaises(AirflowException):
            CloudSecretsManagerBackend(connections_prefix=prefix, sep=sep)

    @parameterized.expand([
        ("dash-", "valid1", "-", True),
        ("underscore_", "isValid", "_", True),
        ("empty_string", "", "", True),
        ("space_prefix", " ", "", False),
        ("space_sep", "", " ", False),
        ("colon:", "not:valid", ":", False)
    ])
    def test_is_valid_prefix_and_sep(self, _, prefix, sep, is_valid):
        backend = CloudSecretsManagerBackend()
        backend.connections_prefix = prefix
        backend.sep = sep
        self.assertEqual(backend._is_valid_prefix_and_sep(), is_valid)

    @parameterized.expand([
        "airflow-connections",
        "connections",
        "airflow"
    ])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri(self, connections_prefix, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = CONN_URI.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretsManagerBackend(connections_prefix=connections_prefix)
        secret_id = secrets_manager_backend.build_path(connections_prefix, CONN_ID, SEP)
        returned_uri = secrets_manager_backend.get_conn_uri(conn_id=CONN_ID)
        self.assertEqual(CONN_URI, returned_uri)
        mock_client.secret_version_path.assert_called_once_with(
            PROJECT_ID, secret_id, "latest"
        )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".CloudSecretsManagerBackend.get_conn_uri")
    def test_get_connections(self, mock_get_uri, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_get_uri.return_value = CONN_URI
        conns = CloudSecretsManagerBackend().get_connections(conn_id=CONN_ID)
        self.assertIsInstance(conns, list)
        self.assertIsInstance(conns[0], Connection)

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri_non_existent_key(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound('test-msg')

        secrets_manager_backend = CloudSecretsManagerBackend(connections_prefix=CONNECTIONS_PREFIX)

        self.assertIsNone(secrets_manager_backend.get_conn_uri(conn_id=CONN_ID))
        self.assertEqual([], secrets_manager_backend.get_connections(conn_id=CONN_ID))

    @parameterized.expand([
        "airflow-variables",
        "variables",
        "airflow"
    ])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable(self, variables_prefix, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = VAR_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretsManagerBackend(variables_prefix=variables_prefix)
        secret_id = secrets_manager_backend.build_path(variables_prefix, VAR_KEY, SEP)
        returned_uri = secrets_manager_backend.get_variable(VAR_KEY)
        self.assertEqual(VAR_VALUE, returned_uri)
        mock_client.secret_version_path.assert_called_once_with(
            PROJECT_ID, secret_id, "latest"
        )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable_non_existent_key(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound('test-msg')

        secrets_manager_backend = CloudSecretsManagerBackend(variables_prefix=VARIABLES_PREFIX)
        self.assertIsNone(secrets_manager_backend.get_variable(VAR_KEY))
