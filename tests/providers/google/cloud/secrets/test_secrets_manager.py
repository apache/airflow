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

from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse
from parameterized import parameterized

from airflow.models import Connection
from airflow.providers.google.cloud.secrets.secrets_manager import CloudSecretsManagerSecretsBackend

CREDENTIALS = 'test-creds'
KEY_FILE = 'test-file.json'
PROJECT_ID = 'test-project-id'
CONN_ID = 'test-postgres'
CONN_URI = 'postgresql://airflow:airflow@host:5432/airflow'

MODULE_NAME = "airflow.providers.google.cloud.secrets.secrets_manager"


class TestCloudSecretsManagerBackend(TestCase):
    @parameterized.expand([
        "airflow/connections",
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

        secrets_manager_backend = CloudSecretsManagerSecretsBackend(connections_prefix=connections_prefix)
        returned_uri = secrets_manager_backend.get_conn_uri(conn_id=CONN_ID)
        self.assertEqual(CONN_URI, returned_uri)
        mock_client.secret_version_path.assert_called_once_with(
            PROJECT_ID, f"{connections_prefix}/{CONN_ID}", "latest"
        )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".CloudSecretsManagerSecretsBackend.get_conn_uri")
    def test_get_connections(self, mock_get_uri, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_get_uri.return_value = CONN_URI
        conns = CloudSecretsManagerSecretsBackend().get_connections(conn_id=CONN_ID)
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

        connections_prefix = "airflow/connections"

        secrets_manager_backend = CloudSecretsManagerSecretsBackend(connections_prefix=connections_prefix)
        with self.assertLogs(secrets_manager_backend.log, level="ERROR") as log_output:
            self.assertIsNone(secrets_manager_backend.get_conn_uri(conn_id=CONN_ID))
            self.assertEqual([], secrets_manager_backend.get_connections(conn_id=CONN_ID))
            self.assertRegex(
                log_output.output[0],
                f"GCP API Call Error \\(NotFound\\): Secret ID {connections_prefix}/{CONN_ID} not found"
            )
