# pylint: disable=no-member
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
import unittest
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.proto.service_pb2 import AccessSecretVersionResponse

from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
)

BASE_PACKAGE = 'airflow.providers.google.common.hooks.base_google.'
SECRETS_HOOK_PACKAGE = 'airflow.providers.google.cloud.hooks.secret_manager.'
INTERNAL_CLIENT_PACKAGE = 'airflow.providers.google.cloud._internal_client.secret_manager_client'


class TestSecretsManagerHook(unittest.TestCase):
    @patch(INTERNAL_CLIENT_PACKAGE + "._SecretManagerClient.client", return_value=MagicMock())
    @patch(
        SECRETS_HOOK_PACKAGE + 'SecretsManagerHook._get_credentials_and_project_id',
        return_value=(MagicMock(), GCP_PROJECT_ID_HOOK_UNIT_TEST),
    )
    @patch(BASE_PACKAGE + 'GoogleBaseHook.__init__', new=mock_base_gcp_hook_default_project_id)
    def test_get_missing_key(self, mock_get_credentials, mock_client):
        mock_client.secret_version_path.return_value = "full-path"
        mock_client.access_secret_version.side_effect = NotFound('test-msg')
        secrets_manager_hook = SecretsManagerHook(gcp_conn_id='test')
        mock_get_credentials.assert_called_once_with()
        secret = secrets_manager_hook.get_secret(secret_id="secret")
        mock_client.secret_version_path.assert_called_once_with('example-project', 'secret', 'latest')
        mock_client.access_secret_version.assert_called_once_with("full-path")
        self.assertIsNone(secret)

    @patch(INTERNAL_CLIENT_PACKAGE + "._SecretManagerClient.client", return_value=MagicMock())
    @patch(
        SECRETS_HOOK_PACKAGE + 'SecretsManagerHook._get_credentials_and_project_id',
        return_value=(MagicMock(), GCP_PROJECT_ID_HOOK_UNIT_TEST),
    )
    @patch(BASE_PACKAGE + 'GoogleBaseHook.__init__', new=mock_base_gcp_hook_default_project_id)
    def test_get_existing_key(self, mock_get_credentials, mock_client):
        mock_client.secret_version_path.return_value = "full-path"
        test_response = AccessSecretVersionResponse()
        test_response.payload.data = "result".encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response
        secrets_manager_hook = SecretsManagerHook(gcp_conn_id='test')
        mock_get_credentials.assert_called_once_with()
        secret = secrets_manager_hook.get_secret(secret_id="secret")
        mock_client.secret_version_path.assert_called_once_with('example-project', 'secret', 'latest')
        mock_client.access_secret_version.assert_called_once_with("full-path")
        self.assertEqual("result", secret)
