# pylint: disable=no-member
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

from google.api_core.exceptions import NotFound, PermissionDenied
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse

from airflow.providers.google.cloud._internal_client.secret_manager_client import _SecretManagerClient  # noqa
from airflow.version import version

INTERNAL_CLIENT_MODULE = "airflow.providers.google.cloud._internal_client.secret_manager_client"


class TestSecretManagerClient(TestCase):
    @mock.patch(INTERNAL_CLIENT_MODULE + ".SecretManagerServiceClient")
    @mock.patch(INTERNAL_CLIENT_MODULE + ".ClientInfo")
    def test_auth(self, mock_client_info, mock_secrets_client):
        mock_client_info_mock = mock.MagicMock()
        mock_client_info.return_value = mock_client_info_mock
        mock_secrets_client.return_value = mock.MagicMock()
        secrets_client = _SecretManagerClient(credentials="credentials")
        _ = secrets_client.client
        mock_client_info.assert_called_with(client_library_version='airflow_v' + version)
        mock_secrets_client.assert_called_with(credentials='credentials', client_info=mock_client_info_mock)

    @mock.patch(INTERNAL_CLIENT_MODULE + ".SecretManagerServiceClient")
    @mock.patch(INTERNAL_CLIENT_MODULE + ".ClientInfo")
    def test_get_non_existing_key(self, mock_client_info, mock_secrets_client):
        mock_client = mock.MagicMock()
        mock_client_info.return_value = mock.MagicMock()
        mock_secrets_client.return_value = mock_client
        mock_client.secret_version_path.return_value = "full-path"
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound('test-msg')
        secrets_client = _SecretManagerClient(credentials="credentials")
        secret = secrets_client.get_secret(secret_id="missing", project_id="project_id")
        mock_client.secret_version_path.assert_called_once_with("project_id", 'missing', 'latest')
        self.assertIsNone(secret)
        mock_client.access_secret_version.assert_called_once_with('full-path')

    @mock.patch(INTERNAL_CLIENT_MODULE + ".SecretManagerServiceClient")
    @mock.patch(INTERNAL_CLIENT_MODULE + ".ClientInfo")
    def test_get_no_permissions(self, mock_client_info, mock_secrets_client):
        mock_client = mock.MagicMock()
        mock_client_info.return_value = mock.MagicMock()
        mock_secrets_client.return_value = mock_client
        mock_client.secret_version_path.return_value = "full-path"
        # No permissions for requested secret id
        mock_client.access_secret_version.side_effect = PermissionDenied('test-msg')
        secrets_client = _SecretManagerClient(credentials="credentials")
        secret = secrets_client.get_secret(secret_id="missing", project_id="project_id")
        mock_client.secret_version_path.assert_called_once_with("project_id", 'missing', 'latest')
        self.assertIsNone(secret)
        mock_client.access_secret_version.assert_called_once_with('full-path')

    @mock.patch(INTERNAL_CLIENT_MODULE + ".SecretManagerServiceClient")
    @mock.patch(INTERNAL_CLIENT_MODULE + ".ClientInfo")
    def test_get_existing_key(self, mock_client_info, mock_secrets_client):
        mock_client = mock.MagicMock()
        mock_client_info.return_value = mock.MagicMock()
        mock_secrets_client.return_value = mock_client
        mock_client.secret_version_path.return_value = "full-path"
        test_response = AccessSecretVersionResponse()
        test_response.payload.data = "result".encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response
        secrets_client = _SecretManagerClient(credentials="credentials")
        secret = secrets_client.get_secret(secret_id="existing", project_id="project_id")
        mock_client.secret_version_path.assert_called_once_with("project_id", 'existing', 'latest')
        self.assertEqual("result", secret)
        mock_client.access_secret_version.assert_called_once_with('full-path')

    @mock.patch(INTERNAL_CLIENT_MODULE + ".SecretManagerServiceClient")
    @mock.patch(INTERNAL_CLIENT_MODULE + ".ClientInfo")
    def test_get_existing_key_with_version(self, mock_client_info, mock_secrets_client):
        mock_client = mock.MagicMock()
        mock_client_info.return_value = mock.MagicMock()
        mock_secrets_client.return_value = mock_client
        mock_client.secret_version_path.return_value = "full-path"
        test_response = AccessSecretVersionResponse()
        test_response.payload.data = "result".encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response
        secrets_client = _SecretManagerClient(credentials="credentials")
        secret = secrets_client.get_secret(
            secret_id="existing", project_id="project_id", secret_version="test-version"
        )
        mock_client.secret_version_path.assert_called_once_with("project_id", 'existing', 'test-version')
        self.assertEqual("result", secret)
        mock_client.access_secret_version.assert_called_once_with('full-path')
