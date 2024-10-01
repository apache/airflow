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

from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types.service import AccessSecretVersionResponse

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.secret_manager import (
    GoogleCloudSecretManagerHook,
    SecretsManagerHook,
)
from airflow.providers.google.common.consts import CLIENT_INFO
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
)

BASE_PACKAGE = "airflow.providers.google.common.hooks.base_google."
SECRETS_HOOK_PACKAGE = "airflow.providers.google.cloud.hooks.secret_manager."
INTERNAL_CLIENT_PACKAGE = "airflow.providers.google.cloud._internal_client.secret_manager_client"
SECRET_ID = "test-secret-id"


class TestSecretsManagerHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            with pytest.warns(AirflowProviderDeprecationWarning):
                SecretsManagerHook(gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to")

    @patch(INTERNAL_CLIENT_PACKAGE + "._SecretManagerClient.client", return_value=MagicMock())
    @patch(
        SECRETS_HOOK_PACKAGE + "SecretsManagerHook.get_credentials_and_project_id",
        return_value=(MagicMock(), GCP_PROJECT_ID_HOOK_UNIT_TEST),
    )
    @patch(BASE_PACKAGE + "GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    def test_get_missing_key(self, mock_get_credentials, mock_client):
        mock_client.secret_version_path.return_value = "full-path"
        mock_client.access_secret_version.side_effect = NotFound("test-msg")
        with pytest.warns(AirflowProviderDeprecationWarning):
            secrets_manager_hook = SecretsManagerHook(gcp_conn_id="test")
        mock_get_credentials.assert_called_once_with()
        secret = secrets_manager_hook.get_secret(secret_id="secret")
        mock_client.secret_version_path.assert_called_once_with("example-project", "secret", "latest")
        mock_client.access_secret_version.assert_called_once_with(request={"name": "full-path"})
        assert secret is None

    @patch(INTERNAL_CLIENT_PACKAGE + "._SecretManagerClient.client", return_value=MagicMock())
    @patch(
        SECRETS_HOOK_PACKAGE + "SecretsManagerHook.get_credentials_and_project_id",
        return_value=(MagicMock(), GCP_PROJECT_ID_HOOK_UNIT_TEST),
    )
    @patch(BASE_PACKAGE + "GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    def test_get_existing_key(self, mock_get_credentials, mock_client):
        mock_client.secret_version_path.return_value = "full-path"
        test_response = AccessSecretVersionResponse()
        test_response.payload.data = b"result"
        mock_client.access_secret_version.return_value = test_response
        with pytest.warns(AirflowProviderDeprecationWarning):
            secrets_manager_hook = SecretsManagerHook(gcp_conn_id="test")
        mock_get_credentials.assert_called_once_with()
        secret = secrets_manager_hook.get_secret(secret_id="secret")
        mock_client.secret_version_path.assert_called_once_with("example-project", "secret", "latest")
        mock_client.access_secret_version.assert_called_once_with(request={"name": "full-path"})
        assert "result" == secret


class TestGoogleCloudSecretManagerHook:
    def setup_method(self, method):
        with patch(f"{BASE_PACKAGE}GoogleBaseHook.get_connection", return_value=MagicMock()):
            self.hook = GoogleCloudSecretManagerHook()

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.get_credentials")
    @patch(f"{SECRETS_HOOK_PACKAGE}SecretManagerServiceClient")
    def test_client(self, mock_client, mock_get_credentials):
        mock_client_result = mock_client.return_value
        mock_credentials = self.hook.get_credentials.return_value

        client_1 = self.hook.client
        client_2 = self.hook.client

        assert client_1 == mock_client_result
        assert client_1 == client_2
        mock_client.assert_called_once_with(credentials=mock_credentials, client_info=CLIENT_INFO)
        mock_get_credentials.assert_called_once()

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_get_conn(self, mock_client):
        mock_client_result = mock_client.return_value

        client_1 = self.hook.get_conn()

        assert client_1 == mock_client_result
        mock_client.assert_called_once()

    @pytest.mark.parametrize(
        "input_secret, expected_secret",
        [
            (None, {"replication": {"automatic": {}}}),
            (mock_secret := MagicMock(), mock_secret),  # type: ignore[name-defined]
        ],
    )
    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_create_secret(self, mock_client, input_secret, expected_secret):
        expected_parent = f"projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}"
        expected_response = mock_client.return_value.create_secret.return_value
        mock_retry, mock_timeout, mock_metadata = MagicMock(), MagicMock(), MagicMock()

        actual_response = self.hook.create_secret(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            secret_id=SECRET_ID,
            secret=input_secret,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        assert actual_response == expected_response
        mock_client.assert_called_once()
        mock_client.return_value.create_secret.assert_called_once_with(
            request={
                "parent": expected_parent,
                "secret_id": SECRET_ID,
                "secret": expected_secret,
            },
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_add_secret_version(self, mock_client):
        expected_parent = f"projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}/secrets/{SECRET_ID}"
        expected_response = mock_client.return_value.add_secret_version.return_value
        mock_payload, mock_retry, mock_timeout, mock_metadata = (MagicMock() for _ in range(4))

        actual_response = self.hook.add_secret_version(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            secret_id=SECRET_ID,
            secret_payload=mock_payload,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        assert actual_response == expected_response
        mock_client.assert_called_once()
        mock_client.return_value.add_secret_version.assert_called_once_with(
            request={
                "parent": expected_parent,
                "payload": mock_payload,
            },
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_list_secrets(self, mock_client):
        expected_parent = f"projects/{GCP_PROJECT_ID_HOOK_UNIT_TEST}"
        expected_response = mock_client.return_value.list_secrets.return_value
        mock_filter, mock_retry, mock_timeout, mock_metadata = (MagicMock() for _ in range(4))
        page_size, page_token = 20, "test-page-token"

        actual_response = self.hook.list_secrets(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            secret_filter=mock_filter,
            page_size=page_size,
            page_token=page_token,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        assert actual_response == expected_response
        mock_client.assert_called_once()
        mock_client.return_value.list_secrets.assert_called_once_with(
            request={
                "parent": expected_parent,
                "page_size": page_size,
                "page_token": page_token,
                "filter": mock_filter,
            },
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

    @pytest.mark.parametrize(
        "secret_names, secret_id, secret_exists_expected",
        [
            ([], SECRET_ID, False),
            (["secret/name"], SECRET_ID, False),
            (["secret/name1", "secret/name1"], SECRET_ID, False),
            ([f"secret/{SECRET_ID}"], SECRET_ID, True),
            ([f"secret/{SECRET_ID}", "secret/name"], SECRET_ID, True),
            (["secret/name", f"secret/{SECRET_ID}"], SECRET_ID, True),
            (["name1", SECRET_ID], SECRET_ID, True),
        ],
    )
    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.list_secrets")
    def test_secret_exists(
        self, mock_list_secrets, mock_client, secret_names, secret_id, secret_exists_expected
    ):
        list_secrets = []
        for secret_name in secret_names:
            mock_secret = MagicMock()
            mock_secret.name = secret_name
            list_secrets.append(mock_secret)
        mock_list_secrets.return_value = list_secrets
        secret_filter = f"name:{secret_id}"

        secret_exists_actual = self.hook.secret_exists(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST, secret_id=secret_id
        )

        assert secret_exists_actual == secret_exists_expected
        mock_client.return_value.secret_path.assert_called_once_with(GCP_PROJECT_ID_HOOK_UNIT_TEST, secret_id)
        mock_list_secrets.assert_called_once_with(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST, page_size=100, secret_filter=secret_filter
        )

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_access_secret(self, mock_client):
        expected_response = mock_client.return_value.access_secret_version.return_value
        mock_retry, mock_timeout, mock_metadata = (MagicMock() for _ in range(3))
        secret_version = "test-secret-version"
        mock_name = mock_client.return_value.secret_version_path.return_value

        actual_response = self.hook.access_secret(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            secret_id=SECRET_ID,
            secret_version=secret_version,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        assert actual_response == expected_response
        assert mock_client.call_count == 2
        mock_client.return_value.secret_version_path.assert_called_once_with(
            GCP_PROJECT_ID_HOOK_UNIT_TEST, SECRET_ID, secret_version
        )
        mock_client.return_value.access_secret_version.assert_called_once_with(
            request={"name": mock_name},
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

    @patch(f"{SECRETS_HOOK_PACKAGE}GoogleCloudSecretManagerHook.client", new_callable=PropertyMock)
    def test_delete_secret(self, mock_client):
        mock_retry, mock_timeout, mock_metadata = (MagicMock() for _ in range(3))
        mock_name = mock_client.return_value.secret_path.return_value

        actual_response = self.hook.delete_secret(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            secret_id=SECRET_ID,
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )

        assert actual_response is None
        assert mock_client.call_count == 2
        mock_client.return_value.secret_path.assert_called_once_with(GCP_PROJECT_ID_HOOK_UNIT_TEST, SECRET_ID)
        mock_client.return_value.delete_secret.assert_called_once_with(
            request={"name": mock_name},
            retry=mock_retry,
            timeout=mock_timeout,
            metadata=mock_metadata,
        )
