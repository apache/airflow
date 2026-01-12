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

import logging
import re
from unittest import mock

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.secrets.secret_manager import CloudSecretManagerBackend

CREDENTIALS = "test-creds"
KEY_FILE = "test-file.json"
PROJECT_ID = "test-project-id"
OVERRIDDEN_PROJECT_ID = "overridden-test-project-id"
CONNECTIONS_PREFIX = "test-connections"
VARIABLES_PREFIX = "test-variables"
SEP = "-"
CONN_ID = "test-postgres"
CONN_URI = "postgresql://airflow:airflow@host:5432/airflow"
VAR_KEY = "hello"
VAR_VALUE = "world"
CONFIG_KEY = "sql_alchemy_conn"
CONFIG_VALUE = "postgresql://airflow:airflow@host:5432/airflow"

MODULE_NAME = "airflow.providers.google.cloud.secrets.secret_manager"
CLIENT_MODULE_NAME = "airflow.providers.google.cloud._internal_client.secret_manager_client"


class TestCloudSecretManagerBackend:
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_default_valid_and_sep(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        backend = CloudSecretManagerBackend()
        assert backend._is_valid_prefix_and_sep()

    @pytest.mark.parametrize(
        ("prefix", "sep"),
        [
            pytest.param("not:valid", ":", id="colon separator"),
            pytest.param("not/valid", "/", id="backslash separator"),
            pytest.param("a b", "", id="space with char and empty separator"),
            pytest.param(" ", "", id="space only and empty separator"),
        ],
    )
    def test_raise_exception_with_invalid_prefix_sep(self, prefix, sep):
        with pytest.raises(AirflowException):
            CloudSecretManagerBackend(connections_prefix=prefix, sep=sep)

    @pytest.mark.parametrize(
        ("prefix", "sep", "is_valid"),
        [
            pytest.param("valid1", "-", True, id="valid: dash separator"),
            pytest.param("isValid", "_", True, id="valid: underscore separator"),
            pytest.param("", "", True, id="valid: empty string and empty separator"),
            pytest.param("", " ", False, id="invalid: empty string and space separator"),
            pytest.param("not:valid", ":", False, id="invalid: colon separator"),
        ],
    )
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_is_valid_prefix_and_sep(self, mock_client_callable, mock_get_creds, prefix, sep, is_valid):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        backend = CloudSecretManagerBackend()
        backend.connections_prefix = prefix
        backend.sep = sep
        assert backend._is_valid_prefix_and_sep() == is_valid

    @pytest.mark.parametrize("connections_prefix", ["airflow-connections", "connections", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri(self, mock_client_callable, mock_get_creds, connections_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = CONN_URI.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=connections_prefix)
        secret_id = secrets_manager_backend.build_path(connections_prefix, CONN_ID, SEP)
        returned_uri = secrets_manager_backend.get_conn_value(conn_id=CONN_ID)
        assert returned_uri == CONN_URI
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(MODULE_NAME + ".CloudSecretManagerBackend.get_conn_value")
    def test_get_connection(self, mock_get_value, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_get_value.return_value = CONN_URI
        conn = CloudSecretManagerBackend().get_connection(conn_id=CONN_ID)
        assert isinstance(conn, Connection)

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_conn_uri_non_existent_key(self, mock_client_callable, mock_get_creds, caplog):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound("test-msg")

        secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=CONNECTIONS_PREFIX)
        secret_id = secrets_manager_backend.build_path(CONNECTIONS_PREFIX, CONN_ID, SEP)
        with caplog.at_level(level=logging.DEBUG, logger=secrets_manager_backend.client.log.name):
            assert secrets_manager_backend.get_conn_value(conn_id=CONN_ID) is None
            assert secrets_manager_backend.get_connection(conn_id=CONN_ID) is None
            assert re.search(
                f"Google Cloud API Call Error \\(NotFound\\): Secret ID {secret_id} not found",
                caplog.messages[0],
            )

    @pytest.mark.parametrize("variables_prefix", ["airflow-variables", "variables", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable(self, mock_client_callable, mock_get_creds, variables_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = VAR_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=variables_prefix)
        secret_id = secrets_manager_backend.build_path(variables_prefix, VAR_KEY, SEP)
        returned_uri = secrets_manager_backend.get_variable(VAR_KEY)
        assert returned_uri == VAR_VALUE
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @pytest.mark.parametrize("config_prefix", ["airflow-config", "config", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_config(self, mock_client_callable, mock_get_creds, config_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = CONFIG_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(config_prefix=config_prefix)
        secret_id = secrets_manager_backend.build_path(config_prefix, CONFIG_KEY, SEP)
        returned_val = secrets_manager_backend.get_config(CONFIG_KEY)
        assert returned_val == CONFIG_VALUE
        mock_client.secret_version_path.assert_called_once_with(PROJECT_ID, secret_id, "latest")

    @pytest.mark.parametrize("variables_prefix", ["airflow-variables", "variables", "airflow"])
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable_override_project_id(self, mock_client_callable, mock_get_creds, variables_prefix):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        test_response = AccessSecretVersionResponse()
        test_response.payload.data = VAR_VALUE.encode("UTF-8")
        mock_client.access_secret_version.return_value = test_response

        secrets_manager_backend = CloudSecretManagerBackend(
            variables_prefix=variables_prefix, project_id=OVERRIDDEN_PROJECT_ID
        )
        secret_id = secrets_manager_backend.build_path(variables_prefix, VAR_KEY, SEP)
        returned_uri = secrets_manager_backend.get_variable(VAR_KEY)
        assert returned_uri == VAR_VALUE
        mock_client.secret_version_path.assert_called_once_with(OVERRIDDEN_PROJECT_ID, secret_id, "latest")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_get_variable_non_existent_key(self, mock_client_callable, mock_get_creds, caplog):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client
        # The requested secret id or secret version does not exist
        mock_client.access_secret_version.side_effect = NotFound("test-msg")

        secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=VARIABLES_PREFIX)
        secret_id = secrets_manager_backend.build_path(VARIABLES_PREFIX, VAR_KEY, SEP)
        with caplog.at_level(level=logging.DEBUG, logger=secrets_manager_backend.client.log.name):
            assert secrets_manager_backend.get_variable(VAR_KEY) is None
            assert re.search(
                f"Google Cloud API Call Error \\(NotFound\\): Secret ID {secret_id} not found",
                caplog.messages[0],
            )

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_connections_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            with mock.patch(
                MODULE_NAME + ".CloudSecretManagerBackend._is_valid_prefix_and_sep"
            ) as mock_is_valid_prefix_sep:
                secrets_manager_backend = CloudSecretManagerBackend(connections_prefix=None)

                mock_is_valid_prefix_sep.assert_not_called()
                assert secrets_manager_backend.get_conn_value(conn_id=CONN_ID) is None
                mock_get_secret.assert_not_called()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_variables_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            secrets_manager_backend = CloudSecretManagerBackend(variables_prefix=None)

            assert secrets_manager_backend.get_variable(VAR_KEY) is None
            mock_get_secret.assert_not_called()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    @mock.patch(CLIENT_MODULE_NAME + ".SecretManagerServiceClient")
    def test_config_prefix_none_value(self, mock_client_callable, mock_get_creds):
        mock_get_creds.return_value = CREDENTIALS, PROJECT_ID
        mock_client = mock.MagicMock()
        mock_client_callable.return_value = mock_client

        with mock.patch(MODULE_NAME + ".CloudSecretManagerBackend._get_secret") as mock_get_secret:
            secrets_manager_backend = CloudSecretManagerBackend(config_prefix=None)

            assert secrets_manager_backend.get_config(CONFIG_KEY) is None
            mock_get_secret.assert_not_called()
