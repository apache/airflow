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

import json
import logging
import os
import re
from io import StringIO
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import ANY
from uuid import uuid4

import pytest
from google.auth.environment_vars import CREDENTIALS
from google.auth.exceptions import DefaultCredentialsError

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.utils.credentials_provider import (
    _DEFAULT_SCOPES,
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT,
    _get_project_id_from_service_account_email,
    _get_scopes,
    _get_target_principal_and_delegates,
    build_gcp_conn,
    get_credentials_and_project_id,
    provide_gcp_conn_and_credentials,
    provide_gcp_connection,
    provide_gcp_credentials,
)

ENV_VALUE = "test_env"
TEMP_VARIABLE = "temp_variable"
KEY = str(uuid4())
ENV_CRED = "temp_cred"
ACCOUNT_1_SAME_PROJECT = "account_1@project_id.iam.gserviceaccount.com"
ACCOUNT_2_SAME_PROJECT = "account_2@project_id.iam.gserviceaccount.com"
ACCOUNT_3_ANOTHER_PROJECT = "account_3@another_project_id.iam.gserviceaccount.com"
ANOTHER_PROJECT_ID = "another_project_id"
CRED_PROVIDER_LOGGER_NAME = "airflow.providers.google.cloud.utils.credentials_provider._CredentialProvider"


class TestHelper:
    def test_build_gcp_conn_path(self):
        value = "test"
        conn = build_gcp_conn(key_file_path=value)
        assert "google-cloud-platform://?key_path=test" == conn

    def test_build_gcp_conn_scopes(self):
        value = ["test", "test2"]
        conn = build_gcp_conn(scopes=value)
        assert "google-cloud-platform://?scope=test%2Ctest2" == conn

    def test_build_gcp_conn_project(self):
        value = "test"
        conn = build_gcp_conn(project_id=value)
        assert "google-cloud-platform://?projects=test" == conn


class TestProvideGcpCredentials:
    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_gcp_credentials_key_content(self, mock_file):
        file_dict = {"foo": "bar"}
        string_file = StringIO()
        file_content = json.dumps(file_dict)
        file_name = "/test/mock-file"
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with provide_gcp_credentials(key_file_dict=file_dict):
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()
        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credentials_keep_environment(self):
        key_path = "/test/key-path"
        with provide_gcp_credentials(key_file_path=key_path):
            assert os.environ[CREDENTIALS] == key_path
        assert os.environ[CREDENTIALS] == ENV_VALUE


class TestProvideGcpConnection:
    @mock.patch.dict(os.environ, {AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: ENV_VALUE})
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.build_gcp_conn")
    def test_provide_gcp_connection(self, mock_builder):
        mock_builder.return_value = TEMP_VARIABLE
        path = "path/to/file.json"
        scopes = ["scopes"]
        project_id = "project_id"
        with provide_gcp_connection(path, scopes, project_id):
            mock_builder.assert_called_once_with(key_file_path=path, scopes=scopes, project_id=project_id)
            assert os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT] == TEMP_VARIABLE
        assert os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT] == ENV_VALUE


class TestProvideGcpConnAndCredentials:
    @mock.patch.dict(
        os.environ,
        {AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: ENV_VALUE, CREDENTIALS: ENV_VALUE},
    )
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.build_gcp_conn")
    def test_provide_gcp_conn_and_credentials(self, mock_builder):
        mock_builder.return_value = TEMP_VARIABLE
        path = "path/to/file.json"
        scopes = ["scopes"]
        project_id = "project_id"
        with provide_gcp_conn_and_credentials(path, scopes, project_id):
            mock_builder.assert_called_once_with(key_file_path=path, scopes=scopes, project_id=project_id)
            assert os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT] == TEMP_VARIABLE
            assert os.environ[CREDENTIALS] == path
        assert os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT] == ENV_VALUE
        assert os.environ[CREDENTIALS] == ENV_VALUE


@pytest.mark.db_test
class TestGetGcpCredentialsAndProjectId:
    test_scopes = _DEFAULT_SCOPES
    test_key_file = "KEY_PATH.json"
    test_project_id = "project_id"

    @mock.patch("google.auth.default", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth(self, mock_auth_default, caplog):
        with caplog.at_level(level=logging.INFO, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            result = get_credentials_and_project_id()
        mock_auth_default.assert_called_once_with(scopes=None)
        assert ("CREDENTIALS", "PROJECT_ID") == result
        assert (
            "Getting connection using `google.auth.default()` since no explicit credentials are provided."
        ) in caplog.messages

    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_delegate(self, mock_auth_default):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(delegate_to="USER")
        mock_auth_default.assert_called_once_with(scopes=None)
        mock_credentials.with_subject.assert_called_once_with("USER")
        assert (mock_credentials.with_subject.return_value, self.test_project_id) == result

    @pytest.mark.parametrize("scopes", [["scope1"], ["scope1", "scope2"]])
    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_scopes(self, mock_auth_default, scopes):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(scopes=scopes)
        mock_auth_default.assert_called_once_with(scopes=scopes)
        assert mock_auth_default.return_value == result

    @mock.patch(
        "airflow.providers.google.cloud.utils.credentials_provider.impersonated_credentials.Credentials"
    )
    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_target_principal(
        self, mock_auth_default, mock_impersonated_credentials
    ):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(
            target_principal=ACCOUNT_3_ANOTHER_PROJECT,
        )
        mock_auth_default.assert_called_once_with(scopes=None)
        mock_impersonated_credentials.assert_called_once_with(
            source_credentials=mock_credentials,
            target_principal=ACCOUNT_3_ANOTHER_PROJECT,
            delegates=None,
            target_scopes=None,
        )
        assert (mock_impersonated_credentials.return_value, ANOTHER_PROJECT_ID) == result

    @mock.patch(
        "airflow.providers.google.cloud.utils.credentials_provider.impersonated_credentials.Credentials"
    )
    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_scopes_and_target_principal(
        self, mock_auth_default, mock_impersonated_credentials
    ):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(
            scopes=["scope1", "scope2"],
            target_principal=ACCOUNT_1_SAME_PROJECT,
        )
        mock_auth_default.assert_called_once_with(scopes=["scope1", "scope2"])
        mock_impersonated_credentials.assert_called_once_with(
            source_credentials=mock_credentials,
            target_principal=ACCOUNT_1_SAME_PROJECT,
            delegates=None,
            target_scopes=["scope1", "scope2"],
        )
        assert (mock_impersonated_credentials.return_value, self.test_project_id) == result

    @mock.patch(
        "airflow.providers.google.cloud.utils.credentials_provider.impersonated_credentials.Credentials"
    )
    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_target_principal_and_delegates(
        self, mock_auth_default, mock_impersonated_credentials
    ):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(
            target_principal=ACCOUNT_3_ANOTHER_PROJECT,
            delegates=[ACCOUNT_1_SAME_PROJECT, ACCOUNT_2_SAME_PROJECT],
        )
        mock_auth_default.assert_called_once_with(scopes=None)
        mock_impersonated_credentials.assert_called_once_with(
            source_credentials=mock_credentials,
            target_principal=ACCOUNT_3_ANOTHER_PROJECT,
            delegates=[ACCOUNT_1_SAME_PROJECT, ACCOUNT_2_SAME_PROJECT],
            target_scopes=None,
        )
        assert (mock_impersonated_credentials.return_value, ANOTHER_PROJECT_ID) == result

    @mock.patch(
        "google.oauth2.service_account.Credentials.from_service_account_file",
    )
    def test_get_credentials_and_project_id_with_service_account_file(
        self, mock_from_service_account_file, caplog
    ):
        mock_from_service_account_file.return_value.project_id = self.test_project_id
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            result = get_credentials_and_project_id(key_path=self.test_key_file)
        mock_from_service_account_file.assert_called_once_with(self.test_key_file, scopes=None)
        assert (mock_from_service_account_file.return_value, self.test_project_id) == result
        assert "Getting connection using JSON key file KEY_PATH.json" in caplog.messages

    @pytest.mark.parametrize(
        "file", [pytest.param("path/to/file.p12", id="p12"), pytest.param("incorrect_file.ext", id="unknown")]
    )
    def test_get_credentials_and_project_id_with_service_account_file_and_non_valid_key(self, file):
        with pytest.raises(AirflowException):
            get_credentials_and_project_id(key_path=file)

    @mock.patch(
        "google.oauth2.service_account.Credentials.from_service_account_info",
    )
    def test_get_credentials_and_project_id_with_service_account_info(
        self, mock_from_service_account_info, caplog
    ):
        mock_from_service_account_info.return_value.project_id = self.test_project_id
        service_account = {"private_key": "PRIVATE_KEY"}
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            result = get_credentials_and_project_id(keyfile_dict=service_account)
        mock_from_service_account_info.assert_called_once_with(service_account, scopes=None)
        assert (mock_from_service_account_info.return_value, self.test_project_id) == result
        assert "Getting connection using JSON Dict" in caplog.messages

    @mock.patch("google.auth.load_credentials_from_file", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_using_credential_config_file(self, mock_load_credentials_from_file, caplog):
        with caplog.at_level(
            level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME
        ), NamedTemporaryFile() as temp_file:
            caplog.clear()
            result = get_credentials_and_project_id(credential_config_file=temp_file.name)
        mock_load_credentials_from_file.assert_called_once_with(temp_file.name, scopes=None)
        assert mock_load_credentials_from_file.return_value == result
        assert (
            f"Getting connection using credential configuration file: `{temp_file.name}`" in caplog.messages
        )

    @mock.patch("google.auth.load_credentials_from_file", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_using_credential_config_dict(self, mock_load_credentials_from_file, caplog):
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            result = get_credentials_and_project_id(credential_config_file={"type": "external_account"})
        mock_load_credentials_from_file.assert_called_once()
        assert mock_load_credentials_from_file.return_value == result
        assert "Getting connection using credential configuration dict." in caplog.messages

    @mock.patch("google.auth.load_credentials_from_file", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_using_credential_config_string(self, mock_load_credentials_from_file, caplog):
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            result = get_credentials_and_project_id(credential_config_file='{"type": "external_account"}')
        mock_load_credentials_from_file.assert_called_once()
        assert mock_load_credentials_from_file.return_value == result
        assert "Getting connection using credential configuration string." in caplog.messages

    def test_get_credentials_using_credential_config_invalid_string(self, caplog):
        with pytest.raises(DefaultCredentialsError), caplog.at_level(
            level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME
        ):
            caplog.clear()
            get_credentials_and_project_id(credential_config_file="invalid json}}}}")
        assert "Getting connection using credential configuration string." in caplog.messages

    @mock.patch("google.auth.default", return_value=("CREDENTIALS", "PROJECT_ID"))
    @mock.patch("google.oauth2.service_account.Credentials.from_service_account_info")
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._SecretManagerClient")
    def test_get_credentials_and_project_id_with_key_secret_name(
        self, mock_secret_manager_client, mock_from_service_account_info, mock_default
    ):
        mock_secret_manager_client.return_value.is_valid_secret_name.return_value = True
        mock_secret_manager_client.return_value.get_secret.return_value = (
            '{"type":"service_account","project_id":"pid",'
            '"private_key_id":"pkid",'
            '"private_key":"payload"}'
        )

        get_credentials_and_project_id(key_secret_name="secret name")
        mock_from_service_account_info.assert_called_once_with(
            {
                "type": "service_account",
                "project_id": "pid",
                "private_key_id": "pkid",
                "private_key": "payload",
            },
            scopes=ANY,
        )

    @mock.patch("google.auth.default", return_value=("CREDENTIALS", "PROJECT_ID"))
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider._SecretManagerClient")
    def test_get_credentials_and_project_id_with_key_secret_name_when_key_is_invalid(
        self, mock_secret_manager_client, mock_default
    ):
        mock_secret_manager_client.return_value.is_valid_secret_name.return_value = True
        mock_secret_manager_client.return_value.get_secret.return_value = ""

        with pytest.raises(
            AirflowException,
            match=re.escape("Key data read from GCP Secret Manager is not valid JSON."),
        ):
            get_credentials_and_project_id(key_secret_name="secret name")

    def test_get_credentials_and_project_id_with_mutually_exclusive_configuration(self):
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "The `keyfile_dict`, `key_path`, and `key_secret_name` fields are all mutually exclusive."
            ),
        ):
            get_credentials_and_project_id(key_path="KEY.json", keyfile_dict={"private_key": "PRIVATE_KEY"})

    @mock.patch("google.auth.default", return_value=("CREDENTIALS", "PROJECT_ID"))
    @mock.patch(
        "google.oauth2.service_account.Credentials.from_service_account_info",
    )
    @mock.patch(
        "google.oauth2.service_account.Credentials.from_service_account_file",
    )
    def test_disable_logging(self, mock_default, mock_info, mock_file, caplog):
        """Test disable logging in ``get_credentials_and_project_id``"""

        # assert no logs
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            get_credentials_and_project_id(disable_logging=True)
            assert not caplog.record_tuples

        # assert no debug logs emitted from get_credentials_and_project_id
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            get_credentials_and_project_id(
                keyfile_dict={"private_key": "PRIVATE_KEY"},
                disable_logging=True,
            )
            assert not caplog.record_tuples

        # assert no debug logs emitted from get_credentials_and_project_id
        with caplog.at_level(level=logging.DEBUG, logger=CRED_PROVIDER_LOGGER_NAME):
            caplog.clear()
            get_credentials_and_project_id(
                key_path="KEY.json",
                disable_logging=True,
            )
            assert not caplog.record_tuples


class TestGetScopes:
    def test_get_scopes_with_default(self):
        assert _get_scopes() == _DEFAULT_SCOPES

    @pytest.mark.parametrize(
        "scopes_str, scopes",
        [
            pytest.param("scope1", ["scope1"], id="single-scope"),
            pytest.param("scope1,scope2", ["scope1", "scope2"], id="multiple-scopes"),
        ],
    )
    def test_get_scopes_with_input(self, scopes_str, scopes):
        assert _get_scopes(scopes_str) == scopes


class TestGetTargetPrincipalAndDelegates:
    def test_get_target_principal_and_delegates_no_argument(self):
        assert _get_target_principal_and_delegates() == (None, None)

    @pytest.mark.parametrize(
        "impersonation_chain, target_principal_and_delegates",
        [
            pytest.param(ACCOUNT_1_SAME_PROJECT, (ACCOUNT_1_SAME_PROJECT, None), id="string"),
            pytest.param([], (None, None), id="empty-list"),
            pytest.param([ACCOUNT_1_SAME_PROJECT], (ACCOUNT_1_SAME_PROJECT, []), id="single-element-list"),
            pytest.param(
                [ACCOUNT_1_SAME_PROJECT, ACCOUNT_2_SAME_PROJECT, ACCOUNT_3_ANOTHER_PROJECT],
                (ACCOUNT_3_ANOTHER_PROJECT, [ACCOUNT_1_SAME_PROJECT, ACCOUNT_2_SAME_PROJECT]),
                id="multiple-elements-list",
            ),
        ],
    )
    def test_get_target_principal_and_delegates_with_input(
        self, impersonation_chain, target_principal_and_delegates
    ):
        assert _get_target_principal_and_delegates(impersonation_chain) == target_principal_and_delegates


class TestGetProjectIdFromServiceAccountEmail:
    def test_get_project_id_from_service_account_email(self):
        assert _get_project_id_from_service_account_email(ACCOUNT_3_ANOTHER_PROJECT) == ANOTHER_PROJECT_ID

    def test_get_project_id_from_service_account_email_wrong_input(self):
        with pytest.raises(AirflowException):
            _get_project_id_from_service_account_email("ACCOUNT_1")
