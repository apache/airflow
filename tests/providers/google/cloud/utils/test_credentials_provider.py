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

import json
import os
import re
import unittest
from io import StringIO
from uuid import uuid4

import mock
from google.auth.environment_vars import CREDENTIALS
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.utils.credentials_provider import (
    _DEFAULT_SCOPES, AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT, _get_scopes, build_gcp_conn,
    get_credentials_and_project_id, provide_gcp_conn_and_credentials, provide_gcp_connection,
    provide_gcp_credentials,
)

ENV_VALUE = "test_env"
TEMP_VARIABLE = "temp_variable"
KEY = str(uuid4())
ENV_CRED = "temp_cred"


class TestHelper(unittest.TestCase):
    def test_build_gcp_conn_path(self):
        value = "test"
        conn = build_gcp_conn(key_file_path=value)
        self.assertEqual(
            "google-cloud-platform://?extra__google_cloud_platform__key_path=test", conn
        )

    def test_build_gcp_conn_scopes(self):
        value = ["test", "test2"]
        conn = build_gcp_conn(scopes=value)
        self.assertEqual(
            "google-cloud-platform://?extra__google_cloud_platform__scope=test%2Ctest2",
            conn,
        )

    def test_build_gcp_conn_project(self):
        value = "test"
        conn = build_gcp_conn(project_id=value)
        self.assertEqual(
            "google-cloud-platform://?extra__google_cloud_platform__projects=test", conn
        )


class TestProvideGcpCredentials(unittest.TestCase):
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
            self.assertEqual(os.environ[CREDENTIALS], file_name)
            self.assertEqual(file_content, string_file.getvalue())
        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credentials_keep_environment(self):
        key_path = "/test/key-path"
        with provide_gcp_credentials(key_file_path=key_path):
            self.assertEqual(os.environ[CREDENTIALS], key_path)
        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)


class TestProvideGcpConnection(unittest.TestCase):
    @mock.patch.dict(os.environ, {AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: ENV_VALUE})
    @mock.patch("airflow.providers.google.cloud.utils.credentials_provider.build_gcp_conn")
    def test_provide_gcp_connection(self, mock_builder):
        mock_builder.return_value = TEMP_VARIABLE
        path = "path/to/file.json"
        scopes = ["scopes"]
        project_id = "project_id"
        with provide_gcp_connection(path, scopes, project_id):
            mock_builder.assert_called_once_with(
                key_file_path=path, scopes=scopes, project_id=project_id
            )
            self.assertEqual(
                os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT], TEMP_VARIABLE
            )
        self.assertEqual(os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT], ENV_VALUE)


class TestProvideGcpConnAndCredentials(unittest.TestCase):
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
            mock_builder.assert_called_once_with(
                key_file_path=path, scopes=scopes, project_id=project_id
            )
            self.assertEqual(
                os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT], TEMP_VARIABLE
            )
            self.assertEqual(os.environ[CREDENTIALS], path)
        self.assertEqual(os.environ[AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT], ENV_VALUE)
        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)


class TestGetGcpCredentialsAndProjectId(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_scopes = _DEFAULT_SCOPES
        cls.test_key_file = "KEY_PATH.json"
        cls.test_project_id = "project_id"

    @mock.patch("google.auth.default", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth(self, mock_auth_default):
        result = get_credentials_and_project_id()
        mock_auth_default.assert_called_once_with(scopes=None)
        self.assertEqual(("CREDENTIALS", "PROJECT_ID"), result)

    @mock.patch('google.auth.default')
    def test_get_credentials_and_project_id_with_default_auth_and_delegate(self, mock_auth_default):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(delegate_to="USER")
        mock_auth_default.assert_called_once_with(scopes=None)
        mock_credentials.with_subject.assert_called_once_with("USER")
        self.assertEqual((mock_credentials.with_subject.return_value, self.test_project_id), result)

    @parameterized.expand([
        (['scope1'], ),
        (['scope1', 'scope2'], )
    ])
    @mock.patch('google.auth.default')
    def test_get_credentials_and_project_id_with_default_auth_and_scopes(self, scopes, mock_auth_default):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, self.test_project_id)

        result = get_credentials_and_project_id(scopes=scopes)
        mock_auth_default.assert_called_once_with(scopes=scopes)
        self.assertEqual(mock_auth_default.return_value, result)

    @mock.patch(
        'google.oauth2.service_account.Credentials.from_service_account_file',
    )
    def test_get_credentials_and_project_id_with_service_account_file(self, mock_from_service_account_file):
        mock_from_service_account_file.return_value.project_id = self.test_project_id
        result = get_credentials_and_project_id(key_path=self.test_key_file)
        mock_from_service_account_file.assert_called_once_with(self.test_key_file, scopes=None)
        self.assertEqual((mock_from_service_account_file.return_value, self.test_project_id), result)

    @parameterized.expand([
        ("p12", "path/to/file.p12"),
        ("unknown", "incorrect_file.ext")
    ])
    def test_get_credentials_and_project_id_with_service_account_file_and_non_valid_key(self, _, file):
        with self.assertRaises(AirflowException):
            get_credentials_and_project_id(key_path=file)

    @mock.patch(
        'google.oauth2.service_account.Credentials.from_service_account_info',
    )
    def test_get_credentials_and_project_id_with_service_account_info(self, mock_from_service_account_info):
        mock_from_service_account_info.return_value.project_id = self.test_project_id
        service_account = {
            'private_key': "PRIVATE_KEY"
        }
        result = get_credentials_and_project_id(keyfile_dict=service_account)
        mock_from_service_account_info.assert_called_once_with(service_account, scopes=None)
        self.assertEqual((mock_from_service_account_info.return_value, self.test_project_id), result)

    def test_get_credentials_and_project_id_with_mutually_exclusive_configuration(
        self,
    ):
        with self.assertRaisesRegex(AirflowException, re.escape(
            'The `keyfile_dict` and `key_path` fields are mutually exclusive.'
        )):
            get_credentials_and_project_id(key_path='KEY.json', keyfile_dict={'private_key': 'PRIVATE_KEY'})


class TestGetScopes(unittest.TestCase):

    def test_get_scopes_with_default(self):
        self.assertEqual(_get_scopes(), _DEFAULT_SCOPES)

    @parameterized.expand([
        ('single_scope', 'scope1', ['scope1']),
        ('multiple_scopes', 'scope1,scope2', ['scope1', 'scope2']),
    ])
    def test_get_scopes_with_input(self, _, scopes_str, scopes):
        self.assertEqual(_get_scopes(scopes_str), scopes)
