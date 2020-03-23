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


import re
import unittest

import mock
import six
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.contrib.utils.gcp_credentials_provider import (
    _DEFAULT_SCOPES, _get_scopes, get_credentials_and_project_id,
)


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
        with six.assertRaisesRegex(self, AirflowException, re.escape(
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
