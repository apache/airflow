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
import unittest
from io import StringIO

from google.auth.environment_vars import CREDENTIALS

from airflow import AirflowException
from airflow.gcp.utils.credentials_provider import assert_not_legacy_key, provide_gcp_credentials
from tests.compat import mock

ENV_VALUE = "test_env"


class TestCredentialsProvider(unittest.TestCase):
    def test_assert_not_legacy_key(self):
        with self.assertRaises(AirflowException) as err:
            assert_not_legacy_key("old_key_path.p12")
        self.assertIn("P12 key file", str(err.exception))

    def test_provide_gcp_credential_key_path(self):
        key_path = '/test/key-path'
        with provide_gcp_credentials(key_file_path=key_path):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_key_content(self, mock_file):
        file_dict = {"foo": "bar"}
        string_file = StringIO()
        file_content = json.dumps(file_dict)
        file_name = '/test/mock-file'
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with provide_gcp_credentials(key_file_dict=file_dict):
            self.assertEqual(os.environ[CREDENTIALS], file_name)
            self.assertEqual(file_content, string_file.getvalue())

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'

        with provide_gcp_credentials(key_file_path=key_path):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'

        with self.assertRaises(Exception):
            with provide_gcp_credentials(key_file_path=key_path):
                raise Exception()

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'

        with provide_gcp_credentials(key_file_path=key_path):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        self.assertNotIn(CREDENTIALS, os.environ)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'

        with self.assertRaises(Exception):
            with provide_gcp_credentials(key_file_path=key_path):
                raise Exception()

        self.assertNotIn(CREDENTIALS, os.environ)
