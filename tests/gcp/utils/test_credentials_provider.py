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
from uuid import uuid4

import mock
from google.auth.environment_vars import CREDENTIALS

from airflow.gcp.utils.credentials_provider import (
    AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT, build_gcp_conn, provide_gcp_conn_and_credentials,
    provide_gcp_connection, provide_gcp_credentials, temporary_environment_variable,
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


class TestTemporaryEnvironmentVariable(unittest.TestCase):
    @mock.patch.dict(os.environ, clear=True)
    def test_temporary_environment_variable_delete(self):
        with temporary_environment_variable(KEY, ENV_VALUE):
            self.assertEqual(os.environ.get(KEY), ENV_VALUE)
        self.assertNotIn(KEY, os.environ)

    @mock.patch.dict(os.environ, {KEY: ENV_VALUE})
    def test_temporary_environment_variable_restore(self):
        with temporary_environment_variable(KEY, TEMP_VARIABLE):
            self.assertEqual(os.environ.get(KEY), TEMP_VARIABLE)
        self.assertEqual(os.environ.get(KEY), ENV_VALUE)

    @mock.patch.dict(os.environ, clear=True)
    def test_temporary_environment_variable_error(self):
        with self.assertRaises(Exception):
            with temporary_environment_variable(KEY, ENV_VALUE):
                self.assertEqual(os.environ.get(KEY), ENV_VALUE)
                raise Exception("test")
            self.assertNotIn(KEY, os.environ)


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
    @mock.patch("airflow.gcp.utils.credentials_provider.build_gcp_conn")
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
    @mock.patch("airflow.gcp.utils.credentials_provider.build_gcp_conn")
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
