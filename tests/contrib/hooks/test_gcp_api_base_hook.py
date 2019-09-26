# -*- coding: utf-8 -*-
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
#
import json
import os
import unittest
from io import StringIO

from parameterized import parameterized

import google.auth
from google.auth.environment_vars import CREDENTIALS
from google.auth.exceptions import GoogleAuthError
from google.api_core.exceptions import RetryError, AlreadyExists
from google.cloud.exceptions import MovedPermanently
from googleapiclient.errors import HttpError

from airflow import AirflowException, LoggingMixin
from airflow.contrib.hooks import gcp_api_base_hook as hook
from airflow.hooks.base_hook import BaseHook
from tests.compat import mock
from tests.gcp.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=hook._DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False

MODULE_NAME = "airflow.contrib.hooks.gcp_api_base_hook"


class TestCatchHttpException(unittest.TestCase):
    # pylint:disable=no-method-argument,unused-argument
    @parameterized.expand(
        [
            ("no_exception", None, LoggingMixin, None, None),
            ("raise_airflowexception", MovedPermanently("MESSAGE"), LoggingMixin, None, AirflowException),
            (
                "raise_airflowexception",
                RetryError("MESSAGE", cause=Exception("MESSAGE")),
                LoggingMixin, None, AirflowException
            ),
            ("raise_airflowexception", ValueError("MESSAGE"), LoggingMixin, None, AirflowException),
            ("raise_alreadyexists", AlreadyExists("MESSAGE"), LoggingMixin, None, AlreadyExists),
            (
                "raise_http_error",
                HttpError(mock.Mock(**{"reason.return_value": None}), b"CONTENT"),
                BaseHook, {"source": None}, AirflowException
            ),
        ]
    )
    def test_catch_exception(self, name, exception, base_class, base_class_args, assert_raised):
        self.called = False  # pylint:disable=attribute-defined-outside-init

        class FixtureClass(base_class):
            @hook.GoogleCloudBaseHook.catch_http_exception
            def test_fixture(*args, **kwargs):  # pylint:disable=unused-argument,no-method-argument
                self.called = True  # pylint:disable=attribute-defined-outside-init
                if exception is not None:
                    raise exception

        if assert_raised is None:
            FixtureClass(base_class_args).test_fixture()
        else:
            with self.assertRaises(assert_raised):
                FixtureClass(base_class_args).test_fixture()
        self.assertTrue(self.called)


class FallbackToDefaultProjectIdFixtureClass:
    def __init__(self, project_id):
        self.mock = mock.Mock()
        self.fixture_project_id = project_id

    @hook.GoogleCloudBaseHook.fallback_to_default_project_id
    def method(self, project_id=None):
        self.mock(project_id=project_id)

    @property
    def project_id(self):
        return self.fixture_project_id


class TestFallbackToDefaultProjectId(unittest.TestCase):

    def test_no_arguments(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method()

        gcp_hook.mock.assert_called_once_with(project_id=321)

    def test_default_project_id(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method(project_id=None)

        gcp_hook.mock.assert_called_once_with(project_id=321)

    def test_provided_project_id(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        gcp_hook.method(project_id=123)

        gcp_hook.mock.assert_called_once_with(project_id=123)

    def test_restrict_positional_arguments(self):
        gcp_hook = FallbackToDefaultProjectIdFixtureClass(321)

        with self.assertRaises(AirflowException) as cm:
            gcp_hook.method(123)

        self.assertEqual(
            str(cm.exception), "You must use keyword arguments in this methods rather than positional"
        )
        self.assertEqual(gcp_hook.mock.call_count, 0)


ENV_VALUE = "/tmp/a"


class TestProvideGcpCredentialFile(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            MODULE_NAME + '.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleCloudBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], file_name)
            self.assertEqual(file_content, string_file.getvalue())

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        assert_gcp_credential_file_in_env(self.instance)
        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with self.assertRaises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        assert_gcp_credential_file_in_env(self.instance)
        self.assertNotIn(CREDENTIALS, os.environ)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with self.assertRaises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        self.assertNotIn(CREDENTIALS, os.environ)


class TestProvideGcpCredentialFileAsContext(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleCloudBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            self.assertEqual(os.environ[CREDENTIALS], key_path)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {'extra__google_cloud_platform__keyfile_dict': file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with self.instance.provide_gcp_credential_file_as_context():
            self.assertEqual(os.environ[CREDENTIALS], file_name)
            self.assertEqual(file_content, string_file.getvalue())

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.assertRaises(Exception):
            with self.instance.provide_gcp_credential_file_as_context():
                raise Exception()

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        self.assertNotIn(CREDENTIALS, os.environ)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        with self.assertRaises(Exception):
            with self.instance.provide_gcp_credential_file_as_context():
                raise Exception()

        self.assertNotIn(CREDENTIALS, os.environ)


class TestGoogleCloudBaseHook(unittest.TestCase):
    def setUp(self):
        self.instance = hook.GoogleCloudBaseHook()

    @mock.patch(MODULE_NAME + '.google.auth.default', return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth(self, mock_auth_default):
        self.instance.extras = {}
        result = self.instance._get_credentials_and_project_id()
        mock_auth_default.assert_called_once_with(scopes=self.instance.scopes)
        self.assertEqual(('CREDENTIALS', 'PROJECT_ID'), result)

    @mock.patch(  # type: ignore
        MODULE_NAME + '.google.oauth2.service_account.Credentials.from_service_account_file',
        **{'return_value.project_id': "PROJECT_ID"}
    )
    def test_get_credentials_and_project_id_with_service_account_file(self, mock_from_service_account_file):
        self.instance.extras = {
            'extra__google_cloud_platform__key_path': "KEY_PATH.json"
        }
        result = self.instance._get_credentials_and_project_id()
        mock_from_service_account_file.assert_called_once_with('KEY_PATH.json', scopes=self.instance.scopes)
        self.assertEqual((mock_from_service_account_file.return_value, 'PROJECT_ID'), result)

    @mock.patch(MODULE_NAME + '.google.oauth2.service_account.Credentials.from_service_account_file')
    def test_get_credentials_and_project_id_with_service_account_file_and_p12_key(
        self,
        mock_from_service_account_file
    ):
        self.instance.extras = {
            'extra__google_cloud_platform__key_path': "KEY_PATH.p12"
        }
        with self.assertRaises(AirflowException):
            self.instance._get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + '.google.oauth2.service_account.Credentials.from_service_account_file')
    def test_get_credentials_and_project_id_with_service_account_file_and_unknown_key(
        self,
        mock_from_service_account_file
    ):
        self.instance.extras = {
            'extra__google_cloud_platform__key_path': "KEY_PATH.unknown"
        }
        with self.assertRaises(AirflowException):
            self.instance._get_credentials_and_project_id()

    @mock.patch(  # type: ignore
        MODULE_NAME + '.google.oauth2.service_account.Credentials.from_service_account_info',
        **{'return_value.project_id': "PROJECT_ID"}
    )
    def test_get_credentials_and_project_id_with_service_account_info(self, mock_from_service_account_file):
        service_account = {
            'private_key': "PRIVATE_KEY"
        }
        self.instance.extras = {
            'extra__google_cloud_platform__keyfile_dict': json.dumps(service_account)
        }
        result = self.instance._get_credentials_and_project_id()
        mock_from_service_account_file.assert_called_once_with(service_account, scopes=self.instance.scopes)
        self.assertEqual((mock_from_service_account_file.return_value, 'PROJECT_ID'), result)

    @mock.patch(MODULE_NAME + '.google.auth.default')
    def test_get_credentials_and_project_id_with_default_auth_and_delegate(self, mock_auth_default):
        mock_credentials = mock.MagicMock()
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")
        self.instance.extras = {}
        self.instance.delegate_to = "USER"
        result = self.instance._get_credentials_and_project_id()
        mock_auth_default.assert_called_once_with(scopes=self.instance.scopes)
        mock_credentials.with_subject.assert_called_once_with("USER")
        self.assertEqual((mock_credentials.with_subject.return_value, "PROJECT_ID"), result)

    @mock.patch(  # type: ignore
        MODULE_NAME + '.google.auth.default',
        return_value=("CREDENTIALS", "PROJECT_ID")
    )
    def test_get_credentials_and_project_id_with_default_auth_and_overridden_project_id(
        self,
        mock_auth_default
    ):
        self.instance.extras = {
            'extra__google_cloud_platform__project': "SECOND_PROJECT_ID"
        }
        result = self.instance._get_credentials_and_project_id()
        mock_auth_default.assert_called_once_with(scopes=self.instance.scopes)
        self.assertEqual(("CREDENTIALS", 'SECOND_PROJECT_ID'), result)

    @unittest.skipIf(not default_creds_available, 'Default GCP credentials not available to run tests')
    def test_default_creds_with_scopes(self):
        self.instance.extras = {
            'extra__google_cloud_platform__project': default_project,
            'extra__google_cloud_platform__scope': (
                ','.join(
                    (
                        'https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/devstorage.read_only',
                    )
                )
            ),
        }

        credentials = self.instance._get_credentials()

        if not hasattr(credentials, 'scopes') or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        self.assertIn('https://www.googleapis.com/auth/bigquery', scopes)
        self.assertIn(
            'https://www.googleapis.com/auth/devstorage.read_only', scopes)

    @unittest.skipIf(
        not default_creds_available,
        'Default GCP credentials not available to run tests')
    def test_default_creds_no_scopes(self):
        self.instance.extras = {
            'extra__google_cloud_platform__project': default_project
        }

        credentials = self.instance._get_credentials()

        if not hasattr(credentials, 'scopes') or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        self.assertEqual(tuple(hook._DEFAULT_SCOPES), tuple(scopes))

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):  # pylint:disable=unused-argument
            self.assertEqual(os.environ[CREDENTIALS],
                             key_path)

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch('tempfile.NamedTemporaryFile')
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = '/test/mock-file'
        self.instance.extras = {
            'extra__google_cloud_platform__keyfile_dict': file_content
        }
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleCloudBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):  # pylint:disable=unused-argument
            self.assertEqual(os.environ[CREDENTIALS],
                             file_name)
            self.assertEqual(file_content, string_file.getvalue())

        assert_gcp_credential_file_in_env(self.instance)

    def test_provided_scopes(self):
        self.instance.extras = {
            'extra__google_cloud_platform__project': default_project,
            'extra__google_cloud_platform__scope': (
                ','.join(
                    (
                        'https://www.googleapis.com/auth/bigquery',
                        'https://www.googleapis.com/auth/devstorage.read_only',
                    )
                )
            ),
        }

        self.assertEqual(
            self.instance.scopes,
            [
                'https://www.googleapis.com/auth/bigquery',
                'https://www.googleapis.com/auth/devstorage.read_only',
            ],
        )

    def test_default_scopes(self):
        self.instance.extras = {'extra__google_cloud_platform__project': default_project}

        self.assertEqual(self.instance.scopes, ('https://www.googleapis.com/auth/cloud-platform',))

    @mock.patch("airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.get_connection")
    def test_num_retries_is_not_none_by_default(self, get_con_mock):
        """
        Verify that if 'num_retires' in extras is not set, the default value
        should not be None
        """
        get_con_mock.return_value.extra_dejson = {
            "extra__google_cloud_platform__num_retries": None
        }
        self.assertEqual(self.instance.num_retries, 5)
