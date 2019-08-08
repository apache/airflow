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


default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=hook._DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False


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


class TestGoogleCloudBaseHook(unittest.TestCase):
    def setUp(self):
        self.instance = hook.GoogleCloudBaseHook()

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
