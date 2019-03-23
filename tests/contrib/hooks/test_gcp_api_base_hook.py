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

from parameterized import parameterized
from google.api_core.exceptions import RetryError, AlreadyExists
from google.cloud.exceptions import MovedPermanently

from airflow import AirflowException, LoggingMixin
from googleapiclient.errors import HttpError

from airflow.contrib.hooks import gcp_api_base_hook as hook

import google.auth
from google.auth.exceptions import GoogleAuthError

from airflow.hooks.base_hook import BaseHook

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=hook._DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False


class TestCatchHttpException(unittest.TestCase):
    def test_no_exception(self):
        self.called = False

        class FixtureClass(LoggingMixin):
            @hook.GoogleCloudBaseHook.catch_http_exception
            def text_fixture(*args, **kwargs):
                self.called = True

        FixtureClass().text_fixture()

        self.assertTrue(self.called)

    @parameterized.expand(
        [
            (MovedPermanently("MESSAGE"),),
            (RetryError("MESSAGE", cause=Exception("MESSAGE")),),
            (ValueError("MESSAGE"),),
        ]
    )
    def test_raise_airflowexception(self, ex_obj):
        self.called = False

        class FixtureClass(LoggingMixin):
            @hook.GoogleCloudBaseHook.catch_http_exception
            def test_fixutre(*args, **kwargs):
                self.called = True
                raise ex_obj

        with self.assertRaises(AirflowException):
            FixtureClass().test_fixutre()

        self.assertTrue(self.called)

    def test_raise_alreadyexists(self):
        self.called = False

        class FixtureClass(LoggingMixin):
            @hook.GoogleCloudBaseHook.catch_http_exception
            def test_fixutre(*args, **kwargs):
                self.called = True
                raise AlreadyExists("MESSAGE")

        with self.assertRaises(AlreadyExists):
            FixtureClass().test_fixutre()

        self.assertTrue(self.called)

    def test_raise_http_error(self):
        self.called = False

        class FixtureClass(BaseHook):
            @hook.GoogleCloudBaseHook.catch_http_exception
            def test_fixtue(*args, **kwargs):
                self.called = True
                raise HttpError(mock.Mock(**{"reason.return_value": None}), b"CONTENT")

        with self.assertRaises(AirflowException):
            FixtureClass(None).test_fixtue()

        self.assertTrue(self.called)


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

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            self.assertEqual(os.environ[hook._G_APP_CRED_ENV_VAR],
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

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            self.assertEqual(os.environ[hook._G_APP_CRED_ENV_VAR],
                             file_name)
            self.assertEqual(file_content, string_file.getvalue())

        assert_gcp_credential_file_in_env(self.instance)
