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

import six
import tenacity
from parameterized import parameterized
from google.api_core.exceptions import RetryError, AlreadyExists
from google.auth.environment_vars import CREDENTIALS
from google.cloud.exceptions import MovedPermanently, Forbidden
from googleapiclient.errors import HttpError

from airflow import AirflowException, LoggingMixin, version
from airflow.contrib.hooks import gcp_api_base_hook as hook

import google.auth
from google.auth.exceptions import GoogleAuthError

from airflow.hooks.base_hook import BaseHook
from tests.compat import mock

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO


default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=hook._DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False


class NoForbiddenAfterCount:
    """Holds counter state for invoking a method several times in a row."""

    def __init__(self, count, **kwargs):
        self.counter = 0
        self.count = count
        self.kwargs = kwargs

    def __call__(self):
        """
        Raise an Forbidden until after count threshold has been crossed.
        Then return True.
        """
        if self.counter < self.count:
            self.counter += 1
            raise Forbidden(**self.kwargs)
        return True


@hook.GoogleCloudBaseHook.quota_retry(wait=tenacity.wait_none())
def _retryable_test_with_temporary_quota_retry(thing):
    return thing()


class QuotaRetryTestCase(unittest.TestCase):  # ptlint: disable=invalid-name
    def test_do_nothing_on_non_error(self):
        result = _retryable_test_with_temporary_quota_retry(lambda: 42)
        self.assertTrue(result, 42)

    def test_retry_on_exception(self):
        message = "POST https://translation.googleapis.com/language/translate/v2: User Rate Limit Exceeded"
        errors = [
            {
                'message': 'User Rate Limit Exceeded',
                'domain': 'usageLimits',
                'reason': 'userRateLimitExceeded',
            }
        ]
        custom_fn = NoForbiddenAfterCount(
            count=5,
            message=message,
            errors=errors
        )
        _retryable_test_with_temporary_quota_retry(custom_fn)
        self.assertEqual(5, custom_fn.counter)

    def test_raise_exception_on_non_quota_exception(self):
        with six.assertRaisesRegex(self, Forbidden, "Daily Limit Exceeded"):
            message = "POST https://translation.googleapis.com/language/translate/v2: Daily Limit Exceeded"
            errors = [
                {'message': 'Daily Limit Exceeded', 'domain': 'usageLimits', 'reason': 'dailyLimitExceeded'}
            ]

            _retryable_test_with_temporary_quota_retry(
                NoForbiddenAfterCount(5, message=message, errors=errors)
            )


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


ENV_VALUE = "/tmp/a"


@unittest.skipIf(not default_creds_available, 'Default GCP credentials not available to run tests')
class ProvideGcpCredentialFileTestCase(unittest.TestCase):
    def setUp(self):
        self.instance = hook.GoogleCloudBaseHook()

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
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

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], file_name)
            self.assertEqual(file_content, string_file.getvalue())

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        assert_gcp_credential_file_in_env(self.instance)
        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with self.assertRaises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        self.assertEqual(os.environ[CREDENTIALS], ENV_VALUE)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            self.assertEqual(os.environ[CREDENTIALS], key_path)

        assert_gcp_credential_file_in_env(self.instance)
        self.assertNotIn(CREDENTIALS, os.environ)

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = '/test/key-path'
        self.instance.extras = {'extra__google_cloud_platform__key_path': key_path}

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise Exception()

        with self.assertRaises(Exception):
            assert_gcp_credential_file_in_env(self.instance)

        self.assertNotIn(CREDENTIALS, os.environ)


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

        @hook.GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            self.assertEqual(os.environ[CREDENTIALS],
                             file_name)
            self.assertEqual(file_content, string_file.getvalue())

        assert_gcp_credential_file_in_env(self.instance)

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

    @mock.patch("airflow.contrib.hooks.gcp_api_base_hook.httplib2.Http")
    @mock.patch("airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook._get_credentials")
    def test_authorize_assert_user_agent_is_sent(self, mock_get_credentials, mock_http):
        """
        Verify that if 'num_retires' in extras is not set, the default value
        should not be None
        """
        request = mock_http.return_value.request
        response = mock.MagicMock(status_code=200)
        content = "CONTENT"
        mock_http.return_value.request.return_value = response, content

        new_response, new_content = self.instance._authorize().request("/test-action")

        request.assert_called_once_with(
            '/test-action',
            body=None,
            connection_type=None,
            headers={'user-agent': 'airflow/' + version.version},
            method='GET',
            redirections=5
        )
        self.assertEqual(response, new_response)
        self.assertEqual(content, new_content)
