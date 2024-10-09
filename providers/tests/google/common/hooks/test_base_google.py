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
from __future__ import annotations

import json
import os
import re
from io import StringIO
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import google.auth
import google.auth.compute_engine
import pytest
import tenacity
from google.auth.environment_vars import CREDENTIALS
from google.auth.exceptions import GoogleAuthError, RefreshError
from google.cloud.exceptions import Forbidden

from airflow import version
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.utils.credentials_provider import _DEFAULT_SCOPES
from airflow.providers.google.common.hooks import base_google as hook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook, is_refresh_credentials_exception

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

default_creds_available = True
default_project = None
try:
    _, default_project = google.auth.default(scopes=_DEFAULT_SCOPES)
except GoogleAuthError:
    default_creds_available = False

MODULE_NAME = "airflow.providers.google.common.hooks.base_google"
PROJECT_ID = "PROJECT_ID"
ENV_VALUE = "/tmp/a"
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


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


@hook.GoogleBaseHook.quota_retry(wait=tenacity.wait_none())
def _retryable_test_with_temporary_quota_retry(thing):
    return thing()


class TestQuotaRetry:
    def test_do_nothing_on_non_error(self):
        result = _retryable_test_with_temporary_quota_retry(lambda: 42)
        assert result, 42

    def test_retry_on_exception(self):
        message = "POST https://translation.googleapis.com/language/translate/v2: User Rate Limit Exceeded"
        errors = [mock.MagicMock(details=mock.PropertyMock(return_value="userRateLimitExceeded"))]
        custom_fn = NoForbiddenAfterCount(count=5, message=message, errors=errors)
        _retryable_test_with_temporary_quota_retry(custom_fn)
        assert 5 == custom_fn.counter

    def test_raise_exception_on_non_quota_exception(self):
        message = "POST https://translation.googleapis.com/language/translate/v2: Daily Limit Exceeded"
        errors = [mock.MagicMock(details=mock.PropertyMock(return_value="dailyLimitExceeded"))]
        with pytest.raises(Forbidden, match="Daily Limit Exceeded"):
            _retryable_test_with_temporary_quota_retry(
                NoForbiddenAfterCount(5, message=message, errors=errors)
            )


class TestRefreshCredentialsRetry:
    @pytest.mark.parametrize(
        "exc, retryable",
        [
            (RefreshError("Other error", "test body"), False),
            (RefreshError("Unable to acquire impersonated credentials", "test body"), True),
            (ValueError(), False),
        ],
    )
    def test_is_refresh_credentials_exception(self, exc, retryable):
        assert is_refresh_credentials_exception(exc) is retryable

    def test_do_nothing_on_non_error(self):
        @hook.GoogleBaseHook.refresh_credentials_retry()
        def func():
            return 42

        assert func() == 42

    def test_raise_non_refresh_error(self):
        @hook.GoogleBaseHook.refresh_credentials_retry()
        def func():
            raise ValueError()

        with pytest.raises(ValueError):
            func()

    @mock.patch("tenacity.nap.time.sleep", mock.MagicMock())
    def test_retry_on_refresh_error(self):
        func_return = mock.Mock(
            side_effect=[RefreshError("Unable to acquire impersonated credentials", "test body"), 42]
        )

        @hook.GoogleBaseHook.refresh_credentials_retry()
        def func():
            return func_return()

        assert func() == 42


class FallbackToDefaultProjectIdFixtureClass:
    def __init__(self, project_id):
        self.mock = mock.Mock()
        self.fixture_project_id = project_id

    @hook.GoogleBaseHook.fallback_to_default_project_id
    def method(self, project_id=None):
        self.mock(project_id=project_id)

    @property
    def project_id(self):
        return self.fixture_project_id


class TestFallbackToDefaultProjectId:
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

        with pytest.raises(AirflowException) as ctx:
            gcp_hook.method(123)

        assert str(ctx.value) == "You must use keyword arguments in this methods rather than positional"
        assert gcp_hook.mock.call_count == 0


class TestProvideGcpCredentialFile:
    def setup_method(self):
        with mock.patch(
            MODULE_NAME + ".GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path_and_keyfile_dict(self):
        key_path = "/test/key-path"
        self.instance.extras = {
            "key_path": key_path,
            "keyfile_dict": '{"foo": "bar"}',
        }

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        with pytest.raises(
            AirflowException,
            match="The `keyfile_dict` and `key_path` fields are mutually exclusive. "
            "Please provide only one value.",
        ):
            assert_gcp_credential_file_in_env(self.instance)

    def test_provide_gcp_credential_keyfile_dict_json(self):
        """
        Historically, keyfile_dict had to be str in the conn extra.  Now it
        can be dict and this is verified here.
        """
        conn_dict = {
            "extra": {
                "keyfile_dict": {"foo": "bar", "private_key": "hi"},  # notice keyfile_dict is dict not str
            }
        }

        @GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(instance):
            assert Path(os.environ[CREDENTIALS]).read_text() == json.dumps(conn_dict["extra"]["keyfile_dict"])

        with patch.dict("os.environ", AIRFLOW_CONN_MY_GOOGLE=json.dumps(conn_dict)):
            # keyfile dict is handled in two different areas

            hook = GoogleBaseHook("my_google")

            # the first is in provide_gcp_credential_file
            assert_gcp_credential_file_in_env(hook)

            with patch("google.oauth2.service_account.Credentials.from_service_account_info") as m:
                # the second is in get_credentials_and_project_id
                hook.get_credentials_and_project_id()
                m.assert_called_once_with(
                    conn_dict["extra"]["keyfile_dict"],
                    scopes=("https://www.googleapis.com/auth/cloud-platform",),
                )

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = "/test/mock-file"
        self.instance.extras = {"keyfile_dict": file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)
        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise RuntimeError("Some exception occurred")

        with pytest.raises(RuntimeError, match="Some exception occurred"):
            assert_gcp_credential_file_in_env(self.instance)

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)
        assert CREDENTIALS not in os.environ

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(_):
            raise RuntimeError("Some exception occurred")

        with pytest.raises(RuntimeError, match="Some exception occurred"):
            assert_gcp_credential_file_in_env(self.instance)

        assert CREDENTIALS not in os.environ


class TestProvideGcpCredentialFileAsContext:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = "/test/mock-file"
        self.instance.extras = {"keyfile_dict": file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, {CREDENTIALS: ENV_VALUE})
    def test_provide_gcp_credential_keep_environment_when_exception(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with pytest.raises(RuntimeError, match="Some exception occurred"):
            with self.instance.provide_gcp_credential_file_as_context():
                raise RuntimeError("Some exception occurred")

        assert os.environ[CREDENTIALS] == ENV_VALUE

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with self.instance.provide_gcp_credential_file_as_context():
            assert os.environ[CREDENTIALS] == key_path

        assert CREDENTIALS not in os.environ

    @mock.patch.dict(os.environ, clear=True)
    def test_provide_gcp_credential_keep_clear_environment_when_exception(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with pytest.raises(RuntimeError, match="Some exception occurred"):
            with self.instance.provide_gcp_credential_file_as_context():
                raise RuntimeError("Some exception occurred")

        assert CREDENTIALS not in os.environ


@pytest.mark.db_test
class TestGoogleBaseHook:
    def setup_method(self):
        self.instance = hook.GoogleBaseHook()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth(self, mock_get_creds_and_proj_id):
        self.instance.extras = {}
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert ("CREDENTIALS", "PROJECT_ID") == result

    @mock.patch("requests.post")
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_connection_success(self, mock_get_creds_and_proj_id, requests_post):
        requests_post.return_value.status_code = 200
        credentials = mock.MagicMock()
        type(credentials).token = mock.PropertyMock(return_value="TOKEN")
        mock_get_creds_and_proj_id.return_value = (credentials, "PROJECT_ID")
        self.instance.extras = {}
        result = self.instance.test_connection()
        assert result == (True, "Connection successfully tested")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_connection_failure(self, mock_get_creds_and_proj_id):
        mock_get_creds_and_proj_id.side_effect = AirflowException("Invalid key JSON.")
        self.instance.extras = {}
        result = self.instance.test_connection()
        assert result == (False, "Invalid key JSON.")

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_get_credentials_and_project_id_with_service_account_file(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        self.instance.extras = {"key_path": "KEY_PATH.json"}
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path="KEY_PATH.json",
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert (mock_credentials, "PROJECT_ID") == result

    def test_get_credentials_and_project_id_with_service_account_file_and_p12_key(self):
        self.instance.extras = {"key_path": "KEY_PATH.p12"}
        with pytest.raises(AirflowException):
            self.instance.get_credentials_and_project_id()

    def test_get_credentials_and_project_id_with_service_account_file_and_unknown_key(self):
        self.instance.extras = {"key_path": "KEY_PATH.unknown"}
        with pytest.raises(AirflowException):
            self.instance.get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_get_credentials_and_project_id_with_service_account_info(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        service_account = {"private_key": "PRIVATE_KEY"}
        self.instance.extras = {"keyfile_dict": json.dumps(service_account)}
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=service_account,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert (mock_credentials, "PROJECT_ID") == result

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_get_credentials_and_project_id_with_default_auth_and_delegate(self, mock_get_creds_and_proj_id):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, "PROJECT_ID")
        self.instance.extras = {}
        self.instance.delegate_to = "USER"
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to="USER",
            target_principal=None,
            delegates=None,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert (mock_credentials, "PROJECT_ID") == result

    @mock.patch("google.auth.default")
    def test_get_credentials_and_project_id_with_default_auth_and_unsupported_delegate(
        self, mock_auth_default
    ):
        self.instance.delegate_to = "TEST_DELEGATE_TO"
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")

        with pytest.raises(
            AirflowException,
            match=re.escape(
                "The `delegate_to` parameter cannot be used here as the current authentication method "
                "does not support account impersonate. Please use service-account for authorization."
            ),
        ):
            self.instance.get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id", return_value=("CREDENTIALS", "PROJECT_ID"))
    def test_get_credentials_and_project_id_with_default_auth_and_overridden_project_id(
        self, mock_get_creds_and_proj_id
    ):
        self.instance.extras = {"project": "SECOND_PROJECT_ID"}
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert ("CREDENTIALS", "SECOND_PROJECT_ID") == result

    def test_get_credentials_and_project_id_with_mutually_exclusive_configuration(self):
        self.instance.extras = {
            "project": "PROJECT_ID",
            "key_path": "KEY_PATH",
            "keyfile_dict": '{"KEY": "VALUE"}',
        }
        with pytest.raises(AirflowException, match="mutually exclusive"):
            self.instance.get_credentials_and_project_id()

    def test_get_credentials_and_project_id_with_invalid_keyfile_dict(self):
        self.instance.extras = {
            "keyfile_dict": "INVALID_DICT",
        }
        with pytest.raises(AirflowException, match=re.escape("Invalid key JSON.")):
            self.instance.get_credentials_and_project_id()

    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id", return_value=("CREDENTIALS", ""))
    def test_get_credentials_and_project_id_with_is_anonymous(self, mock_get_creds_and_proj_id):
        self.instance.extras = {
            "is_anonymous": True,
        }
        self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=None,
            delegates=None,
            is_anonymous=True,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )

    @pytest.mark.skipif(
        not default_creds_available, reason="Default Google Cloud credentials not available to run tests"
    )
    def test_default_creds_with_scopes(self):
        self.instance.extras = {
            "project": default_project,
            "scope": (
                "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/devstorage.read_only"
            ),
        }

        credentials = self.instance.get_credentials()

        if not hasattr(credentials, "scopes") or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        assert "https://www.googleapis.com/auth/bigquery" in scopes
        assert "https://www.googleapis.com/auth/devstorage.read_only" in scopes

    @pytest.mark.skipif(
        not default_creds_available, reason="Default Google Cloud credentials not available to run tests"
    )
    def test_default_creds_no_scopes(self):
        self.instance.extras = {"project": default_project}

        credentials = self.instance.get_credentials()

        if not hasattr(credentials, "scopes") or credentials.scopes is None:
            # Some default credentials don't have any scopes associated with
            # them, and that's okay.
            return

        scopes = credentials.scopes
        assert tuple(_DEFAULT_SCOPES) == tuple(scopes)

    def test_provide_gcp_credential_file_decorator_key_path(self):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            assert os.environ[CREDENTIALS] == key_path

        assert_gcp_credential_file_in_env(self.instance)

    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_gcp_credential_file_decorator_key_content(self, mock_file):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = "/test/mock-file"
        self.instance.extras = {"keyfile_dict": file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        @hook.GoogleBaseHook.provide_gcp_credential_file
        def assert_gcp_credential_file_in_env(hook_instance):
            assert os.environ[CREDENTIALS] == file_name
            assert file_content == string_file.getvalue()

        assert_gcp_credential_file_in_env(self.instance)

    def test_provided_scopes(self):
        self.instance.extras = {
            "project": default_project,
            "scope": (
                "https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/devstorage.read_only"
            ),
        }

        assert self.instance.scopes == [
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_only",
        ]

    def test_default_scopes(self):
        self.instance.extras = {"project": default_project}

        assert self.instance.scopes == ("https://www.googleapis.com/auth/cloud-platform",)

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection")
    def test_num_retries_is_not_none_by_default(self, get_con_mock):
        """
        Verify that if 'num_retries' in extras is not set, the default value
        should not be None
        """
        get_con_mock.return_value.extra_dejson = {"num_retries": None}
        assert self.instance.num_retries == 5

    @mock.patch("airflow.providers.google.common.hooks.base_google.build_http")
    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials")
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
            "/test-action",
            body=None,
            connection_type=None,
            headers={"user-agent": "airflow/" + version.version},
            method="GET",
            redirections=5,
        )
        assert response == new_response
        assert content == new_content

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials")
    def test_authorize_assert_http_308_is_excluded(self, mock_get_credentials):
        """
        Verify that 308 status code is excluded from httplib2's redirect codes
        """
        http_authorized = self.instance._authorize().http
        assert 308 not in http_authorized.redirect_codes

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials")
    def test_authorize_assert_http_timeout_is_present(self, mock_get_credentials):
        """
        Verify that http client has a timeout set
        """
        http_authorized = self.instance._authorize().http
        assert http_authorized.timeout is not None

    @pytest.mark.parametrize(
        "impersonation_chain, impersonation_chain_from_conn, target_principal, delegates",
        [
            pytest.param("ACCOUNT_1", None, "ACCOUNT_1", None, id="string"),
            pytest.param(None, "ACCOUNT_1", "ACCOUNT_1", None, id="string_in_conn"),
            pytest.param("ACCOUNT_2", "ACCOUNT_1", "ACCOUNT_2", None, id="string_with_override"),
            pytest.param(["ACCOUNT_1"], None, "ACCOUNT_1", [], id="single_element_list"),
            pytest.param(None, ["ACCOUNT_1"], "ACCOUNT_1", [], id="single_element_list_in_conn"),
            pytest.param(
                ["ACCOUNT_1"], ["ACCOUNT_2"], "ACCOUNT_1", [], id="single_element_list_with_override"
            ),
            pytest.param(
                ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"],
                None,
                "ACCOUNT_3",
                ["ACCOUNT_1", "ACCOUNT_2"],
                id="multiple_elements_list",
            ),
            pytest.param(
                None,
                ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"],
                "ACCOUNT_3",
                ["ACCOUNT_1", "ACCOUNT_2"],
                id="multiple_elements_list_in_conn",
            ),
            pytest.param(
                ["ACCOUNT_2", "ACCOUNT_3", "ACCOUNT_4"],
                ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"],
                "ACCOUNT_4",
                ["ACCOUNT_2", "ACCOUNT_3"],
                id="multiple_elements_list_with_override",
            ),
            pytest.param(
                None,
                "ACCOUNT_1,ACCOUNT_2,ACCOUNT_3",
                "ACCOUNT_3",
                ["ACCOUNT_1", "ACCOUNT_2"],
                id="multiple_elements_list_as_string",
            ),
            pytest.param(
                None,
                "ACCOUNT_1, ACCOUNT_2, ACCOUNT_3",
                "ACCOUNT_3",
                ["ACCOUNT_1", "ACCOUNT_2"],
                id="multiple_elements_list_as_string_with_space",
            ),
        ],
    )
    @mock.patch(MODULE_NAME + ".get_credentials_and_project_id")
    def test_get_credentials_and_project_id_with_impersonation_chain(
        self,
        mock_get_creds_and_proj_id,
        impersonation_chain,
        impersonation_chain_from_conn,
        target_principal,
        delegates,
    ):
        mock_credentials = mock.MagicMock()
        mock_get_creds_and_proj_id.return_value = (mock_credentials, PROJECT_ID)
        self.instance.impersonation_chain = impersonation_chain
        self.instance.extras = {"impersonation_chain": impersonation_chain_from_conn}
        result = self.instance.get_credentials_and_project_id()
        mock_get_creds_and_proj_id.assert_called_once_with(
            key_path=None,
            keyfile_dict=None,
            credential_config_file=None,
            key_secret_name=None,
            key_secret_project_id=None,
            scopes=self.instance.scopes,
            delegate_to=None,
            target_principal=target_principal,
            delegates=delegates,
            is_anonymous=None,
            idp_issuer_url=None,
            client_id=None,
            client_secret=None,
            idp_extra_params_dict=None,
        )
        assert (mock_credentials, PROJECT_ID) == result


class TestProvideAuthorizedGcloud:
    def setup_method(self):
        with mock.patch(
            MODULE_NAME + ".GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.instance = hook.GoogleBaseHook(gcp_conn_id="google-cloud-default")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + ".check_output")
    def test_provide_authorized_gcloud_key_path_and_keyfile_dict(self, mock_check_output, mock_default):
        key_path = "/test/key-path"
        self.instance.extras = {
            "key_path": key_path,
            "keyfile_dict": '{"foo": "bar"}',
        }

        with pytest.raises(
            AirflowException,
            match="The `keyfile_dict` and `key_path` fields are mutually exclusive. "
            "Please provide only one value.",
        ):
            with self.instance.provide_authorized_gcloud():
                assert os.environ[CREDENTIALS] == key_path

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + ".check_output")
    def test_provide_authorized_gcloud_key_path(self, mock_check_output, mock_project_id):
        key_path = "/test/key-path"
        self.instance.extras = {"key_path": key_path}

        with self.instance.provide_authorized_gcloud():
            assert os.environ[CREDENTIALS] == key_path

        calls = [
            mock.call(["gcloud", "auth", "activate-service-account", "--key-file=/test/key-path"]),
            mock.call(["gcloud", "config", "set", "core/project", "PROJECT_ID"]),
        ]
        mock_check_output.assert_has_calls(calls)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + ".check_output")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_authorized_gcloud_keyfile_dict(self, mock_file, mock_check_output, mock_project_id):
        string_file = StringIO()
        file_content = '{"foo": "bar"}'
        file_name = "/test/mock-file"
        self.instance.extras = {"keyfile_dict": file_content}
        mock_file_handler = mock_file.return_value.__enter__.return_value
        mock_file_handler.name = file_name
        mock_file_handler.write = string_file.write

        with self.instance.provide_authorized_gcloud():
            assert os.environ[CREDENTIALS] == file_name

        calls = [
            mock.call(["gcloud", "auth", "activate-service-account", "--key-file=/test/mock-file"]),
            mock.call(["gcloud", "config", "set", "core/project", "PROJECT_ID"]),
        ]
        mock_check_output.assert_has_calls(calls)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id",
        new_callable=mock.PropertyMock,
        return_value="PROJECT_ID",
    )
    @mock.patch(MODULE_NAME + "._cloud_sdk")
    @mock.patch(MODULE_NAME + ".check_output")
    @mock.patch("tempfile.NamedTemporaryFile")
    def test_provide_authorized_gcloud_via_gcloud_application_default(
        self, mock_file, mock_check_output, mock_cloud_sdk, mock_project_id
    ):
        # This file always exists.
        mock_cloud_sdk.get_application_default_credentials_path.return_value = __file__

        file_content = json.dumps(
            {
                "client_id": "CLIENT_ID",
                "client_secret": "CLIENT_SECRET",
                "refresh_token": "REFRESH_TOKEN",
                "type": "authorized_user",
            }
        )
        with mock.patch(MODULE_NAME + ".open", mock.mock_open(read_data=file_content)):
            with self.instance.provide_authorized_gcloud():
                # Do nothing
                pass

        mock_check_output.assert_has_calls(
            [
                mock.call(["gcloud", "config", "set", "auth/client_id", "CLIENT_ID"]),
                mock.call(["gcloud", "config", "set", "auth/client_secret", "CLIENT_SECRET"]),
                mock.call(["gcloud", "auth", "activate-refresh-token", "CLIENT_ID", "REFRESH_TOKEN"]),
                mock.call(["gcloud", "config", "set", "core/project", "PROJECT_ID"]),
            ],
            any_order=False,
        )


class TestNumRetry:
    @pytest.mark.db_test
    def test_should_return_int_when_set_int_via_connection(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        instance.extras = {
            "num_retries": 10,
        }

        assert isinstance(instance.num_retries, int)
        assert 10 == instance.num_retries

    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=("google-cloud-platform://?num_retries=5"),
    )
    def test_should_return_int_when_set_via_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        assert isinstance(instance.num_retries, int)

    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=("google-cloud-platform://?num_retries=cat"),
    )
    def test_should_raise_when_invalid_value_via_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        with pytest.raises(AirflowException, match=re.escape("The num_retries field should be a integer.")):
            assert isinstance(instance.num_retries, int)

    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=("google-cloud-platform://?num_retries="),
    )
    def test_should_fallback_when_empty_string_in_env_var(self):
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        assert isinstance(instance.num_retries, int)
        assert 5 == instance.num_retries


class TestCredentialsToken:
    @pytest.mark.asyncio
    async def test_get_project(self):
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        token = hook._CredentialsToken(mock_credentials, project=PROJECT_ID, scopes=SCOPES)
        assert await token.get_project() == PROJECT_ID

    @pytest.mark.asyncio
    async def test_get(self):
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        token = hook._CredentialsToken(mock_credentials, project=PROJECT_ID, scopes=SCOPES)
        assert await token.get() == "ACCESS_TOKEN"
        mock_credentials.refresh.assert_called_once()
        # ensure token caching works on subsequent calls of `get`
        mock_credentials.reset_mock()
        assert await token.get() == "ACCESS_TOKEN"
        mock_credentials.refresh.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE_NAME}.get_credentials_and_project_id", return_value=("CREDENTIALS", "PROJECT_ID"))
    async def test_from_hook(self, get_creds_and_project, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT",
            "google-cloud-platform://",
        )
        instance = hook.GoogleBaseHook(gcp_conn_id="google_cloud_default")
        token = await hook._CredentialsToken.from_hook(instance)
        assert token.credentials == "CREDENTIALS"
        assert token.project == "PROJECT_ID"


class TestGoogleBaseAsyncHook:
    @pytest.mark.asyncio
    @mock.patch("google.auth.default")
    async def test_get_token(self, mock_auth_default, monkeypatch) -> None:
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")
        monkeypatch.setenv(
            "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT",
            "google-cloud-platform://?project=CONN_PROJECT_ID",
        )

        instance = hook.GoogleBaseAsyncHook(gcp_conn_id="google_cloud_default")
        instance.sync_hook_class = hook.GoogleBaseHook
        token = await instance.get_token()
        assert await token.get_project() == "CONN_PROJECT_ID"
        assert await token.get() == "ACCESS_TOKEN"
        mock_credentials.refresh.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch("google.auth.default")
    async def test_get_token_impersonation(self, mock_auth_default, monkeypatch, requests_mock) -> None:
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")
        monkeypatch.setenv(
            "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT",
            "google-cloud-platform://?project=CONN_PROJECT_ID",
        )
        requests_mock.post(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT@SA_PROJECT.iam.gserviceaccount.com:generateAccessToken",
            text='{"accessToken": "IMPERSONATED_ACCESS_TOKEN", "expireTime": "2014-10-02T15:01:23Z"}',
        )

        instance = hook.GoogleBaseAsyncHook(
            gcp_conn_id="google_cloud_default",
            impersonation_chain="SERVICE_ACCOUNT@SA_PROJECT.iam.gserviceaccount.com",
        )
        instance.sync_hook_class = hook.GoogleBaseHook
        token = await instance.get_token()
        assert await token.get_project() == "CONN_PROJECT_ID"
        assert await token.get() == "IMPERSONATED_ACCESS_TOKEN"

    @pytest.mark.asyncio
    @mock.patch("google.auth.default")
    async def test_get_token_impersonation_conn(self, mock_auth_default, monkeypatch, requests_mock) -> None:
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_auth_default.return_value = (mock_credentials, "PROJECT_ID")
        monkeypatch.setenv(
            "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT",
            "google-cloud-platform://?project=CONN_PROJECT_ID&impersonation_chain=SERVICE_ACCOUNT@SA_PROJECT.iam.gserviceaccount.com",
        )
        requests_mock.post(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT@SA_PROJECT.iam.gserviceaccount.com:generateAccessToken",
            text='{"accessToken": "IMPERSONATED_ACCESS_TOKEN", "expireTime": "2014-10-02T15:01:23Z"}',
        )

        instance = hook.GoogleBaseAsyncHook(gcp_conn_id="google_cloud_default")
        instance.sync_hook_class = hook.GoogleBaseHook
        token = await instance.get_token()
        assert await token.get_project() == "CONN_PROJECT_ID"
        assert await token.get() == "IMPERSONATED_ACCESS_TOKEN"
