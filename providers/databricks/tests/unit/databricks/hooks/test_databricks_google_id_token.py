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

import base64
import json
import time
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.databricks.hooks.databricks_base import (
    DEFAULT_GCP_SCOPES,
    GOOGLE_ID_TOKEN_SETTING_KEY,
    BaseDatabricksHook,
)

AUDIENCE = "test-audience"
IMPERSONATED_SERVICE_ACCOUNT = "databricks@test-project.iam.gserviceaccount.com"
IMPERSONATE_URL = (
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/"
    f"{IMPERSONATED_SERVICE_ACCOUNT}:generateIdToken"
)


def _make_jwt(exp: int) -> str:
    """Build an unsigned JWT string carrying only an ``exp`` claim (test fixture)."""

    def _b64(payload: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")

    return f"{_b64({'alg': 'none', 'typ': 'JWT'})}.{_b64({'exp': exp})}.signature"


def _valid_jwt() -> str:
    return _make_jwt(int(time.time()) + 3600)


def _expired_jwt() -> str:
    return _make_jwt(int(time.time()) - 3600)


def _hook_with_extra(extra: dict) -> BaseDatabricksHook:
    hook = BaseDatabricksHook()
    hook.databricks_conn = Connection(host="example.databricks.com", extra=extra)
    return hook


class TestGoogleIdTokenDispatch:
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @mock.patch.object(BaseDatabricksHook, "_get_google_id_token")
    def test_get_token_google_id_token_success(self, mock_get_google_id_token, mock_conn):
        mock_conn.return_value = Connection(extra={GOOGLE_ID_TOKEN_SETTING_KEY: True})
        mock_get_google_id_token.return_value = "google_id_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = hook._get_token()
            assert token == "google_id_token"
            mock_get_google_id_token.assert_called_once_with()
            mock_log_debug.assert_called_once_with("Using Google ID Token.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_get_google_id_token",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_google_id_token_success(self, mock_conn, mock_a_get_google_id_token):
        mock_conn.return_value = Connection(extra={GOOGLE_ID_TOKEN_SETTING_KEY: True})
        mock_a_get_google_id_token.return_value = "google_id_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = await hook._a_get_token()
            assert token == "google_id_token"
            mock_a_get_google_id_token.assert_awaited_once_with()
            mock_log_debug.assert_called_once_with("Using Google ID Token.")


class TestGetGoogleIdToken:
    def test_missing_audience_raises(self):
        hook = _hook_with_extra({GOOGLE_ID_TOKEN_SETTING_KEY: True})
        with pytest.raises(AirflowException, match="google_id_token_audience"):
            hook._get_google_id_token()

    @mock.patch("google.oauth2.id_token.fetch_id_token")
    @mock.patch("google.auth.transport.requests.AuthorizedSession")
    @mock.patch("google.auth.default")
    def test_impersonated_service_account(self, mock_default, mock_session_class, mock_fetch_id_token):
        credentials = mock.Mock()
        mock_default.return_value = (credentials, "test-project")
        session = mock_session_class.return_value
        response = session.post.return_value
        response.json.return_value = {"token": _valid_jwt()}

        hook = _hook_with_extra(
            {
                GOOGLE_ID_TOKEN_SETTING_KEY: True,
                "google_id_token_audience": AUDIENCE,
                "google_impersonated_service_account": IMPERSONATED_SERVICE_ACCOUNT,
            }
        )
        token = hook._get_google_id_token()

        assert token == response.json.return_value["token"]
        mock_default.assert_called_once_with(scopes=DEFAULT_GCP_SCOPES)
        mock_session_class.assert_called_once_with(credentials)
        session.post.assert_called_once_with(
            IMPERSONATE_URL,
            json={"audience": AUDIENCE, "includeEmail": True},
            timeout=hook.token_timeout_seconds,
        )
        mock_fetch_id_token.assert_not_called()
        # The fetched token is cached under the audience/impersonation key.
        assert hook.jwt_tokens[f"{AUDIENCE}-{IMPERSONATED_SERVICE_ACCOUNT}"] == token

    @mock.patch("google.oauth2.id_token.fetch_id_token")
    @mock.patch("google.auth.default")
    def test_direct_fetch_without_impersonation(self, mock_default, mock_fetch_id_token):
        mock_fetch_id_token.return_value = _valid_jwt()
        hook = _hook_with_extra(
            {
                GOOGLE_ID_TOKEN_SETTING_KEY: True,
                "google_id_token_audience": AUDIENCE,
            }
        )
        token = hook._get_google_id_token()

        assert token == mock_fetch_id_token.return_value
        mock_fetch_id_token.assert_called_once()
        assert mock_fetch_id_token.call_args.args[1] == AUDIENCE
        mock_default.assert_not_called()
        assert hook.jwt_tokens[f"{AUDIENCE}-None"] == token

    @mock.patch("google.oauth2.id_token.fetch_id_token")
    def test_valid_cached_token_is_reused(self, mock_fetch_id_token):
        hook = _hook_with_extra(
            {
                GOOGLE_ID_TOKEN_SETTING_KEY: True,
                "google_id_token_audience": AUDIENCE,
            }
        )
        hook.jwt_tokens[f"{AUDIENCE}-None"] = _valid_jwt()
        assert hook._get_google_id_token() == hook.jwt_tokens[f"{AUDIENCE}-None"]
        mock_fetch_id_token.assert_not_called()

    @mock.patch("google.oauth2.id_token.fetch_id_token")
    def test_expired_cached_token_is_refetched(self, mock_fetch_id_token):
        mock_fetch_id_token.return_value = _valid_jwt()
        hook = _hook_with_extra(
            {
                GOOGLE_ID_TOKEN_SETTING_KEY: True,
                "google_id_token_audience": AUDIENCE,
            }
        )
        hook.jwt_tokens[f"{AUDIENCE}-None"] = _expired_jwt()
        assert hook._get_google_id_token() == mock_fetch_id_token.return_value
        mock_fetch_id_token.assert_called_once()

    @mock.patch("google.auth.default", side_effect=ImportError("No module named 'google'"))
    def test_missing_google_auth_raises_optional_feature_exception(self, mock_default):
        hook = _hook_with_extra(
            {
                GOOGLE_ID_TOKEN_SETTING_KEY: True,
                "google_id_token_audience": AUDIENCE,
                "google_impersonated_service_account": IMPERSONATED_SERVICE_ACCOUNT,
            }
        )
        with pytest.raises(AirflowOptionalProviderFeatureException, match="google-auth"):
            hook._get_google_id_token()

    @pytest.mark.asyncio
    @mock.patch.object(BaseDatabricksHook, "_get_google_id_token")
    async def test_a_get_google_id_token_delegates_to_sync_implementation(self, mock_get_google_id_token):
        mock_get_google_id_token.return_value = "google_id_token"
        hook = BaseDatabricksHook()
        assert await hook._a_get_google_id_token() == "google_id_token"
        mock_get_google_id_token.assert_called_once_with()


class TestIsJwtTokenValid:
    def test_valid_token(self):
        assert BaseDatabricksHook._is_jwt_token_valid(_valid_jwt())

    def test_expired_token(self):
        assert not BaseDatabricksHook._is_jwt_token_valid(_expired_jwt())

    @pytest.mark.parametrize("token", ["", "not-a-jwt", "a.b", "a.!!!.c"])
    def test_malformed_token(self, token):
        assert not BaseDatabricksHook._is_jwt_token_valid(token)

    def test_token_without_exp_claim(self):
        payload = base64.urlsafe_b64encode(json.dumps({"sub": "x"}).encode()).decode().rstrip("=")
        assert not BaseDatabricksHook._is_jwt_token_valid(f"header.{payload}.signature")
