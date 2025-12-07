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

from datetime import datetime, timedelta
from unittest import mock

import aiohttp
import pytest
import time_machine
from aiohttp.client_exceptions import ClientConnectorError
from requests import exceptions as requests_exceptions
from requests.auth import HTTPBasicAuth
from tenacity import AsyncRetrying, Future, RetryError, retry_if_exception, stop_after_attempt, wait_fixed

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks_base import (
    DEFAULT_AZURE_CREDENTIAL_SETTING_KEY,
    DEFAULT_DATABRICKS_SCOPE,
    TOKEN_REFRESH_LEAD_TIME,
    BaseDatabricksHook,
)

DEFAULT_CONN_ID = "databricks_default"


class TestBaseDatabricksHook:
    def test_init_exception(self):
        """
        Tests handling incorrect parameters passed to ``__init__``
        """
        with pytest.raises(ValueError, match="Retry limit must be greater than or equal to 1"):
            BaseDatabricksHook(databricks_conn_id=DEFAULT_CONN_ID, retry_limit=0)

    def test_init_with_custom_params(self):
        custom_conn_id = "custom_databricks_conn"
        custom_timeout = 300
        custom_retry_limit = 5
        custom_retry_delay = 2.5
        custom_retry_args = {"stop": "custom_stop", "wait": "custom_wait"}
        custom_caller = "CustomOperator"

        hook = BaseDatabricksHook(
            databricks_conn_id=custom_conn_id,
            timeout_seconds=custom_timeout,
            retry_limit=custom_retry_limit,
            retry_delay=custom_retry_delay,
            retry_args=custom_retry_args,
            caller=custom_caller,
        )

        assert hook.databricks_conn_id == custom_conn_id
        assert hook.timeout_seconds == custom_timeout
        assert hook.retry_limit == custom_retry_limit
        assert hook.retry_delay == custom_retry_delay
        assert hook.caller == custom_caller
        assert hook.oauth_tokens == {}
        assert hook.token_timeout_seconds == 10
        assert "stop" in hook.retry_args
        assert "wait" in hook.retry_args
        assert "retry" in hook.retry_args
        assert "after" in hook.retry_args

    def test_init_with_default_params(self):
        hook = BaseDatabricksHook()

        assert hook.databricks_conn_id == "databricks_default"
        assert hook.timeout_seconds == 180
        assert hook.retry_limit == 3
        assert hook.retry_delay == 1.0
        assert hook.caller == "Unknown"
        assert hook.oauth_tokens == {}
        assert hook.token_timeout_seconds == 10
        assert "stop" in hook.retry_args
        assert "wait" in hook.retry_args
        assert "retry" in hook.retry_args
        assert "after" in hook.retry_args

    @pytest.mark.parametrize(
        ("input_url", "expected_host"),
        [
            ("https://xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("http://xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("https://my-workspace.cloud.databricks.com", "my-workspace.cloud.databricks.com"),
            ("http://my-workspace.cloud.databricks.com", "my-workspace.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com:443", "xx.cloud.databricks.com"),
            ("http://xx.cloud.databricks.com:80", "xx.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com/path", "xx.cloud.databricks.com"),
        ],
    )
    def test_parse_host(self, input_url, expected_host):
        assert BaseDatabricksHook._parse_host(input_url) == expected_host

    @mock.patch("requests.post")
    @time_machine.travel("2025-07-12 12:00:00")
    def test_get_sp_token(self, mock_post):
        mock_response = mock.Mock()
        expiry_date = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        mock_response.json.return_value = {
            "access_token": "test_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        mock_conn = mock.Mock()
        mock_conn.login = "client_id"
        mock_conn.password = "client_secret"
        hook = BaseDatabricksHook()
        hook.databricks_conn = mock_conn
        hook.user_agent_header = {"User-Agent": "test-agent"}
        resource = ""
        token = hook._get_sp_token(resource)

        assert token == "test_token"
        assert resource in hook.oauth_tokens
        assert hook.oauth_tokens[resource]["access_token"] == "test_token"
        assert hook.oauth_tokens[resource]["expires_on"] == expiry_date
        mock_post.assert_called_once_with(
            resource,
            auth=HTTPBasicAuth("client_id", "client_secret"),
            data="grant_type=client_credentials&scope=all-apis",
            headers={
                "User-Agent": "test-agent",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=hook.token_timeout_seconds,
        )

    @mock.patch("requests.post")
    def test_get_sp_token_cached_valid(self, mock_post):
        hook = BaseDatabricksHook()
        resource = ""
        hook.oauth_tokens[resource] = {"access_token": "cached_token", "expires_on": 10}
        with mock.patch.object(hook, "_is_oauth_token_valid", return_value=True):
            token = hook._get_sp_token(resource)
        assert token == "cached_token"
        mock_post.assert_not_called()

    @mock.patch("requests.post")
    def test_get_sp_token_http_error(self, mock_post):
        mock_response = mock.Mock()
        mock_response.status_code = 401
        mock_response.content.decode.return_value = "Unauthorized"
        http_error = requests_exceptions.HTTPError()
        http_error.response = mock_response
        mock_post.side_effect = http_error
        hook = BaseDatabricksHook()
        hook.databricks_conn = mock.Mock()
        hook.databricks_conn.login = "client_id"
        hook.databricks_conn.password = "client_secret"
        hook.user_agent_header = {"User-Agent": "test-agent"}
        resource = ""
        with pytest.raises(AirflowException, match="Response: Unauthorized, Status Code: 401"):
            hook._get_sp_token(resource)

    @mock.patch("requests.post")
    def test_get_sp_token_retry_error(self, mock_post):
        mock_post.side_effect = requests_exceptions.ConnectionError("Connection failed")
        hook = BaseDatabricksHook(retry_limit=2)
        hook.databricks_conn = mock.Mock()
        hook.databricks_conn.login = "client_id"
        hook.databricks_conn.password = "client_secret"
        hook.user_agent_header = {"User-Agent": "test-agent"}
        resource = ""

        with pytest.raises(AirflowException, match="API requests to Databricks failed 2 times. Giving up."):
            hook._get_sp_token(resource)

    @pytest.mark.asyncio
    @time_machine.travel("2025-07-12 12:00:00")
    @mock.patch("aiohttp.ClientSession.post")
    async def test_a_get_sp_token(self, mock_post):
        expiry_date = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        mock_response = mock.AsyncMock()
        mock_response.__aenter__.return_value = mock_response
        mock_response.__aexit__.return_value = None
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "access_token": "async_test_token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_response
        mock_conn = mock.Mock()
        mock_conn.login = "client_id"
        mock_conn.password = "client_secret"

        hook = BaseDatabricksHook()
        hook.databricks_conn = mock_conn
        hook.user_agent_header = {"User-Agent": "test-agent"}
        hook.token_timeout_seconds = 10
        async with aiohttp.ClientSession() as session:
            hook._session = session
            mock_attempt = mock.Mock()
            mock_attempt.__enter__ = mock.Mock(return_value=None)
            mock_attempt.__exit__ = mock.Mock(return_value=None)

            async def mock_retry_generator():
                yield mock_attempt

            hook._a_get_retry_object = mock.Mock(return_value=mock_retry_generator())
            hook._is_oauth_token_valid = mock.Mock(return_value=False)
            resource = ""
            token = await hook._a_get_sp_token(resource)
            assert token == "async_test_token"
            assert resource in hook.oauth_tokens
            assert hook.oauth_tokens[resource]["access_token"] == "async_test_token"
            assert hook.oauth_tokens[resource]["expires_on"] == expiry_date

            mock_post.assert_called_once_with(
                resource,
                auth=aiohttp.BasicAuth("client_id", "client_secret"),
                data="grant_type=client_credentials&scope=all-apis",
                headers={
                    "User-Agent": "test-agent",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                timeout=10,
            )

    @pytest.mark.asyncio
    @mock.patch("time.time", return_value=0)
    async def test_a_get_sp_token_retry_error(self, mock_time):
        mock_conn = mock.Mock()
        mock_conn.login = "client_id"
        mock_conn.password = "client_secret"
        hook = BaseDatabricksHook()
        hook.databricks_conn = mock_conn
        hook.user_agent_header = {"User-Agent": "test-agent"}
        hook.token_timeout_seconds = 10
        hook.retry_limit = 3
        if not hasattr(hook, "oauth_tokens"):
            hook.oauth_tokens = {}
        hook._is_oauth_token_valid = mock.Mock(return_value=False)
        mock_session = mock.AsyncMock()
        hook._session = mock_session

        async def patched_method(resource: str) -> str:
            sp_token = hook.oauth_tokens.get(resource)
            if sp_token and hook._is_oauth_token_valid(sp_token):
                return sp_token["access_token"]
            try:
                future = Future(attempt_number=3)
                raise RetryError(future)
            except RetryError:
                raise AirflowException(
                    f"API requests to Databricks failed {hook.retry_limit} times. Giving up."
                )

        hook._a_get_sp_token = patched_method
        resource = ""
        with pytest.raises(AirflowException) as exc_info:
            await hook._a_get_sp_token(resource)
        assert "API requests to Databricks failed 3 times. Giving up." in str(exc_info.value)

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.post")
    async def test_a_get_sp_token_cached_valid_token(self, mock_post):
        hook = BaseDatabricksHook()
        resource = "https://test.databricks.com/oidc/v1/token"
        hook.oauth_tokens = {resource: {"access_token": "cached_token", "expires_on": 2000}}
        async with aiohttp.ClientSession() as session:
            hook._session = session
            with mock.patch.object(hook, "_is_oauth_token_valid", return_value=True):
                token = await hook._a_get_sp_token(resource)
            assert token == "cached_token"
            mock_post.assert_not_called()

    @time_machine.travel("2025-07-12 12:00:00")
    def test_valid_token_not_expired(self):
        expiry_data = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "token_type": "Bearer", "expires_on": expiry_data}
        hook = BaseDatabricksHook()
        assert hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_valid_token_expired(self):
        expiry_data = int((datetime(2025, 7, 12, 12, 0, 0) - timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "token_type": "Bearer", "expires_on": expiry_data}
        hook = BaseDatabricksHook()
        assert not hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_token_expires_within_lead_time(self):
        expires_on = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(seconds=60)).timestamp())
        token = {
            "access_token": "valid_token",
            "token_type": "Bearer",
            "expires_on": expires_on,
        }
        hook = BaseDatabricksHook()
        assert hook._is_oauth_token_valid(token) is False

    @time_machine.travel("2025-07-12 12:00:00")
    def test_missing_access_token_raises_exception(self):
        expiry_date = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"token_type": "Bearer", "expires_on": expiry_date}
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Can't get necessary data from OAuth token"):
            hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_wrong_token_type_raises_exception(self):
        expiry_date = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "token_type": "Basic", "expires_on": expiry_date}
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Can't get necessary data from OAuth token"):
            hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_missing_token_type_raises_exception(self):
        expiry_date = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "expires_on": expiry_date}
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Can't get necessary data from OAuth token"):
            hook._is_oauth_token_valid(token)

    def test_missing_expires_on_raises_exception(self):
        token = {"access_token": "valid_token", "token_type": "Bearer"}
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Can't get necessary data from OAuth token"):
            hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_custom_time_key(self):
        expiry_data = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "token_type": "Bearer", "custom_expires": expiry_data}
        hook = BaseDatabricksHook()
        assert hook._is_oauth_token_valid(token, time_key="custom_expires")

    def test_empty_token_dict_raises_exception(self):
        token = {}
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Can't get necessary data from OAuth token"):
            hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_string_expiration_time(self):
        expiry_data = int((datetime(2025, 7, 12, 12, 0, 0) + timedelta(minutes=60)).timestamp())
        token = {"access_token": "valid_token", "token_type": "Bearer", "expires_on": expiry_data}
        hook = BaseDatabricksHook()
        assert hook._is_oauth_token_valid(token)

    @time_machine.travel("2025-07-12 12:00:00")
    def test_exact_boundary_conditions(self):
        expiry_data = int((datetime(2025, 7, 12, 12, 0, 0)).timestamp())
        hook = BaseDatabricksHook()
        token = {
            "access_token": "valid_token",
            "token_type": "Bearer",
            "expires_on": expiry_data + TOKEN_REFRESH_LEAD_TIME,
        }
        assert not hook._is_oauth_token_valid(token)

        token["expires_on"] = expiry_data + TOKEN_REFRESH_LEAD_TIME + 1
        assert hook._is_oauth_token_valid(token)

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    def test_get_token_from_extra_dejson(self, mock_conn):
        extra = {"token": "test_token"}
        mock_conn.return_value = Connection(extra=extra)
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "info") as mock_log_info:
            token = hook._get_token()
            assert token == "test_token"
            mock_log_info.assert_called_once_with(
                "Using token auth. For security reasons, please set token in Password field instead of extra"
            )

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    def test_token_from_password_when_login_missing(self, mock_conn):
        mock_conn.return_value = Connection(login=None, password="pw-token")
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            password = hook._get_token()
            assert password == "pw-token"
            mock_log_debug.assert_called_once_with("Using token auth.")

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @mock.patch.object(BaseDatabricksHook, "_get_sp_token")
    def test_get_token_service_principal_oauth_success(self, mock_get_sp_token, mock_conn):
        mock_conn.return_value = Connection(
            host="example.databricks.com",
            login="spn_user",
            password="spn_pass",
            extra={"service_principal_oauth": True},
        )
        mock_get_sp_token.return_value = "spn_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = hook._get_token()
            assert token == "spn_token"
            mock_get_sp_token.assert_called_once_with("example.databricks.com/oidc/v1/token")
            mock_log_debug.assert_called_once_with("Using Service Principal Token.")

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @mock.patch.object(BaseDatabricksHook, "_get_aad_token")
    def test_get_token_azure_spn_success(self, mock_get_aad_token, mock_conn):
        extra = {"azure_tenant_id": "tenant_id"}
        mock_conn.return_value = Connection(login="spn_client_id", password="spn_client_secret", extra=extra)
        mock_get_aad_token.return_value = "aad_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = hook._get_token()
            assert token == "aad_token"
            mock_get_aad_token.assert_called_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using AAD Token for SPN.")

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    def test_get_token_azure_spn_missing_credentials_raises(self, mock_conn):
        extra = {"azure_tenant_id": "tenant_id"}
        mock_conn.return_value = Connection(login="", password="", extra=extra)
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Azure SPN credentials aren't provided"):
            hook._get_token()

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @mock.patch.object(BaseDatabricksHook, "_get_aad_token")
    @mock.patch.object(BaseDatabricksHook, "_check_azure_metadata_service")
    def test_get_token_managed_identity(self, mock_check_metadata, mock_get_aad_token, mock_conn):
        extra = {"use_azure_managed_identity": True}
        mock_conn.return_value = Connection(extra=extra)
        mock_get_aad_token.return_value = "mi_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = hook._get_token()
            assert token == "mi_token"
            mock_check_metadata.assert_called_once()
            mock_get_aad_token.assert_called_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using AAD Token for managed identity.")

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @mock.patch.object(BaseDatabricksHook, "_get_aad_token_for_default_az_credential")
    def test_get_token_default_azure_credential(self, mock_get_default_cred_token, mock_conn):
        extra = {DEFAULT_AZURE_CREDENTIAL_SETTING_KEY: True}
        mock_conn.return_value = Connection(extra=extra)
        mock_get_default_cred_token.return_value = "default_cred_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = hook._get_token()
            assert token == "default_cred_token"
            mock_get_default_cred_token.assert_called_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using default Azure Credential authentication.")

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    def test_get_token_service_principal_oauth_missing_credentials(self, mock_conn):
        mock_conn.return_value = Connection(
            host="adb-host", login="", password="", extra={"service_principal_oauth": True}
        )
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Service Principal credentials aren't provided"):
            hook._get_token()

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    def test_get_token_not_configured_raises(self, mock_conn):
        mock_conn.return_value = Connection(host="adb-host", login="", password="", extra={})
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Token authentication isn't configured"):
            hook._get_token(raise_error=True)

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_from_extra_dejson(self, mock_conn):
        extra = {"token": "test_token"}
        mock_conn.return_value = Connection(extra=extra)
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "info") as mock_log_info:
            token = await hook._a_get_token()
            assert token == "test_token"
            mock_log_info.assert_called_once_with(
                "Using token auth. For security reasons, please set token in Password field instead of extra"
            )

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_token_from_password_when_login_missing(self, mock_conn):
        mock_conn.return_value = Connection(login=None, password="pw-token")
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            password = await hook._a_get_token()
            assert password == "pw-token"
            mock_log_debug.assert_called_once_with("Using token auth.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_get_sp_token",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_service_principal_oauth_success(self, mock_conn, mock_get_sp_token):
        mock_conn.return_value = Connection(
            host="example.databricks.com",
            login="spn_user",
            password="spn_pass",
            extra={"service_principal_oauth": True},
        )
        mock_get_sp_token.return_value = "spn_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = await hook._a_get_token()
            assert token == "spn_token"
            mock_get_sp_token.assert_awaited_once_with("example.databricks.com/oidc/v1/token")
            mock_log_debug.assert_called_once_with("Using Service Principal Token.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_get_aad_token",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_azure_spn_success(self, mock_conn, mock_get_aad_token):
        extra = {"azure_tenant_id": "tenant_id"}
        mock_conn.return_value = Connection(login="spn_client_id", password="spn_client_secret", extra=extra)
        mock_get_aad_token.return_value = "aad_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = await hook._a_get_token()
            assert token == "aad_token"
            mock_get_aad_token.assert_awaited_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using AAD Token for SPN.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_azure_spn_missing_credentials_raises(self, mock_conn):
        mock_conn.return_value = Connection(login="", password="", extra={"azure_tenant_id": "tenant_id"})
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Azure SPN credentials aren't provided"):
            await hook._a_get_token()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_check_azure_metadata_service",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_get_aad_token",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_managed_identity(self, mock_conn, mock_get_aad_token, mock_check_metadata):
        mock_conn.return_value = Connection(extra={"use_azure_managed_identity": True})
        mock_get_aad_token.return_value = "mi_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = await hook._a_get_token()
            assert token == "mi_token"
            mock_check_metadata.assert_awaited_once()
            mock_get_aad_token.assert_awaited_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using AAD Token for managed identity.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook._a_get_aad_token_for_default_az_credential",
        new_callable=mock.AsyncMock,
    )
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_default_azure_credential(self, mock_conn, mock_get_default_cred_token):
        extra = {DEFAULT_AZURE_CREDENTIAL_SETTING_KEY: True}
        mock_conn.return_value = Connection(extra=extra)
        mock_get_default_cred_token.return_value = "default_cred_token"
        hook = BaseDatabricksHook()
        with mock.patch.object(hook.log, "debug") as mock_log_debug:
            token = await hook._a_get_token()
            assert token == "default_cred_token"
            mock_get_default_cred_token.assert_awaited_once_with(DEFAULT_DATABRICKS_SCOPE)
            mock_log_debug.assert_called_once_with("Using AzureDefaultCredential for authentication.")

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_service_principal_oauth_missing_credentials(self, mock_conn):
        mock_conn.return_value = Connection(
            host="host", login="", password="", extra={"service_principal_oauth": True}
        )
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Service Principal credentials aren't provided"):
            await hook._a_get_token()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    async def test_a_get_token_not_configured_raises(self, mock_conn):
        mock_conn.return_value = Connection(
            host="host",
            login="",
            password="",
        )
        hook = BaseDatabricksHook()
        with pytest.raises(AirflowException, match="Token authentication isn't configured"):
            await hook._a_get_token(raise_error=True)

    @mock.patch(
        "airflow.providers.databricks.hooks.databricks_base.BaseDatabricksHook.databricks_conn",
        new_callable=mock.PropertyMock,
    )
    @pytest.mark.parametrize(
        ("schema", "port", "host", "endpoint", "expected_url"),
        [
            ("https", 443, "example.com", "api/2.0/jobs/list", "https://example.com:443/api/2.0/jobs/list"),
            ("http", 8080, "localhost", "status", "http://localhost:8080/status"),
            (None, None, "my.db.net", "api", "https://my.db.net/api"),
            ("https", None, "myhost", "v1/info", "https://myhost/v1/info"),
            ("https", 443, "host.com", "api", "https://host.com:443/api"),
        ],
    )
    def test_endpoint_url(self, mock_conn, schema, port, host, endpoint, expected_url):
        mock_conn.return_value = Connection(
            host=host,
            schema=schema,
            port=port,
        )
        hook = BaseDatabricksHook()
        assert hook._endpoint_url(endpoint) == expected_url

    def test_requests_connection_error_is_retryable(self):
        exception = requests_exceptions.ConnectionError()
        assert BaseDatabricksHook._retryable_error(exception)

    def test_requests_timeout_is_retryable(self):
        exception = requests_exceptions.Timeout()
        assert BaseDatabricksHook._retryable_error(exception)

    def test_requests_exception_with_500_status_is_retryable(self):
        mock_response = mock.Mock()
        mock_response.status_code = 500
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        assert BaseDatabricksHook._retryable_error(exception)

    def test_requests_exception_with_400_and_lock_error_is_retryable(self):
        mock_response = mock.Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"error_code": "COULD_NOT_ACQUIRE_LOCK"}
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        assert BaseDatabricksHook._retryable_error(exception)

    def test_requests_exception_with_400_and_other_error_is_not_retryable(self):
        mock_response = mock.Mock()
        mock_response.status_code = 400
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        assert not BaseDatabricksHook._retryable_error(exception)

    def test_requests_exception_with_300_status_is_not_retryable(self):
        mock_response = mock.Mock()
        mock_response.status_code = 301
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        assert not BaseDatabricksHook._retryable_error(exception)

    def test_requests_exception_with_no_response_is_not_retryable(self):
        exception = requests_exceptions.HTTPError()
        exception.response = None
        assert not BaseDatabricksHook._retryable_error(exception)

    def test_aiohttp_client_response_error_with_500_is_retryable(self):
        exception = aiohttp.ClientResponseError(request_info=mock.Mock(), status=500, history=())
        assert BaseDatabricksHook._retryable_error(exception)

    def test_aiohttp_client_response_error_with_429_is_retryable(self):
        exception = aiohttp.ClientResponseError(request_info=mock.Mock(), status=429, history=())
        assert BaseDatabricksHook._retryable_error(exception)

    def test_aiohttp_client_response_error_with_404_is_not_retryable(self):
        exception = aiohttp.ClientResponseError(request_info=mock.Mock(), status=404, history=())
        assert not BaseDatabricksHook._retryable_error(exception)

    def test_aiohttp_client_connector_error_is_retryable(self):
        exception = ClientConnectorError(connection_key=mock.Mock(), os_error=OSError())
        assert BaseDatabricksHook._retryable_error(exception)

    def test_timeout_error_is_retryable(self):
        exception = requests_exceptions.Timeout()
        assert BaseDatabricksHook._retryable_error(exception)

    def test_generic_exception_is_not_retryable(self):
        exception = ValueError()
        assert not BaseDatabricksHook._retryable_error(exception)

    def test_get_error_code_with_airflow_exception_returns_empty_string(self):
        exception = AirflowException()
        hook = BaseDatabricksHook()
        assert hook._get_error_code(exception) == ""

    def test_get_error_code_with_http_error_and_empty_json_returns_empty_string(self):
        mock_response = mock.Mock()
        mock_response.json.return_value = {}
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        hook = BaseDatabricksHook()
        assert hook._get_error_code(exception) == ""

    def test_get_error_code_with_http_error_and_valid_error_code(self):
        mock_response = mock.Mock()
        mock_response.json.return_value = {"error_code": "INVALID_REQUEST"}
        exception = requests_exceptions.HTTPError()
        exception.response = mock_response
        hook = BaseDatabricksHook()
        assert hook._get_error_code(exception) == "INVALID_REQUEST"

    @mock.patch("requests.get")
    @time_machine.travel("2025-07-12 12:00:00")
    def test_check_azure_metadata_service_normal(self, mock_get):
        travel_time = int(datetime(2025, 7, 12, 12, 0, 0).timestamp())
        hook = BaseDatabricksHook()
        mock_response = {"compute": {"azEnvironment": "AzurePublicCloud"}}
        mock_get.return_value.json.return_value = mock_response

        hook._check_azure_metadata_service()

        assert hook._metadata_cache == mock_response
        assert int(hook._metadata_expiry) == travel_time + hook._metadata_ttl

    @mock.patch("requests.get")
    @time_machine.travel("2025-07-12 12:00:00")
    def test_check_azure_metadata_service_cached(self, mock_get):
        travel_time = int(datetime(2025, 7, 12, 12, 0, 0).timestamp())
        hook = BaseDatabricksHook()
        mock_response = {"compute": {"azEnvironment": "AzurePublicCloud"}}
        hook._metadata_cache = mock_response
        hook._metadata_expiry = travel_time + 1000

        hook._check_azure_metadata_service()
        mock_get.assert_not_called()

    @mock.patch("requests.get")
    def test_check_azure_metadata_service_http_error(self, mock_get):
        hook = BaseDatabricksHook()
        mock_get.side_effect = requests_exceptions.RequestException("Fail")

        with pytest.raises(ConnectionError, match="Can't reach Azure Metadata Service"):
            hook._check_azure_metadata_service()
        assert hook._metadata_cache == {}
        assert hook._metadata_expiry == 0

    @mock.patch("requests.get")
    def test_check_azure_metadata_service_retry_error(self, mock_get):
        hook = BaseDatabricksHook()

        resp_429 = mock.Mock()
        resp_429.status_code = 429
        resp_429.content = b"Too many requests"
        http_error = requests_exceptions.HTTPError(response=resp_429)
        mock_get.side_effect = http_error

        with pytest.raises(ConnectionError, match="Failed to reach Azure Metadata Service after 3 retries."):
            hook._check_azure_metadata_service()
        assert mock_get.call_count == 3

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.get")
    async def test_a_check_azure_metadata_service_normal(self, mock_get):
        hook = BaseDatabricksHook()

        async_mock = mock.AsyncMock()
        async_mock.__aenter__.return_value = async_mock
        async_mock.__aexit__.return_value = None
        async_mock.json.return_value = {"compute": {"azEnvironment": "AzurePublicCloud"}}

        mock_get.return_value = async_mock

        async with aiohttp.ClientSession() as session:
            hook._session = session
            mock_attempt = mock.Mock()
            mock_attempt.__enter__ = mock.Mock(return_value=None)
            mock_attempt.__exit__ = mock.Mock(return_value=None)

            async def mock_retry_generator():
                yield mock_attempt

            hook._a_get_retry_object = mock.Mock(return_value=mock_retry_generator())
            await hook._a_check_azure_metadata_service()

            assert hook._metadata_cache["compute"]["azEnvironment"] == "AzurePublicCloud"
            assert hook._metadata_expiry > 0

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.get")
    @time_machine.travel("2025-07-12 12:00:00")
    async def test_a_check_azure_metadata_service_cached(self, mock_get):
        travel_time = int(datetime(2025, 7, 12, 12, 0, 0).timestamp())
        hook = BaseDatabricksHook()
        hook._metadata_cache = {"compute": {"azEnvironment": "AzurePublicCloud"}}
        hook._metadata_expiry = travel_time + 1000

        async with aiohttp.ClientSession() as session:
            hook._session = session
            await hook._a_check_azure_metadata_service()
            mock_get.assert_not_called()

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.get")
    async def test_a_check_azure_metadata_service_http_error(self, mock_get):
        hook = BaseDatabricksHook()

        async_mock = mock.AsyncMock()
        async_mock.__aenter__.side_effect = aiohttp.ClientError("Fail")
        async_mock.__aexit__.return_value = None
        mock_get.return_value = async_mock

        async with aiohttp.ClientSession() as session:
            hook._session = session
            mock_attempt = mock.Mock()
            mock_attempt.__enter__ = mock.Mock(return_value=None)
            mock_attempt.__exit__ = mock.Mock(return_value=None)

            async def mock_retry_generator():
                yield mock_attempt

            hook._a_get_retry_object = mock.Mock(return_value=mock_retry_generator())

            with pytest.raises(ConnectionError, match="Can't reach Azure Metadata Service"):
                await hook._a_check_azure_metadata_service()
            assert hook._metadata_cache == {}
            assert hook._metadata_expiry == 0

    @pytest.mark.asyncio
    @mock.patch("aiohttp.ClientSession.get")
    async def test_a_check_azure_metadata_service_retry_error(self, mock_get):
        hook = BaseDatabricksHook()

        mock_get.side_effect = aiohttp.ClientResponseError(
            request_info=mock.Mock(), history=(), status=429, message="429 Too Many Requests"
        )

        async with aiohttp.ClientSession() as session:
            hook._session = session

            hook._a_get_retry_object = lambda: AsyncRetrying(
                stop=stop_after_attempt(hook.retry_limit),
                wait=wait_fixed(0),
                retry=retry_if_exception(hook._retryable_error),
            )

            hook._validate_azure_metadata_service = mock.Mock()

            with pytest.raises(
                ConnectionError, match="Failed to reach Azure Metadata Service after 3 retries."
            ):
                await hook._a_check_azure_metadata_service()
            assert mock_get.call_count == 3
