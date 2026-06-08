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

from unittest import mock

import pytest

from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.cli import api_client as cli_api_client

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Reset the process-wide client singleton around each test."""
    cli_api_client._api_client = None
    yield
    cli_api_client._api_client = None


class TestResolveBaseUrl:
    @conf_vars({("api", "base_url"): "https://airflow.example.com:9999"})
    def test_explicit_base_url(self):
        assert cli_api_client._resolve_base_url() == "https://airflow.example.com:9999"

    @conf_vars({("api", "base_url"): "", ("api", "host"): "myhost", ("api", "port"): "1234"})
    def test_host_port_fallback(self):
        assert cli_api_client._resolve_base_url() == "http://myhost:1234"


class TestMintCliToken:
    def test_uses_env_token(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CLI_TOKEN", "tok-123")
        with mock.patch("airflow.api_fastapi.app.get_auth_manager") as get_auth_manager:
            assert cli_api_client._mint_cli_token() == "tok-123"
            # The auth manager is never consulted when a token is supplied explicitly.
            get_auth_manager.assert_not_called()

    def test_mints_via_auth_manager(self, monkeypatch):
        monkeypatch.delenv("AIRFLOW_CLI_TOKEN", raising=False)
        auth_manager = mock.MagicMock()
        auth_manager.get_cli_user.return_value = "cli-user"
        auth_manager.generate_jwt.return_value = "signed-jwt"
        with mock.patch("airflow.api_fastapi.app.get_auth_manager", return_value=auth_manager):
            assert cli_api_client._mint_cli_token() == "signed-jwt"

        auth_manager.generate_jwt.assert_called_once()
        assert auth_manager.generate_jwt.call_args.args[0] == "cli-user"
        # Token must be short-lived.
        assert auth_manager.generate_jwt.call_args.kwargs["expiration_time_in_seconds"] > 0

    def test_initializes_auth_manager_when_not_initialized(self, monkeypatch):
        # In the CLI the auth manager singleton is usually not initialized yet, so
        # ``get_auth_manager`` raises and we must initialize it on demand.
        monkeypatch.delenv("AIRFLOW_CLI_TOKEN", raising=False)
        auth_manager = mock.MagicMock()
        auth_manager.get_cli_user.return_value = "cli-user"
        auth_manager.generate_jwt.return_value = "signed-jwt"
        with (
            mock.patch(
                "airflow.api_fastapi.app.get_auth_manager",
                side_effect=RuntimeError("Auth Manager has not been initialized yet."),
            ),
            mock.patch(
                "airflow.api_fastapi.app.init_auth_manager", return_value=auth_manager
            ) as init_auth_manager,
        ):
            assert cli_api_client._mint_cli_token() == "signed-jwt"

        init_auth_manager.assert_called_once()
        auth_manager.generate_jwt.assert_called_once()


class TestGetCliApiClient:
    def test_builds_singleton(self):
        with (
            mock.patch.object(cli_api_client, "_resolve_base_url", return_value="http://h:8080"),
            mock.patch.object(cli_api_client, "_mint_cli_token", return_value="tok"),
            mock.patch.object(cli_api_client, "Client") as client_cls,
        ):
            first = cli_api_client.get_cli_api_client()
            second = cli_api_client.get_cli_api_client()

        assert first is second
        client_cls.assert_called_once()
        kwargs = client_cls.call_args.kwargs
        assert kwargs["base_url"] == "http://h:8080"
        assert kwargs["token"] == "tok"
        assert kwargs["kind"] == cli_api_client.ClientKind.CLI


class TestProvideApiClient:
    def test_injects_when_missing(self):
        with mock.patch.object(cli_api_client, "get_cli_api_client", return_value="CLIENT"):

            @cli_api_client.provide_api_client
            def command(args, api_client=None):
                return api_client

            assert command("args") == "CLIENT"

    def test_uses_explicit_client(self):
        with mock.patch.object(cli_api_client, "get_cli_api_client") as get_client:

            @cli_api_client.provide_api_client
            def command(args, api_client=None):
                return api_client

            assert command("args", api_client="EXPLICIT") == "EXPLICIT"
            get_client.assert_not_called()


class TestGetCliUser:
    def test_base_default_raises(self):
        # The generic auth manager cannot know which user is authorized for the CLI.
        with pytest.raises(NotImplementedError, match="AIRFLOW_CLI_TOKEN"):
            BaseAuthManager.get_cli_user(mock.Mock())

    def test_simple_auth_manager_returns_admin(self):
        user = SimpleAuthManager.get_cli_user(mock.Mock())
        assert user.get_id() == "cli"
        assert user.role == "ADMIN"
