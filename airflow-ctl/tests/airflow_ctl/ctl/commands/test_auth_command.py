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
import tempfile
from unittest import mock
from unittest.mock import patch

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.api.datamodels.auth_generated import LoginResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import auth_command
from airflowctl.ctl.utils.browser_login import AuthorizationCode

SSO_METADATA = {
    "grant_types_supported": ["password", "refresh_token", "authorization_code"],
    "code_challenge_methods_supported": ["S256"],
    "authorization_endpoint": "/auth/login/cli",
    "device_authorization_endpoint": None,
    "token_endpoint": "/auth/token/cli",
}
AUTHORIZATION_CODE = AuthorizationCode(
    code="code", code_verifier="verifier", redirect_uri="http://127.0.0.1:1234/callback"
)


def _make_sso_api_client(metadata: dict) -> Client:
    def handle_request(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v2/auth/cli/metadata":
            return httpx.Response(200, json=metadata)
        assert str(request.url) == "http://localhost:8080/auth/token/cli"
        assert json.loads(request.content)["code_verifier"] == "verifier"
        return httpx.Response(201, json={"access_token": "SSO_TOKEN"})

    return Client(
        base_url="test://server",
        transport=httpx.MockTransport(handle_request),
        token="",
        kind=ClientKind.AUTH,
    )


class TestCliAuthCommands:
    parser = cli_parser.get_parser()
    login_response = LoginResponse(
        access_token="TEST_TOKEN",
    )

    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_AUTH_LOGIN"})
    @patch("airflowctl.api.client.keyring")
    @pytest.mark.flaky(reruns=3, reruns_delay=10)
    def test_login(self, mock_keyring, api_client_maker, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_airflow_home:
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            api_client = api_client_maker(
                path="/auth/token/cli",
                response_json=self.login_response.model_dump(),
                expected_http_status_code=201,
                kind=ClientKind.AUTH,
            )

            mock_keyring.set_password = mock.MagicMock()
            mock_keyring.get_password.return_value = None
            env = "TEST_AUTH_LOGIN"

            auth_command.login(
                self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080"]),
                api_client=api_client,
            )

            config_path = os.path.join(temp_airflow_home, f"{env}.json")
            assert os.path.exists(config_path)
            with open(config_path) as f:
                assert json.load(f) == {"api_url": "http://localhost:8080"}

            mock_keyring.set_password.assert_called_once_with(
                "airflowctl", "api_token_TEST_AUTH_LOGIN", "TEST_TOKEN"
            )

    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_login_with_skip_keyring(self, mock_keyring, api_client_maker):
        from keyring.errors import NoKeyringError

        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password.side_effect = NoKeyringError("no backend")
        auth_command.login(
            self.parser.parse_args(["auth", "login", "--skip-keyring", "--api-url", "http://localhost:8080"]),
            api_client=api_client,
        )

    @patch("airflowctl.api.client.keyring")
    def test_login_without_skip_keyring_raises_on_no_keyring(self, mock_keyring, api_client_maker):
        from keyring.errors import NoKeyringError

        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password.side_effect = NoKeyringError("no backend")
        non_tty_stdin = mock.MagicMock()
        non_tty_stdin.isatty.return_value = False
        with (
            patch("sys.stdin", non_tty_stdin),
            pytest.raises(SystemExit, match="1"),
        ):
            auth_command.login(
                self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080"]),
                api_client=api_client,
            )

    # Test auth login with username and password
    @patch("airflowctl.api.client.keyring")
    def test_login_with_username_and_password(self, mock_keyring, api_client_maker):
        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password = mock.MagicMock()
        mock_keyring.get_password.return_value = None
        auth_command.login(
            self.parser.parse_args(
                [
                    "auth",
                    "login",
                    "--api-url",
                    "http://localhost:8080",
                    "--username",
                    "test_user",
                    "--password",
                    "test_password",
                ]
            ),
            api_client=api_client,
        )
        mock_keyring.set_password.assert_has_calls(
            [
                mock.call("airflowctl", "api_token_production", "TEST_TOKEN"),
            ]
        )

    @patch("airflowctl.api.client.keyring")
    def test_login_prompts_for_credentials_interactively(self, mock_keyring, api_client_maker):
        """Test that login prompts for username and password when no credentials are supplied on a TTY."""
        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password = mock.MagicMock()
        mock_keyring.get_password.return_value = None

        tty_stdin = mock.MagicMock()
        tty_stdin.isatty.return_value = True

        with (
            patch("sys.stdin", tty_stdin),
            patch("builtins.input", return_value="prompted_user"),
            patch("airflowctl.ctl.commands.auth_command.getpass.getpass", return_value="prompted_pass"),
        ):
            auth_command.login(
                self.parser.parse_args(
                    ["auth", "login", "--api-url", "http://localhost:8080", "--env", "staging"]
                ),
                api_client=api_client,
            )

        mock_keyring.set_password.assert_called_once_with("airflowctl", "api_token_staging", "TEST_TOKEN")

    @patch("airflowctl.api.client.keyring")
    def test_login_prompts_for_password_when_username_provided(self, mock_keyring, api_client_maker):
        """Test that login prompts only for password when --username is supplied but --password is not."""
        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password = mock.MagicMock()
        mock_keyring.get_password.return_value = None

        tty_stdin = mock.MagicMock()
        tty_stdin.isatty.return_value = True

        with (
            patch("sys.stdin", tty_stdin),
            patch("builtins.input") as mock_input,
            patch("airflowctl.ctl.commands.auth_command.getpass.getpass", return_value="prompted_pass"),
        ):
            auth_command.login(
                self.parser.parse_args(
                    ["auth", "login", "--api-url", "http://localhost:8080", "--username", "known_user"]
                ),
                api_client=api_client,
            )
            mock_input.assert_not_called()

        mock_keyring.set_password.assert_called_once_with("airflowctl", "api_token_production", "TEST_TOKEN")

    def test_login_no_credentials_non_interactive_exits(self, api_client_maker):
        """Test that login exits with an error when no credentials are supplied in a non-interactive context."""
        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        non_tty_stdin = mock.MagicMock()
        non_tty_stdin.isatty.return_value = False

        with (
            patch("sys.stdin", non_tty_stdin),
            pytest.raises(SystemExit, match="1"),
        ):
            auth_command.login(
                self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080"]),
                api_client=api_client,
            )

    @patch("airflowctl.api.client.keyring")
    def test_login_with_username_and_password_no_keyring_backend(self, mock_keyring, api_client_maker):
        """Test that login fails when no keyring backend is available."""
        from keyring.errors import NoKeyringError

        api_client = api_client_maker(
            path="/auth/token/cli",
            response_json=self.login_response.model_dump(),
            expected_http_status_code=201,
            kind=ClientKind.AUTH,
        )

        mock_keyring.set_password.side_effect = NoKeyringError("no backend")
        with pytest.raises(SystemExit, match="1"):
            auth_command.login(
                self.parser.parse_args(
                    [
                        "auth",
                        "login",
                        "--api-url",
                        "http://localhost:8080",
                        "--username",
                        "test_user",
                        "--password",
                        "test_password",
                    ]
                ),
                api_client=api_client,
            )


class TestListEnvs:
    parser = cli_parser.get_parser()

    @patch("airflowctl.ctl.commands.auth_command.get_authorization_code", autospec=True)
    @patch("airflowctl.api.client.keyring")
    def test_login_with_sso(self, mock_keyring, mock_get_authorization_code):
        mock_get_authorization_code.return_value = AUTHORIZATION_CODE
        mock_keyring.get_password.return_value = None

        auth_command.login(
            self.parser.parse_args(
                [
                    "auth",
                    "login",
                    "--api-url",
                    "http://localhost:8080",
                    "--sso",
                    "--no-browser",
                    "--timeout",
                    "30",
                ]
            ),
            api_client=_make_sso_api_client(SSO_METADATA),
        )

        mock_get_authorization_code.assert_called_once_with(
            api_url="http://localhost:8080",
            authorization_endpoint="/auth/login/cli",
            open_browser=False,
            timeout=30,
        )
        mock_keyring.set_password.assert_called_once_with("airflowctl", "api_token_production", "SSO_TOKEN")

    @pytest.mark.parametrize(
        "metadata",
        [
            pytest.param({**SSO_METADATA, "grant_types_supported": ["password"]}, id="no-authorization-code"),
            pytest.param({**SSO_METADATA, "code_challenge_methods_supported": ["plain"]}, id="no-s256"),
            pytest.param({**SSO_METADATA, "authorization_endpoint": None}, id="no-authorization-endpoint"),
        ],
    )
    @patch("airflowctl.ctl.commands.auth_command.get_authorization_code", autospec=True)
    def test_login_with_sso_not_offered(self, mock_get_authorization_code, metadata):
        with pytest.raises(SystemExit):
            auth_command.login(
                self.parser.parse_args(["auth", "login", "--api-url", "http://localhost:8080", "--sso"]),
                api_client=_make_sso_api_client(metadata),
            )

        mock_get_authorization_code.assert_not_called()

    @pytest.mark.parametrize(
        "extra_args",
        [["--username", "user"], ["--password", "pass"], ["--api-token", "token"], ["--skip-keyring"]],
    )
    @patch("airflowctl.ctl.commands.auth_command.get_authorization_code", autospec=True)
    def test_login_with_sso_incompatible_args(self, mock_get_authorization_code, extra_args):
        with pytest.raises(SystemExit):
            auth_command.login(
                self.parser.parse_args(
                    ["auth", "login", "--api-url", "http://localhost:8080", "--sso", *extra_args]
                ),
                api_client=_make_sso_api_client(SSO_METADATA),
            )

        mock_get_authorization_code.assert_not_called()

    @patch("airflowctl.ctl.commands.auth_command.get_authorization_code", autospec=True)
    def test_get_token_with_sso(self, mock_get_authorization_code, capsys):
        mock_get_authorization_code.return_value = AUTHORIZATION_CODE

        auth_command.get_token(
            self.parser.parse_args(["auth", "token", "--api-url", "http://localhost:8080", "--sso"]),
            api_client=_make_sso_api_client(SSO_METADATA),
        )

        assert capsys.readouterr().out == "SSO_TOKEN\n"

    def test_list_envs_empty_airflow_home(self, monkeypatch):
        """Test list-envs with no AIRFLOW_HOME directory."""
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch("keyring.get_password"),
        ):
            non_existent_dir = os.path.join(temp_dir, "non_existent")
            monkeypatch.setenv("AIRFLOW_HOME", non_existent_dir)

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_no_environments(self, monkeypatch):
        """Test list-envs with empty AIRFLOW_HOME."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password"),
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_single_authenticated(self, monkeypatch):
        """Test list-envs with a single authenticated environment."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            config_path = os.path.join(temp_airflow_home, "production.json")
            with open(config_path, "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            # Mock keyring to return a token
            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

            mock_get_password.assert_called_once_with("airflowctl", "api_token_production")

    def test_list_envs_multiple_mixed_status(self, monkeypatch):
        """Test list-envs with multiple environments with different statuses."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create authenticated environment
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            # Create not authenticated environment
            with open(os.path.join(temp_airflow_home, "staging.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8081"}, f)

            # Mock keyring to return token only for production
            def mock_get_password_func(service, key):
                if key == "api_token_production":
                    return "prod_token"
                return None

            mock_get_password.side_effect = mock_get_password_func

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_json_output(self, monkeypatch):
        """Test list-envs with JSON output format."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs", "--output", "json"])
            auth_command.list_envs(args)

    def test_list_envs_yaml_output(self, monkeypatch):
        """Test list-envs with YAML output format."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs", "--output", "yaml"])
            auth_command.list_envs(args)

    def test_list_envs_plain_output(self, monkeypatch):
        """Test list-envs with plain output format."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs", "--output", "plain"])
            auth_command.list_envs(args)

    def test_list_envs_keyring_unavailable(self, monkeypatch):
        """Test list-envs when keyring is unavailable."""
        from keyring.errors import NoKeyringError

        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.side_effect = NoKeyringError("no backend")

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_keyring_error(self, monkeypatch):
        """Test list-envs when keyring has an error."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.side_effect = ValueError("incorrect password")

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_corrupted_config(self, monkeypatch):
        """Test list-envs with corrupted config file."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password"),
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a corrupted config file
            config_path = os.path.join(temp_airflow_home, "production.json")
            with open(config_path, "w") as f:
                f.write("invalid json content {{{")

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_debug_mode(self, monkeypatch):
        """Test list-envs in debug mode."""
        with tempfile.TemporaryDirectory() as temp_airflow_home:
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)
            monkeypatch.setenv("AIRFLOW_CLI_DEBUG_MODE", "true")

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            # Create debug credentials file
            debug_creds_path = os.path.join(temp_airflow_home, "debug_creds_production.json")
            with open(debug_creds_path, "w") as f:
                json.dump({"api_token_production": "debug_token"}, f)

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

    def test_list_envs_filters_special_files(self, monkeypatch):
        """Test list-envs filters out special files."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create regular config
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            # Create files that should be filtered out
            with open(os.path.join(temp_airflow_home, "debug_creds_production.json"), "w") as f:
                json.dump({"api_token_production": "token"}, f)

            with open(os.path.join(temp_airflow_home, "some_generated.json"), "w") as f:
                json.dump({"data": "generated"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

            # Only production environment should be checked, not the special files
            mock_get_password.assert_called_once_with("airflowctl", "api_token_production")

    def test_list_envs_environment_name_with_json_substring(self, monkeypatch):
        """Test list-envs keeps '.json' substrings in environment name for key lookup."""
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            with open(os.path.join(temp_airflow_home, "prod.json.region.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs"])
            auth_command.list_envs(args)

            mock_get_password.assert_called_once_with("airflowctl", "api_token_prod.json.region")
