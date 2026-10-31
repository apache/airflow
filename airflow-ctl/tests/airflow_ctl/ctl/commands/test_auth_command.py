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

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.auth_generated import LoginResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import auth_command


class TestCliAuthCommands:
    parser = cli_parser.get_parser()
    login_response = LoginResponse(
        access_token="TEST_TOKEN",
    )

    @pytest.mark.parametrize(
        ("cli_args", "expected_api_url"),
        [
            ([], "http://localhost:18080"),
            (["--api-url", "http://localhost:19090"], "http://localhost:19090"),
        ],
    )
    @patch.dict(
        os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN", "AIRFLOW_CLI_API_URL": "http://localhost:18080"}
    )
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_AUTH_LOGIN_API_URL"})
    @patch("airflowctl.api.client.keyring")
    def test_login_api_url_resolution(self, mock_keyring, cli_args, expected_api_url, monkeypatch):
        with tempfile.TemporaryDirectory() as temp_airflow_home:
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)
            mock_keyring.get_password.return_value = None

            auth_command.login(
                self.parser.parse_args(["auth", "login", *cli_args]), api_client=mock.MagicMock()
            )

            with open(os.path.join(temp_airflow_home, "TEST_AUTH_LOGIN_API_URL.json")) as f:
                assert json.load(f) == {"api_url": expected_api_url}

    @pytest.mark.parametrize(
        ("cli_args", "expected_api_url"),
        [
            ([], "http://localhost:18080"),
            (["--api-url", "http://localhost:19090"], "http://localhost:19090"),
        ],
    )
    @patch.dict(os.environ, {"AIRFLOW_CLI_API_URL": "http://localhost:18080"})
    def test_get_token_api_url_resolution(self, cli_args, expected_api_url):
        api_client = mock.MagicMock()
        api_client.login.login_with_username_and_password.return_value = self.login_response

        auth_command.get_token(
            self.parser.parse_args(["auth", "token", "--username", "u", "--password", "p", *cli_args]),
            api_client=api_client,
        )

        api_client.refresh_base_url.assert_called_once_with(base_url=expected_api_url, kind=ClientKind.AUTH)

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


class TestCliAuthToken:
    parser = cli_parser.get_parser()
    token_args = ["auth", "token", "--api-url", "http://localhost:8080"]

    @staticmethod
    def _stdin(is_tty: bool):
        stdin = mock.Mock(spec_set=["isatty"])
        stdin.isatty.return_value = is_tty
        return stdin

    def _api_client(self, api_client_maker, status_code: int = 201, response_json: dict | None = None):
        return api_client_maker(
            path="/auth/token/cli",
            response_json=response_json or LoginResponse(access_token="TEST_TOKEN").model_dump(),
            expected_http_status_code=status_code,
            kind=ClientKind.AUTH,
        )

    @patch("airflowctl.ctl.commands.auth_command.getpass.getpass", return_value="prompted_pass")
    @patch("builtins.input", return_value="prompted_user")
    def test_token_prompts_on_stderr_interactively(self, mock_input, mock_getpass, api_client_maker, capsys):
        with patch("sys.stdin", self._stdin(is_tty=True)):
            auth_command.get_token(
                self.parser.parse_args(self.token_args),
                api_client=self._api_client(api_client_maker),
            )

        mock_input.assert_called_once_with()
        mock_getpass.assert_called_once_with("Password: ")
        captured = capsys.readouterr()
        assert captured.out == "TEST_TOKEN\n"
        assert captured.err == "Username: "

    @pytest.mark.parametrize(
        "credential_args",
        [
            pytest.param([], id="no-credentials"),
            pytest.param(["--password", "pass"], id="password-only"),
        ],
    )
    @pytest.mark.parametrize("closed_stdin", [False, True], ids=["non-tty-stdin", "closed-stdin"])
    @patch("airflowctl.ctl.commands.auth_command.getpass.getpass")
    @patch("builtins.input")
    def test_token_missing_username_non_interactive_exits(
        self, mock_input, mock_getpass, closed_stdin, credential_args, api_client_maker, capsys
    ):
        stdin = None if closed_stdin else self._stdin(is_tty=False)
        with patch("sys.stdin", stdin), pytest.raises(SystemExit) as exc_info:
            auth_command.get_token(
                self.parser.parse_args([*self.token_args, *credential_args]),
                api_client=self._api_client(api_client_maker),
            )

        assert exc_info.value.code == 1
        mock_input.assert_not_called()
        mock_getpass.assert_not_called()
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "No username provided." in captured.err
        assert "--username" in captured.err

    @patch("airflowctl.ctl.commands.auth_command.getpass.getpass", return_value="piped_pass")
    def test_token_reads_piped_password_non_interactive(self, mock_getpass, api_client_maker, capsys):
        with patch("sys.stdin", self._stdin(is_tty=False)):
            auth_command.get_token(
                self.parser.parse_args([*self.token_args, "--username", "user"]),
                api_client=self._api_client(api_client_maker),
            )

        mock_getpass.assert_called_once_with("Password: ")
        assert capsys.readouterr().out == "TEST_TOKEN\n"

    @patch("airflowctl.ctl.commands.auth_command.getpass.getpass", side_effect=EOFError)
    def test_token_missing_password_non_interactive_exits(self, mock_getpass, api_client_maker, capsys):
        with patch("sys.stdin", self._stdin(is_tty=False)), pytest.raises(SystemExit) as exc_info:
            auth_command.get_token(
                self.parser.parse_args([*self.token_args, "--username", "user"]),
                api_client=self._api_client(api_client_maker),
            )

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert captured.out == ""
        assert "No password provided." in captured.err
        assert "--password" in captured.err

    def test_token_failure_reported_on_stderr(self, api_client_maker, capsys):
        with (
            patch("sys.stdin", self._stdin(is_tty=False)),
            pytest.raises(SystemExit) as exc_info,
        ):
            auth_command.get_token(
                self.parser.parse_args([*self.token_args, "--username", "user", "--password", "wrong"]),
                api_client=self._api_client(
                    api_client_maker, status_code=401, response_json={"detail": "Invalid credentials"}
                ),
            )

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Token generation failed" in captured.err
        assert "Token generation failed" not in captured.out


class TestListEnvs:
    parser = cli_parser.get_parser()

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

    @pytest.mark.parametrize("output_format", ["json", "yaml", "plain"])
    def test_list_envs_output_format(self, monkeypatch, output_format):
        with (
            tempfile.TemporaryDirectory() as temp_airflow_home,
            patch("keyring.get_password") as mock_get_password,
        ):
            monkeypatch.setenv("AIRFLOW_HOME", temp_airflow_home)

            # Create a config file
            with open(os.path.join(temp_airflow_home, "production.json"), "w") as f:
                json.dump({"api_url": "http://localhost:8080"}, f)

            mock_get_password.return_value = "test_token"

            args = self.parser.parse_args(["auth", "list-envs", "--output", output_format])
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
