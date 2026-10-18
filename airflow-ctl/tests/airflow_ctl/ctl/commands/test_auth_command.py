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
import threading
from unittest import mock
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.api.datamodels.auth_generated import LoginResponse
from airflowctl.api.operations import LoginOperations
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import auth_command
from airflowctl.utils.browser_login import LoopbackCallbackServer, compute_code_challenge


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


class TestLoginWithBrowser:
    """``auth login --web``: the browser is simulated by calling back into the loopback listener."""

    parser = cli_parser.get_parser()
    api_url = "http://localhost:8080"
    token_response = {"access_token": "WEB_TOKEN", "expires_in_seconds": 3600}

    @pytest.fixture(autouse=True)
    def isolate(self, monkeypatch, tmp_path):
        monkeypatch.setenv("AIRFLOW_HOME", str(tmp_path))
        monkeypatch.delenv("AIRFLOW_CLI_ENVIRONMENT", raising=False)
        monkeypatch.delenv("AIRFLOW_CLI_TOKEN", raising=False)
        monkeypatch.delenv("SSH_CONNECTION", raising=False)
        monkeypatch.delenv("SSH_TTY", raising=False)
        # Never wait the real five minutes, and never open a real browser.
        monkeypatch.setattr(auth_command, "BROWSER_LOGIN_TIMEOUT_SECONDS", 2)
        with patch.object(auth_command.webbrowser, "open", return_value=True) as self.open_browser:
            yield

    def make_client(self, has_feature: bool = True, exchange=None) -> Client:
        """Answer the feature probe and the code exchange like an API server with or without the feature."""
        opened = self.opened

        def default_exchange(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert body["code"] == "one-time-code"
            assert compute_code_challenge(body["code_verifier"]) == opened["code_challenge"]
            assert body["port"] == int(opened["port"])
            return httpx.Response(200, json=self.token_response)

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v2/auth/cli/token":
                if not has_feature:
                    # An older server has only its catch-all route for the path, which refuses a POST.
                    return httpx.Response(405, json={"detail": "Method Not Allowed"})
                if json.loads(request.content) == {}:
                    return httpx.Response(
                        422, json={"detail": [{"type": "missing", "loc": ["body", "code"]}]}
                    )
                return (exchange or default_exchange)(request)
            return httpx.Response(404, json={"detail": "Not Found"})

        return Client(
            base_url="test://server",
            transport=httpx.MockTransport(handle_request),
            token="",
            kind=ClientKind.AUTH,
        )

    opened: dict[str, str] = {}

    def browser(self, approve: bool = True, state: str | None = None):
        """Record what airflowctl opened and answer like a user who approved or denied in the UI."""
        self.opened = {}

        def open_browser(url: str) -> bool:
            self.opened.update({key: values[0] for key, values in parse_qs(urlsplit(url).query).items()})
            self.opened["url"] = url
            callback_state = state if state is not None else self.opened["state"]
            params = (
                {"code": "one-time-code", "state": callback_state}
                if approve
                else {"error": "access_denied", "state": callback_state}
            )
            threading.Thread(
                target=lambda: httpx.get(f"http://127.0.0.1:{self.opened['port']}/callback", params=params),
                daemon=True,
            ).start()
            return True

        self.open_browser.side_effect = open_browser

    def login(self, *extra_args: str, api_client: Client) -> None:
        auth_command.login(
            self.parser.parse_args(["auth", "login", "--api-url", self.api_url, "--web", *extra_args]),
            api_client=api_client,
        )

    @patch("airflowctl.api.client.keyring")
    def test_saves_the_token_the_browser_flow_produced(self, mock_keyring, tmp_path):
        self.browser(approve=True)

        self.login(api_client=self.make_client())

        assert self.opened["url"].startswith(f"{self.api_url}/cli/authorize?")
        assert self.opened["code_challenge_method"] == "S256"
        assert 1024 <= int(self.opened["port"]) <= 65535
        mock_keyring.set_password.assert_called_once_with("airflowctl", "api_token_production", "WEB_TOKEN")
        with open(tmp_path / "production.json") as f:
            assert json.load(f) == {"api_url": self.api_url}

    @patch("airflowctl.api.client.keyring")
    def test_exits_when_the_login_is_denied_in_the_browser(self, mock_keyring):
        self.browser(approve=False)

        with pytest.raises(SystemExit, match="1"):
            self.login(api_client=self.make_client())

        mock_keyring.set_password.assert_not_called()

    @patch("airflowctl.api.client.keyring")
    def test_ignores_a_callback_for_another_login_and_times_out(self, mock_keyring):
        self.browser(approve=True, state="someone-elses-state")

        with pytest.raises(SystemExit, match="1"):
            self.login(api_client=self.make_client())

        mock_keyring.set_password.assert_not_called()

    @patch("airflowctl.api.client.keyring")
    def test_reports_a_failed_exchange(self, mock_keyring):
        self.browser(approve=True)

        with pytest.raises(SystemExit, match="1"):
            self.login(
                api_client=self.make_client(
                    exchange=lambda request: httpx.Response(400, json={"detail": "The code has expired"})
                )
            )

        mock_keyring.set_password.assert_not_called()

    @pytest.mark.parametrize(
        "extra_args",
        [
            pytest.param(["--api-token", "TOKEN"], id="api-token"),
            pytest.param(["--username", "jdoe"], id="username"),
            pytest.param(["--password", "secret"], id="password"),
            pytest.param(["--skip-keyring"], id="skip-keyring"),
        ],
    )
    def test_rejects_options_that_do_not_combine_with_web(self, extra_args):
        with pytest.raises(SystemExit, match="1"):
            self.login(*extra_args, api_client=self.make_client())

        self.open_browser.assert_not_called()

    def test_stops_on_a_server_without_the_feature(self):
        with pytest.raises(SystemExit, match="1"):
            self.login(api_client=self.make_client(has_feature=False))

        self.open_browser.assert_not_called()

    def test_stops_when_the_server_cannot_be_reached(self):
        with (
            patch.object(
                LoginOperations,
                "supports_browser_login",
                side_effect=httpx.ConnectError("connection refused"),
            ),
            pytest.raises(SystemExit, match="1"),
        ):
            self.login(api_client=self.make_client())

        self.open_browser.assert_not_called()

    def test_stops_in_an_ssh_session(self, monkeypatch):
        monkeypatch.setenv("SSH_CONNECTION", "10.0.0.1 50000 10.0.0.2 22")

        with pytest.raises(SystemExit, match="1"):
            self.login(api_client=self.make_client())

        self.open_browser.assert_not_called()

    def test_stops_when_no_browser_can_be_opened(self):
        self.open_browser.return_value = False

        with pytest.raises(SystemExit, match="1"):
            self.login(api_client=self.make_client())

    def test_stops_when_cancelled_from_the_terminal(self):
        with (
            patch.object(LoopbackCallbackServer, "wait_for_callback", side_effect=KeyboardInterrupt),
            pytest.raises(SystemExit, match="1"),
        ):
            self.login(api_client=self.make_client())
