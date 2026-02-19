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

import io
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
        with (
            patch("sys.stdin", io.StringIO("test_password")),
            patch("airflowctl.ctl.cli_config.getpass.getpass", return_value="test_password"),
        ):
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
                    ]
                ),
                api_client=api_client,
            )
            mock_keyring.set_password.assert_has_calls(
                [
                    mock.call("airflowctl", "api_token_production", ""),
                    mock.call("airflowctl", "api_token_production", "TEST_TOKEN"),
                ]
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
        with (
            patch("sys.stdin", io.StringIO("test_password")),
            patch("airflowctl.ctl.cli_config.getpass.getpass", return_value="test_password"),
            pytest.raises(SystemExit, match="1"),
        ):
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
