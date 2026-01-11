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
