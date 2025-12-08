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

import argparse
import importlib
from unittest.mock import patch

import pytest

from airflow.cli import cli_parser
from airflow.providers.keycloak.auth_manager.cli.definition import KEYCLOAK_AUTH_MANAGER_COMMANDS, Password

from tests_common.test_utils.config import conf_vars


class TestKeycloakCliDefinition:
    @classmethod
    def setup_class(cls):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager",
            }
        ):
            importlib.reload(cli_parser)
            cls.arg_parser = cli_parser.get_parser()

    def test_keycloak_auth_manager_cli_commands(self):
        assert len(KEYCLOAK_AUTH_MANAGER_COMMANDS) == 4

    @pytest.mark.parametrize(
        "command",
        ["create-scopes", "create-resources", "create-permissions", "create-all"],
    )
    def test_password_with_explicit_value(self, command):
        """Test commands are defined correctly to allow passing password explicitly via --password value."""
        params = [
            "keycloak-auth-manager",
            command,
            "--username",
            "test",
            "--password",
            "my_password",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.password == "my_password"

    @pytest.mark.parametrize(
        "command",
        ["create-scopes", "create-resources", "create-permissions", "create-all"],
    )
    @patch("getpass.getpass", return_value="stdin_password")
    def test_password_from_stdin(self, mock_getpass, command):
        """Test commands are defined correctly to allow password prompting from stdin when --password has no value."""
        params = [
            "keycloak-auth-manager",
            command,
            "--username",
            "test",
            "--password",
        ]
        args = self.arg_parser.parse_args(params)
        mock_getpass.assert_called_once_with(prompt="Password: ")
        assert args.password == "stdin_password"


class TestPasswordAction:
    """Tests for the Password argparse action."""

    def test_password_with_explicit_value(self):
        """Test passing password explicitly via --password value."""

        parser = argparse.ArgumentParser()
        parser.add_argument("--password", action=Password, nargs="?", dest="password")

        args = parser.parse_args(["--password", "my_password"])
        assert args.password == "my_password"

    @patch("getpass.getpass", return_value="stdin_password")
    def test_password_from_stdin(self, mock_getpass):
        """Test password prompted from stdin when --password has no value."""

        parser = argparse.ArgumentParser()
        parser.add_argument("--password", action=Password, nargs="?", dest="password")

        args = parser.parse_args(["--password"])
        mock_getpass.assert_called_once_with(prompt="Password: ")
        assert args.password == "stdin_password"
