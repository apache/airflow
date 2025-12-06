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

import importlib
from typing import get_args
from unittest.mock import Mock, call, patch

import pytest

from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
from airflow.api_fastapi.common.types import MenuItem
from airflow.cli import cli_parser
from airflow.providers.keycloak.auth_manager.cli.commands import (
    create_all_command,
    create_permissions_command,
    create_resources_command,
    create_scopes_command,
)
from airflow.providers.keycloak.auth_manager.resources import KeycloakResource

from tests_common.test_utils.config import conf_vars


@pytest.mark.db_test
class TestCommands:
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

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_scopes(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]

        params = [
            "keycloak-auth-manager",
            "create-scopes",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_scopes_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        scopes = [{"name": method} for method in get_args(ResourceMethod)]
        calls = [call(client_id="test-id", payload=scope) for scope in scopes]
        client.create_client_authz_scopes.assert_has_calls(calls)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_scopes_with_client_not_found(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
        ]

        params = [
            "keycloak-auth-manager",
            "create-scopes",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            with pytest.raises(ValueError, match="Client with ID='test_client_id' not found in realm"):
                create_scopes_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        client.create_client_authz_scopes.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_resources(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [{"id": "1", "name": "GET"}, {"id": "2", "name": "MENU"}]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes

        params = [
            "keycloak-auth-manager",
            "create-resources",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_resources_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        client.get_client_authz_scopes.assert_called_once_with("test-id")
        calls = []
        for resource in KeycloakResource:
            calls.append(
                call(
                    client_id="test-id",
                    payload={
                        "name": resource.value,
                        "scopes": [{"id": "1", "name": "GET"}],
                    },
                    skip_exists=True,
                )
            )
        client.create_client_authz_resource.assert_has_calls(calls)

        calls = []
        for item in MenuItem:
            calls.append(
                call(
                    client_id="test-id",
                    payload={
                        "name": item.value,
                        "scopes": [{"id": "2", "name": "MENU"}],
                    },
                    skip_exists=True,
                )
            )
        client.create_client_authz_resource.assert_has_calls(calls)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_permissions(
        self,
        mock_get_client,
    ):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [{"id": "1", "name": "GET"}, {"id": "2", "name": "MENU"}, {"id": "3", "name": "LIST"}]
        resources = [
            {"_id": "r1", "name": "Dag"},
            {"_id": "r2", "name": "Asset"},
            {"_id": "r3", "name": "Connection"},
        ]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes
        client.get_client_authz_resources.return_value = resources

        params = [
            "keycloak-auth-manager",
            "create-permissions",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_permissions_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        client.get_client_authz_scopes.assert_called_once_with("test-id")
        client.get_client_authz_resources.assert_called_once_with("test-id")

        # Verify scope-based permissions are created with correct payloads
        scope_calls = [
            call(
                client_id="test-id",
                payload={
                    "name": "ReadOnly",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "UNANIMOUS",
                    "scopes": ["1", "2", "3"],  # GET, MENU, LIST
                },
            ),
            call(
                client_id="test-id",
                payload={
                    "name": "Admin",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "UNANIMOUS",
                    "scopes": ["1", "2", "3"],  # GET, MENU, LIST (only these exist in mock)
                },
            ),
        ]
        client.create_client_authz_scope_permission.assert_has_calls(scope_calls, any_order=True)

        # Verify resource-based permissions are created with correct payloads
        resource_calls = [
            call(
                client_id="test-id",
                payload={
                    "name": "User",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "UNANIMOUS",
                    "resources": ["r1", "r2"],  # Dag, Asset
                },
                skip_exists=True,
            ),
            call(
                client_id="test-id",
                payload={
                    "name": "Op",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "UNANIMOUS",
                    "resources": ["r3"],  # Connection
                },
                skip_exists=True,
            ),
        ]
        client.create_client_authz_resource_based_permission.assert_has_calls(resource_calls, any_order=True)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_scopes")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_all(
        self,
        mock_get_client,
        mock_create_scopes,
        mock_create_resources,
        mock_create_permissions,
    ):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [{"id": "1", "name": "GET"}]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes

        params = [
            "keycloak-auth-manager",
            "create-all",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_all_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        mock_create_scopes.assert_called_once_with(client, "test-id", False)
        mock_create_resources.assert_called_once_with(client, "test-id", False)
        mock_create_permissions.assert_called_once_with(client, "test-id", False)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_scopes_dry_run(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]

        params = [
            "keycloak-auth-manager",
            "create-scopes",
            "--username",
            "test",
            "--password",
            "test",
            "--dry-run",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_scopes_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        # In dry-run mode, no scopes should be created
        client.create_client_authz_scopes.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_resources_dry_run(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [{"id": "1", "name": "GET"}, {"id": "2", "name": "MENU"}]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes

        params = [
            "keycloak-auth-manager",
            "create-resources",
            "--username",
            "test",
            "--password",
            "test",
            "--dry-run",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_resources_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        client.get_client_authz_scopes.assert_called_once_with("test-id")
        # In dry-run mode, no resources should be created
        client.create_client_authz_resource.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_permissions_dry_run(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [{"id": "1", "name": "GET"}, {"id": "2", "name": "MENU"}, {"id": "3", "name": "LIST"}]
        resources = [
            {"_id": "r1", "name": "Dag"},
            {"_id": "r2", "name": "Asset"},
        ]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes
        client.get_client_authz_resources.return_value = resources

        params = [
            "keycloak-auth-manager",
            "create-permissions",
            "--username",
            "test",
            "--password",
            "test",
            "--dry-run",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_permissions_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        client.get_client_authz_scopes.assert_called_once_with("test-id")
        client.get_client_authz_resources.assert_called_once_with("test-id")
        # In dry-run mode, no permissions should be created
        client.create_client_authz_scope_permission.assert_not_called()
        client.create_client_authz_resource_based_permission.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_scopes")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_all_dry_run(
        self,
        mock_get_client,
        mock_create_scopes,
        mock_create_resources,
        mock_create_permissions,
    ):
        client = Mock()
        mock_get_client.return_value = client

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]

        params = [
            "keycloak-auth-manager",
            "create-all",
            "--username",
            "test",
            "--password",
            "test",
            "--dry-run",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
            }
        ):
            create_all_command(self.arg_parser.parse_args(params))

        client.get_clients.assert_called_once_with()
        # In dry-run mode, all helper functions should be called with dry_run=True
        mock_create_scopes.assert_called_once_with(client, "test-id", True)
        mock_create_resources.assert_called_once_with(client, "test-id", True)
        mock_create_permissions.assert_called_once_with(client, "test-id", True)
