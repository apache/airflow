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
from unittest.mock import Mock, call, patch

import pytest

from airflow.api_fastapi.common.types import MenuItem
from airflow.cli import cli_parser
from airflow.providers.keycloak.auth_manager.cli.commands import (
    TEAM_SCOPED_RESOURCE_NAMES,
    _get_extended_resource_methods,
    _get_resource_methods,
    add_user_to_team_command,
    create_all_command,
    create_permissions_command,
    create_resources_command,
    create_scopes_command,
    create_team_command,
)
from airflow.providers.keycloak.auth_manager.resources import KeycloakResource

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS


@pytest.mark.db_test
class TestCommands:
    @pytest.fixture(autouse=True)
    def setup_parser(self):
        if AIRFLOW_V_3_2_PLUS:
            importlib.reload(cli_parser)
            self.arg_parser = cli_parser.get_parser()
        else:
            with conf_vars(
                {
                    (
                        "core",
                        "auth_manager",
                    ): "airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager",
                }
            ):
                importlib.reload(cli_parser)
                self.arg_parser = cli_parser.get_parser()

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
        scopes = [{"name": method} for method in _get_resource_methods()]
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
                ("core", "multi_team"): "True",
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
    def test_create_resources_with_teams(self, mock_get_client):
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
            "--teams",
            "team-a",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            create_resources_command(self.arg_parser.parse_args(params))

        expected_team_resources = {f"{name}:team-a" for name in TEAM_SCOPED_RESOURCE_NAMES}
        created_resource_names = {
            call.kwargs["payload"]["name"]
            for call in client.create_client_authz_resource.mock_calls
            if "payload" in call.kwargs
        }
        assert expected_team_resources.issubset(created_resource_names)

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
        client.connection = Mock()
        client.connection.raw_get = Mock(return_value=Mock(text="[]"))
        client.connection.realm_name = "test-realm"
        client.get_realm_role.return_value = {"id": "role-id"}

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
                ("core", "multi_team"): "True",
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

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_permissions_with_teams(self, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [
            {"id": "1", "name": "GET"},
            {"id": "2", "name": "MENU"},
            {"id": "3", "name": "LIST"},
        ]
        resources = [
            {"_id": "r1", "name": "Dag:team-a"},
            {"_id": "r2", "name": "Connection:team-a"},
            {"_id": "r3", "name": "Pool:team-a"},
            {"_id": "r4", "name": "Variable:team-a"},
            {"_id": "r5", "name": "View"},
            {"_id": "r6", "name": "Dag"},
            {"_id": "r7", "name": "Connection"},
            {"_id": "r8", "name": "Pool"},
            {"_id": "r9", "name": "Variable"},
            {"_id": "r10", "name": "Asset"},
            {"_id": "r11", "name": "AssetAlias"},
            {"_id": "r12", "name": "Configuration"},
        ]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes
        client.get_client_authz_resources.return_value = resources
        client.connection = Mock()
        client.connection.raw_get = Mock(return_value=Mock(text="[]"))
        client.connection.realm_name = "test-realm"
        client.get_realm_role.return_value = {"id": "role-id"}

        params = [
            "keycloak-auth-manager",
            "create-permissions",
            "--username",
            "test",
            "--password",
            "test",
            "--teams",
            "team-a",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            create_permissions_command(self.arg_parser.parse_args(params))

        client.create_client_authz_scope_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "Admin-team-a",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "scopes": ["1", "2", "3"],
                "resources": ["r1", "r2", "r3", "r4"],
            },
        )
        client.create_client_authz_scope_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "GlobalList",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "scopes": ["3"],
                "resources": ["r6", "r7", "r8", "r9", "r10", "r11", "r12"],
            },
        )
        client.create_client_authz_scope_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "ViewAccess",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "scopes": ["1"],
                "resources": ["r5"],
            },
        )
        client.create_client_authz_scope_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "ReadOnly-team-a",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "scopes": ["1", "3"],
                "resources": ["r1"],
            },
        )
        client.create_client_authz_resource_based_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "User-team-a",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "resources": ["r1"],
            },
            skip_exists=True,
        )

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._update_admin_permission_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_scope_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_policy_to_resource_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_policy_to_scope_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_aggregate_policy")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_group_policy")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_group")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_create_team_command(
        self,
        mock_get_client,
        mock_ensure_group,
        mock_create_resources,
        mock_create_permissions,
        mock_ensure_group_policy,
        mock_ensure_aggregate_policy,
        mock_attach_policy,
        mock_attach_resource_policy,
        mock_ensure_scope_permission,
        mock_update_admin_permission_resources,
    ):
        client = Mock()
        mock_get_client.return_value = client
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.connection = Mock()
        client.connection.raw_get = Mock(return_value=Mock(text="[]"))
        client.connection.raw_post = Mock(
            return_value=Mock(status_code=201, json=Mock(return_value={"message": ""}), text="{}")
        )
        client.connection.realm_name = "test-realm"

        params = [
            "keycloak-auth-manager",
            "create-team",
            "team-a",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            create_team_command(self.arg_parser.parse_args(params))

        mock_ensure_group.assert_called_once_with(client, "team-a", _dry_run=False)
        mock_create_resources.assert_called_once_with(client, "test-id", teams=["team-a"], _dry_run=False)
        mock_create_permissions.assert_called_once_with(
            client, "test-id", teams=["team-a"], include_global_admin=False, _dry_run=False
        )
        mock_update_admin_permission_resources.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_ensure_group_policy.assert_called_once_with(client, "test-id", "team-a", _dry_run=False)
        assert mock_ensure_aggregate_policy.call_count == 4
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="ReadOnly-team-a",
            policy_name="Allow-Viewer-team-a",
            scope_names=["GET", "LIST"],
            resource_names=["Dag:team-a"],
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="Admin-team-a",
            policy_name="Allow-Admin-team-a",
            scope_names=_get_extended_resource_methods() + ["LIST"],
            resource_names=[
                "Connection:team-a",
                "Dag:team-a",
                "Pool:team-a",
                "Variable:team-a",
            ],
            _dry_run=False,
        )
        mock_attach_resource_policy.assert_any_call(
            client,
            "test-id",
            permission_name="User-team-a",
            policy_name="Allow-User-team-a",
            resource_names=["Dag:team-a"],
            _dry_run=False,
        )
        mock_attach_resource_policy.assert_any_call(
            client,
            "test-id",
            permission_name="Op-team-a",
            policy_name="Allow-Op-team-a",
            resource_names=[
                "Connection:team-a",
                "Pool:team-a",
                "Variable:team-a",
            ],
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="MenuAccess-team-a",
            policy_name="Allow-Viewer-team-a",
            scope_names=["MENU"],
            resource_names=["Assets", "Dags", "Docs"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="MenuAccess-Admin-team-a",
            policy_name="Allow-Admin-team-a",
            scope_names=["MENU"],
            resource_names=["Assets", "Connections", "Dags", "Docs", "Pools", "Variables", "XComs"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="Admin",
            policy_name="Allow-SuperAdmin",
            scope_names=_get_extended_resource_methods() + ["LIST"],
            resource_names=[
                "Connection:team-a",
                "Dag:team-a",
                "Pool:team-a",
                "Variable:team-a",
            ],
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="MenuAccess",
            policy_name="Allow-SuperAdmin",
            scope_names=["MENU"],
            resource_names=[item.value for item in sorted(MenuItem, key=lambda item: item.value)],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="ViewAccess",
            policy_name="Allow-Viewer-team-a",
            scope_names=["GET"],
            resource_names=["View"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_ensure_scope_permission.assert_any_call(
            client,
            "test-id",
            name="MenuAccess-team-a",
            scope_names=["MENU"],
            resource_names=["Assets", "Dags", "Docs"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_ensure_scope_permission.assert_any_call(
            client,
            "test-id",
            name="MenuAccess-Admin-team-a",
            scope_names=["MENU"],
            resource_names=["Assets", "Connections", "Dags", "Docs", "Pools", "Variables", "XComs"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._add_user_to_group")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_group")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    def test_add_user_to_team_command(self, mock_get_client, mock_ensure_group, mock_add_user):
        client = Mock()
        mock_get_client.return_value = client
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]

        params = [
            "keycloak-auth-manager",
            "add-user-to-team",
            "user-a",
            "team-a",
            "--username",
            "admin",
            "--password",
            "admin",
        ]
        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            add_user_to_team_command(self.arg_parser.parse_args(params))

        mock_ensure_group.assert_called_once_with(client, "team-a", _dry_run=False)
        mock_add_user.assert_called_once_with(client, username="user-a", team="team-a", _dry_run=False)

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
        client.connection = Mock()
        client.connection.raw_get = Mock(return_value=Mock(text="[]"))
        client.connection.raw_post = Mock(
            return_value=Mock(status_code=201, json=Mock(return_value={"message": ""}), text="{}")
        )
        client.connection.realm_name = "test-realm"

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
        mock_create_scopes.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_create_resources.assert_called_once_with(client, "test-id", teams=[], _dry_run=False)
        mock_create_permissions.assert_called_once_with(client, "test-id", teams=[], _dry_run=False)

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
        client.connection = Mock()
        client.connection.raw_get = Mock(return_value=Mock(text="[]"))
        client.connection.raw_post = Mock(
            return_value=Mock(status_code=201, json=Mock(return_value={"message": ""}), text="{}")
        )
        client.connection.realm_name = "test-realm"

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
        mock_create_scopes.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_create_resources.assert_called_once_with(client, "test-id", teams=[], _dry_run=True)
        mock_create_permissions.assert_called_once_with(client, "test-id", teams=[], _dry_run=True)
