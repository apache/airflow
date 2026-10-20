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
import json
from unittest.mock import DEFAULT, Mock, call, patch

import pytest

from airflow.api_fastapi.common.types import MenuItem
from airflow.cli import cli_parser
from airflow.providers.keycloak.auth_manager.cli.commands import (
    SUPER_ADMIN_ROLE_NAME,
    TEAM_ROLE_NAMES,
    TEAM_SCOPED_RESOURCE_NAMES,
    _attach_default_role_permissions,
    _attach_policy_to_resource_permission,
    _attach_policy_to_scope_permission,
    _attach_superadmin_permissions,
    _attach_team_menu_permissions,
    _attach_team_permissions,
    _create_permissions,
    _ensure_resource_server_decision_strategy,
    _get_default_permission_configs,
    _get_exported_permission_names,
    _get_extended_resource_methods,
    _get_permissions_to_create,
    _get_resource_methods,
    _get_scopes_to_create,
    _preview_permission_update,
    _preview_resources,
    _update_admin_permission_resources,
    _update_read_only_permission_resources,
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

# "Dag Bundles" is granted to every team role rather than only the admin one: the page is scoped
# by the Dags a caller can read, so a Viewer who can already reach the bundles through the API has
# to be able to find them under Browse. The provider still supports Airflow versions predating the
# menu item, where ``TEAM_MENU_ITEMS`` drops it, so the expected grants follow the same guard the
# source does rather than hard-coding a name that does not exist there.
DAG_BUNDLES_MENU = ["Dag Bundles"] if hasattr(MenuItem, "DAG_BUNDLES") else []
EXPECTED_TEAM_MENU_RESOURCES = sorted(["Assets", "Dags", "Docs", *DAG_BUNDLES_MENU])
EXPECTED_TEAM_ADMIN_MENU_RESOURCES = sorted(
    ["Assets", "Connections", "Dags", "Docs", "Pools", "Variables", "XComs", *DAG_BUNDLES_MENU]
)


@pytest.mark.parametrize(
    ("permission_name", "resources", "scopes", "roles"),
    [
        (
            "PoolRead",
            ["Pool"],
            ["GET", "LIST"],
            ["Viewer", "User", "Op", "Admin", "SuperAdmin"],
        ),
        (
            "PoolWrite",
            ["Pool"],
            ["POST", "PUT", "DELETE"],
            ["Op", "Admin", "SuperAdmin"],
        ),
        (
            "DagWrite",
            ["Dag"],
            ["POST", "PUT", "DELETE"],
            ["User", "Op", "Admin", "SuperAdmin"],
        ),
        (
            "ConfigurationWrite",
            ["Configuration"],
            ["POST", "PUT", "DELETE"],
            ["Admin", "SuperAdmin"],
        ),
        (
            "DefaultMenuAccess",
            [item.value for item in MenuItem],
            ["MENU"],
            ["Viewer", "User", "Op", "Admin", "SuperAdmin"],
        ),
    ],
)
def test_get_default_permission_configs(permission_name, resources, scopes, roles):
    matches = [
        permission
        for permission in _get_default_permission_configs()
        if permission["name"] == permission_name
    ]

    assert matches == [
        {
            "name": permission_name,
            "type": "scope-based",
            "resources": resources,
            "scope_names": scopes,
            "role_names": roles,
        }
    ]


@pytest.mark.parametrize(
    ("teams", "missing_resource", "missing_scope"),
    [
        ([], "Pool", None),
        ([], None, "POST"),
        ([], "Pool", "POST"),
        (["team-a"], "Pool:team-a", None),
        (["team-a"], None, "POST"),
        (["team-a"], "Pool:team-a", "POST"),
    ],
)
def test_default_permissions_reject_missing_resources_or_scopes(teams, missing_resource, missing_scope):
    client = Mock(
        spec=[
            "get_client_authz_resources",
            "get_client_authz_scopes",
        ]
    )
    resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]

    resource_names.extend(f"{name}:{team}" for team in teams for name in TEAM_SCOPED_RESOURCE_NAMES)
    client.get_client_authz_resources.return_value = [
        {"_id": f"resource-{name}", "name": name} for name in resource_names if name != missing_resource
    ]
    client.get_client_authz_scopes.return_value = [
        {"id": name.lower(), "name": name}
        for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        if name != missing_scope
    ]

    with pytest.raises(ValueError, match="Cannot create permissions:") as exc_info:
        _get_permissions_to_create(client, "test-id", teams=teams)

    expected_resources = [missing_resource] if missing_resource else []
    expected_scopes = [missing_scope] if missing_scope else []
    expected_hint = (
        "Run create-scopes and create-resources --teams team-a first."
        if teams
        else "Run create-scopes and create-resources first."
    )

    assert str(exc_info.value) == (
        "Cannot create permissions: "
        f"missing resources={expected_resources}, "
        f"missing scopes={expected_scopes}. " + expected_hint
    )


def test_default_permission_preview_without_existing_resources(capsys):
    client = Mock(spec=["get_client_authz_permissions"])
    client.get_client_authz_permissions.return_value = []

    _create_permissions(
        client,
        "test-id",
        teams=[],
        _dry_run=True,
    )

    output = capsys.readouterr().out

    assert "PoolRead (type: scope-based, scopes: GET, LIST, resources: Pool)" in output
    assert "PoolWrite (type: scope-based, scopes: POST, PUT, DELETE, resources: Pool)" in output
    assert client.mock_calls == [
        call.get_client_authz_permissions("test-id"),
    ]


def test_default_permissions_exclude_generic_menu_resource():
    permissions = _get_default_permission_configs()

    assert all(KeycloakResource.MENU.value not in permission["resources"] for permission in permissions)


@pytest.mark.parametrize(
    ("strategy", "dry_run", "should_update"),
    [
        ("UNANIMOUS", False, True),
        ("UNANIMOUS", True, False),
        ("AFFIRMATIVE", False, False),
        ("AFFIRMATIVE", True, False),
    ],
)
@patch(
    "airflow.providers.keycloak.auth_manager.cli.commands.raise_error_from_response",
    autospec=True,
)
def test_ensure_resource_server_decision_strategy(
    mock_check_response, strategy, dry_run, should_update, capsys
):
    client = Mock(spec=["connection"])
    client.connection = Mock(spec=["realm_name", "raw_get", "raw_put"])
    client.connection.realm_name = "test-realm"

    settings = {
        "decisionStrategy": strategy,
        "policyEnforcementMode": "ENFORCING",
        "allowRemoteResourceManagement": False,
    }
    mock_check_response.return_value = settings

    _ensure_resource_server_decision_strategy(
        client,
        "test-id",
        _dry_run=dry_run,
    )

    url = "admin/realms/test-realm/clients/test-id/authz/resource-server"
    client.connection.raw_get.assert_called_once_with(url)

    if should_update:
        client.connection.raw_put.assert_called_once()
        args, kwargs = client.connection.raw_put.call_args
        assert args == (url,)
        assert json.loads(kwargs["data"]) == {
            **settings,
            "decisionStrategy": "AFFIRMATIVE",
        }
        assert mock_check_response.call_count == 2
    else:
        client.connection.raw_put.assert_not_called()
        assert mock_check_response.call_count == 1

    assert settings["decisionStrategy"] == strategy

    output = capsys.readouterr().out
    if strategy != "AFFIRMATIVE":
        action = "Would set" if dry_run else "Set"
        assert output == (
            f"{action} resource-server decision strategy to AFFIRMATIVE. "
            "This client-wide setting affects all permissions, including custom permissions. "
            "A permitting permission can override another permission's denial; "
            "review existing custom authorization rules.\n"
        )
    else:
        assert output == ""


@pytest.mark.parametrize("settings", [b"invalid settings", []], ids=["bytes", "list"])
@pytest.mark.parametrize("dry_run", [False, True])
@patch(
    "airflow.providers.keycloak.auth_manager.cli.commands.raise_error_from_response",
    autospec=True,
)
def test_ensure_resource_server_decision_strategy_rejects_invalid_settings(
    mock_check_response, settings, dry_run, capsys
):
    client = Mock(spec=["connection"])
    client.connection = Mock(spec=["realm_name", "raw_get", "raw_put"])
    client.connection.realm_name = "test-realm"
    mock_check_response.return_value = settings

    with pytest.raises(ValueError, match="Expected Keycloak resource-server settings to be a JSON object"):
        _ensure_resource_server_decision_strategy(client, "test-id", _dry_run=dry_run)

    client.connection.raw_get.assert_called_once_with(
        "admin/realms/test-realm/clients/test-id/authz/resource-server"
    )
    client.connection.raw_put.assert_not_called()
    assert mock_check_response.call_count == 1
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("include_global_admin", [True, False])
def test_team_permission_preview_without_existing_objects(include_global_admin, capsys):
    client = Mock(spec=["get_client_authz_permissions"])
    client.get_client_authz_permissions.return_value = []

    _create_permissions(
        client,
        "test-id",
        teams=["team-a"],
        include_global_admin=include_global_admin,
        _dry_run=True,
    )

    output = capsys.readouterr().out

    assert "ReadOnly-team-a (type: scope-based, scopes: GET, LIST," in output
    assert "User-team-a (type: resource-based, resources: Dag:team-a)" in output
    assert ("  - Admin (type: scope-based," in output) is include_global_admin
    assert client.mock_calls == [
        call.get_client_authz_permissions("test-id"),
    ]


@pytest.mark.parametrize("teams", [[], ["team-a"]])
def test_resource_preview_uses_planned_scopes_without_keycloak_objects(teams, capsys):
    client = Mock(spec=[])
    planned_scopes = [{"name": name} for name in ["GET", "POST", "PUT", "DELETE", "MENU", "LIST"]]

    _preview_resources(
        client,
        "test-id",
        teams=teams,
        planned_scopes=planned_scopes,
    )

    output = capsys.readouterr().out

    assert "Pool (scopes: GET, POST, PUT, DELETE, LIST)" in output
    assert "Pools (scopes: MENU)" in output
    if teams:
        assert "Pool:team-a (scopes: GET, POST, PUT, DELETE, LIST)" in output
    assert client.mock_calls == []


@pytest.mark.parametrize("all_exist", [False, True])
def test_permission_preview_separates_existing_permissions(all_exist, capsys):
    client = Mock(spec=["get_client_authz_permissions"])

    if all_exist:
        existing_names = {permission["name"] for permission in _get_default_permission_configs()}
    else:
        existing_names = {"PoolRead"}

    client.get_client_authz_permissions.return_value = [{"name": name} for name in sorted(existing_names)]

    _create_permissions(
        client,
        "test-id",
        teams=[],
        _dry_run=True,
    )

    output = capsys.readouterr().out
    creation_output, existing_output = output.split(
        "Existing permissions (creation skipped):",
        1,
    )

    assert "PoolRead (type:" not in creation_output
    assert "\n  - PoolRead\n" in existing_output

    if all_exist:
        assert "  None." in creation_output
        assert "PoolWrite (type:" not in creation_output
    else:
        assert "PoolWrite (type:" in creation_output

    assert client.mock_calls == [
        call.get_client_authz_permissions("test-id"),
    ]


@pytest.mark.parametrize(
    ("changes", "config_changes", "expected_output"),
    [
        ({}, {}, ""),
        ({}, {"applyPolicies": '["Allow-Viewer", "CustomPolicy"]'}, ""),
        ({}, {"scopes": '["LIST", "GET"]'}, ""),
        ({}, {"applyPolicies": "[]"}, "Would attach policy 'Allow-Viewer' to permission 'PoolRead'.\n"),
        (
            {"decisionStrategy": "UNANIMOUS"},
            {},
            "Would configure permission 'PoolRead': decisionStrategy=AFFIRMATIVE.\n",
        ),
        ({"logic": "NEGATIVE"}, {}, "Would configure permission 'PoolRead': logic=POSITIVE.\n"),
        ({"type": "resource"}, {}, "Would configure permission 'PoolRead': type=scope.\n"),
        ({}, {"scopes": '["GET"]'}, "Would configure permission 'PoolRead': scopes=['GET', 'LIST'].\n"),
        ({}, {"resources": "[]"}, "Would configure permission 'PoolRead': resources=['Pool'].\n"),
        (
            {},
            {"resources": '["Pool", "Dag"]'},
            "Would configure permission 'PoolRead': resources=['Pool'].\n",
        ),
        (
            {"decisionStrategy": "UNANIMOUS"},
            {"applyPolicies": "[]"},
            "Would configure permission 'PoolRead': decisionStrategy=AFFIRMATIVE.\n"
            "Would attach policy 'Allow-Viewer' to permission 'PoolRead'.\n",
        ),
    ],
)
def test_permission_update_preview_reports_only_changes(changes, config_changes, expected_output, capsys):
    permission = {
        "name": "PoolRead",
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "AFFIRMATIVE",
        "config": {
            "applyPolicies": '["Allow-Viewer"]',
            "scopes": '["GET", "LIST"]',
            "resources": '["Pool"]',
            **config_changes,
        },
        **changes,
    }

    _preview_permission_update(
        {"policies": [permission]},
        permission_name="PoolRead",
        permission_type="scope",
        policy_names=["Allow-Viewer"],
        scope_names=["GET", "LIST"],
        resource_names=["Pool"],
        decision_strategy="AFFIRMATIVE",
    )

    assert capsys.readouterr().out == expected_output


@pytest.mark.parametrize("value", ['"GET"', '{"GET": true}', '["GET", 123]', None, "invalid-json"])
def test_exported_permission_names_reject_invalid_values(value):
    with pytest.raises(ValueError, match="Invalid 'scopes' in exported permission 'PoolRead'"):
        _get_exported_permission_names({"name": "PoolRead", "config": {"scopes": value}}, "scopes")


@pytest.mark.parametrize("permission_type", ["scope", "resource"])
@pytest.mark.parametrize("exists", [False, True])
@pytest.mark.parametrize("provided_snapshot", [False, True])
def test_attachment_dry_run_uses_export_without_writes(permission_type, exists, provided_snapshot, capsys):
    client = Mock(spec=["get_client_authz_settings"])
    scope_names = ["GET", "LIST"] if permission_type == "scope" else []
    permission = {
        "name": "TestPermission",
        "type": permission_type,
        "logic": "POSITIVE",
        "decisionStrategy": "AFFIRMATIVE",
        "config": {
            "applyPolicies": '["Allow-Viewer"]',
            "resources": '["Pool"]',
            "scopes": json.dumps(scope_names),
        },
    }
    settings = {"policies": [permission] if exists else []}
    client.get_client_authz_settings.return_value = settings
    kwargs = {
        "permission_name": "TestPermission",
        "policy_name": "Allow-Viewer",
        "resource_names": ["Pool"],
        "decision_strategy": "AFFIRMATIVE",
        "_dry_run": True,
    }
    if provided_snapshot:
        kwargs["authz_settings"] = settings

    if permission_type == "scope":
        _attach_policy_to_scope_permission(client, "test-id", scope_names=scope_names, **kwargs)
    else:
        _attach_policy_to_resource_permission(client, "test-id", **kwargs)

    output = capsys.readouterr().out
    if exists:
        assert output == ""
    else:
        assert f"type={permission_type}" in output
        assert "Would attach policy 'Allow-Viewer' to permission 'TestPermission'." in output
    assert client.mock_calls == (
        [] if provided_snapshot else [call.get_client_authz_settings(client_id="test-id")]
    )


@pytest.mark.parametrize(
    "attach_permissions",
    [_attach_team_permissions, _attach_team_menu_permissions, _attach_superadmin_permissions],
)
@pytest.mark.parametrize("provided_snapshot", [False, True])
def test_team_attachment_dry_run_shares_snapshot(attach_permissions, provided_snapshot):
    client = Mock(spec=["get_client_authz_settings"])
    settings = {"policies": []}
    client.get_client_authz_settings.return_value = settings

    if provided_snapshot:
        attach_permissions(client, "test-id", "team-a", _dry_run=True, authz_settings=settings)
        assert client.mock_calls == []
    else:
        attach_permissions(client, "test-id", "team-a", _dry_run=True)
        assert client.mock_calls == [call.get_client_authz_settings(client_id="test-id")]
        attach_permissions(client, "test-id", "team-a", _dry_run=True)
        assert client.mock_calls == [call.get_client_authz_settings(client_id="test-id")] * 2


@pytest.mark.parametrize("missing_policy", [None, "Allow-Viewer"])
def test_default_attachment_preview_reads_one_snapshot(missing_policy, capsys):
    client = Mock(spec=["get_client_authz_settings"])
    permissions = []
    for config in _get_default_permission_configs():
        policies = [f"Allow-{role}" for role in config["role_names"]]
        if config["name"] == "PoolRead" and missing_policy:
            policies.remove(missing_policy)
        permissions.append(
            {
                "name": config["name"],
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "AFFIRMATIVE",
                "config": {
                    "applyPolicies": json.dumps(policies),
                    "scopes": json.dumps(config["scope_names"]),
                    "resources": json.dumps(config["resources"]),
                },
            }
        )
    client.get_client_authz_settings.return_value = {"policies": permissions}

    _attach_default_role_permissions(client, "test-id", _dry_run=True)

    expected = "Would attach policy 'Allow-Viewer' to permission 'PoolRead'.\n" if missing_policy else ""
    assert capsys.readouterr().out == expected
    assert client.mock_calls == [call.get_client_authz_settings(client_id="test-id")]


@pytest.mark.parametrize(
    ("missing_kind", "message"),
    [
        ("permission", "Permission 'PoolRead' not found"),
        ("policy", "Policy 'Allow-Viewer' not found"),
    ],
)
@patch("airflow.providers.keycloak.auth_manager.cli.commands._get_policy_id", autospec=True)
def test_default_attachment_rejects_missing_objects_before_updates(mock_get_policy_id, missing_kind, message):
    client = Mock(spec=["get_client_authz_permissions"])
    client.get_client_authz_permissions.return_value = [
        {"id": config["name"], "name": config["name"]}
        for config in _get_default_permission_configs()
        if not (missing_kind == "permission" and config["name"] == "PoolRead")
    ]
    mock_get_policy_id.return_value = None

    with pytest.raises(ValueError, match=message):
        _attach_default_role_permissions(client, "test-id")

    assert client.mock_calls == [call.get_client_authz_permissions("test-id")]
    if missing_kind == "permission":
        mock_get_policy_id.assert_not_called()
    else:
        mock_get_policy_id.assert_called_once_with(client, "test-id", "Allow-Viewer")


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

    @patch.multiple(
        "airflow.providers.keycloak.auth_manager.cli.commands",
        autospec=True,
        _get_client=DEFAULT,
        _create_resources=DEFAULT,
        _create_group_membership_mapper=DEFAULT,
        _create_permissions=DEFAULT,
        _ensure_group=DEFAULT,
        _ensure_team_policies=DEFAULT,
        _attach_team_permissions=DEFAULT,
        _attach_team_menu_permissions=DEFAULT,
        _attach_superadmin_permissions=DEFAULT,
        _update_read_only_permission_resources=DEFAULT,
        _update_admin_permission_resources=DEFAULT,
        _ensure_resource_server_decision_strategy=DEFAULT,
    )
    def test_create_team_dry_run_shares_authorization_snapshot(self, **mocked):
        client = Mock(spec=["get_clients", "get_client_authz_scopes", "get_client_authz_settings"])
        client.get_clients.return_value = [{"id": "test-id", "clientId": "test_client_id"}]
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]
        settings = {"policies": []}
        client.get_client_authz_settings.return_value = settings
        mocked["_get_client"].return_value = client
        params = [
            "keycloak-auth-manager",
            "create-team",
            "team-a",
            "--username",
            "test",
            "--password",
            "test",
            "--dry-run",
        ]

        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            create_team_command(self.arg_parser.parse_args(params))

        client.get_client_authz_settings.assert_called_once_with(client_id="test-id")
        for name in [
            "_attach_team_permissions",
            "_attach_team_menu_permissions",
            "_attach_superadmin_permissions",
        ]:
            mocked[name].assert_called_once_with(
                client, "test-id", "team-a", _dry_run=True, authz_settings=settings
            )

    @pytest.mark.parametrize("missing_scope", ["POST", "MENU"])
    @pytest.mark.parametrize("dry_run", [False, True])
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._get_client",
        autospec=True,
    )
    def test_create_team_rejects_missing_scopes_before_writes(self, mock_get_client, missing_scope, dry_run):
        client = Mock(
            spec=[
                "get_clients",
                "get_client_authz_scopes",
            ]
        )
        mock_get_client.return_value = client
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name}
            for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
            if name != missing_scope
        ]

        params = [
            "keycloak-auth-manager",
            "create-team",
            "team-a",
            "--username",
            "test",
            "--password",
            "test",
        ]
        if dry_run:
            params.append("--dry-run")

        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            with pytest.raises(ValueError, match="Cannot create permissions:"):
                create_team_command(self.arg_parser.parse_args(params))

        assert client.mock_calls == [
            call.get_clients(),
            call.get_client_authz_scopes("test-id"),
        ]

    @pytest.mark.parametrize("teams", [[], ["team-a"]])
    @pytest.mark.parametrize("missing_kind", ["resource", "scope"])
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._create_scopes",
        autospec=True,
    )
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._create_resources",
        autospec=True,
    )
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._get_client",
        autospec=True,
    )
    def test_create_all_stops_when_created_prerequisites_are_incomplete(
        self, mock_get_client, mock_create_resources, mock_create_scopes, teams, missing_kind
    ):
        client = Mock(
            spec=[
                "get_clients",
                "get_client_authz_scopes",
                "get_client_authz_resources",
            ]
        )
        mock_get_client.return_value = client
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name}
            for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
            if not (missing_kind == "scope" and name == "POST")
        ]

        resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]
        resource_names.extend(f"{name}:{team}" for team in teams for name in TEAM_SCOPED_RESOURCE_NAMES)
        missing_resource = "Pool:team-a" if teams else "Pool"
        client.get_client_authz_resources.return_value = [
            {"_id": f"resource-{name}", "name": name}
            for name in resource_names
            if not (missing_kind == "resource" and name == missing_resource)
        ]

        params = [
            "keycloak-auth-manager",
            "create-all",
            "--username",
            "test",
            "--password",
            "test",
        ]
        if teams:
            params.extend(["--teams", ",".join(teams)])

        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            with pytest.raises(ValueError, match="Cannot create permissions:"):
                create_all_command(self.arg_parser.parse_args(params))

        mock_create_scopes.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_create_resources.assert_called_once_with(client, "test-id", teams=teams, _dry_run=False)
        assert client.mock_calls == [
            call.get_clients(),
            call.get_client_authz_scopes("test-id"),
            call.get_client_authz_resources("test-id"),
        ]

    @pytest.mark.parametrize("teams", [[], ["team-a"]])
    @pytest.mark.parametrize("dry_run", [False, True])
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._get_client",
        autospec=True,
    )
    def test_create_permissions_rejects_missing_prerequisites_before_writes(
        self, mock_get_client, teams, dry_run
    ):
        client = Mock(
            spec=[
                "get_clients",
                "get_client_authz_scopes",
                "get_client_authz_resources",
            ]
        )
        mock_get_client.return_value = client
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]

        resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]
        resource_names.extend(f"{name}:{team}" for team in teams for name in TEAM_SCOPED_RESOURCE_NAMES)
        missing_resource = "Pool:team-a" if teams else "Pool"
        client.get_client_authz_resources.return_value = [
            {"_id": f"resource-{name}", "name": name} for name in resource_names if name != missing_resource
        ]

        params = [
            "keycloak-auth-manager",
            "create-permissions",
            "--username",
            "test",
            "--password",
            "test",
        ]
        if teams:
            params.extend(["--teams", ",".join(teams)])
        if dry_run:
            params.append("--dry-run")

        with conf_vars(
            {
                ("keycloak_auth_manager", "client_id"): "test_client_id",
                ("core", "multi_team"): "True",
            }
        ):
            with pytest.raises(ValueError, match="Cannot create permissions:"):
                create_permissions_command(self.arg_parser.parse_args(params))

        assert client.mock_calls == [
            call.get_clients(),
            call.get_client_authz_scopes("test-id"),
            call.get_client_authz_resources("test-id"),
        ]

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

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_default_role_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_default_role_policies")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_permissions(
        self,
        mock_ensure_strategy,
        mock_get_client,
        mock_ensure_default_role_policies,
        mock_attach_default_role_permissions,
    ):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]
        resources = [
            {"_id": f"resource-{resource.value}", "name": resource.value} for resource in KeycloakResource
        ]
        resources.extend({"_id": f"menu-{item.value}", "name": item.value} for item in MenuItem)

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
        client.get_client_authz_scopes.assert_has_calls([call("test-id"), call("test-id")])
        assert client.get_client_authz_scopes.call_count == 2
        client.get_client_authz_resources.assert_has_calls([call("test-id"), call("test-id")])
        assert client.get_client_authz_resources.call_count == 2

        # Verify scope-based permissions are created with correct payloads
        expected_calls = []

        for resource in KeycloakResource:
            if resource == KeycloakResource.MENU:
                continue
            for suffix, scope_ids in [
                ("Read", ["get", "list"]),
                ("Write", ["post", "put", "delete"]),
            ]:
                expected_calls.append(
                    call(
                        client_id="test-id",
                        payload={
                            "name": f"{resource.value}{suffix}",
                            "type": "scope",
                            "logic": "POSITIVE",
                            "decisionStrategy": "UNANIMOUS",
                            "scopes": scope_ids,
                            "resources": [f"resource-{resource.value}"],
                        },
                    )
                )

        expected_calls.append(
            call(
                client_id="test-id",
                payload={
                    "name": "DefaultMenuAccess",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "UNANIMOUS",
                    "scopes": ["menu"],
                    "resources": [f"menu-{item.value}" for item in MenuItem],
                },
            )
        )

        client.create_client_authz_scope_permission.assert_has_calls(expected_calls, any_order=True)
        assert client.create_client_authz_scope_permission.call_count == len(expected_calls)
        client.create_client_authz_resource_based_permission.assert_not_called()
        mock_ensure_default_role_policies.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_attach_default_role_permissions.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=False)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_permissions_with_teams(self, mock_ensure_strategy, mock_get_client):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [
            {"id": "1", "name": "GET"},
            {"id": "2", "name": "MENU"},
            {"id": "3", "name": "LIST"},
            {"id": "4", "name": "POST"},
            {"id": "5", "name": "PUT"},
            {"id": "6", "name": "DELETE"},
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
            {"_id": "r13", "name": "AdminView"},
            {"_id": "r14", "name": "Team"},
            {"_id": "r15", "name": "Team:team-a"},
        ]
        resources.extend({"_id": f"menu-{item.value}", "name": item.value} for item in MenuItem)

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
                "scopes": ["1", "2", "3", "4", "5", "6"],
                "resources": ["r1", "r2", "r3", "r4", "r15"],
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
                "resources": ["r6", "r7", "r8", "r9", "r10", "r11", "r12", "r14"],
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
                "name": "AdminViewAccess",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "scopes": ["1"],
                "resources": ["r13"],
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
                "resources": ["r1", "r15"],
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
        client.create_client_authz_resource_based_permission.assert_any_call(
            client_id="test-id",
            payload={
                "name": "Op-team-a",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "UNANIMOUS",
                "resources": ["r1", "r2", "r3", "r4"],  # Dag, Connection, Pool, Variable
            },
            skip_exists=True,
        )
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=False)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_permission_policy_ids")
    def test_update_read_only_permission_resources_excludes_team_resources(
        self, mock_get_permission_policy_ids
    ):
        client = Mock()
        client.get_client_authz_permissions.return_value = [{"id": "readonly-id", "name": "ReadOnly"}]
        client.get_client_authz_scopes.return_value = [
            {"id": "scope-get", "name": "GET"},
            {"id": "scope-list", "name": "LIST"},
            {"id": "scope-menu", "name": "MENU"},
            {"id": "scope-put", "name": "PUT"},
        ]
        client.get_client_authz_resources.return_value = [
            {"_id": "dag-global", "name": "Dag"},
            {"_id": "dag-team-a", "name": "Dag:team-a"},
            {"_id": "variable-global", "name": "Variable"},
            {"_id": "variable-team-a", "name": "Variable:team-a"},
            {"_id": "asset-global", "name": "Asset"},
        ]
        mock_get_permission_policy_ids.return_value = ["viewer-policy", "admin-policy"]

        _update_read_only_permission_resources(client, "test-id")

        client.update_client_authz_scope_permission.assert_called_once_with(
            client_id="test-id",
            scope_id="readonly-id",
            payload={
                "id": "readonly-id",
                "name": "ReadOnly",
                "type": "scope",
                "logic": "POSITIVE",
                "decisionStrategy": "AFFIRMATIVE",
                "scopes": ["scope-get", "scope-list", "scope-menu"],
                "resources": ["dag-global", "variable-global", "asset-global"],
                "policies": ["viewer-policy", "admin-policy"],
            },
        )

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_policy_id")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_permission_policy_ids")
    def test_update_admin_permission_resources_removes_admin_role_policy(
        self, mock_get_permission_policy_ids, mock_get_policy_id
    ):
        client = Mock()
        client.get_client_authz_permissions.return_value = [{"id": "admin-id", "name": "Admin"}]
        client.get_client_authz_scopes.return_value = [
            {"id": "scope-get", "name": "GET"},
            {"id": "scope-list", "name": "LIST"},
            {"id": "scope-put", "name": "PUT"},
        ]
        client.get_client_authz_resources.return_value = [
            {"_id": "dag-global", "name": "Dag"},
            {"_id": "dag-team-a", "name": "Dag:team-a"},
            {"_id": "dag-team-b", "name": "Dag:team-b"},
            {"_id": "asset-global", "name": "Asset"},
        ]
        mock_get_permission_policy_ids.return_value = ["admin-policy", "superadmin-policy"]
        mock_get_policy_id.return_value = "superadmin-policy"

        _update_admin_permission_resources(client, "test-id")

        client.update_client_authz_scope_permission.assert_called_once()
        payload = client.update_client_authz_scope_permission.call_args.kwargs["payload"]
        assert payload["policies"] == ["superadmin-policy"]
        assert payload["resources"] == ["dag-global", "dag-team-a", "dag-team-b", "asset-global"]

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_policy_to_resource_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_permission_policy_ids", autospec=True)
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_policy_id", autospec=True)
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_role_policy")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_permissions_attaches_default_role_policies(
        self,
        mock_ensure_strategy,
        mock_get_client,
        mock_create_permissions,
        mock_ensure_role_policy,
        mock_get_policy_id,
        mock_get_permission_policy_ids,
        mock_attach_resource_policy,
    ):
        client = Mock(
            spec=[
                "get_clients",
                "get_client_authz_permissions",
                "get_client_authz_scopes",
                "get_client_authz_resources",
                "update_client_authz_scope_permission",
            ]
        )
        mock_get_client.return_value = client
        mock_get_policy_id.side_effect = lambda client, client_uuid, name: f"policy-{name}"
        mock_get_permission_policy_ids.return_value = [
            "custom-policy",
            "policy-Allow-Viewer",
            "custom-policy",
        ]
        client.get_client_authz_permissions.return_value = [
            {"id": f"permission-{config['name']}", "name": config["name"]}
            for config in _get_default_permission_configs()
        ]
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]
        resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]
        client.get_client_authz_resources.return_value = [
            {"_id": f"resource-{name}", "name": name} for name in resource_names
        ]
        client.get_clients.return_value = [
            {"id": "test-id", "clientId": "test_client_id"},
        ]

        params = [
            "keycloak-auth-manager",
            "create-permissions",
            "--username",
            "test",
            "--password",
            "test",
        ]
        with conf_vars({("keycloak_auth_manager", "client_id"): "test_client_id"}):
            create_permissions_command(self.arg_parser.parse_args(params))

        for role_name in (*TEAM_ROLE_NAMES, SUPER_ADMIN_ROLE_NAME):
            mock_ensure_role_policy.assert_any_call(client, "test-id", role_name, _dry_run=False)

        mock_create_permissions.assert_called_once_with(client, "test-id", teams=[], _dry_run=False)
        read_roles = ["Viewer", "User", "Op", "Admin", "SuperAdmin"]

        write_roles_by_resource = {
            "AdminView": ["Admin", "SuperAdmin"],
            "Asset": ["User", "Op", "Admin", "SuperAdmin"],
            "AssetAlias": ["Admin", "SuperAdmin"],
            "Backfill": ["Op", "Admin", "SuperAdmin"],
            "Configuration": ["Admin", "SuperAdmin"],
            "Connection": ["Op", "Admin", "SuperAdmin"],
            "Custom": ["Admin", "SuperAdmin"],
            "Dag": ["User", "Op", "Admin", "SuperAdmin"],
            "Pool": ["Op", "Admin", "SuperAdmin"],
            "Team": ["Admin", "SuperAdmin"],
            "Variable": ["Op", "Admin", "SuperAdmin"],
            "View": ["Admin", "SuperAdmin"],
        }

        expected_calls = []

        for resource, write_roles in write_roles_by_resource.items():
            for suffix, scopes, roles in [
                ("Read", ["GET", "LIST"], read_roles),
                ("Write", ["POST", "PUT", "DELETE"], write_roles),
            ]:
                expected_calls.append(
                    call(
                        client_id="test-id",
                        scope_id=f"permission-{resource}{suffix}",
                        payload={
                            "id": f"permission-{resource}{suffix}",
                            "name": f"{resource}{suffix}",
                            "type": "scope",
                            "logic": "POSITIVE",
                            "decisionStrategy": "AFFIRMATIVE",
                            "scopes": [scope.lower() for scope in scopes],
                            "resources": [f"resource-{resource}"],
                            "policies": list(
                                dict.fromkeys(
                                    [
                                        "custom-policy",
                                        "policy-Allow-Viewer",
                                        *[f"policy-Allow-{role}" for role in roles],
                                    ]
                                )
                            ),
                        },
                    )
                )

        expected_calls.append(
            call(
                client_id="test-id",
                scope_id="permission-DefaultMenuAccess",
                payload={
                    "id": "permission-DefaultMenuAccess",
                    "name": "DefaultMenuAccess",
                    "type": "scope",
                    "logic": "POSITIVE",
                    "decisionStrategy": "AFFIRMATIVE",
                    "scopes": ["menu"],
                    "resources": [f"resource-{item.value}" for item in MenuItem],
                    "policies": ["custom-policy", *[f"policy-Allow-{role}" for role in read_roles]],
                },
            )
        )
        client.update_client_authz_scope_permission.assert_has_calls(expected_calls, any_order=True)
        assert client.update_client_authz_scope_permission.call_count == 25
        client.get_client_authz_permissions.assert_called_once_with("test-id")
        assert client.get_client_authz_scopes.call_count == 2
        assert client.get_client_authz_resources.call_count == 2
        assert mock_get_permission_policy_ids.call_count == 25
        assert mock_get_policy_id.call_count == 5
        for role in read_roles:
            mock_get_policy_id.assert_any_call(client, "test-id", f"Allow-{role}")
        mock_attach_resource_policy.assert_not_called()
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=False)

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._update_admin_permission_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._update_read_only_permission_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_scope_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_policy_to_resource_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_policy_to_scope_permission")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_aggregate_policy")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_group_policy")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_group")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_team_command(
        self,
        mock_ensure_strategy,
        mock_get_client,
        mock_ensure_group,
        mock_create_resources,
        mock_create_permissions,
        mock_ensure_group_policy,
        mock_ensure_aggregate_policy,
        mock_attach_policy,
        mock_attach_resource_policy,
        mock_ensure_scope_permission,
        mock_update_read_only_permission_resources,
        mock_update_admin_permission_resources,
    ):
        client = Mock()
        mock_get_client.return_value = client
        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]
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
        mock_update_read_only_permission_resources.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_update_admin_permission_resources.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_ensure_group_policy.assert_called_once_with(client, "test-id", "team-a", _dry_run=False)
        assert mock_ensure_aggregate_policy.call_count == 4
        for role_name in TEAM_ROLE_NAMES:
            mock_attach_policy.assert_any_call(
                client,
                "test-id",
                permission_name="ReadOnly-team-a",
                policy_name=f"Allow-{role_name}-team-a",
                scope_names=["GET", "LIST"],
                resource_names=["Dag:team-a", "Team:team-a"],
                decision_strategy="AFFIRMATIVE",
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
                "Team:team-a",
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
                "Dag:team-a",
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
            resource_names=EXPECTED_TEAM_MENU_RESOURCES,
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="MenuAccess-Admin-team-a",
            policy_name="Allow-Admin-team-a",
            scope_names=["MENU"],
            resource_names=EXPECTED_TEAM_ADMIN_MENU_RESOURCES,
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
                "Team:team-a",
                "Variable:team-a",
            ],
            decision_strategy="AFFIRMATIVE",
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
        for role_name in TEAM_ROLE_NAMES:
            mock_attach_policy.assert_any_call(
                client,
                "test-id",
                permission_name="ViewAccess",
                policy_name=f"Allow-{role_name}-team-a",
                scope_names=["GET"],
                resource_names=["View"],
                decision_strategy="AFFIRMATIVE",
                _dry_run=False,
            )
        mock_attach_policy.assert_any_call(
            client,
            "test-id",
            permission_name="AdminViewAccess",
            policy_name="Allow-SuperAdmin",
            scope_names=["GET"],
            resource_names=["AdminView"],
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        admin_view_policies = [
            c.kwargs["policy_name"]
            for c in mock_attach_policy.call_args_list
            if c.kwargs["permission_name"] == "AdminViewAccess" or "AdminView" in c.kwargs["resource_names"]
        ]
        assert admin_view_policies == ["Allow-SuperAdmin"]
        mock_ensure_scope_permission.assert_any_call(
            client,
            "test-id",
            name="MenuAccess-team-a",
            scope_names=["MENU"],
            resource_names=EXPECTED_TEAM_MENU_RESOURCES,
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_ensure_scope_permission.assert_any_call(
            client,
            "test-id",
            name="MenuAccess-Admin-team-a",
            scope_names=["MENU"],
            resource_names=EXPECTED_TEAM_ADMIN_MENU_RESOURCES,
            decision_strategy="AFFIRMATIVE",
            _dry_run=False,
        )
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=False)
        client.get_client_authz_scopes.assert_called_once_with("test-id")

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

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_default_role_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_default_role_policies")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_scopes")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_all(
        self,
        mock_ensure_strategy,
        mock_get_client,
        mock_create_scopes,
        mock_create_resources,
        mock_create_permissions,
        mock_ensure_default_role_policies,
        mock_attach_default_role_permissions,
    ):
        client = Mock()
        mock_get_client.return_value = client
        scopes = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]

        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]
        client.get_client_authz_scopes.return_value = scopes
        resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]
        client.get_client_authz_resources.return_value = [
            {"_id": f"resource-{name}", "name": name} for name in resource_names
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
        mock_ensure_default_role_policies.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_attach_default_role_permissions.assert_called_once_with(client, "test-id", _dry_run=False)
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=False)

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

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_default_role_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_default_role_policies")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    def test_create_permissions_dry_run(
        self,
        mock_ensure_strategy,
        mock_get_client,
        mock_ensure_default_role_policies,
        mock_attach_default_role_permissions,
    ):
        client = Mock()
        mock_get_client.return_value = client
        client.get_client_authz_permissions.return_value = []

        client.get_client_authz_scopes.return_value = [
            {"id": name.lower(), "name": name} for name in ["GET", "LIST", "POST", "PUT", "DELETE", "MENU"]
        ]
        resource_names = [resource.value for resource in KeycloakResource] + [item.value for item in MenuItem]
        client.get_client_authz_resources.return_value = [
            {"_id": f"resource-{name}", "name": name} for name in resource_names
        ]
        client.get_clients.return_value = [
            {"id": "dummy-id", "clientId": "dummy-client"},
            {"id": "test-id", "clientId": "test_client_id"},
        ]

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
        mock_ensure_default_role_policies.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_attach_default_role_permissions.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=True)
        client.get_client_authz_permissions.assert_called_once_with("test-id")

    @patch("airflow.providers.keycloak.auth_manager.cli.commands._attach_default_role_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._ensure_default_role_policies")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_permissions")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_resources")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._create_scopes")
    @patch("airflow.providers.keycloak.auth_manager.cli.commands._get_client")
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._ensure_resource_server_decision_strategy",
        autospec=True,
    )
    @patch(
        "airflow.providers.keycloak.auth_manager.cli.commands._preview_resources",
        autospec=True,
    )
    def test_create_all_dry_run(
        self,
        mock_preview_resources,
        mock_ensure_strategy,
        mock_get_client,
        mock_create_scopes,
        mock_create_resources,
        mock_create_permissions,
        mock_ensure_default_role_policies,
        mock_attach_default_role_permissions,
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
        mock_create_scopes.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_create_permissions.assert_called_once_with(client, "test-id", teams=[], _dry_run=True)
        mock_ensure_default_role_policies.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_create_resources.assert_not_called()
        mock_preview_resources.assert_called_once_with(
            client,
            "test-id",
            teams=[],
            planned_scopes=_get_scopes_to_create(),
        )
        mock_attach_default_role_permissions.assert_called_once_with(client, "test-id", _dry_run=True)
        mock_ensure_strategy.assert_called_once_with(client, "test-id", _dry_run=True)
