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
import logging
from enum import Enum
from typing import get_args

from keycloak import KeycloakAdmin, KeycloakError
from keycloak.exceptions import KeycloakGetError, KeycloakPostError, raise_error_from_response

from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
from airflow.api_fastapi.common.types import MenuItem
from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.cli.utils import dry_run_message_wrap, dry_run_preview
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
)
from airflow.providers.keycloak.auth_manager.resources import KeycloakResource
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

try:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
except ImportError:
    # Fallback for older Airflow versions where ExtendedResourceMethod doesn't exist
    from airflow.api_fastapi.auth.managers.base_auth_manager import (
        ResourceMethod as ExtendedResourceMethod,  # type: ignore[assignment]
    )

log = logging.getLogger(__name__)
TEAM_SCOPED_RESOURCE_NAMES = {
    KeycloakResource.DAG.value,
    KeycloakResource.CONNECTION.value,
    KeycloakResource.VARIABLE.value,
    KeycloakResource.POOL.value,
}
GLOBAL_SCOPED_RESOURCE_NAMES = {
    KeycloakResource.ASSET.value,
    KeycloakResource.ASSET_ALIAS.value,
    KeycloakResource.CONFIGURATION.value,
}
TEAM_MENU_ITEMS = {
    MenuItem.DAGS,
    MenuItem.ASSETS,
    MenuItem.DOCS,
}
TEAM_ADMIN_MENU_ITEMS = TEAM_MENU_ITEMS | {
    MenuItem.CONNECTIONS,
    MenuItem.POOLS,
    MenuItem.VARIABLES,
    MenuItem.XCOMS,
}
TEAM_ROLE_NAMES = ("Viewer", "User", "Op", "Admin")
SUPER_ADMIN_ROLE_NAME = "SuperAdmin"


def _get_resource_methods() -> list[str]:
    """
    Get list of resource method values.

    Provides backwards compatibility for Airflow <3.2 where ResourceMethod
    was a Literal type, and Airflow >=3.2 where it's an Enum.
    """
    if isinstance(ResourceMethod, type) and issubclass(ResourceMethod, Enum):
        return [method.value for method in ResourceMethod]
    return list(get_args(ResourceMethod))


def _get_extended_resource_methods() -> list[str]:
    """
    Get list of extended resource method values.

    Provides backwards compatibility for Airflow <3.2 where ExtendedResourceMethod
    was a Literal type, and Airflow >=3.2 where it's an Enum.
    """
    if isinstance(ExtendedResourceMethod, type) and issubclass(ExtendedResourceMethod, Enum):
        return [method.value for method in ExtendedResourceMethod]
    return list(get_args(ExtendedResourceMethod))


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_scopes_command(args):
    """Create Keycloak auth manager scopes in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_scopes(client, client_uuid, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_resources_command(args):
    """Create Keycloak auth manager resources in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)
    teams = _parse_teams(args.teams)
    _ensure_multi_team_enabled(teams=teams, command_name="create-resources")

    _create_resources(client, client_uuid, teams=teams, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_permissions_command(args):
    """Create Keycloak auth manager permissions in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    teams = _parse_teams(args.teams)
    _ensure_multi_team_enabled(teams=teams, command_name="create-permissions")
    if teams:
        # Role policies are only needed for team-scoped (group+role) authorization.
        for role_name in TEAM_ROLE_NAMES:
            _ensure_role_policy(client, client_uuid, role_name, _dry_run=args.dry_run)
        _ensure_role_policy(client, client_uuid, SUPER_ADMIN_ROLE_NAME, _dry_run=args.dry_run)
    _create_permissions(client, client_uuid, teams=teams, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_all_command(args):
    """Create all Keycloak auth manager entities in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)
    teams = _parse_teams(args.teams)
    _ensure_multi_team_enabled(teams=teams, command_name="create-all")

    _create_scopes(client, client_uuid, _dry_run=args.dry_run)
    _create_resources(client, client_uuid, teams=teams, _dry_run=args.dry_run)
    _create_group_membership_mapper(client, client_uuid, _dry_run=args.dry_run)
    if teams:
        # Role policies are only needed for team-scoped (group+role) authorization.
        for role_name in TEAM_ROLE_NAMES:
            _ensure_role_policy(client, client_uuid, role_name, _dry_run=args.dry_run)
        _ensure_role_policy(client, client_uuid, SUPER_ADMIN_ROLE_NAME, _dry_run=args.dry_run)
    _create_permissions(client, client_uuid, teams=teams, _dry_run=args.dry_run)


def _get_client(args):
    server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)
    realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)

    return KeycloakAdmin(
        server_url=server_url,
        username=args.username,
        password=args.password,
        realm_name=realm,
        user_realm_name=args.user_realm,
        client_id=args.client_id,
        verify=True,
    )


def _get_client_uuid(args):
    client = _get_client(args)
    clients = client.get_clients()
    client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)

    matches = [client for client in clients if client["clientId"] == client_id]
    if not matches:
        raise ValueError(f"Client with ID='{client_id}' not found in realm '{client.realm_name}'")

    return matches[0]["id"]


def _create_group_membership_mapper(
    client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False
) -> None:
    realm = client.connection.realm_name
    url = f"admin/realms/{realm}/clients/{client_uuid}/protocol-mappers/models"
    data_raw = client.connection.raw_get(url)
    mappers = json.loads(data_raw.text)
    for mapper in mappers:
        if mapper.get("name") == "groups":
            return
        if mapper.get("protocolMapper") == "oidc-group-membership-mapper":
            if mapper.get("config", {}).get("claim.name") == "groups":
                return

    if _dry_run:
        print("Would create protocol mapper 'groups'.")
        return

    payload = {
        "name": "groups",
        "protocol": "openid-connect",
        "protocolMapper": "oidc-group-membership-mapper",
        "consentRequired": False,
        "config": {
            "full.path": "false",
            "id.token.claim": "false",
            "access.token.claim": "true",
            "userinfo.token.claim": "false",
            "claim.name": "groups",
            "jsonType.label": "String",
        },
    }
    data_raw = client.connection.raw_post(url, data=json.dumps(payload), max=-1)
    raise_error_from_response(data_raw, KeycloakPostError, expected_codes=[201])


def _get_scopes_to_create() -> list[dict]:
    """Get the list of scopes to be created."""
    scopes = [{"name": method} for method in _get_resource_methods()]
    scopes.extend([{"name": "MENU"}, {"name": "LIST"}])
    return scopes


def _parse_teams(teams: str | None) -> list[str]:
    if not teams:
        return []
    return [team.strip() for team in teams.split(",") if team.strip()]


def _ensure_multi_team_enabled(*, teams: list[str], command_name: str) -> None:
    if not teams:
        return
    if not conf.getboolean("core", "multi_team", fallback=False):
        raise SystemExit(f"{command_name} requires core.multi_team=True when --teams is used.")


def _preview_scopes(*args, **kwargs):
    """Preview scopes that would be created."""
    scopes = _get_scopes_to_create()
    print("Scopes to be created:")
    for scope in scopes:
        print(f"  - {scope['name']}")
    print()


@dry_run_preview(_preview_scopes)
def _create_scopes(client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False):
    """Create Keycloak scopes."""
    scopes = _get_scopes_to_create()

    for scope in scopes:
        client.create_client_authz_scopes(client_id=client_uuid, payload=scope)

    print("Scopes created successfully.")


def _get_resources_to_create(
    client: KeycloakAdmin,
    client_uuid: str,
    teams: list[str],
) -> tuple[list[tuple[str, list[dict]]], list[tuple[str, list[dict]]]]:
    """
    Get the list of resources to be created.

    Returns a tuple of (standard_resources, menu_resources).
    Each is a list of tuples (resource_name, scopes_list).
    """
    all_scopes = client.get_client_authz_scopes(client_uuid)

    scopes = [
        {"id": scope["id"], "name": scope["name"]}
        for scope in all_scopes
        if scope["name"] in ["GET", "POST", "PUT", "DELETE", "LIST"]
    ]
    menu_scopes = [
        {"id": scope["id"], "name": scope["name"]} for scope in all_scopes if scope["name"] == "MENU"
    ]

    standard_resources = [(resource.value, scopes) for resource in KeycloakResource]

    if teams:
        existing = {resource_name for resource_name, _ in standard_resources}
        for team in teams:
            for resource_name in TEAM_SCOPED_RESOURCE_NAMES:
                name = f"{resource_name}:{team}"
                if name in existing:
                    continue
                standard_resources.append((name, scopes))
                existing.add(name)
    menu_resources = [(item.value, menu_scopes) for item in MenuItem]

    return standard_resources, menu_resources


def _preview_resources(client: KeycloakAdmin, client_uuid: str, teams: list[str]):
    """Preview resources that would be created."""
    standard_resources, menu_resources = _get_resources_to_create(client, client_uuid, teams=teams)

    print("Resources to be created:")
    if standard_resources:
        for resource_name, resource_scopes in standard_resources:
            actual_scope_names = ", ".join([s["name"] for s in resource_scopes])
            print(f"  - {resource_name} (scopes: {actual_scope_names})")
    print("\nMenu item resources to be created:")
    for resource_name, resource_scopes in menu_resources:
        actual_scope_names = ", ".join([s["name"] for s in resource_scopes])
        print(f"  - {resource_name} (scopes: {actual_scope_names})")
    print()


@dry_run_preview(_preview_resources)
def _create_resources(client: KeycloakAdmin, client_uuid: str, *, teams: list[str], _dry_run: bool = False):
    """Create Keycloak resources."""
    standard_resources, menu_resources = _get_resources_to_create(client, client_uuid, teams=teams)

    for resource_name, scopes in standard_resources:
        client.create_client_authz_resource(
            client_id=client_uuid,
            payload={
                "name": resource_name,
                "scopes": scopes,
            },
            skip_exists=True,
        )

    # Create menu item resources
    for resource_name, scopes in menu_resources:
        client.create_client_authz_resource(
            client_id=client_uuid,
            payload={
                "name": resource_name,
                "scopes": scopes,
            },
            skip_exists=True,
        )

    print("Resources created successfully.")


def _get_permissions_to_create(
    client: KeycloakAdmin,
    client_uuid: str,
    teams: list[str],
    *,
    include_global_admin: bool = True,
) -> list[dict]:
    """
    Get the actual permissions to be created with filtered scopes/resources.

    Returns a list of permission descriptors with actual filtered data.
    """
    if not teams:
        perm_configs = [
            {
                "name": "ReadOnly",
                "type": "scope-based",
                "scope_names": ["GET", "MENU", "LIST"],
            },
            {
                "name": "Admin",
                "type": "scope-based",
                "scope_names": _get_extended_resource_methods() + ["LIST"],
            },
            {
                "name": "User",
                "type": "resource-based",
                "resources": [KeycloakResource.DAG.value, KeycloakResource.ASSET.value],
            },
            {
                "name": "Op",
                "type": "resource-based",
                "resources": [
                    KeycloakResource.CONNECTION.value,
                    KeycloakResource.POOL.value,
                    KeycloakResource.VARIABLE.value,
                    KeycloakResource.BACKFILL.value,
                ],
            },
        ]
    else:
        perm_configs = []
        for team in teams:
            perm_configs.extend(
                [
                    {
                        "name": f"Admin-{team}",
                        "type": "scope-based",
                        "scope_names": _get_extended_resource_methods() + ["LIST"],
                        "resources": [f"{resource}:{team}" for resource in TEAM_SCOPED_RESOURCE_NAMES],
                    },
                    {
                        "name": f"ReadOnly-{team}",
                        "type": "scope-based",
                        "scope_names": ["GET", "LIST"],
                        "resources": [
                            f"{KeycloakResource.DAG.value}:{team}",
                        ],
                    },
                    {
                        "name": f"User-{team}",
                        "type": "resource-based",
                        "resources": [
                            f"{KeycloakResource.DAG.value}:{team}",
                        ],
                    },
                    {
                        "name": f"Op-{team}",
                        "type": "resource-based",
                        "resources": [
                            f"{KeycloakResource.CONNECTION.value}:{team}",
                            f"{KeycloakResource.POOL.value}:{team}",
                            f"{KeycloakResource.VARIABLE.value}:{team}",
                        ],
                    },
                ]
            )
        if include_global_admin:
            perm_configs.append(
                {
                    "name": "Admin",
                    "type": "scope-based",
                    "scope_names": _get_extended_resource_methods() + ["LIST"],
                    "resources": [
                        f"{resource}:{team}" for team in teams for resource in TEAM_SCOPED_RESOURCE_NAMES
                    ],
                }
            )
        perm_configs.append(
            {
                "name": "GlobalList",
                "type": "scope-based",
                "scope_names": ["LIST"],
                "resources": list(TEAM_SCOPED_RESOURCE_NAMES) + list(GLOBAL_SCOPED_RESOURCE_NAMES),
            }
        )
        perm_configs.append(
            {
                "name": "ViewAccess",
                "type": "scope-based",
                "scope_names": ["GET"],
                "resources": [KeycloakResource.VIEW.value],
            }
        )
        perm_configs.append(
            {
                "name": "MenuAccess",
                "type": "scope-based",
                "scope_names": ["MENU"],
                "resources": [item.value for item in MenuItem],
            }
        )

    all_scopes = client.get_client_authz_scopes(client_uuid)
    all_resources = client.get_client_authz_resources(client_uuid)

    result = []

    for config in perm_configs:
        perm = {"name": config["name"], "type": config["type"]}
        if config["type"] == "scope-based":
            # Filter to get actual scope IDs that exist and match
            filtered_scope_ids = [s["id"] for s in all_scopes if s["name"] in config["scope_names"]]
            filtered_scope_names = [s["name"] for s in all_scopes if s["name"] in config["scope_names"]]
            perm["scope_ids"] = filtered_scope_ids
            perm["scope_names"] = filtered_scope_names
            if "resources" in config:
                filtered_resource_ids = [r["_id"] for r in all_resources if r["name"] in config["resources"]]
                filtered_resource_names = [
                    r["name"] for r in all_resources if r["name"] in config["resources"]
                ]
                perm["resource_ids"] = filtered_resource_ids
                perm["resource_names"] = filtered_resource_names
        else:  # resource-based
            # Filter to get actual resource IDs that exist and match
            filtered_resource_ids = [r["_id"] for r in all_resources if r["name"] in config["resources"]]
            filtered_resource_names = [r["name"] for r in all_resources if r["name"] in config["resources"]]
            perm["resource_ids"] = filtered_resource_ids
            perm["resource_names"] = filtered_resource_names
        result.append(perm)

    return result


def _preview_permissions(client: KeycloakAdmin, client_uuid: str, teams: list[str]):
    """Preview permissions that would be created."""
    permissions = _get_permissions_to_create(client, client_uuid, teams=teams)

    print("Permissions to be created:")
    for perm in permissions:
        if perm["type"] == "scope-based":
            scope_names = ", ".join(perm["scope_names"])
            resource_names = ", ".join(perm.get("resource_names", []))
            resource_suffix = f", resources: {resource_names}" if resource_names else ""
            print(f"  - {perm['name']} (type: scope-based, scopes: {scope_names}{resource_suffix})")
        else:  # resource-based
            resource_names = ", ".join(perm["resource_names"])
            print(f"  - {perm['name']} (type: resource-based, resources: {resource_names})")
    print()


@dry_run_preview(_preview_permissions)
def _create_permissions(
    client: KeycloakAdmin,
    client_uuid: str,
    *,
    teams: list[str],
    include_global_admin: bool = True,
    _dry_run: bool = False,
):
    """Create Keycloak permissions."""
    permissions = _get_permissions_to_create(
        client, client_uuid, teams=teams, include_global_admin=include_global_admin
    )

    for perm in permissions:
        if perm["type"] == "scope-based":
            _create_scope_based_permission(
                client, client_uuid, perm["name"], perm["scope_ids"], perm.get("resource_ids", [])
            )
        else:  # resource-based
            _create_resource_based_permission(client, client_uuid, perm["name"], perm["resource_ids"])

    print("Permissions created successfully.")


def _create_scope_based_permission(
    client: KeycloakAdmin,
    client_uuid: str,
    name: str,
    scope_ids: list[str],
    resource_ids: list[str] | None = None,
    decision_strategy: str = "UNANIMOUS",
):
    payload = {
        "name": name,
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": decision_strategy,
        "scopes": scope_ids,
    }
    if resource_ids:
        payload["resources"] = resource_ids

    try:
        client.create_client_authz_scope_permission(
            client_id=client_uuid,
            payload=payload,
        )
    except KeycloakError as e:
        if e.response_body:
            error = json.loads(e.response_body.decode("utf-8"))
            if error.get("error_description") == "Conflicting policy":
                print(f"Policy creation skipped. {error.get('error')}")


def _create_resource_based_permission(
    client: KeycloakAdmin, client_uuid: str, name: str, resource_ids: list[str]
):
    payload = {
        "name": name,
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "resources": resource_ids,
    }
    client.create_client_authz_resource_based_permission(
        client_id=client_uuid,
        payload=payload,
        skip_exists=True,
    )


def _ensure_scope_permission(
    client: KeycloakAdmin,
    client_uuid: str,
    *,
    name: str,
    scope_names: list[str],
    resource_names: list[str],
    decision_strategy: str = "UNANIMOUS",
    _dry_run: bool = False,
) -> None:
    if _dry_run:
        print(f"Would create scope permission '{name}'.")
        return

    permissions = client.get_client_authz_permissions(client_uuid)
    if any(perm.get("name") == name for perm in permissions):
        return

    scopes = client.get_client_authz_scopes(client_uuid)
    resources = client.get_client_authz_resources(client_uuid)
    scope_ids = [s["id"] for s in scopes if s["name"] in scope_names]
    resource_ids = [r["_id"] for r in resources if r["name"] in resource_names]
    _create_scope_based_permission(
        client,
        client_uuid,
        name,
        scope_ids,
        resource_ids,
        decision_strategy=decision_strategy,
    )


def _update_admin_permission_resources(
    client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False
) -> None:
    if _dry_run:
        print("Would update permission 'Admin' with team-scoped and global resources.")
        return

    permissions = client.get_client_authz_permissions(client_uuid)
    match = next((perm for perm in permissions if perm.get("name") == "Admin"), None)
    if not match:
        return

    permission_id = match["id"]
    scopes = client.get_client_authz_scopes(client_uuid)
    resources = client.get_client_authz_resources(client_uuid)
    scope_names = _get_extended_resource_methods() + ["LIST"]
    scope_ids = [s["id"] for s in scopes if s["name"] in scope_names]

    resource_ids = [
        r["_id"]
        for r in resources
        if any(r["name"].startswith(f"{resource}:") for resource in TEAM_SCOPED_RESOURCE_NAMES)
        or r["name"] in GLOBAL_SCOPED_RESOURCE_NAMES
    ]

    policy_ids = _get_permission_policy_ids(client, client_uuid, permission_id)
    payload = {
        "id": permission_id,
        "name": "Admin",
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "scopes": scope_ids,
        "resources": resource_ids,
        "policies": policy_ids,
    }
    client.update_client_authz_scope_permission(
        payload=payload, client_id=client_uuid, scope_id=permission_id
    )


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_team_command(args):
    """Create team resources, permissions, and Keycloak group."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)
    team = args.team
    _ensure_multi_team_enabled(teams=[team], command_name="create-team")

    _create_resources(client, client_uuid, teams=[team], _dry_run=args.dry_run)
    _create_group_membership_mapper(client, client_uuid, _dry_run=args.dry_run)
    _create_permissions(client, client_uuid, teams=[team], include_global_admin=False, _dry_run=args.dry_run)
    _ensure_group(client, team, _dry_run=args.dry_run)
    _ensure_team_policies(client, client_uuid, team, _dry_run=args.dry_run)
    _attach_team_permissions(client, client_uuid, team, _dry_run=args.dry_run)
    _attach_team_menu_permissions(client, client_uuid, team, _dry_run=args.dry_run)
    _attach_superadmin_permissions(client, client_uuid, team, _dry_run=args.dry_run)
    _update_admin_permission_resources(client, client_uuid, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def add_user_to_team_command(args):
    """Add a user to a Keycloak team group."""
    client = _get_client(args)
    team = args.team
    username = args.target_username
    _ensure_multi_team_enabled(teams=[team], command_name="add-user-to-team")

    dry_run = getattr(args, "dry_run", False)
    _ensure_group(client, team, _dry_run=dry_run)
    _add_user_to_group(client, username=username, team=team, _dry_run=dry_run)


def _ensure_team_policies(
    client: KeycloakAdmin, client_uuid: str, team: str, *, _dry_run: bool = False
) -> None:
    _ensure_group_policy(
        client,
        client_uuid,
        team,
        _dry_run=_dry_run,
    )
    for role_name in TEAM_ROLE_NAMES:
        _ensure_aggregate_policy(
            client,
            client_uuid,
            _team_role_policy_name(team, role_name),
            [
                (_team_group_policy_name(team), "group"),
                (_role_policy_name(role_name), "role"),
            ],
            _dry_run=_dry_run,
        )


def _attach_team_permissions(
    client: KeycloakAdmin, client_uuid: str, team: str, *, _dry_run: bool = False
) -> None:
    team_dag_resources = [
        f"{KeycloakResource.DAG.value}:{team}",
    ]
    team_scoped_resources = [f"{resource}:{team}" for resource in sorted(TEAM_SCOPED_RESOURCE_NAMES)]

    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name=f"ReadOnly-{team}",
        policy_name=_team_role_policy_name(team, "Viewer"),
        scope_names=["GET", "LIST"],
        resource_names=team_dag_resources,
        _dry_run=_dry_run,
    )
    for role_name in ("User", "Op", "Admin"):
        _attach_policy_to_scope_permission(
            client,
            client_uuid,
            permission_name=f"ReadOnly-{team}",
            policy_name=_team_role_policy_name(team, role_name),
            scope_names=["GET", "LIST"],
            resource_names=team_dag_resources,
            _dry_run=_dry_run,
        )
    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name=f"Admin-{team}",
        policy_name=_team_role_policy_name(team, "Admin"),
        scope_names=_get_extended_resource_methods() + ["LIST"],
        resource_names=team_scoped_resources,
        _dry_run=_dry_run,
    )
    _attach_policy_to_resource_permission(
        client,
        client_uuid,
        permission_name=f"User-{team}",
        policy_name=_team_role_policy_name(team, "User"),
        resource_names=team_dag_resources,
        _dry_run=_dry_run,
    )
    _attach_policy_to_resource_permission(
        client,
        client_uuid,
        permission_name=f"Op-{team}",
        policy_name=_team_role_policy_name(team, "Op"),
        resource_names=[
            f"{KeycloakResource.CONNECTION.value}:{team}",
            f"{KeycloakResource.POOL.value}:{team}",
            f"{KeycloakResource.VARIABLE.value}:{team}",
        ],
        _dry_run=_dry_run,
    )
    for role_name in TEAM_ROLE_NAMES:
        _attach_policy_to_scope_permission(
            client,
            client_uuid,
            permission_name="ViewAccess",
            policy_name=_team_role_policy_name(team, role_name),
            scope_names=["GET"],
            resource_names=[KeycloakResource.VIEW.value],
            decision_strategy="AFFIRMATIVE",
            _dry_run=_dry_run,
        )

    _attach_global_list_permissions(client, client_uuid, _dry_run=_dry_run)


def _attach_global_list_permissions(
    client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False
) -> None:
    resource_names = list(TEAM_SCOPED_RESOURCE_NAMES) + list(GLOBAL_SCOPED_RESOURCE_NAMES)
    for role_name in (*TEAM_ROLE_NAMES, SUPER_ADMIN_ROLE_NAME):
        _attach_policy_to_scope_permission(
            client,
            client_uuid,
            permission_name="GlobalList",
            policy_name=_role_policy_name(role_name),
            scope_names=["LIST"],
            resource_names=resource_names,
            decision_strategy="AFFIRMATIVE",
            _dry_run=_dry_run,
        )


def _attach_team_menu_permissions(
    client: KeycloakAdmin, client_uuid: str, team: str, *, _dry_run: bool = False
) -> None:
    menu_permission_name = f"MenuAccess-{team}"
    menu_admin_permission_name = f"MenuAccess-Admin-{team}"
    team_menu_resources = [item.value for item in sorted(TEAM_MENU_ITEMS, key=lambda item: item.value)]
    team_admin_menu_resources = [
        item.value for item in sorted(TEAM_ADMIN_MENU_ITEMS, key=lambda item: item.value)
    ]
    _ensure_scope_permission(
        client,
        client_uuid,
        name=menu_permission_name,
        scope_names=["MENU"],
        resource_names=team_menu_resources,
        decision_strategy="AFFIRMATIVE",
        _dry_run=_dry_run,
    )
    _ensure_scope_permission(
        client,
        client_uuid,
        name=menu_admin_permission_name,
        scope_names=["MENU"],
        resource_names=team_admin_menu_resources,
        decision_strategy="AFFIRMATIVE",
        _dry_run=_dry_run,
    )
    for role_name in TEAM_ROLE_NAMES:
        _attach_policy_to_scope_permission(
            client,
            client_uuid,
            permission_name=menu_permission_name,
            policy_name=_team_role_policy_name(team, role_name),
            scope_names=["MENU"],
            resource_names=team_menu_resources,
            decision_strategy="AFFIRMATIVE",
            _dry_run=_dry_run,
        )
    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name=menu_admin_permission_name,
        policy_name=_team_role_policy_name(team, "Admin"),
        scope_names=["MENU"],
        resource_names=team_admin_menu_resources,
        decision_strategy="AFFIRMATIVE",
        _dry_run=_dry_run,
    )


def _attach_superadmin_permissions(
    client: KeycloakAdmin, client_uuid: str, team: str, *, _dry_run: bool = False
) -> None:
    team_scoped_resources = [f"{resource}:{team}" for resource in sorted(TEAM_SCOPED_RESOURCE_NAMES)]
    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name="Admin",
        policy_name=_role_policy_name(SUPER_ADMIN_ROLE_NAME),
        scope_names=_get_extended_resource_methods() + ["LIST"],
        resource_names=team_scoped_resources,
        _dry_run=_dry_run,
    )
    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name="ViewAccess",
        policy_name=_role_policy_name(SUPER_ADMIN_ROLE_NAME),
        scope_names=["GET"],
        resource_names=[KeycloakResource.VIEW.value],
        decision_strategy="AFFIRMATIVE",
        _dry_run=_dry_run,
    )
    _attach_policy_to_scope_permission(
        client,
        client_uuid,
        permission_name="MenuAccess",
        policy_name=_role_policy_name(SUPER_ADMIN_ROLE_NAME),
        scope_names=["MENU"],
        resource_names=[item.value for item in sorted(MenuItem, key=lambda item: item.value)],
        decision_strategy="AFFIRMATIVE",
        _dry_run=_dry_run,
    )


def _team_group_name(team: str) -> str:
    return team


def _team_group_policy_name(team: str) -> str:
    return f"Allow-Team-{team}"


def _role_policy_name(role_name: str) -> str:
    return f"Allow-{role_name}"


def _team_role_policy_name(team: str, role_name: str) -> str:
    return f"Allow-{role_name}-{team}"


def _ensure_group(client: KeycloakAdmin, team: str, *, _dry_run: bool = False) -> dict | None:
    group_name = _team_group_name(team)
    group_path = f"/{group_name}"
    try:
        group = client.get_group_by_path(group_path)
        if group:
            return group
    except KeycloakError:
        pass

    if _dry_run:
        print(f"Would create group '{group_name}'.")
        return None

    group_id = client.create_group(payload={"name": group_name}, skip_exists=True)
    if not group_id:
        group = client.get_group_by_path(group_path)
        return group
    return {"id": group_id, "name": group_name, "path": group_path}


def _ensure_group_policy(
    client: KeycloakAdmin, client_uuid: str, team: str, *, _dry_run: bool = False
) -> None:
    group_name = _team_group_name(team)
    group_path = f"/{group_name}"
    policy_name = _team_group_policy_name(team)
    try:
        group = client.get_group_by_path(group_path)
    except KeycloakError:
        group = None
    if not group:
        raise ValueError(f"Group '{group_name}' not found.")

    if _get_policy_id(client, client_uuid, policy_name, policy_type="group"):
        return

    payload = {
        "name": policy_name,
        "type": "group",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "groups": [{"id": group["id"], "path": group_path}],
    }

    if _dry_run:
        print(f"Would create group policy '{policy_name}'.")
        return

    url = _policy_url(client, client_uuid, policy_type="group")
    data_raw = client.connection.raw_post(url, data=json.dumps(payload), max=-1, permission=False)
    try:
        raise_error_from_response(data_raw, KeycloakPostError, expected_codes=[201])
    except KeycloakPostError as exc:
        if exc.response_body:
            error = json.loads(exc.response_body.decode("utf-8"))
            if "Conflicting policy" in error.get("error_description", ""):
                return
        raise


def _policy_url(client: KeycloakAdmin, client_uuid: str, *, policy_type: str | None = None) -> str:
    realm = client.connection.realm_name
    if policy_type:
        return f"admin/realms/{realm}/clients/{client_uuid}/authz/resource-server/policy/{policy_type}"
    return f"admin/realms/{realm}/clients/{client_uuid}/authz/resource-server/policy"


def _get_policy_id(
    client: KeycloakAdmin, client_uuid: str, policy_name: str, *, policy_type: str | None = None
) -> str | None:
    url = _policy_url(client, client_uuid, policy_type=policy_type)
    data_raw = client.connection.raw_get(url)
    policies = json.loads(data_raw.text)
    match = next((policy for policy in policies if policy.get("name") == policy_name), None)
    return match.get("id") if match else None


def _get_role_id(client: KeycloakAdmin, client_uuid: str, role_name: str) -> str:
    try:
        role = client.get_realm_role(role_name)
        return role["id"]
    except KeycloakGetError:
        role = client.get_client_role(client_id=client_uuid, role_name=role_name)
        return role["id"]


def _ensure_role_policy(
    client: KeycloakAdmin, client_uuid: str, role_name: str, *, _dry_run: bool = False
) -> None:
    policy_name = _role_policy_name(role_name)
    if _get_policy_id(client, client_uuid, policy_name, policy_type="role"):
        return

    role_id = _get_role_id(client, client_uuid, role_name)
    payload = {
        "name": policy_name,
        "type": "role",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "roles": [{"id": role_id}],
    }

    if _dry_run:
        print(f"Would create role policy '{policy_name}'.")
        return

    try:
        client.create_client_authz_role_based_policy(client_id=client_uuid, payload=payload, skip_exists=True)
    except KeycloakError as exc:
        if exc.response_body:
            error = json.loads(exc.response_body.decode("utf-8"))
            if "Conflicting policy" in error.get("error_description", ""):
                return
        raise


def _ensure_aggregate_policy(
    client: KeycloakAdmin,
    client_uuid: str,
    policy_name: str,
    policy_refs: list[tuple[str, str | None]],
    *,
    _dry_run: bool = False,
) -> None:
    if _get_policy_id(client, client_uuid, policy_name, policy_type="aggregate"):
        return

    policy_ids = []
    for name, policy_type in policy_refs:
        # Aggregate policy enforces group+role; missing inputs should fail fast.
        policy_id = _get_policy_id(client, client_uuid, name, policy_type=policy_type)
        if not policy_id:
            policy_label = f"{policy_type} policy '{name}'" if policy_type else f"policy '{name}'"
            raise ValueError(f"{policy_label} not found.")
        policy_ids.append(policy_id)

    payload = {
        "name": policy_name,
        "type": "aggregate",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "policies": policy_ids,
    }

    if _dry_run:
        print(f"Would create aggregate policy '{policy_name}'.")
        return

    url = _policy_url(client, client_uuid, policy_type="aggregate")
    data_raw = client.connection.raw_post(url, data=json.dumps(payload), max=-1, permission=False)
    try:
        raise_error_from_response(data_raw, KeycloakPostError, expected_codes=[201])
    except KeycloakPostError as exc:
        if exc.response_body:
            error = json.loads(exc.response_body.decode("utf-8"))
            if "Conflicting policy" in error.get("error_description", ""):
                return
        raise


def _attach_policy_to_scope_permission(
    client: KeycloakAdmin,
    client_uuid: str,
    *,
    permission_name: str,
    policy_name: str,
    scope_names: list[str],
    resource_names: list[str],
    decision_strategy: str = "UNANIMOUS",
    _dry_run: bool = False,
) -> None:
    if _dry_run:
        print(f"Would attach policy '{policy_name}' to permission '{permission_name}'.")
        return

    permissions = client.get_client_authz_permissions(client_uuid)
    match = next((perm for perm in permissions if perm.get("name") == permission_name), None)
    if not match:
        raise ValueError(f"Permission '{permission_name}' not found.")

    permission_id = match["id"]
    policy_id = _get_policy_id(client, client_uuid, policy_name)
    if not policy_id:
        raise ValueError(f"Policy '{policy_name}' not found.")

    existing_policy_ids = _get_permission_policy_ids(client, client_uuid, permission_id)
    policy_ids = list(dict.fromkeys([*existing_policy_ids, policy_id]))

    scopes = client.get_client_authz_scopes(client_uuid)
    resources = client.get_client_authz_resources(client_uuid)
    scope_ids = [s["id"] for s in scopes if s["name"] in scope_names]
    resource_ids = [r["_id"] for r in resources if r["name"] in resource_names]

    payload = {
        "id": permission_id,
        "name": permission_name,
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": decision_strategy,
        "scopes": scope_ids,
        "resources": resource_ids,
        "policies": policy_ids,
    }
    client.update_client_authz_scope_permission(
        payload=payload, client_id=client_uuid, scope_id=permission_id
    )


def _get_permission_policy_ids(client: KeycloakAdmin, client_uuid: str, permission_id: str) -> list[str]:
    realm = client.connection.realm_name
    url = (
        f"admin/realms/{realm}/clients/{client_uuid}/authz/resource-server/permission/scope/"
        f"{permission_id}/associatedPolicies"
    )
    data_raw = client.connection.raw_get(url)
    policies = json.loads(data_raw.text)
    return [policy.get("id") for policy in policies if policy.get("id")]


def _attach_policy_to_resource_permission(
    client: KeycloakAdmin,
    client_uuid: str,
    *,
    permission_name: str,
    policy_name: str,
    resource_names: list[str],
    decision_strategy: str = "UNANIMOUS",
    _dry_run: bool = False,
) -> None:
    if _dry_run:
        print(f"Would attach policy '{policy_name}' to permission '{permission_name}'.")
        return

    permissions = client.get_client_authz_permissions(client_uuid)
    match = next((perm for perm in permissions if perm.get("name") == permission_name), None)
    if not match:
        raise ValueError(f"Permission '{permission_name}' not found.")

    permission_id = match["id"]
    policy_id = _get_policy_id(client, client_uuid, policy_name)
    if not policy_id:
        raise ValueError(f"Policy '{policy_name}' not found.")

    existing_policy_ids = _get_resource_permission_policy_ids(client, client_uuid, permission_id)
    policy_ids = list(dict.fromkeys([*existing_policy_ids, policy_id]))

    resources = client.get_client_authz_resources(client_uuid)
    resource_ids = [r["_id"] for r in resources if r["name"] in resource_names]

    payload = {
        "id": permission_id,
        "name": permission_name,
        "type": "resource",
        "logic": "POSITIVE",
        "decisionStrategy": decision_strategy,
        "resources": resource_ids,
        "scopes": [],
        "policies": policy_ids,
    }
    client.update_client_authz_resource_permission(
        payload=payload, client_id=client_uuid, resource_id=permission_id
    )


def _get_resource_permission_policy_ids(
    client: KeycloakAdmin, client_uuid: str, permission_id: str
) -> list[str]:
    realm = client.connection.realm_name
    url = (
        f"admin/realms/{realm}/clients/{client_uuid}/authz/resource-server/permission/resource/"
        f"{permission_id}/associatedPolicies"
    )
    data_raw = client.connection.raw_get(url)
    policies = json.loads(data_raw.text)
    return [policy.get("id") for policy in policies if policy.get("id")]


def _add_user_to_group(client: KeycloakAdmin, *, username: str, team: str, _dry_run: bool = False) -> None:
    group_name = _team_group_name(team)
    group_path = f"/{group_name}"
    group = client.get_group_by_path(group_path)
    if not group:
        raise ValueError(f"Group '{group_name}' not found.")

    users = client.get_users(query={"username": username})
    user = next((u for u in users if u.get("username") == username), None)
    if not user:
        raise ValueError(f"User '{username}' not found.")

    existing_groups = client.get_user_groups(user_id=user["id"])
    if any(g.get("id") == group["id"] for g in existing_groups):
        print(f"User '{username}' is already in group '{group_name}'.")
        return

    if _dry_run:
        print(f"Would add user '{username}' to group '{group_name}'.")
        return

    client.group_user_add(user_id=user["id"], group_id=group["id"])
