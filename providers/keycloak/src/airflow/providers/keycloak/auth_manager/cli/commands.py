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
from typing import get_args

from keycloak import KeycloakAdmin, KeycloakError

from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod

try:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
except ImportError:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod as ExtendedResourceMethod
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

log = logging.getLogger(__name__)


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

    _create_resources(client, client_uuid, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_permissions_command(args):
    """Create Keycloak auth manager permissions in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_permissions(client, client_uuid, _dry_run=args.dry_run)


@cli_utils.action_cli
@providers_configuration_loaded
@dry_run_message_wrap
def create_all_command(args):
    """Create all Keycloak auth manager entities in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_scopes(client, client_uuid, _dry_run=args.dry_run)
    _create_resources(client, client_uuid, _dry_run=args.dry_run)
    _create_permissions(client, client_uuid, _dry_run=args.dry_run)


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


def _get_scopes_to_create() -> list[dict]:
    """Get the list of scopes to be created."""
    scopes = [{"name": method} for method in get_args(ResourceMethod)]
    scopes.extend([{"name": "MENU"}, {"name": "LIST"}])
    return scopes


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
    menu_resources = [(item.value, menu_scopes) for item in MenuItem]

    return standard_resources, menu_resources


def _preview_resources(client: KeycloakAdmin, client_uuid: str):
    """Preview resources that would be created."""
    standard_resources, menu_resources = _get_resources_to_create(client, client_uuid)

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
def _create_resources(client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False):
    """Create Keycloak resources."""
    standard_resources, menu_resources = _get_resources_to_create(client, client_uuid)

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


def _get_permissions_to_create(client: KeycloakAdmin, client_uuid: str) -> list[dict]:
    """
    Get the actual permissions to be created with filtered scopes/resources.

    Returns a list of permission descriptors with actual filtered data.
    """
    perm_configs = [
        {
            "name": "ReadOnly",
            "type": "scope-based",
            "scope_names": ["GET", "MENU", "LIST"],
        },
        {
            "name": "Admin",
            "type": "scope-based",
            "scope_names": list(get_args(ExtendedResourceMethod)) + ["LIST"],
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
        else:  # resource-based
            # Filter to get actual resource IDs that exist and match
            filtered_resource_ids = [r["_id"] for r in all_resources if r["name"] in config["resources"]]
            filtered_resource_names = [r["name"] for r in all_resources if r["name"] in config["resources"]]
            perm["resource_ids"] = filtered_resource_ids
            perm["resource_names"] = filtered_resource_names
        result.append(perm)

    return result


def _preview_permissions(client: KeycloakAdmin, client_uuid: str):
    """Preview permissions that would be created."""
    permissions = _get_permissions_to_create(client, client_uuid)

    print("Permissions to be created:")
    for perm in permissions:
        if perm["type"] == "scope-based":
            scope_names = ", ".join(perm["scope_names"])
            print(f"  - {perm['name']} (type: scope-based, scopes: {scope_names})")
        else:  # resource-based
            resource_names = ", ".join(perm["resource_names"])
            print(f"  - {perm['name']} (type: resource-based, resources: {resource_names})")
    print()


@dry_run_preview(_preview_permissions)
def _create_permissions(client: KeycloakAdmin, client_uuid: str, *, _dry_run: bool = False):
    """Create Keycloak permissions."""
    permissions = _get_permissions_to_create(client, client_uuid)

    for perm in permissions:
        if perm["type"] == "scope-based":
            _create_scope_based_permission(client, client_uuid, perm["name"], perm["scope_ids"])
        else:  # resource-based
            _create_resource_based_permission(client, client_uuid, perm["name"], perm["resource_ids"])

    print("Permissions created successfully.")


def _create_scope_based_permission(client: KeycloakAdmin, client_uuid: str, name: str, scope_ids: list[str]):
    payload = {
        "name": name,
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "scopes": scope_ids,
    }

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
