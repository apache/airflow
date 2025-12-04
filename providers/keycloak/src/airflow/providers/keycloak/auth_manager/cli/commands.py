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
from airflow.configuration import conf
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
def create_scopes_command(args):
    """Create Keycloak auth manager scopes in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_scopes(client, client_uuid)


@cli_utils.action_cli
@providers_configuration_loaded
def create_resources_command(args):
    """Create Keycloak auth manager resources in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_resources(client, client_uuid)


@cli_utils.action_cli
@providers_configuration_loaded
def create_permissions_command(args):
    """Create Keycloak auth manager permissions in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_permissions(client, client_uuid)


@cli_utils.action_cli
@providers_configuration_loaded
def create_all_command(args):
    """Create all Keycloak auth manager entities in Keycloak."""
    client = _get_client(args)
    client_uuid = _get_client_uuid(args)

    _create_scopes(client, client_uuid)
    _create_resources(client, client_uuid)
    _create_permissions(client, client_uuid)


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


def _create_scopes(client: KeycloakAdmin, client_uuid: str):
    scopes = [{"name": method} for method in get_args(ResourceMethod)]
    scopes.extend([{"name": "MENU"}, {"name": "LIST"}])
    for scope in scopes:
        client.create_client_authz_scopes(client_id=client_uuid, payload=scope)

    print("Scopes created successfully.")


def _create_resources(client: KeycloakAdmin, client_uuid: str):
    all_scopes = client.get_client_authz_scopes(client_uuid)
    scopes = [
        {"id": scope["id"], "name": scope["name"]}
        for scope in all_scopes
        if scope["name"] in ["GET", "POST", "PUT", "DELETE", "LIST"]
    ]

    for resource in KeycloakResource:
        client.create_client_authz_resource(
            client_id=client_uuid,
            payload={
                "name": resource.value,
                "scopes": scopes,
            },
            skip_exists=True,
        )

    # Create menu item resources
    scopes = [{"id": scope["id"], "name": scope["name"]} for scope in all_scopes if scope["name"] == "MENU"]

    for item in MenuItem:
        client.create_client_authz_resource(
            client_id=client_uuid,
            payload={
                "name": item.value,
                "scopes": scopes,
            },
            skip_exists=True,
        )

    print("Resources created successfully.")


def _create_permissions(client: KeycloakAdmin, client_uuid: str):
    _create_read_only_permission(client, client_uuid)
    _create_admin_permission(client, client_uuid)
    _create_user_permission(client, client_uuid)
    _create_op_permission(client, client_uuid)

    print("Permissions created successfully.")


def _create_read_only_permission(client: KeycloakAdmin, client_uuid: str):
    all_scopes = client.get_client_authz_scopes(client_uuid)
    scopes = [scope["id"] for scope in all_scopes if scope["name"] in ["GET", "MENU", "LIST"]]
    payload = {
        "name": "ReadOnly",
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "scopes": scopes,
    }
    _create_permission(client, client_uuid, payload)


def _create_admin_permission(client: KeycloakAdmin, client_uuid: str):
    all_scopes = client.get_client_authz_scopes(client_uuid)
    scope_names = get_args(ExtendedResourceMethod) + ("LIST",)
    scopes = [scope["id"] for scope in all_scopes if scope["name"] in scope_names]
    payload = {
        "name": "Admin",
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "scopes": scopes,
    }
    _create_permission(client, client_uuid, payload)


def _create_user_permission(client: KeycloakAdmin, client_uuid: str):
    _create_resource_based_permission(
        client, client_uuid, "User", [KeycloakResource.DAG.value, KeycloakResource.ASSET.value]
    )


def _create_op_permission(client: KeycloakAdmin, client_uuid: str):
    _create_resource_based_permission(
        client,
        client_uuid,
        "Op",
        [
            KeycloakResource.CONNECTION.value,
            KeycloakResource.POOL.value,
            KeycloakResource.VARIABLE.value,
            KeycloakResource.BACKFILL.value,
        ],
    )


def _create_permission(client: KeycloakAdmin, client_uuid: str, payload: dict):
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
    client: KeycloakAdmin, client_uuid: str, name: str, allowed_resources: list[str]
):
    all_resources = client.get_client_authz_resources(client_uuid)
    resources = [resource["_id"] for resource in all_resources if resource["name"] in allowed_resources]

    payload = {
        "name": name,
        "type": "scope",
        "logic": "POSITIVE",
        "decisionStrategy": "UNANIMOUS",
        "resources": resources,
    }
    client.create_client_authz_resource_based_permission(
        client_id=client_uuid,
        payload=payload,
        skip_exists=True,
    )
