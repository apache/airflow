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
import json
import logging
import time
from base64 import urlsafe_b64decode
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urljoin

import requests
from fastapi import FastAPI
from keycloak import KeycloakOpenID

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager

try:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
except ImportError:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod as ExtendedResourceMethod

from airflow.api_fastapi.common.types import MenuItem
from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.keycloak.auth_manager.cli.definition import KEYCLOAK_AUTH_MANAGER_COMMANDS
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
)
from airflow.providers.keycloak.auth_manager.resources import KeycloakResource
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AccessView,
        AssetAliasDetails,
        AssetDetails,
        BackfillDetails,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        PoolDetails,
        VariableDetails,
    )

ContextAttributes = dict[str, str | None]
OptionalContextAttributes = ContextAttributes | None

log = logging.getLogger(__name__)

RESOURCE_ID_ATTRIBUTE_NAME = "resource_id"
DAG_IDS_ATTRIBUTE_NAME = "dag_ids"
DAG_IDS_ATTRIBUTE_SEPARATOR = ","


def _summarize(values: Iterable[str], *, limit: int = 5) -> str:
    items = [value for value in values if value]
    if not items:
        return "-"
    items.sort()
    sample = items[:limit]
    suffix = ", ..." if len(items) > limit else ""
    return ", ".join(sample) + suffix


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in KeycloakAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser


class KeycloakAuthManager(BaseAuthManager[KeycloakAuthManagerUser]):
    """
    Keycloak auth manager.

    Leverages Keycloak to perform authentication and authorization in Airflow.
    """

    def __init__(self) -> None:
        super().__init__()

    def deserialize_user(self, token: dict[str, Any]) -> KeycloakAuthManagerUser:
        return KeycloakAuthManagerUser(
            user_id=token.pop("user_id"),
            name=token.pop("name"),
            access_token=token.pop("access_token"),
            refresh_token=token.pop("refresh_token"),
        )

    def serialize_user(self, user: KeycloakAuthManagerUser) -> dict[str, Any]:
        return {
            "user_id": user.get_id(),
            "name": user.get_name(),
            "access_token": user.access_token,
            "refresh_token": user.refresh_token,
        }

    def get_url_login(self, **kwargs) -> str:
        base_url = conf.get("api", "base_url", fallback="/")
        return urljoin(base_url, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login")

    def get_url_logout(self) -> str | None:
        base_url = conf.get("api", "base_url", fallback="/")
        return urljoin(base_url, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout")

    def refresh_user(self, *, user: KeycloakAuthManagerUser) -> KeycloakAuthManagerUser | None:
        if self._token_expired(user.access_token):
            log.debug("Refreshing the token")
            client = self.get_keycloak_client()
            tokens = client.refresh_token(user.refresh_token)
            user.refresh_token = tokens["refresh_token"]
            user.access_token = tokens["access_token"]
            return user

        return None

    def _authorize_resource_from_details(
        self,
        *,
        method: ResourceMethod | str,
        user: KeycloakAuthManagerUser,
        resource_type: KeycloakResource,
        details: Any | None = None,
        detail_attr: str | None = None,
        transform: Callable[[Any], str] | None = None,
        attributes: dict[str, str | None] | None = None,
    ) -> bool:
        """Pull the desired resource-id from ``details`` (when provided) and proxy to ``_is_authorized``."""
        resource_id = None
        if details and detail_attr:
            value = getattr(details, detail_attr, None)
            if value is not None:
                resource_id = transform(value) if transform else value

        return self._is_authorized(
            method=method,
            resource_type=resource_type,
            user=user,
            resource_id=resource_id,
            attributes=attributes,
        )

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.CONFIGURATION,
            details=details,
            detail_attr="section",
        )

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConnectionDetails | None = None,
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.CONNECTION,
            details=details,
            detail_attr="conn_id",
        )

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
    ) -> bool:
        dag_id = details.id if details else None
        access_entity_str = access_entity.value if access_entity else None
        return self._is_authorized(
            method=method,
            resource_type=KeycloakResource.DAG,
            user=user,
            resource_id=dag_id,
            attributes={"dag_entity": access_entity_str},
        )

    def filter_authorized_dag_ids(
        self,
        *,
        dag_ids: set[str],
        user: KeycloakAuthManagerUser,
        method: ResourceMethod = "GET",
        team_name: str | None = None,
    ) -> set[str]:
        method_value = cast("ExtendedResourceMethod", method)
        if not dag_ids:
            return set()

        attributes: ContextAttributes = {}
        if team_name:
            attributes["team_name"] = team_name
        attributes[DAG_IDS_ATTRIBUTE_NAME] = DAG_IDS_ATTRIBUTE_SEPARATOR.join(sorted(dag_ids))

        if log.isEnabledFor(logging.INFO):
            log.info(
                "Keycloak DAG visibility request user=%s team=%s requested=%d sample=[%s]",
                user.get_id(),
                team_name or "-",
                len(dag_ids),
                _summarize(dag_ids),
            )

        permissions = self._is_batch_authorized(
            permissions=[(method_value, KeycloakResource.DAG.value)],
            user=user,
            attributes=attributes,
        )
        authorized = self._extract_authorized_dag_ids(permissions, dag_ids)

        if log.isEnabledFor(logging.INFO):
            log.info(
                "Keycloak DAG visibility grant user=%s team=%s count=%d sample=[%s]",
                user.get_id(),
                team_name or "-",
                len(authorized),
                _summarize(authorized),
            )

        return authorized

    def is_authorized_backfill(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: BackfillDetails | None = None
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.BACKFILL,
            details=details,
            detail_attr="id",
            transform=str,
        )

    def is_authorized_asset(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: AssetDetails | None = None
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.ASSET,
            details=details,
            detail_attr="id",
        )

    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: AssetAliasDetails | None = None,
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.ASSET_ALIAS,
            details=details,
            detail_attr="id",
        )

    def is_authorized_variable(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: VariableDetails | None = None
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.VARIABLE,
            details=details,
            detail_attr="key",
        )

    def is_authorized_pool(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: PoolDetails | None = None
    ) -> bool:
        return self._authorize_resource_from_details(
            method=method,
            user=user,
            resource_type=KeycloakResource.POOL,
            details=details,
            detail_attr="name",
        )

    def is_authorized_view(self, *, access_view: AccessView, user: KeycloakAuthManagerUser) -> bool:
        return self._is_authorized(
            method="GET",
            resource_type=KeycloakResource.VIEW,
            user=user,
            resource_id=access_view.value,
        )

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: KeycloakAuthManagerUser
    ) -> bool:
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.CUSTOM, user=user, resource_id=resource_name
        )

    def filter_authorized_menu_items(
        self, menu_items: list[MenuItem], *, user: KeycloakAuthManagerUser
    ) -> list[MenuItem]:
        authorized_menus = self._is_batch_authorized(
            permissions=[("MENU", menu_item.value) for menu_item in menu_items],
            user=user,
        )
        target_values = {menu_item.value for menu_item in menu_items}
        return [
            MenuItem(permission["rsname"])
            for permission in authorized_menus
            if permission.get("rsname") in target_values
        ]

    def get_fastapi_app(self) -> FastAPI | None:
        from airflow.providers.keycloak.auth_manager.routes.login import login_router
        from airflow.providers.keycloak.auth_manager.routes.token import token_router

        app = FastAPI(
            title="Keycloak auth manager sub application",
            description=(
                "This is the Keycloak auth manager fastapi sub application. This API is only available if the "
                "auth manager used in the Airflow environment is Keycloak auth manager. "
                "This sub application provides login routes."
            ),
        )
        app.include_router(login_router)
        app.include_router(token_router)

        return app

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI."""
        return [
            GroupCommand(
                name="keycloak-auth-manager",
                help="Manage resources used by Keycloak auth manager",
                subcommands=KEYCLOAK_AUTH_MANAGER_COMMANDS,
            ),
        ]

    @staticmethod
    def get_keycloak_client() -> KeycloakOpenID:
        client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)
        client_secret = conf.get(CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY)
        realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)
        server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)

        return KeycloakOpenID(
            server_url=server_url,
            client_id=client_id,
            client_secret_key=client_secret,
            realm_name=realm,
        )

    def _is_authorized(
        self,
        *,
        method: ResourceMethod | str,
        resource_type: KeycloakResource,
        user: KeycloakAuthManagerUser,
        resource_id: str | None = None,
        attributes: dict[str, str | None] | None = None,
    ) -> bool:
        client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)
        realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)
        server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)

        context_attributes = prune_dict(attributes or {})
        if resource_id:
            context_attributes[RESOURCE_ID_ATTRIBUTE_NAME] = resource_id
        elif method == "GET":
            method = "LIST"

        if resource_type == KeycloakResource.DAG and method == "LIST":
            return True

        if resource_type == KeycloakResource.DAG:
            requested_ids: set[str] = set()

            dag_ids_attr = context_attributes.get(DAG_IDS_ATTRIBUTE_NAME)
            if dag_ids_attr:
                requested_ids.update(
                    dag_id.strip()
                    for dag_id in dag_ids_attr.split(DAG_IDS_ATTRIBUTE_SEPARATOR)
                    if dag_id.strip()
                )

            if resource_id:
                requested_ids.add(resource_id)

            if requested_ids:
                context_attributes[DAG_IDS_ATTRIBUTE_NAME] = DAG_IDS_ATTRIBUTE_SEPARATOR.join(
                    sorted(requested_ids)
                )
            else:
                context_attributes.pop(DAG_IDS_ATTRIBUTE_NAME, None)

        resp = requests.post(
            self._get_token_url(server_url, realm),
            data=self._get_payload(client_id, f"{resource_type.value}#{method}", context_attributes),
            headers=self._get_headers(user.access_token),
        )

        if resp.status_code == 200:
            return True
        if resp.status_code == 403:
            return False
        if resp.status_code == 400:
            error = json.loads(resp.text)
            raise AirflowException(
                f"Request not recognized by Keycloak. {error.get('error')}. {error.get('error_description')}"
            )
        raise AirflowException(f"Unexpected error: {resp.status_code} - {resp.text}")

    def _is_batch_authorized(
        self,
        *,
        permissions: list[tuple[ExtendedResourceMethod, str]],
        user: KeycloakAuthManagerUser,
        attributes: OptionalContextAttributes = None,
    ) -> list[dict[str, Any]]:
        client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)
        realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)
        server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)

        context_attributes = prune_dict(attributes or {}) if attributes else None

        if (
            log.isEnabledFor(logging.DEBUG)
            and context_attributes
            and context_attributes.get(DAG_IDS_ATTRIBUTE_NAME)
        ):
            dag_ids_preview = context_attributes[DAG_IDS_ATTRIBUTE_NAME]
            log.debug(
                "Submitting UMA batch request dag_ids=%s team=%s",
                dag_ids_preview,
                context_attributes.get("team_name"),
            )

        resp = requests.post(
            self._get_token_url(server_url, realm),
            data=self._get_batch_payload(client_id, permissions, context_attributes),
            headers=self._get_headers(user.access_token),
        )

        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list):
                return data
            return []
        if resp.status_code == 403:
            if context_attributes and context_attributes.get(DAG_IDS_ATTRIBUTE_NAME):
                log.warning("Keycloak denied DAG visibility request with 403 response.")
            return []
        if resp.status_code == 400:
            error = json.loads(resp.text)
            raise AirflowException(
                f"Request not recognized by Keycloak. {error.get('error')}. {error.get('error_description')}"
            )
        raise AirflowException(f"Unexpected error: {resp.status_code} - {resp.text}")

    def _extract_authorized_dag_ids(
        self,
        permissions: list[dict[str, Any]],
        requested_dag_ids: set[str],
    ) -> set[str]:
        """
        Extract DAG ids from UMA permission responses.

        The Keycloak policy should emit the DAG ids as individual scopes on the granted permission entries.
        """
        if not permissions:
            return set()

        authorized: set[str] = set()
        for permission in permissions:
            scopes = permission.get("scopes") or []
            for scope in scopes:
                if scope in requested_dag_ids:
                    authorized.add(scope)

            resource_name = permission.get("rsname")
            if resource_name and resource_name in requested_dag_ids:
                authorized.add(resource_name)

            resource_id = permission.get("rsid")
            if resource_id and resource_id in requested_dag_ids:
                authorized.add(resource_id)

        return authorized

    @staticmethod
    def _get_token_url(server_url, realm):
        return f"{server_url}/realms/{realm}/protocol/openid-connect/token"

    @staticmethod
    def _get_payload(client_id: str, permission: str, attributes: dict[str, str] | None = None):
        payload: dict[str, Any] = {
            "grant_type": "urn:ietf:params:oauth:grant-type:uma-ticket",
            "audience": client_id,
            "permission": permission,
        }
        if attributes:
            payload["context"] = {"attributes": attributes}

        return payload

    @staticmethod
    def _get_batch_payload(
        client_id: str,
        permissions: list[tuple[ExtendedResourceMethod, str]],
        attributes: dict[str, str] | None = None,
    ):
        payload: dict[str, Any] = {
            "grant_type": "urn:ietf:params:oauth:grant-type:uma-ticket",
            "audience": client_id,
            "permission": [f"{permission[1]}#{permission[0]}" for permission in permissions],
            "response_mode": "permissions",
        }
        if attributes:
            payload["context"] = {"attributes": attributes}

        return payload

    @staticmethod
    def _get_headers(access_token):
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    @staticmethod
    def _token_expired(token: str) -> bool:
        """
        Check whether a JWT token is expired.

        :meta private:

        :param token: the token
        """
        payload_b64 = token.split(".")[1] + "=="
        payload_bytes = urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_bytes)
        return payload["exp"] < int(time.time())
