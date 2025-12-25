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
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import requests
from fastapi import FastAPI
from keycloak import KeycloakOpenID, KeycloakPostError
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager

try:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ExtendedResourceMethod
except ImportError:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod as ExtendedResourceMethod

from airflow.api_fastapi.common.types import MenuItem
from airflow.cli.cli_config import CLICommand, DefaultHelpParser, GroupCommand
from airflow.providers.common.compat.sdk import AirflowException, conf
from airflow.providers.keycloak.auth_manager.cli.definition import KEYCLOAK_AUTH_MANAGER_COMMANDS
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
    CONF_REALM_KEY,
    CONF_REQUESTS_POOL_SIZE_KEY,
    CONF_REQUESTS_RETRIES_KEY,
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

log = logging.getLogger(__name__)

RESOURCE_ID_ATTRIBUTE_NAME = "resource_id"


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

    def __init__(self):
        super().__init__()
        self._http_session = None

    @property
    def http_session(self) -> requests.Session:
        """Lazy-initialize and return the requests session with connection pooling."""
        if self._http_session is not None:
            return self._http_session

        self._http_session = requests.Session()

        pool_size = conf.getint(CONF_SECTION_NAME, CONF_REQUESTS_POOL_SIZE_KEY, fallback=10)
        retry_total = conf.getint(CONF_SECTION_NAME, CONF_REQUESTS_RETRIES_KEY, fallback=3)

        retry_strategy = Retry(
            total=retry_total,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )

        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry_strategy)

        self._http_session.mount("https://", adapter)
        self._http_session.mount("http://", adapter)

        return self._http_session

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
            tokens = self.refresh_tokens(user=user)

            if tokens:
                user.refresh_token = tokens["refresh_token"]
                user.access_token = tokens["access_token"]
                return user

        return None

    def refresh_tokens(self, *, user: KeycloakAuthManagerUser) -> dict[str, str]:
        try:
            log.debug("Refreshing the token")
            client = self.get_keycloak_client()
            return client.refresh_token(user.refresh_token)
        except KeycloakPostError as exc:
            log.warning(
                "KeycloakPostError encountered during token refresh. "
                "Suppressing the exception and returning None.",
                exc_info=exc,
            )

        return {}

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        config_section = details.section if details else None
        return self._is_authorized(
            method=method,
            resource_type=KeycloakResource.CONFIGURATION,
            user=user,
            resource_id=config_section,
        )

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConnectionDetails | None = None,
    ) -> bool:
        connection_id = details.conn_id if details else None
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.CONNECTION, user=user, resource_id=connection_id
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

    def is_authorized_backfill(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: BackfillDetails | None = None
    ) -> bool:
        backfill_id = str(details.id) if details else None
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.BACKFILL, user=user, resource_id=backfill_id
        )

    def is_authorized_asset(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: AssetDetails | None = None
    ) -> bool:
        asset_id = details.id if details else None
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.ASSET, user=user, resource_id=asset_id
        )

    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: AssetAliasDetails | None = None,
    ) -> bool:
        asset_alias_id = details.id if details else None
        return self._is_authorized(
            method=method,
            resource_type=KeycloakResource.ASSET_ALIAS,
            user=user,
            resource_id=asset_alias_id,
        )

    def is_authorized_variable(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: VariableDetails | None = None
    ) -> bool:
        variable_key = details.key if details else None
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.VARIABLE, user=user, resource_id=variable_key
        )

    def is_authorized_pool(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: PoolDetails | None = None
    ) -> bool:
        pool_name = details.name if details else None
        return self._is_authorized(
            method=method, resource_type=KeycloakResource.POOL, user=user, resource_id=pool_name
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
        return [MenuItem(menu[1]) for menu in authorized_menus]

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
        from airflow.providers.keycloak.auth_manager.cli import get_keycloak_cli_commands

        return get_keycloak_cli_commands()

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

        resp = self.http_session.post(
            self._get_token_url(server_url, realm),
            data=self._get_payload(client_id, f"{resource_type.value}#{method}", context_attributes),
            headers=self._get_headers(user.access_token),
            timeout=5,
        )

        if resp.status_code == 200:
            return True
        if resp.status_code == 401:
            log.debug("Received 401 from Keycloak: %s", resp.text)
            return False
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
    ) -> set[tuple[ExtendedResourceMethod, str]]:
        client_id = conf.get(CONF_SECTION_NAME, CONF_CLIENT_ID_KEY)
        realm = conf.get(CONF_SECTION_NAME, CONF_REALM_KEY)
        server_url = conf.get(CONF_SECTION_NAME, CONF_SERVER_URL_KEY)

        resp = self.http_session.post(
            self._get_token_url(server_url, realm),
            data=self._get_batch_payload(client_id, permissions),
            headers=self._get_headers(user.access_token),
            timeout=5,
        )

        if resp.status_code == 200:
            return {(perm["scopes"][0], perm["rsname"]) for perm in resp.json()}
        if resp.status_code == 403:
            return set()
        if resp.status_code == 400:
            error = json.loads(resp.text)
            raise AirflowException(
                f"Request not recognized by Keycloak. {error.get('error')}. {error.get('error_description')}"
            )
        raise AirflowException(f"Unexpected error: {resp.status_code} - {resp.text}")

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
    def _get_batch_payload(client_id: str, permissions: list[tuple[ExtendedResourceMethod, str]]):
        payload: dict[str, Any] = {
            "grant_type": "urn:ietf:params:oauth:grant-type:uma-ticket",
            "audience": client_id,
            "permission": [f"{permission[1]}#{permission[0]}" for permission in permissions],
            "response_mode": "permissions",
        }

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
