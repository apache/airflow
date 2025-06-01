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

from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager, T
from airflow.configuration import conf
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

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
    from airflow.api_fastapi.common.types import MenuItem


class KeycloakAuthManager(BaseAuthManager[KeycloakAuthManagerUser]):
    """
    Keycloak auth manager.

    Leverages Keycloak to perform authentication and authorization in Airflow.
    """

    @cached_property
    def api_server_endpoint(self) -> str:
        return conf.get("api", "base_url", fallback="/")

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
        return urljoin(self.api_server_endpoint, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login")

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        return True

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: ConnectionDetails | None = None,
    ) -> bool:
        return True

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: T,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
    ) -> bool:
        return True

    def is_authorized_backfill(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: BackfillDetails | None = None
    ) -> bool:
        return True

    def is_authorized_asset(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: AssetDetails | None = None
    ) -> bool:
        return True

    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        user: KeycloakAuthManagerUser,
        details: AssetAliasDetails | None = None,
    ) -> bool:
        return True

    def is_authorized_variable(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: VariableDetails | None = None
    ) -> bool:
        return True

    def is_authorized_pool(
        self, *, method: ResourceMethod, user: KeycloakAuthManagerUser, details: PoolDetails | None = None
    ) -> bool:
        return True

    def is_authorized_view(self, *, access_view: AccessView, user: KeycloakAuthManagerUser) -> bool:
        return True

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: KeycloakAuthManagerUser
    ) -> bool:
        return True

    def filter_authorized_menu_items(
        self, menu_items: list[MenuItem], *, user: KeycloakAuthManagerUser
    ) -> list[MenuItem]:
        return menu_items

    def get_fastapi_app(self) -> FastAPI | None:
        from airflow.providers.keycloak.auth_manager.routes.login import login_router

        app = FastAPI(
            title="Keycloak auth manager sub application",
            description=(
                "This is the Keycloak auth manager fastapi sub application. This API is only available if the "
                "auth manager used in the Airflow environment is Keycloak auth manager. "
                "This sub application provides login routes."
            ),
        )
        # authlib requires ``SessionMiddleware``
        app.add_middleware(
            SessionMiddleware,
            secret_key=conf.get("api_auth", "jwt_secret"),
            https_only=False,
        )
        app.include_router(login_router)

        return app
