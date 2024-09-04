#
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

from enum import Enum
from typing import TYPE_CHECKING

from flask import session, url_for

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.auth.managers.simple.views.auth import SimpleAuthManagerAuthenticationViews

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        DatasetDetails,
        PoolDetails,
        VariableDetails,
    )
    from airflow.auth.managers.simple.user import SimpleAuthManagerUser


class SimpleAuthManagerRole(Enum):
    """List of pre-defined roles in simple auth manager."""

    # Admin role gives all permissions
    ADMIN = "admin"

    # Viewer role gives all read-only permissions
    VIEWER = "viewer"

    # User role gives viewer role permissions + access to DAGs
    USER = "user"

    # OP role gives user role permissions + access to connections, config, pools, variables
    OP = "op"


class SimpleAuthManager(BaseAuthManager):
    """
    Simple auth manager.

    Default auth manager used in Airflow. This auth manager should not be used in production.
    This auth manager is very basic and only intended for development and testing purposes.

    :param appbuilder: the flask app builder
    """

    def is_logged_in(self) -> bool:
        return "user" in session

    def get_url_login(self, **kwargs) -> str:
        return url_for("SimpleAuthManagerAuthenticationViews.login")

    def get_url_logout(self) -> str:
        return url_for("SimpleAuthManagerAuthenticationViews.logout")

    def get_user(self) -> SimpleAuthManagerUser | None:
        return session["user"] if self.is_logged_in() else None

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, roles_to_allow=[SimpleAuthManagerRole.OP.value])

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, roles_to_allow=[SimpleAuthManagerRole.OP.value])

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(
            method=method,
            roles_to_allow=[SimpleAuthManagerRole.USER.value, SimpleAuthManagerRole.OP.value],
        )

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, roles_to_allow=[SimpleAuthManagerRole.OP.value])

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, roles_to_allow=[SimpleAuthManagerRole.OP.value])

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, roles_to_allow=[SimpleAuthManagerRole.OP.value])

    def is_authorized_view(self, *, access_view: AccessView, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method="GET")

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseUser | None = None
    ):
        return self.is_logged_in()

    def register_views(self) -> None:
        self.appbuilder.add_view_no_menu(
            SimpleAuthManagerAuthenticationViews(
                users=self.appbuilder.get_app.config.get("SIMPLE_AUTH_MANAGER_USERS", [])
            )
        )

    def _is_authorized(
        self,
        *,
        method: ResourceMethod,
        roles_to_allow: list[str] | None = None,
    ):
        """
        Return whether the user is authorized to access a given resource.

        :param method: the method to perform
        :param roles_to_allow: list of roles giving access to the resource, if the user's role is one of these roles, they have access
        """
        user = self.get_user()
        if not user:
            return False
        role = user.get_role()
        if role == SimpleAuthManagerRole.ADMIN.value:
            return True
        if method == "GET":
            return True

        if not roles_to_allow:
            roles_to_allow = []

        return role in roles_to_allow
