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

import json
import os
import random
from collections import namedtuple
from enum import Enum
from typing import TYPE_CHECKING

from flask import session, url_for
from termcolor import colored

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.auth.managers.simple.views.auth import SimpleAuthManagerAuthenticationViews
from hatch_build import AIRFLOW_ROOT_PATH

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


class SimpleAuthManagerRole(namedtuple("SimpleAuthManagerRole", "name order"), Enum):
    """
    List of pre-defined roles in simple auth manager.

    The first attribute defines the name that references this role in the config.
    The second attribute defines the order between roles. The role with order X means it grants access to
    resources under its umbrella and all resources under the umbrella of roles of lower order
    """

    # VIEWER role gives all read-only permissions
    VIEWER = "VIEWER", 0

    # USER role gives viewer role permissions + access to DAGs
    USER = "USER", 1

    # OP role gives user role permissions + access to connections, config, pools, variables
    OP = "OP", 2

    # ADMIN role gives all permissions
    ADMIN = "ADMIN", 3


class SimpleAuthManager(BaseAuthManager):
    """
    Simple auth manager.

    Default auth manager used in Airflow. This auth manager should not be used in production.
    This auth manager is very basic and only intended for development and testing purposes.

    :param appbuilder: the flask app builder
    """

    # File that contains the generated passwords
    GENERATED_PASSWORDS_FILE = (
        AIRFLOW_ROOT_PATH / "generated" / "simple_auth_manager_passwords.json.generated"
    )

    # Cache containing the password associated to a username
    passwords: dict[str, str] = {}

    def init(self) -> None:
        user_passwords_from_file = {}

        # Read passwords from file
        if os.path.isfile(self.GENERATED_PASSWORDS_FILE):
            with open(self.GENERATED_PASSWORDS_FILE) as file:
                passwords_str = file.read().strip()
                user_passwords_from_file = json.loads(passwords_str)

        users = self.appbuilder.get_app.config.get("SIMPLE_AUTH_MANAGER_USERS", [])
        usernames = {user["username"] for user in users}
        self.passwords = {
            username: password
            for username, password in user_passwords_from_file.items()
            if username in usernames
        }
        for user in users:
            if user["username"] not in self.passwords:
                # User dot not exist in the file, adding it
                self.passwords[user["username"]] = self._generate_password()

            self._print_output(f"Password for user '{user['username']}': {self.passwords[user['username']]}")

        with open(self.GENERATED_PASSWORDS_FILE, "w") as file:
            file.write(json.dumps(self.passwords))

    def is_logged_in(self) -> bool:
        return "user" in session or self.appbuilder.get_app.config.get(
            "SIMPLE_AUTH_MANAGER_ALL_ADMINS", False
        )

    def get_url_login(self, **kwargs) -> str:
        return url_for("SimpleAuthManagerAuthenticationViews.login")

    def get_url_logout(self) -> str:
        return url_for("SimpleAuthManagerAuthenticationViews.logout")

    def get_user(self) -> SimpleAuthManagerUser | None:
        if not self.is_logged_in():
            return None
        if self.appbuilder.get_app.config.get("SIMPLE_AUTH_MANAGER_ALL_ADMINS", False):
            return SimpleAuthManagerUser(username="anonymous", role="admin")
        else:
            return session["user"]

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP)

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP)

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
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.USER,
        )

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(
            method=method,
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.OP,
        )

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(
            method=method,
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.OP,
        )

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP)

    def is_authorized_view(self, *, access_view: AccessView, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method="GET", allow_role=SimpleAuthManagerRole.VIEWER)

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseUser | None = None
    ):
        return self._is_authorized(method="GET", allow_role=SimpleAuthManagerRole.VIEWER)

    def register_views(self) -> None:
        self.appbuilder.add_view_no_menu(
            SimpleAuthManagerAuthenticationViews(
                users=self.appbuilder.get_app.config.get("SIMPLE_AUTH_MANAGER_USERS", []),
                passwords=self.passwords,
            )
        )

    def _is_authorized(
        self,
        *,
        method: ResourceMethod,
        allow_role: SimpleAuthManagerRole,
        allow_get_role: SimpleAuthManagerRole | None = None,
    ):
        """
        Return whether the user is authorized to access a given resource.

        :param method: the method to perform
        :param allow_role: minimal role giving access to the resource, if the user's role is greater or
            equal than this role, they have access
        :param allow_get_role: minimal role giving access to the resource, if the user's role is greater or
            equal than this role, they have access. If not provided, ``allow_role`` is used
        """
        user = self.get_user()
        if not user:
            return False

        user_role = user.get_role()
        if not user_role:
            return False

        role_str = user_role.upper()
        role = SimpleAuthManagerRole[role_str]
        if role == SimpleAuthManagerRole.ADMIN:
            return True

        if not allow_get_role:
            allow_get_role = allow_role

        if method == "GET":
            return role.order >= allow_get_role.order
        return role.order >= allow_role.order

    @staticmethod
    def _generate_password() -> str:
        return "".join(random.choices("abcdefghkmnpqrstuvwxyzABCDEFGHKMNPQRSTUVWXYZ23456789", k=16))

    @staticmethod
    def _print_output(output: str):
        name = "Simple auth manager"
        colorized_name = colored(f"{name:10}", "white")
        for line in output.splitlines():
            print(f"{colorized_name} | {line.strip()}")
