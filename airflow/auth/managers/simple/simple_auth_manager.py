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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI
from flask import session
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from termcolor import colored

from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import AIRFLOW_HOME, conf
from airflow.settings import AIRFLOW_PATH

if TYPE_CHECKING:
    from flask_appbuilder.menu import MenuItem

    from airflow.auth.managers.base_auth_manager import ResourceMethod
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        AssetDetails,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
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


class SimpleAuthManager(BaseAuthManager[SimpleAuthManagerUser]):
    """
    Simple auth manager.

    Default auth manager used in Airflow. This auth manager should not be used in production.
    This auth manager is very basic and only intended for development and testing purposes.
    """

    @staticmethod
    def get_generated_password_file() -> str:
        return os.path.join(
            os.getenv("AIRFLOW_AUTH_MANAGER_CREDENTIAL_DIRECTORY", AIRFLOW_HOME),
            "simple_auth_manager_passwords.json.generated",
        )

    @staticmethod
    def get_users() -> list[dict[str, str]]:
        users = [u.split(":") for u in conf.getlist("core", "simple_auth_manager_users")]
        return [{"username": username, "role": role} for username, role in users]

    @staticmethod
    def get_passwords(users: list[dict[str, str]]) -> dict[str, str]:
        user_passwords_from_file = {}

        # Read passwords from file
        password_file = SimpleAuthManager.get_generated_password_file()
        if os.path.isfile(password_file):
            with open(password_file) as file:
                passwords_str = file.read().strip()
                user_passwords_from_file = json.loads(passwords_str)

        usernames = {user["username"] for user in users}
        return {
            username: password
            for username, password in user_passwords_from_file.items()
            if username in usernames
        }

    def init(self) -> None:
        users = self.get_users()
        passwords = self.get_passwords(users)
        for user in users:
            if user["username"] not in passwords:
                # User dot not exist in the file, adding it
                passwords[user["username"]] = self._generate_password()

            self._print_output(f"Password for user '{user['username']}': {passwords[user['username']]}")

        with open(self.get_generated_password_file(), "w") as file:
            file.write(json.dumps(passwords))

    def is_logged_in(self) -> bool:
        # Remove this method when legacy UI is removed
        return "user" in session or conf.getboolean("core", "simple_auth_manager_all_admins")

    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""
        return "/auth/webapp/login"

    def get_url_logout(self) -> str:
        # Remove this method when legacy UI is removed
        raise NotImplementedError()

    def get_user(self) -> SimpleAuthManagerUser | None:
        # Remove this method when legacy UI is removed
        if not self.is_logged_in():
            return None
        if conf.getboolean("core", "simple_auth_manager_all_admins"):
            return SimpleAuthManagerUser(username="anonymous", role="admin")
        else:
            return session["user"]

    def deserialize_user(self, token: dict[str, Any]) -> SimpleAuthManagerUser:
        return SimpleAuthManagerUser(username=token["username"], role=token["role"])

    def serialize_user(self, user: SimpleAuthManagerUser) -> dict[str, Any]:
        return {"username": user.username, "role": user.role}

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP, user=user)

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        details: ConnectionDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP, user=user)

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
    ) -> bool:
        return self._is_authorized(
            method=method,
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.USER,
            user=user,
        )

    def is_authorized_asset(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        details: AssetDetails | None = None,
    ) -> bool:
        return self._is_authorized(
            method=method,
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.OP,
            user=user,
        )

    def is_authorized_pool(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        details: PoolDetails | None = None,
    ) -> bool:
        return self._is_authorized(
            method=method,
            allow_get_role=SimpleAuthManagerRole.VIEWER,
            allow_role=SimpleAuthManagerRole.OP,
            user=user,
        )

    def is_authorized_variable(
        self,
        *,
        method: ResourceMethod,
        user: SimpleAuthManagerUser,
        details: VariableDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, allow_role=SimpleAuthManagerRole.OP, user=user)

    def is_authorized_view(self, *, access_view: AccessView, user: SimpleAuthManagerUser) -> bool:
        return self._is_authorized(method="GET", allow_role=SimpleAuthManagerRole.VIEWER, user=user)

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: SimpleAuthManagerUser
    ):
        return self._is_authorized(method="GET", allow_role=SimpleAuthManagerRole.VIEWER, user=user)

    def filter_permitted_menu_items(self, menu_items: list[MenuItem]) -> list[MenuItem]:
        return menu_items

    def get_fastapi_app(self) -> FastAPI | None:
        """
        Specify a sub FastAPI application specific to the auth manager.

        This sub application, if specified, is mounted in the main FastAPI application.
        """
        from airflow.auth.managers.simple.routes.login import login_router

        dev_mode = os.environ.get("DEV_MODE", False) == "true"
        directory = Path(AIRFLOW_PATH) / (
            "airflow/auth/managers/simple/ui/dev" if dev_mode else "airflow/auth/managers/simple/ui/dist"
        )
        Path(directory).mkdir(exist_ok=True)

        templates = Jinja2Templates(directory=directory)

        app = FastAPI(
            title="Simple auth manager sub application",
            description=(
                "This is the simple auth manager fastapi sub application. This API is only available if the "
                "auth manager used in the Airflow environment is simple auth manager. "
                "This sub application provides the login form for users to log in."
            ),
        )
        app.include_router(login_router)
        app.mount(
            "/static",
            StaticFiles(
                directory=directory,
                html=True,
            ),
            name="simple_auth_manager_ui_folder",
        )

        @app.get("/webapp/{rest_of_path:path}", response_class=HTMLResponse, include_in_schema=False)
        def webapp(request: Request, rest_of_path: str):
            return templates.TemplateResponse("/index.html", {"request": request}, media_type="text/html")

        return app

    @staticmethod
    def _is_authorized(
        *,
        method: ResourceMethod,
        allow_role: SimpleAuthManagerRole,
        user: SimpleAuthManagerUser,
        allow_get_role: SimpleAuthManagerRole | None = None,
    ):
        """
        Return whether the user is authorized to access a given resource.

        :param method: the method to perform
        :param allow_role: minimal role giving access to the resource, if the user's role is greater or
            equal than this role, they have access
        :param user: the user to check the authorization for
        :param allow_get_role: minimal role giving access to the resource, if the user's role is greater or
            equal than this role, they have access. If not provided, ``allow_role`` is used
        """
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
