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

import warnings
from typing import TYPE_CHECKING

from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.auth.managers.fab.cli_commands.definition import (
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)
from airflow.cli.cli_config import (
    GroupCommand,
)
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import User
    from airflow.cli.cli_config import (
        CLICommand,
    )


class FabAuthManager(BaseAuthManager):
    """
    Flask-AppBuilder auth manager.

    This auth manager is responsible for providing a backward compatible user management experience to users.
    """

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI."""
        return [
            GroupCommand(
                name="users",
                help="Manage users",
                subcommands=USERS_COMMANDS,
            ),
            GroupCommand(
                name="roles",
                help="Manage roles",
                subcommands=ROLES_COMMANDS,
            ),
            SYNC_PERM_COMMAND,  # not in a command group
        ]

    def get_user_display_name(self) -> str:
        """Return the user's display name associated to the user in session."""
        user = self.get_user()
        first_name = user.first_name.strip() if isinstance(user.first_name, str) else ""
        last_name = user.last_name.strip() if isinstance(user.last_name, str) else ""
        return f"{first_name} {last_name}".strip()

    def get_user_name(self) -> str:
        """
        Return the username associated to the user in session.

        For backward compatibility reasons, the username in FAB auth manager can be any of username,
        email, or the database user ID.
        """
        user = self.get_user()
        return user.username or user.email or self.get_user_id()

    def get_user(self) -> User:
        """Return the user associated to the user in session."""
        from flask_login import current_user

        return current_user

    def get_user_id(self) -> str:
        """Return the user ID associated to the user in session."""
        return str(self.get_user().get_id())

    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        return not self.get_user().is_anonymous

    def get_security_manager_override_class(self) -> type:
        """Return the security manager override."""
        from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride
        from airflow.www.security import AirflowSecurityManager

        sm_from_config = self.app.config.get("SECURITY_MANAGER_CLASS")
        if sm_from_config:
            if not issubclass(sm_from_config, AirflowSecurityManager):
                raise Exception(
                    """Your CUSTOM_SECURITY_MANAGER must extend FabAirflowSecurityManagerOverride,
                     not FAB's own security manager."""
                )
            if not issubclass(sm_from_config, FabAirflowSecurityManagerOverride):
                warnings.warn(
                    "Please make your custom security manager inherit from "
                    "FabAirflowSecurityManagerOverride instead of AirflowSecurityManager.",
                    DeprecationWarning,
                )
            return sm_from_config

        return FabAirflowSecurityManagerOverride  # default choice

    def url_for(self, *args, **kwargs):
        """Wrapper to allow mocking without having to import at the top of the file."""
        from flask import url_for

        return url_for(*args, **kwargs)

    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        if "next_url" in kwargs and kwargs["next_url"]:
            return self.url_for(f"{self.security_manager.auth_view.endpoint}.login", next=kwargs["next_url"])
        else:
            return self.url_for(f"{self.security_manager.auth_view.endpoint}.login")

    def get_url_logout(self):
        """Return the logout page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        return self.url_for(f"{self.security_manager.auth_view.endpoint}.logout")

    def get_url_user_profile(self) -> str | None:
        """Return the url to a page displaying info about the current user."""
        if not self.security_manager.user_view:
            return None
        return self.url_for(f"{self.security_manager.user_view.endpoint}.userinfo")
