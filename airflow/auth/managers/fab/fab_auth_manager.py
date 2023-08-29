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

from typing import TYPE_CHECKING

from airflow import AirflowException
from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.auth.managers.fab.cli_commands.definition import (
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)
from airflow.auth.managers.models.base_user import BaseUser
from airflow.auth.managers.models.resource_details import ResourceDetails
from airflow.auth.managers.models.resource_method import ResourceMethod
from airflow.auth.managers.models.resource_type import ResourceType
from airflow.cli.cli_config import (
    GroupCommand,
)
from airflow.models import DagModel
from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
    RESOURCE_DAG,
    RESOURCE_DAG_PREFIX,
)

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import User
    from airflow.cli.cli_config import (
        CLICommand,
    )

_MAP_ACTION_NAME_TO_FAB_ACTION_NAME = {
    ResourceMethod.POST: ACTION_CAN_CREATE,
    ResourceMethod.GET: ACTION_CAN_READ,
    ResourceMethod.PUT: ACTION_CAN_EDIT,
    ResourceMethod.DELETE: ACTION_CAN_DELETE,
}


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

    def get_user_name(self) -> str:
        """
        Return the username associated to the user in session.

        For backward compatibility reasons, the username in FAB auth manager is the concatenation of the
        first name and the last name.
        """
        user = self.get_user()
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        return f"{first_name} {last_name}".strip()

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

    def is_authorized(
        self,
        action: ResourceMethod,
        resource_type: ResourceType,
        resource_details: ResourceDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action.

        :param action: the action to perform
        :param resource_type: the type of resource the user attempts to perform the action on
        :param resource_details: optional details about the resource itself
        :param user: the user to perform the action on. If not provided (or None), it uses the current user
        """
        if not user:
            user = self.get_user()

        fab_action = self._get_fab_action(action)
        user_permissions = self._get_user_permissions(user)

        if (fab_action, resource_type.value) in user_permissions:
            return True

        if self.is_dag_resource(resource_type):
            if resource_details and resource_details.id:
                # Check whether the user has permissions to access a specific DAG
                resource_dag_name = self._resource_name_for_dag(resource_details.id)
                return (fab_action, resource_dag_name) in user_permissions

        return False

    def get_security_manager_override_class(self) -> type:
        """Return the security manager override."""
        from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride

        return FabAirflowSecurityManagerOverride

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

    @staticmethod
    def _get_fab_action(action: ResourceMethod) -> str:
        """
        Convert the action to a FAB action.

        :param action: the action to convert

        :meta private:
        """
        if action not in _MAP_ACTION_NAME_TO_FAB_ACTION_NAME:
            raise AirflowException(f"Unknown action: {action}")
        return _MAP_ACTION_NAME_TO_FAB_ACTION_NAME[action]

    def _resource_name_for_dag(self, dag_id: str) -> str:
        """
        Returns the FAB resource name for a DAG id.

        :param dag_id: the DAG id

        :meta private:
        """
        root_dag_id = self._get_root_dag_id(dag_id)
        if root_dag_id == RESOURCE_DAG:
            return root_dag_id
        if root_dag_id.startswith(RESOURCE_DAG_PREFIX):
            return root_dag_id
        return f"{RESOURCE_DAG_PREFIX}{root_dag_id}"

    @staticmethod
    def _get_user_permissions(user: BaseUser):
        """
        Return the user permissions.

        ACTION_CAN_READ and ACTION_CAN_ACCESS_MENU are merged into because they are very similar.
        We can assume that if a user has permissions to read variables, they also have permissions to access
        the menu "Variables".

        :param user: the user to get permissions for

        :meta private:
        """
        return [
            (ACTION_CAN_READ if perm[0] == ACTION_CAN_ACCESS_MENU else perm[0], perm[1])
            for perm in user.perms
        ]

    def _get_root_dag_id(self, dag_id: str) -> str:
        """
        Return the root DAG id in case of sub DAG, return the DAG id otherwise.

        :param dag_id: the DAG id
        """
        if "." in dag_id:
            dm = (
                self.security_manager.appbuilder.get_session.query(DagModel.dag_id, DagModel.root_dag_id)
                .filter(DagModel.dag_id == dag_id)
                .first()
            )
            return dm.root_dag_id or dm.dag_id
        return dag_id
