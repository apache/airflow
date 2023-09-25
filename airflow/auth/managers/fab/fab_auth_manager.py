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
from pathlib import Path
from typing import TYPE_CHECKING

from connexion import FlaskApi
from flask import url_for
from sqlalchemy import select

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.auth.managers.fab.cli_commands.definition import (
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)
from airflow.auth.managers.models.resource_details import ConnectionDetails, DagAccessEntity, DagDetails
from airflow.cli.cli_config import (
    GroupCommand,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_CREATE,
    ACTION_CAN_DELETE,
    ACTION_CAN_EDIT,
    ACTION_CAN_READ,
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_PREFIX,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TASK_LOG,
    RESOURCE_VARIABLE,
    RESOURCE_WEBSITE,
    RESOURCE_XCOM,
)
from airflow.utils.yaml import safe_load
from airflow.www.extensions.init_views import _CustomErrorRequestBodyValidator, _LazyResolver

if TYPE_CHECKING:
    from flask import Blueprint

    from airflow.auth.managers.fab.models import User
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.cli.cli_config import (
        CLICommand,
    )

_MAP_METHOD_NAME_TO_FAB_ACTION_NAME: dict[ResourceMethod, str] = {
    "POST": ACTION_CAN_CREATE,
    "GET": ACTION_CAN_READ,
    "PUT": ACTION_CAN_EDIT,
    "DELETE": ACTION_CAN_DELETE,
}

_MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE = {
    DagAccessEntity.AUDIT_LOG: RESOURCE_AUDIT_LOG,
    DagAccessEntity.CODE: RESOURCE_DAG_CODE,
    DagAccessEntity.DATASET: RESOURCE_DATASET,
    DagAccessEntity.DEPENDENCIES: RESOURCE_DAG_DEPENDENCIES,
    DagAccessEntity.RUN: RESOURCE_DAG_RUN,
    DagAccessEntity.TASK_INSTANCE: RESOURCE_TASK_INSTANCE,
    DagAccessEntity.TASK_LOGS: RESOURCE_TASK_LOG,
    DagAccessEntity.XCOM: RESOURCE_XCOM,
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

    def get_api_blueprint(self) -> None | Blueprint:
        """Return a blueprint of the API endpoints proposed by this auth manager."""
        folder = Path(__file__).parents[0].resolve()  # this is airflow/auth/managers/fab/
        with folder.joinpath("openapi", "v1.yaml").open() as f:
            specification = safe_load(f)
        api = FlaskApi(
            specification=specification,
            resolver=_LazyResolver(),
            base_path="/auth/fab/v1",
            options={
                "swagger_ui": conf.getboolean("webserver", "enable_swagger_ui", fallback=True),
            },
            strict_validation=True,
            validate_responses=True,
            validator_map={"body": _CustomErrorRequestBodyValidator},
        )
        return api.blueprint

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

    def is_authorized_configuration(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONFIG, user=user)

    def is_authorized_cluster_activity(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CLUSTER_ACTIVITY, user=user)

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        connection_details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONNECTION, user=user)

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        dag_access_entity: DagAccessEntity | None = None,
        dag_details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to access the dag.

        There are multiple scenarios:

        1. ``dag_access`` is not provided which means the user wants to access the DAG itself and not a sub
        entity (e.g. DAG runs).
        2. ``dag_access`` is provided which means the user wants to access a sub entity of the DAG
        (e.g. DAG runs).
            a. If ``method`` is GET, then check the user has READ permissions on the DAG and the sub entity
            b. Else, check the user has EDIT permissions on the DAG and ``method`` on the sub entity

        :param method: The method to authorize.
        :param dag_access_entity: The dag access entity.
        :param dag_details: The dag details.
        :param user: The user.
        """
        if not dag_access_entity:
            # Scenario 1
            return self._is_authorized_dag(method=method, dag_details=dag_details, user=user)
        else:
            # Scenario 2
            resource_type = self._get_fab_resource_type(dag_access_entity)
            dag_method: ResourceMethod = "GET" if method == "GET" else "PUT"

            return self._is_authorized_dag(
                method=dag_method, dag_details=dag_details, user=user
            ) and self._is_authorized(method=method, resource_type=resource_type, user=user)

    def is_authorized_dataset(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_DATASET, user=user)

    def is_authorized_variable(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_VARIABLE, user=user)

    def is_authorized_website(self, *, user: BaseUser | None = None) -> bool:
        return self._is_authorized(method="GET", resource_type=RESOURCE_WEBSITE, user=user)

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

    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        if "next_url" in kwargs and kwargs["next_url"]:
            return url_for(f"{self.security_manager.auth_view.endpoint}.login", next=kwargs["next_url"])
        else:
            return url_for(f"{self.security_manager.auth_view.endpoint}.login")

    def get_url_logout(self):
        """Return the logout page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        return url_for(f"{self.security_manager.auth_view.endpoint}.logout")

    def get_url_user_profile(self) -> str | None:
        """Return the url to a page displaying info about the current user."""
        if not self.security_manager.user_view:
            return None
        return url_for(f"{self.security_manager.user_view.endpoint}.userinfo")

    def _is_authorized(
        self,
        *,
        method: ResourceMethod,
        resource_type: str,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action.

        :param method: the method to perform
        :param resource_type: the type of resource the user attempts to perform the action on
        :param user: the user to perform the action on. If not provided (or None), it uses the current user

        :meta private:
        """
        if not user:
            user = self.get_user()

        fab_action = self._get_fab_action(method)
        user_permissions = self._get_user_permissions(user)

        return (fab_action, resource_type) in user_permissions

    def _is_authorized_dag(
        self,
        method: ResourceMethod,
        dag_details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG.

        :param method: the method to perform
        :param dag_details: optional details about the DAG
        :param user: the user to perform the action on. If not provided (or None), it uses the current user

        :meta private:
        """
        is_global_authorized = self._is_authorized(method=method, resource_type=RESOURCE_DAG, user=user)
        if is_global_authorized:
            return True

        if dag_details and dag_details.id:
            # Check whether the user has permissions to access a specific DAG
            resource_dag_name = self._resource_name_for_dag(dag_details.id)
            return self._is_authorized(method=method, resource_type=resource_dag_name, user=user)

        return False

    @staticmethod
    def _get_fab_action(method: ResourceMethod) -> str:
        """
        Convert the method to a FAB action.

        :param method: the method to convert

        :meta private:
        """
        if method not in _MAP_METHOD_NAME_TO_FAB_ACTION_NAME:
            raise AirflowException(f"Unknown method: {method}")
        return _MAP_METHOD_NAME_TO_FAB_ACTION_NAME[method]

    @staticmethod
    def _get_fab_resource_type(dag_access_entity: DagAccessEntity):
        """
        Convert a DAG access entity to a FAB resource type.

        :param dag_access_entity: the DAG access entity

        :meta private:
        """
        if dag_access_entity not in _MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE:
            raise AirflowException(f"Unknown DAG access entity: {dag_access_entity}")
        return _MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE[dag_access_entity]

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

        :meta private:
        """
        if "." in dag_id:
            dm = self.security_manager.appbuilder.get_session.scalar(
                select(DagModel.dag_id, DagModel.root_dag_id).where(DagModel.dag_id == dag_id).limit(1)
            )
            return dm.root_dag_id or dm.dag_id
        return dag_id
