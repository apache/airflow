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

import argparse
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Container

from connexion import FlaskApi
from flask import Blueprint, url_for
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
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
from airflow.auth.managers.utils.fab import get_fab_action_from_method_map, get_method_from_fab_action_map
from airflow.cli.cli_config import (
    DefaultHelpParser,
    GroupCommand,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.models import DagModel
from airflow.providers.fab.auth_manager.cli_commands.definition import (
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)
from airflow.providers.fab.auth_manager.models import Permission, Role, User
from airflow.security import permissions
from airflow.security.permissions import (
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DAG_WARNING,
    RESOURCE_DATASET,
    RESOURCE_DOCS,
    RESOURCE_IMPORT_ERROR,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_POOL,
    RESOURCE_PROVIDER,
    RESOURCE_SLA_MISS,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TASK_LOG,
    RESOURCE_TASK_RESCHEDULE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_WEBSITE,
    RESOURCE_XCOM,
)
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.yaml import safe_load
from airflow.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
from airflow.www.extensions.init_views import _CustomErrorRequestBodyValidator, _LazyResolver

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.cli.cli_config import (
        CLICommand,
    )
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride

_MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE: dict[DagAccessEntity, tuple[str, ...]] = {
    DagAccessEntity.AUDIT_LOG: (RESOURCE_AUDIT_LOG,),
    DagAccessEntity.CODE: (RESOURCE_DAG_CODE,),
    DagAccessEntity.DEPENDENCIES: (RESOURCE_DAG_DEPENDENCIES,),
    DagAccessEntity.RUN: (RESOURCE_DAG_RUN,),
    DagAccessEntity.SLA_MISS: (RESOURCE_SLA_MISS,),
    # RESOURCE_TASK_INSTANCE has been originally misused. RESOURCE_TASK_INSTANCE referred to task definition
    # AND task instances without making the difference
    # To be backward compatible, we translate DagAccessEntity.TASK_INSTANCE to RESOURCE_TASK_INSTANCE AND
    # RESOURCE_DAG_RUN
    # See https://github.com/apache/airflow/pull/34317#discussion_r1355917769
    DagAccessEntity.TASK: (RESOURCE_TASK_INSTANCE,),
    DagAccessEntity.TASK_INSTANCE: (RESOURCE_DAG_RUN, RESOURCE_TASK_INSTANCE),
    DagAccessEntity.TASK_LOGS: (RESOURCE_TASK_LOG,),
    DagAccessEntity.TASK_RESCHEDULE: (RESOURCE_TASK_RESCHEDULE,),
    DagAccessEntity.WARNING: (RESOURCE_DAG_WARNING,),
    DagAccessEntity.XCOM: (RESOURCE_XCOM,),
}

_MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE = {
    AccessView.CLUSTER_ACTIVITY: RESOURCE_CLUSTER_ACTIVITY,
    AccessView.DOCS: RESOURCE_DOCS,
    AccessView.IMPORT_ERRORS: RESOURCE_IMPORT_ERROR,
    AccessView.JOBS: RESOURCE_JOB,
    AccessView.PLUGINS: RESOURCE_PLUGIN,
    AccessView.PROVIDERS: RESOURCE_PROVIDER,
    AccessView.TRIGGERS: RESOURCE_TRIGGER,
    AccessView.WEBSITE: RESOURCE_WEBSITE,
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

    def get_api_endpoints(self) -> None | Blueprint:
        folder = Path(__file__).parents[0].resolve()  # this is airflow/auth/managers/fab/
        with folder.joinpath("openapi", "v1.yaml").open() as f:
            specification = safe_load(f)
        return FlaskApi(
            specification=specification,
            resolver=_LazyResolver(),
            base_path="/auth/fab/v1",
            options={"swagger_ui": SWAGGER_ENABLED, "swagger_path": SWAGGER_BUNDLE.__fspath__()},
            strict_validation=True,
            validate_responses=True,
            validator_map={"body": _CustomErrorRequestBodyValidator},
        ).blueprint

    def get_user_display_name(self) -> str:
        """Return the user's display name associated to the user in session."""
        user = self.get_user()
        first_name = user.first_name.strip() if isinstance(user.first_name, str) else ""
        last_name = user.last_name.strip() if isinstance(user.last_name, str) else ""
        return f"{first_name} {last_name}".strip()

    def get_user(self) -> User:
        """Return the user associated to the user in session."""
        from flask_login import current_user

        return current_user

    def init(self) -> None:
        """Run operations when Airflow is initializing."""
        self._sync_appbuilder_roles()

    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        user = self.get_user()
        return not user.is_anonymous and user.is_active

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONFIG, user=user)

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONNECTION, user=user)

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to access the dag.

        There are multiple scenarios:

        1. ``dag_access`` is not provided which means the user wants to access the DAG itself and not a sub
        entity (e.g. DAG runs).
        2. ``dag_access`` is provided which means the user wants to access a sub entity of the DAG
        (e.g. DAG runs).

            a. If ``method`` is GET, then check the user has READ permissions on the DAG and the sub entity.
            b. Else, check the user has EDIT permissions on the DAG and ``method`` on the sub entity. However,
                if no specific DAG is targeted, just check the sub entity.

        :param method: The method to authorize.
        :param access_entity: The dag access entity.
        :param details: The dag details.
        :param user: The user.
        """
        if not access_entity:
            # Scenario 1
            return self._is_authorized_dag(method=method, details=details, user=user)
        else:
            # Scenario 2
            resource_types = self._get_fab_resource_types(access_entity)
            dag_method: ResourceMethod = "GET" if method == "GET" else "PUT"

            if (details and details.id) and not self._is_authorized_dag(
                method=dag_method, details=details, user=user
            ):
                return False

            return all(
                self._is_authorized(method=method, resource_type=resource_type, user=user)
                if resource_type != RESOURCE_DAG_RUN or not hasattr(permissions, "resource_name")
                else self._is_authorized_dag_run(method=method, details=details, user=user)
                for resource_type in resource_types
            )

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_DATASET, user=user)

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_POOL, user=user)

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_VARIABLE, user=user)

    def is_authorized_view(self, *, access_view: AccessView, user: BaseUser | None = None) -> bool:
        # "Docs" are only links in the menu, there is no page associated
        method: ResourceMethod = "MENU" if access_view == AccessView.DOCS else "GET"
        return self._is_authorized(
            method=method, resource_type=_MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE[access_view], user=user
        )

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseUser | None = None
    ):
        if not user:
            user = self.get_user()
        fab_action_name = get_fab_action_from_method_map().get(method, method)
        return (fab_action_name, resource_name) in self._get_user_permissions(user)

    @provide_session
    def get_permitted_dag_ids(
        self,
        *,
        methods: Container[ResourceMethod] | None = None,
        user=None,
        session: Session = NEW_SESSION,
    ) -> set[str]:
        if not methods:
            methods = ["PUT", "GET"]

        if not user:
            user = self.get_user()

        if not self.is_logged_in():
            roles = user.roles
        else:
            if ("GET" in methods and self.is_authorized_dag(method="GET", user=user)) or (
                "PUT" in methods and self.is_authorized_dag(method="PUT", user=user)
            ):
                # If user is authorized to read/edit all DAGs, return all DAGs
                return {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}
            user_query = session.scalar(
                select(User)
                .options(
                    joinedload(User.roles)
                    .subqueryload(Role.permissions)
                    .options(joinedload(Permission.action), joinedload(Permission.resource))
                )
                .where(User.id == user.id)
            )
            roles = user_query.roles

        map_fab_action_name_to_method_name = get_method_from_fab_action_map()
        resources = set()
        for role in roles:
            for permission in role.permissions:
                action = permission.action.name
                if (
                    action in map_fab_action_name_to_method_name
                    and map_fab_action_name_to_method_name[action] in methods
                ):
                    resource = permission.resource.name
                    if resource == permissions.RESOURCE_DAG:
                        return {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}
                    if resource.startswith(permissions.RESOURCE_DAG_PREFIX):
                        resources.add(resource[len(permissions.RESOURCE_DAG_PREFIX) :])
                    else:
                        resources.add(resource)
        return {
            dag.dag_id
            for dag in session.execute(select(DagModel.dag_id).where(DagModel.dag_id.in_(resources)))
        }

    @cached_property
    def security_manager(self) -> FabAirflowSecurityManagerOverride:
        """Return the security manager specific to FAB."""
        from airflow.providers.fab.auth_manager.security_manager.override import (
            FabAirflowSecurityManagerOverride,
        )

        sm_from_config = self.appbuilder.get_app.config.get("SECURITY_MANAGER_CLASS")
        if sm_from_config:
            if not issubclass(sm_from_config, FabAirflowSecurityManagerOverride):
                raise AirflowConfigException(
                    """Your CUSTOM_SECURITY_MANAGER must extend FabAirflowSecurityManagerOverride."""
                )
            return sm_from_config(self.appbuilder)

        return FabAirflowSecurityManagerOverride(self.appbuilder)

    def get_url_login(self, **kwargs) -> str:
        """Return the login page url."""
        if not self.security_manager.auth_view:
            raise AirflowException("`auth_view` not defined in the security manager.")
        if next_url := kwargs.get("next_url"):
            return url_for(f"{self.security_manager.auth_view.endpoint}.login", next=next_url)
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

    def register_views(self) -> None:
        self.security_manager.register_views()

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
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG.

        :param method: the method to perform
        :param details: optional details about the DAG
        :param user: the user to perform the action on. If not provided (or None), it uses the current user

        :meta private:
        """
        is_global_authorized = self._is_authorized(method=method, resource_type=RESOURCE_DAG, user=user)
        if is_global_authorized:
            return True

        if details and details.id:
            # Check whether the user has permissions to access a specific DAG
            resource_dag_name = self._resource_name(details.id, RESOURCE_DAG)
            return self._is_authorized(method=method, resource_type=resource_dag_name, user=user)

        return False

    def _is_authorized_dag_run(
        self,
        method: ResourceMethod,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG Run.

        :param method: the method to perform
        :param details: optional, details about the DAG
        :param user: optional, the user to perform the action on. If not provided, it uses the current user

        :meta private:
        """
        is_global_authorized = self._is_authorized(method=method, resource_type=RESOURCE_DAG_RUN, user=user)
        if is_global_authorized:
            return True

        if details and details.id:
            # Check whether the user has permissions to access a specific DAG Run permission on a DAG Level
            resource_dag_name = self._resource_name(details.id, RESOURCE_DAG_RUN)
            return self._is_authorized(method=method, resource_type=resource_dag_name, user=user)

        return False

    @staticmethod
    def _get_fab_action(method: ResourceMethod) -> str:
        """
        Convert the method to a FAB action.

        :param method: the method to convert

        :meta private:
        """
        fab_action_from_method_map = get_fab_action_from_method_map()
        if method not in fab_action_from_method_map:
            raise AirflowException(f"Unknown method: {method}")
        return fab_action_from_method_map[method]

    @staticmethod
    def _get_fab_resource_types(dag_access_entity: DagAccessEntity) -> tuple[str, ...]:
        """
        Convert a DAG access entity to a tuple of FAB resource type.

        :param dag_access_entity: the DAG access entity

        :meta private:
        """
        if dag_access_entity not in _MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE:
            raise AirflowException(f"Unknown DAG access entity: {dag_access_entity}")
        return _MAP_DAG_ACCESS_ENTITY_TO_FAB_RESOURCE_TYPE[dag_access_entity]

    def _resource_name(self, dag_id: str, resource_type: str) -> str:
        """
        Return the FAB resource name for a DAG id.

        :param dag_id: the DAG id

        :meta private:
        """
        root_dag_id = self._get_root_dag_id(dag_id)
        if hasattr(permissions, "resource_name"):
            return getattr(permissions, "resource_name")(root_dag_id, resource_type)
        return getattr(permissions, "resource_name_for_dag")(root_dag_id)

    @staticmethod
    def _get_user_permissions(user: BaseUser):
        """
        Return the user permissions.

        :param user: the user to get permissions for

        :meta private:
        """
        return getattr(user, "perms") or []

    def _get_root_dag_id(self, dag_id: str) -> str:
        """
        Return the root DAG id in case of sub DAG, return the DAG id otherwise.

        :param dag_id: the DAG id

        :meta private:
        """
        if "." in dag_id and hasattr(DagModel, "root_dag_id"):
            return self.appbuilder.get_session.scalar(
                select(DagModel.dag_id, DagModel.root_dag_id).where(DagModel.dag_id == dag_id).limit(1)
            )
        return dag_id

    def _sync_appbuilder_roles(self):
        """
        Sync appbuilder roles to DB.

        :meta private:
        """
        # Garbage collect old permissions/views after they have been modified.
        # Otherwise, when the name of a view or menu is changed, the framework
        # will add the new Views and Menus names to the backend, but will not
        # delete the old ones.
        from packaging.version import Version

        from airflow.version import version

        if Version(Version(version).base_version) >= Version("3.0.0"):
            fallback = None
        else:
            fallback = conf.getboolean("webserver", "UPDATE_FAB_PERMS")
        if conf.getboolean("fab", "UPDATE_FAB_PERMS", fallback=fallback):
            self.security_manager.sync_roles()


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in FabAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser
