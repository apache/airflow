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
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import packaging.version
from connexion import FlaskApi
from fastapi import FastAPI
from flask import Blueprint, g
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from starlette.middleware.wsgi import WSGIMiddleware

from airflow import __version__ as airflow_version
from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    BackfillDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.api_fastapi.common.types import ExtraMenuItem, MenuItem
from airflow.cli.cli_config import (
    DefaultHelpParser,
    GroupCommand,
)
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException, AirflowException
from airflow.models import DagModel
from airflow.providers.fab.auth_manager.cli_commands.definition import (
    DB_COMMANDS,
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)
from airflow.providers.fab.auth_manager.models import Permission, Role, User
from airflow.providers.fab.auth_manager.models.anonymous_user import AnonymousUser
from airflow.providers.fab.www.app import create_app
from airflow.providers.fab.www.constants import SWAGGER_BUNDLE, SWAGGER_ENABLED
from airflow.providers.fab.www.extensions.init_views import (
    _CustomErrorRequestBodyValidator,
    _LazyResolver,
)
from airflow.providers.fab.www.security import permissions
from airflow.providers.fab.www.security.permissions import (
    RESOURCE_AUDIT_LOG,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DAG_WARNING,
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
from airflow.providers.fab.www.utils import (
    get_fab_action_from_method_map,
    get_method_from_fab_action_map,
)
from airflow.security.permissions import RESOURCE_BACKFILL
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.yaml import safe_load

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.cli.cli_config import (
        CLICommand,
    )
    from airflow.providers.common.compat.assets import AssetAliasDetails, AssetDetails
    from airflow.providers.fab.auth_manager.security_manager.override import (
        FabAirflowSecurityManagerOverride,
    )
    from airflow.providers.fab.www.extensions.init_appbuilder import AirflowAppBuilder
    from airflow.providers.fab.www.security.permissions import (
        RESOURCE_ASSET,
        RESOURCE_ASSET_ALIAS,
    )
else:
    from airflow.providers.common.compat.security.permissions import (
        RESOURCE_ASSET,
        RESOURCE_ASSET_ALIAS,
    )


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

_MAP_MENU_ITEM_TO_FAB_RESOURCE_TYPE = {
    MenuItem.ASSETS: RESOURCE_ASSET,
    MenuItem.ASSET_EVENTS: RESOURCE_ASSET,
    MenuItem.CONNECTIONS: RESOURCE_CONNECTION,
    MenuItem.DAGS: RESOURCE_DAG,
    MenuItem.DOCS: RESOURCE_DOCS,
    MenuItem.PLUGINS: RESOURCE_PLUGIN,
    MenuItem.POOLS: RESOURCE_POOL,
    MenuItem.PROVIDERS: RESOURCE_PROVIDER,
    MenuItem.VARIABLES: RESOURCE_VARIABLE,
    MenuItem.XCOMS: RESOURCE_XCOM,
}


class FabAuthManager(BaseAuthManager[User]):
    """
    Flask-AppBuilder auth manager.

    This auth manager is responsible for providing a backward compatible user management experience to users.
    """

    appbuilder: AirflowAppBuilder | None = None

    def init(self) -> None:
        """Run operations when Airflow is initializing."""
        if self.appbuilder:
            self._sync_appbuilder_roles()

    @cached_property
    def apiserver_endpoint(self) -> str:
        return conf.get("api", "base_url")

    @staticmethod
    def get_cli_commands() -> list[CLICommand]:
        """Vends CLI commands to be included in Airflow CLI."""
        commands: list[CLICommand] = [
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
        # If Airflow version is 3.0.0 or higher, add the fab-db command group
        if packaging.version.parse(
            packaging.version.parse(airflow_version).base_version
        ) >= packaging.version.parse("3.0.0"):
            commands.append(GroupCommand(name="fab-db", help="Manage FAB", subcommands=DB_COMMANDS))
        return commands

    def get_fastapi_app(self) -> FastAPI | None:
        """Get the FastAPI app."""
        from airflow.providers.fab.auth_manager.api_fastapi.routes.login import (
            login_router,
        )

        flask_app = create_app(enable_plugins=False)

        app = FastAPI(
            title="FAB auth manager API",
            description=(
                "This is FAB auth manager API. This API is only available if the auth manager used in "
                "the Airflow environment is FAB auth manager. "
                "This API provides endpoints to manage users and permissions managed by the FAB auth "
                "manager."
            ),
        )

        # Add the login router to the FastAPI app
        app.include_router(login_router)

        app.mount("/", WSGIMiddleware(flask_app))

        return app

    def get_api_endpoints(self) -> None | Blueprint:
        folder = Path(__file__).parents[0].resolve()  # this is airflow/auth/managers/fab/
        with folder.joinpath("openapi", "v1.yaml").open() as f:
            specification = safe_load(f)
        return FlaskApi(
            specification=specification,
            resolver=_LazyResolver(),
            base_path="/fab/v1",
            options={
                "swagger_ui": SWAGGER_ENABLED,
                "swagger_path": SWAGGER_BUNDLE.__fspath__(),
            },
            strict_validation=True,
            validate_responses=True,
            validator_map={"body": _CustomErrorRequestBodyValidator},
        ).blueprint

    def get_user(self) -> User:
        """
        Return the user associated to the user in session.

        Attempt to find the current user in g.user, as defined by the kerberos authentication backend.
        If no such user is found, return the `current_user` local proxy object, linked to the user session.

        """
        from flask_login import current_user

        # If a user has gone through the Kerberos dance, the kerberos authentication manager
        # has linked it with a User model, stored in g.user, and not the session.
        if current_user.is_anonymous and getattr(g, "user", None) is not None and not g.user.is_anonymous:
            return g.user

        return current_user

    def deserialize_user(self, token: dict[str, Any]) -> User:
        with create_session() as session:
            return session.get(User, token["id"])

    def serialize_user(self, user: User) -> dict[str, Any]:
        return {"id": user.id}

    def is_logged_in(self) -> bool:
        """Return whether the user is logged in."""
        user = self.get_user()
        return (
            self.appbuilder
            and self.appbuilder.get_app.config.get("AUTH_ROLE_PUBLIC", None)
            or (not user.is_anonymous and user.is_active)
        )

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        user: User,
        details: ConfigurationDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONFIG, user=user)

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        user: User,
        details: ConnectionDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_CONNECTION, user=user)

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        user: User,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
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
        :param user: The user performing the action.
        :param access_entity: The dag access entity.
        :param details: The dag details.
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
                (
                    self._is_authorized(method=method, resource_type=resource_type, user=user)
                    if resource_type != RESOURCE_DAG_RUN or not hasattr(permissions, "resource_name")
                    else self._is_authorized_dag_run(method=method, details=details, user=user)
                )
                for resource_type in resource_types
            )

    def is_authorized_backfill(
        self,
        *,
        method: ResourceMethod,
        user: User,
        details: BackfillDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_BACKFILL, user=user)

    def is_authorized_asset(
        self, *, method: ResourceMethod, user: User, details: AssetDetails | None = None
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_ASSET, user=user)

    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        user: User,
        details: AssetAliasDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_ASSET_ALIAS, user=user)

    def is_authorized_pool(
        self, *, method: ResourceMethod, user: User, details: PoolDetails | None = None
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_POOL, user=user)

    def is_authorized_variable(
        self,
        *,
        method: ResourceMethod,
        user: User,
        details: VariableDetails | None = None,
    ) -> bool:
        return self._is_authorized(method=method, resource_type=RESOURCE_VARIABLE, user=user)

    def is_authorized_view(self, *, access_view: AccessView, user: User) -> bool:
        # "Docs" are only links in the menu, there is no page associated
        method: ResourceMethod = "MENU" if access_view == AccessView.DOCS else "GET"
        return self._is_authorized(
            method=method,
            resource_type=_MAP_ACCESS_VIEW_TO_FAB_RESOURCE_TYPE[access_view],
            user=user,
        )

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: User
    ) -> bool:
        fab_action_name = get_fab_action_from_method_map().get(method, method)
        return (fab_action_name, resource_name) in self._get_user_permissions(user)

    def filter_authorized_menu_items(self, menu_items: list[MenuItem], user: User) -> list[MenuItem]:
        return [
            menu_item
            for menu_item in menu_items
            if self._is_authorized(
                method="MENU",
                resource_type=_MAP_MENU_ITEM_TO_FAB_RESOURCE_TYPE.get(menu_item, menu_item.value),
                user=user,
            )
        ]

    @provide_session
    def get_permitted_dag_ids(
        self,
        *,
        user: User,
        method: ResourceMethod = "GET",
        session: Session = NEW_SESSION,
    ) -> set[str]:
        if self._is_authorized(method=method, resource_type=RESOURCE_DAG, user=user):
            # If user is authorized to access all DAGs, return all DAGs
            return {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}
        if isinstance(user, AnonymousUser):
            return set()
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
                    and map_fab_action_name_to_method_name[action] == method
                ):
                    resource = permission.resource.name
                    if resource == permissions.RESOURCE_DAG:
                        return {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}
                    if resource.startswith(permissions.RESOURCE_DAG_PREFIX):
                        resources.add(resource[len(permissions.RESOURCE_DAG_PREFIX) :])
                    else:
                        resources.add(resource)
        return set(session.scalars(select(DagModel.dag_id).where(DagModel.dag_id.in_(resources))))

    @cached_property
    def security_manager(self) -> FabAirflowSecurityManagerOverride:
        """Return the security manager specific to FAB."""
        from airflow.providers.fab.auth_manager.security_manager.override import (
            FabAirflowSecurityManagerOverride,
        )

        if not self.appbuilder:
            raise AirflowException("AppBuilder is not initialized.")

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
        return urljoin(self.apiserver_endpoint, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login/")

    def get_url_logout(self) -> str | None:
        """Return the logout page url."""
        return urljoin(self.apiserver_endpoint, f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout/")

    def register_views(self) -> None:
        self.security_manager.register_views()

    def get_extra_menu_items(self, *, user: User) -> list[ExtraMenuItem]:
        # Contains the list of menu items. ``resource_type`` is the name of the resource in FAB
        # permission model to check whether the user is allowed to see this menu item
        items = [
            {
                "resource_type": "List Users",
                "text": "Users",
                "href": f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/users/list/",
            },
            {
                "resource_type": "List Roles",
                "text": "Roles",
                "href": f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/roles/list/",
            },
            {
                "resource_type": "Actions",
                "text": "Actions",
                "href": f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/actions/list/",
            },
            {
                "resource_type": "Resources",
                "text": "Resources",
                "href": f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/resources/list/",
            },
            {
                "resource_type": "Permission Pairs",
                "text": "Permissions",
                "href": f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/permissions/list/",
            },
        ]

        return [
            ExtraMenuItem(text=item["text"], href=item["href"])
            for item in items
            if self._is_authorized(method="MENU", resource_type=item["resource_type"], user=user)
        ]

    def _is_authorized(
        self,
        *,
        method: ResourceMethod,
        resource_type: str,
        user: User,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action.

        :param method: the method to perform
        :param resource_type: the type of resource the user attempts to perform the action on
        :param user: the user to performing the action

        :meta private:
        """
        fab_action = self._get_fab_action(method)
        user_permissions = self._get_user_permissions(user)

        return (fab_action, resource_type) in user_permissions

    def _is_authorized_dag(
        self,
        method: ResourceMethod,
        details: DagDetails | None,
        user: User,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG.

        :param method: the method to perform
        :param details: details about the DAG
        :param user: the user to performing the action

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
        details: DagDetails | None,
        user: User,
    ) -> bool:
        """
        Return whether the user is authorized to perform a given action on a DAG Run.

        :param method: the method to perform
        :param details: details about the DAG
        :param user: the user to performing the action

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
    def _get_user_permissions(user: User):
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
        if not self.appbuilder:
            raise AirflowException("AppBuilder is not initialized.")

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
        if conf.getboolean("fab", "UPDATE_FAB_PERMS"):
            self.security_manager.sync_roles()


def get_parser() -> argparse.ArgumentParser:
    """Generate documentation; used by Sphinx argparse."""
    from airflow.cli.cli_parser import AirflowHelpFormatter, _add_command

    parser = DefaultHelpParser(prog="airflow", formatter_class=AirflowHelpFormatter)
    subparsers = parser.add_subparsers(dest="subcommand", metavar="GROUP_OR_COMMAND")
    for group_command in FabAuthManager.get_cli_commands():
        _add_command(subparsers, group_command)
    return parser
