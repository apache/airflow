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
from typing import TYPE_CHECKING, Callable

from flask import g

from airflow.auth.managers.fab.security_manager.constants import EXISTING_ROLES as FAB_EXISTING_ROLES
from airflow.auth.managers.models.resource_details import AccessView, DagAccessEntity
from airflow.auth.managers.utils.fab import (
    get_method_from_fab_action_map,
)
from airflow.security.permissions import (
    ACTION_CAN_ACCESS_MENU,
    ACTION_CAN_READ,
    RESOURCE_ADMIN_MENU,
    RESOURCE_AUDIT_LOG,
    RESOURCE_BROWSE_MENU,
    RESOURCE_CLUSTER_ACTIVITY,
    RESOURCE_CONFIG,
    RESOURCE_CONNECTION,
    RESOURCE_DAG,
    RESOURCE_DAG_CODE,
    RESOURCE_DAG_DEPENDENCIES,
    RESOURCE_DAG_RUN,
    RESOURCE_DATASET,
    RESOURCE_DOCS,
    RESOURCE_DOCS_MENU,
    RESOURCE_JOB,
    RESOURCE_PLUGIN,
    RESOURCE_POOL,
    RESOURCE_PROVIDER,
    RESOURCE_SLA_MISS,
    RESOURCE_TASK_INSTANCE,
    RESOURCE_TASK_RESCHEDULE,
    RESOURCE_TRIGGER,
    RESOURCE_VARIABLE,
    RESOURCE_XCOM,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.extensions.init_auth_manager import get_auth_manager
from airflow.www.fab_security.sqla.manager import SecurityManager
from airflow.www.utils import CustomSQLAInterface

EXISTING_ROLES = FAB_EXISTING_ROLES

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser


class AirflowSecurityManagerV2(SecurityManager, LoggingMixin):
    """Custom security manager, which introduces a permission model adapted to Airflow.

    It's named V2 to differentiate it from the obsolete airflow.www.security.AirflowSecurityManager.
    """

    def __init__(self, appbuilder) -> None:
        super().__init__(appbuilder=appbuilder)

        # Go and fix up the SQLAInterface used from the stock one to our subclass.
        # This is needed to support the "hack" where we had to edit
        # FieldConverter.conversion_table in place in airflow.www.utils
        for attr in dir(self):
            if attr.endswith("view"):
                view = getattr(self, attr, None)
                if view and getattr(view, "datamodel", None):
                    view.datamodel = CustomSQLAInterface(view.datamodel.obj)

    def has_access(self, action_name: str, resource_name: str, user=None) -> bool:
        """
        Verify whether a given user could perform a certain action on the given resource.

        Example actions might include can_read, can_write, can_delete, etc.

        This function is called by FAB when accessing a view. See
        https://github.com/dpgaspar/Flask-AppBuilder/blob/c6fecdc551629e15467fde5d06b4437379d90592/flask_appbuilder/security/decorators.py#L134

        The resource ID (e.g. the connection ID) is not passed to this function (see above link). Therefore,
        it is not possible to perform fine-grained access authorization with the resource ID yet. In other
        words, we can only verify the user has access to all connections and not to a specific connection.
        To make it happen, we either need to:
         - Override all views in 'airflow/www/views.py' inheriting from `AirflowModelView` and use a custom
         `has_access` decorator.
         - Wait for the new Airflow UI to come.

        :param action_name: action_name on resource (e.g can_read, can_edit).
        :param resource_name: name of view-menu or resource.
        :param user: user
        :return: Whether user could perform certain action on the resource.
        :rtype bool
        """
        if not user:
            user = g.user

        is_authorized_method = self._get_auth_manager_is_authorized_method(resource_name)
        if is_authorized_method:
            return is_authorized_method(action_name, user)
        else:
            # This means the page the user is trying to access is specific to the auth manager used
            # Example: the user list view in FabAuthManager
            action_name = ACTION_CAN_READ if action_name == ACTION_CAN_ACCESS_MENU else action_name
            return get_auth_manager().is_authorized_custom_view(
                fab_action_name=action_name, fab_resource_name=resource_name, user=user
            )

    def create_admin_standalone(self) -> tuple[str | None, str | None]:
        """Perform the required steps when initializing airflow for standalone mode.

        If necessary, returns the username and password to be printed in the console for users to log in.
        """
        return None, None

    @cached_property
    def _auth_manager_is_authorized_map(self) -> dict[str, Callable[[str, BaseUser | None], bool]]:
        """
        Return the map associating a FAB resource name to the corresponding auth manager is_authorized_ API.

        The function returned takes the FAB action name and the user as parameter.
        """
        auth_manager = get_auth_manager()
        methods = get_method_from_fab_action_map()

        return {
            RESOURCE_AUDIT_LOG: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.AUDIT_LOG,
                user=user,
            ),
            RESOURCE_CLUSTER_ACTIVITY: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.CLUSTER_ACTIVITY,
                user=user,
            ),
            RESOURCE_CONFIG: lambda action, user: auth_manager.is_authorized_configuration(
                method=methods[action],
                user=user,
            ),
            RESOURCE_CONNECTION: lambda action, user: auth_manager.is_authorized_connection(
                method=methods[action],
                user=user,
            ),
            RESOURCE_DAG: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                user=user,
            ),
            RESOURCE_DAG_CODE: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.CODE,
                user=user,
            ),
            RESOURCE_DAG_DEPENDENCIES: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.DEPENDENCIES,
                user=user,
            ),
            RESOURCE_DAG_RUN: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.RUN,
                user=user,
            ),
            RESOURCE_DATASET: lambda action, user: auth_manager.is_authorized_dataset(
                method=methods[action],
                user=user,
            ),
            RESOURCE_DOCS: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.DOCS,
                user=user,
            ),
            RESOURCE_PLUGIN: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.PLUGINS,
                user=user,
            ),
            RESOURCE_JOB: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.JOBS,
                user=user,
            ),
            RESOURCE_POOL: lambda action, user: auth_manager.is_authorized_pool(
                method=methods[action],
                user=user,
            ),
            RESOURCE_PROVIDER: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.PROVIDERS,
                user=user,
            ),
            RESOURCE_SLA_MISS: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.SLA,
                user=user,
            ),
            RESOURCE_TASK_INSTANCE: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.TASK_INSTANCE,
                user=user,
            ),
            RESOURCE_TASK_RESCHEDULE: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.TASK_RESCHEDULE,
                user=user,
            ),
            RESOURCE_TRIGGER: lambda action, user: auth_manager.is_authorized_view(
                access_view=AccessView.TRIGGERS,
                user=user,
            ),
            RESOURCE_VARIABLE: lambda action, user: auth_manager.is_authorized_variable(
                method=methods[action],
                user=user,
            ),
            RESOURCE_XCOM: lambda action, user: auth_manager.is_authorized_dag(
                method=methods[action],
                access_entity=DagAccessEntity.XCOM,
                user=user,
            ),
        }

    def _get_auth_manager_is_authorized_method(self, fab_resource_name: str) -> Callable | None:
        is_authorized_method = self._auth_manager_is_authorized_map.get(fab_resource_name)
        if is_authorized_method:
            return is_authorized_method
        elif fab_resource_name in [RESOURCE_DOCS_MENU, RESOURCE_ADMIN_MENU, RESOURCE_BROWSE_MENU]:
            # Display the "Browse", "Admin" and "Docs" dropdowns in the menu if the user has access to at
            # least one dropdown child
            return self._is_authorized_category_menu(fab_resource_name)
        else:
            return None

    def _is_authorized_category_menu(self, category: str) -> Callable:
        items = {item.name for item in self.appbuilder.menu.find(category).childs}
        return lambda action, user: any(
            self._get_auth_manager_is_authorized_method(fab_resource_name=item) for item in items
        )
