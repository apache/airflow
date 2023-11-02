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
from typing import TYPE_CHECKING, Any, Collection, Sequence

from flask import g
from sqlalchemy import select

from airflow.auth.managers.fab.security_manager.constants import EXISTING_ROLES as FAB_EXISTING_ROLES
from airflow.auth.managers.fab.views.permissions import (
    ActionModelView,
    PermissionPairModelView,
    ResourceModelView,
)
from airflow.auth.managers.fab.views.roles_list import CustomRoleModelView
from airflow.auth.managers.fab.views.user import (
    CustomUserDBModelView,
    CustomUserLDAPModelView,
    CustomUserOAuthModelView,
    CustomUserOIDModelView,
    CustomUserRemoteUserModelView,
)
from airflow.auth.managers.fab.views.user_edit import (
    CustomResetMyPasswordView,
    CustomResetPasswordView,
    CustomUserInfoEditView,
)
from airflow.auth.managers.fab.views.user_stats import CustomUserStatsChartView
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.models import DagModel
from airflow.security import permissions
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.fab_security.sqla.manager import SecurityManager
from airflow.www.utils import CustomSQLAInterface

EXISTING_ROLES = FAB_EXISTING_ROLES

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import Permission, Resource

    pass


class AirflowSecurityManagerV2(SecurityManager, LoggingMixin):
    """Custom security manager, which introduces a permission model adapted to Airflow.

    It's named V2 to differentiate it from the obsolete airflow.www.security.AirflowSecurityManager.
    """

    ###########################################################################
    #                               PERMISSIONS
    ###########################################################################

    # [START security_viewer_perms]
    VIEWER_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CLUSTER_ACTIVITY),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PASSWORD),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_BROWSE_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_DEPENDENCIES),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DATASET),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CLUSTER_ACTIVITY),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DOCS_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_JOB),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_AUDIT_LOG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_SLA_MISS),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_INSTANCE),
    ]
    # [END security_viewer_perms]

    # [START security_user_perms]
    USER_PERMISSIONS = [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
    ]
    # [END security_user_perms]

    # [START security_op_perms]
    OP_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONFIG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_ADMIN_MENU),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CONFIG),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PROVIDER),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_XCOM),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PROVIDER),
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VARIABLE),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_XCOM),
    ]
    # [END security_op_perms]

    ADMIN_PERMISSIONS = [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_RESCHEDULE),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TASK_RESCHEDULE),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_TRIGGER),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_TRIGGER),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_PASSWORD),
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
    ]

    # global resource for dag-level access
    DAG_ACTIONS = permissions.DAG_ACTIONS

    ###########################################################################
    #                     DEFAULT ROLE CONFIGURATIONS
    ###########################################################################

    ROLE_CONFIGS: list[dict[str, Any]] = [
        {"role": "Public", "perms": []},
        {"role": "Viewer", "perms": VIEWER_PERMISSIONS},
        {
            "role": "User",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS,
        },
        {
            "role": "Op",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS,
        },
        {
            "role": "Admin",
            "perms": VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS + ADMIN_PERMISSIONS,
        },
    ]

    actionmodelview = ActionModelView
    permissionmodelview = PermissionPairModelView
    rolemodelview = CustomRoleModelView
    resourcemodelview = ResourceModelView
    userdbmodelview = CustomUserDBModelView
    resetmypasswordview = CustomResetMyPasswordView
    resetpasswordview = CustomResetPasswordView
    userinfoeditview = CustomUserInfoEditView
    userldapmodelview = CustomUserLDAPModelView
    useroauthmodelview = CustomUserOAuthModelView
    userremoteusermodelview = CustomUserRemoteUserModelView
    useroidmodelview = CustomUserOIDModelView
    userstatschartview = CustomUserStatsChartView

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
        self.perms = None

    def _get_root_dag_id(self, dag_id: str) -> str:
        if "." in dag_id:
            dm = self.appbuilder.get_session.execute(
                select(DagModel.dag_id, DagModel.root_dag_id).where(DagModel.dag_id == dag_id)
            ).one()
            return dm.root_dag_id or dm.dag_id
        return dag_id

    @staticmethod
    def get_user_roles(user=None):
        """
        Get all the roles associated with the user.

        :param user: the ab_user in FAB model.
        :return: a list of roles associated with the user.
        """
        if user is None:
            user = g.user
        return user.roles

    def prefixed_dag_id(self, dag_id: str) -> str:
        """Return the permission name for a DAG id."""
        warnings.warn(
            "`prefixed_dag_id` has been deprecated. "
            "Please use `airflow.security.permissions.resource_name_for_dag` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        root_dag_id = self._get_root_dag_id(dag_id)
        return permissions.resource_name_for_dag(root_dag_id)

    def is_dag_resource(self, resource_name: str) -> bool:
        """Determine if a resource belongs to a DAG or all DAGs."""
        if resource_name == permissions.RESOURCE_DAG:
            return True
        return resource_name.startswith(permissions.RESOURCE_DAG_PREFIX)

    def has_access(self, action_name: str, resource_name: str, user=None) -> bool:
        """
        Verify whether a given user could perform a certain action on the given resource.

        Example actions might include can_read, can_write, can_delete, etc.

        :param action_name: action_name on resource (e.g can_read, can_edit).
        :param resource_name: name of view-menu or resource.
        :param user: user name
        :return: Whether user could perform certain action on the resource.
        :rtype bool
        """
        if not user:
            user = g.user
        if (action_name, resource_name) in user.perms:
            return True

        if self.is_dag_resource(resource_name):
            if (action_name, permissions.RESOURCE_DAG) in user.perms:
                return True
            return (action_name, resource_name) in user.perms

        return False

    def create_admin_standalone(self) -> tuple[str | None, str | None]:
        """Perform the required steps when initializing airflow for standalone mode.

        If necessary, returns the username and password to be printed in the console for users to log in.
        """
        return None, None

    def sync_perm_for_dag(
        self,
        dag_id: str,
        access_control: dict[str, Collection[str]] | None = None,
    ) -> None:
        """
        Sync permissions for given dag id.

        The dag id surely exists in our dag bag as only / refresh button or DagBag will call this function.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :param access_control: a dict where each key is a rolename and
            each value is a set() of action names (e.g.,
            {'can_read'}
        :return:
        """
        dag_resource_name = permissions.resource_name_for_dag(dag_id)
        for dag_action_name in self.DAG_ACTIONS:
            self.create_permission(dag_action_name, dag_resource_name)

        if access_control is not None:
            self.log.debug("Syncing DAG-level permissions for DAG '%s'", dag_resource_name)
            self._sync_dag_view_permissions(dag_resource_name, access_control)
        else:
            self.log.debug(
                "Not syncing DAG-level permissions for DAG '%s' as access control is unset.",
                dag_resource_name,
            )

    def _sync_dag_view_permissions(self, dag_id: str, access_control: dict[str, Collection[str]]) -> None:
        """
        Set the access policy on the given DAG's ViewModel.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :param access_control: a dict where each key is a rolename and
            each value is a set() of action names (e.g. {'can_read'})
        """
        dag_resource_name = permissions.resource_name_for_dag(dag_id)

        def _get_or_create_dag_permission(action_name: str) -> Permission | None:
            perm = self.get_permission(action_name, dag_resource_name)
            if not perm:
                self.log.info("Creating new action '%s' on resource '%s'", action_name, dag_resource_name)
                perm = self.create_permission(action_name, dag_resource_name)

            return perm

        def _revoke_stale_permissions(resource: Resource):
            existing_dag_perms = self.get_resource_permissions(resource)
            for perm in existing_dag_perms:
                non_admin_roles = [role for role in perm.role if role.name != "Admin"]
                for role in non_admin_roles:
                    target_perms_for_role = access_control.get(role.name, ())
                    if perm.action.name not in target_perms_for_role:
                        self.log.info(
                            "Revoking '%s' on DAG '%s' for role '%s'",
                            perm.action,
                            dag_resource_name,
                            role.name,
                        )
                        self.remove_permission_from_role(role, perm)

        resource = self.get_resource(dag_resource_name)
        if resource:
            _revoke_stale_permissions(resource)

        for rolename, action_names in access_control.items():
            role = self.find_role(rolename)
            if not role:
                raise AirflowException(
                    f"The access_control mapping for DAG '{dag_id}' includes a role named "
                    f"'{rolename}', but that role does not exist"
                )

            action_names = set(action_names)
            invalid_action_names = action_names - self.DAG_ACTIONS
            if invalid_action_names:
                raise AirflowException(
                    f"The access_control map for DAG '{dag_resource_name}' includes "
                    f"the following invalid permissions: {invalid_action_names}; "
                    f"The set of valid permissions is: {self.DAG_ACTIONS}"
                )

            for action_name in action_names:
                dag_perm = _get_or_create_dag_permission(action_name)
                if dag_perm:
                    self.add_permission_to_role(role, dag_perm)

    def check_authorization(
        self,
        perms: Sequence[tuple[str, str]] | None = None,
        dag_id: str | None = None,
    ) -> bool:
        raise NotImplementedError(
            "The method 'check_authorization' is only available with the auth manager FabAuthManager"
        )
