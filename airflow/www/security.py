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
#

import warnings
from typing import Dict, Optional, Sequence, Set, Tuple

from flask import current_app, g
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.security.sqla.manager import SecurityManager
from flask_appbuilder.security.sqla.models import PermissionView, Role, User
from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel
from airflow.security import permissions
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.www.utils import CustomSQLAInterface
from airflow.www.views import (
    CustomPermissionModelView,
    CustomPermissionViewModelView,
    CustomResetMyPasswordView,
    CustomResetPasswordView,
    CustomRoleModelView,
    CustomUserDBModelView,
    CustomUserInfoEditView,
    CustomUserLDAPModelView,
    CustomUserOAuthModelView,
    CustomUserOIDModelView,
    CustomUserRemoteUserModelView,
    CustomUserStatsChartView,
    CustomViewMenuModelView,
)

EXISTING_ROLES = {
    'Admin',
    'Viewer',
    'User',
    'Op',
    'Public',
}


class AirflowSecurityManager(SecurityManager, LoggingMixin):
    """Custom security manager, which introduces a permission model adapted to Airflow"""

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
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_IMPORT_ERROR),
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
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_DAG_RUN),
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
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_CONNECTION),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_POOL),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_VARIABLE),
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
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_PASSWORD),
    ]

    # global view-menu for dag-level access
    DAG_VMS = {permissions.RESOURCE_DAG}

    READ_DAG_PERMS = {permissions.ACTION_CAN_READ}
    DAG_PERMS = permissions.DAG_PERMS

    ###########################################################################
    #                     DEFAULT ROLE CONFIGURATIONS
    ###########################################################################

    ROLE_CONFIGS = [
        {'role': 'Public', 'perms': []},
        {'role': 'Viewer', 'perms': VIEWER_PERMISSIONS},
        {
            'role': 'User',
            'perms': VIEWER_PERMISSIONS + USER_PERMISSIONS,
        },
        {
            'role': 'Op',
            'perms': VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS,
        },
        {
            'role': 'Admin',
            'perms': VIEWER_PERMISSIONS + USER_PERMISSIONS + OP_PERMISSIONS + ADMIN_PERMISSIONS,
        },
    ]

    permissionmodelview = CustomPermissionModelView
    permissionviewmodelview = CustomPermissionViewModelView
    rolemodelview = CustomRoleModelView
    viewmenumodelview = CustomViewMenuModelView
    userdbmodelview = CustomUserDBModelView
    resetmypasswordview = CustomResetMyPasswordView
    resetpasswordview = CustomResetPasswordView
    userinfoeditview = CustomUserInfoEditView
    userldapmodelview = CustomUserLDAPModelView
    useroauthmodelview = CustomUserOAuthModelView
    userremoteusermodelview = CustomUserRemoteUserModelView
    useroidmodelview = CustomUserOIDModelView
    userstatschartview = CustomUserStatsChartView

    def __init__(self, appbuilder):
        super().__init__(appbuilder)

        # Go and fix up the SQLAInterface used from the stock one to our subclass.
        # This is needed to support the "hack" where we had to edit
        # FieldConverter.conversion_table in place in airflow.www.utils
        for attr in dir(self):
            if not attr.endswith('view'):
                continue
            view = getattr(self, attr, None)
            if not view or not getattr(view, 'datamodel', None):
                continue
            view.datamodel = CustomSQLAInterface(view.datamodel.obj)
        self.perms = None

    def init_role(self, role_name, perms):
        """
        Initialize the role with the permissions and related view-menus.
        :param role_name:
        :param perms:
        :return:
        """
        warnings.warn(
            "`init_role` has been deprecated. Please use `bulk_sync_roles` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.bulk_sync_roles([{'role': role_name, 'perms': perms}])

    def bulk_sync_roles(self, roles):
        """Sync the provided roles and permissions."""
        existing_roles = self._get_all_roles_with_permissions()
        pvs = self._get_all_non_dag_permissionviews()

        for config in roles:
            role_name = config['role']
            perms = config['perms']
            role = existing_roles.get(role_name) or self.add_role(role_name)

            for perm_name, view_name in perms:
                perm_view = pvs.get((perm_name, view_name)) or self.add_permission_view_menu(
                    perm_name, view_name
                )

                if perm_view not in role.permissions:
                    self.add_permission_role(role, perm_view)

    def add_permissions(self, role, perms):
        """Adds resource permissions to a given role."""
        for perm_name, view_name in perms:
            perm_view = self.add_permission_view_menu(perm_name, view_name)
            self.add_permission_role(role, perm_view)

    def delete_role(self, role_name):
        """
        Delete the given Role

        :param role_name: the name of a role in the ab_role table
        """
        session = self.get_session
        role = session.query(sqla_models.Role).filter(sqla_models.Role.name == role_name).first()
        if role:
            self.log.info("Deleting role '%s'", role_name)
            session.delete(role)
            session.commit()
        else:
            raise AirflowException(f"Role named '{role_name}' does not exist")

    @staticmethod
    def get_user_roles(user=None):
        """
        Get all the roles associated with the user.

        :param user: the ab_user in FAB model.
        :return: a list of roles associated with the user.
        """
        if user is None:
            user = g.user
        if user.is_anonymous:
            public_role = current_app.appbuilder.get_app.config["AUTH_ROLE_PUBLIC"]
            return [current_app.appbuilder.sm.find_role(public_role)] if public_role else []
        return user.roles

    def get_current_user_permissions(self):
        """Returns permissions for logged in user as a set of tuples with the perm name and view menu name"""
        perms_views = set()
        for role in self.get_user_roles():
            perms_views.update(
                {(perm_view.permission.name, perm_view.view_menu.name) for perm_view in role.permissions}
            )
        return perms_views

    def get_readable_dags(self, user):
        """Gets the DAGs readable by authenticated user."""
        return self.get_accessible_dags([permissions.ACTION_CAN_READ], user)

    def get_editable_dags(self, user):
        """Gets the DAGs editable by authenticated user."""
        return self.get_accessible_dags([permissions.ACTION_CAN_EDIT], user)

    def get_readable_dag_ids(self, user) -> Set[str]:
        """Gets the DAG IDs readable by authenticated user."""
        return {dag.dag_id for dag in self.get_readable_dags(user)}

    def get_editable_dag_ids(self, user) -> Set[str]:
        """Gets the DAG IDs editable by authenticated user."""
        return {dag.dag_id for dag in self.get_editable_dags(user)}

    def get_accessible_dag_ids(self, user) -> Set[str]:
        """Gets the DAG IDs editable or readable by authenticated user."""
        accessible_dags = self.get_accessible_dags(
            [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ], user
        )
        return {dag.dag_id for dag in accessible_dags}

    @provide_session
    def get_accessible_dags(self, user_actions, user, session=None):
        """Generic function to get readable or writable DAGs for user."""
        if user.is_anonymous:
            roles = self.get_user_roles(user)
        else:
            user_query = (
                session.query(User)
                .options(
                    joinedload(User.roles)
                    .subqueryload(Role.permissions)
                    .options(joinedload(PermissionView.permission), joinedload(PermissionView.view_menu))
                )
                .filter(User.id == user.id)
                .first()
            )
            roles = user_query.roles

        resources = set()
        for role in roles:
            for permission in role.permissions:
                action = permission.permission.name
                if action not in user_actions:
                    continue

                resource = permission.view_menu.name
                if resource == permissions.RESOURCE_DAG:
                    return session.query(DagModel)

                if resource.startswith(permissions.RESOURCE_DAG_PREFIX):
                    resources.add(resource[len(permissions.RESOURCE_DAG_PREFIX) :])
                else:
                    resources.add(resource)

        return session.query(DagModel).filter(DagModel.dag_id.in_(resources))

    def can_access_some_dags(self, action: str, dag_id: Optional[str] = None) -> bool:
        """Checks if user has read or write access to some dags."""
        if dag_id and dag_id != '~':
            return self.has_access(action, permissions.resource_name_for_dag(dag_id))

        user = g.user
        if action == permissions.ACTION_CAN_READ:
            return any(self.get_readable_dags(user))
        return any(self.get_editable_dags(user))

    def can_read_dag(self, dag_id, user=None) -> bool:
        """Determines whether a user has DAG read access."""
        if not user:
            user = g.user
        dag_resource_name = permissions.resource_name_for_dag(dag_id)
        return self._has_view_access(
            user, permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG
        ) or self._has_view_access(user, permissions.ACTION_CAN_READ, dag_resource_name)

    def can_edit_dag(self, dag_id, user=None) -> bool:
        """Determines whether a user has DAG edit access."""
        if not user:
            user = g.user
        dag_resource_name = permissions.resource_name_for_dag(dag_id)

        return self._has_view_access(
            user, permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG
        ) or self._has_view_access(user, permissions.ACTION_CAN_EDIT, dag_resource_name)

    def prefixed_dag_id(self, dag_id):
        """Returns the permission name for a DAG id."""
        warnings.warn(
            "`prefixed_dag_id` has been deprecated. "
            "Please use `airflow.security.permissions.resource_name_for_dag` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return permissions.resource_name_for_dag(dag_id)

    def is_dag_resource(self, resource_name):
        """Determines if a permission belongs to a DAG or all DAGs."""
        if resource_name == permissions.RESOURCE_DAG:
            return True
        return resource_name.startswith(permissions.RESOURCE_DAG_PREFIX)

    def has_access(self, permission, resource, user=None) -> bool:
        """
        Verify whether a given user could perform certain permission
        (e.g can_read, can_write) on the given resource.

        :param permission: permission on resource (e.g can_read, can_edit).
        :type permission: str
        :param resource: name of view-menu or resource.
        :type resource: str
        :param user: user name
        :type user: str
        :return: a bool whether user could perform certain permission on the resource.
        :rtype bool
        """
        if not user:
            user = g.user

        if user.is_anonymous:
            user.roles = self.get_user_roles(user)

        has_access = self._has_view_access(user, permission, resource)
        # FAB built-in view access method. Won't work for AllDag access.

        if self.is_dag_resource(resource):
            if permission == permissions.ACTION_CAN_READ:
                has_access |= self.can_read_dag(resource, user)
            elif permission == permissions.ACTION_CAN_EDIT:
                has_access |= self.can_edit_dag(resource, user)

        return has_access

    def _get_and_cache_perms(self):
        """Cache permissions-views"""
        self.perms = self.get_current_user_permissions()

    def _has_role(self, role_name_or_list):
        """Whether the user has this role name"""
        if not isinstance(role_name_or_list, list):
            role_name_or_list = [role_name_or_list]
        return any(r.name in role_name_or_list for r in self.get_user_roles())

    def _has_perm(self, permission_name, view_menu_name):
        """Whether the user has this perm"""
        if hasattr(self, 'perms') and self.perms is not None:
            if (permission_name, view_menu_name) in self.perms:
                return True
        # rebuild the permissions set
        self._get_and_cache_perms()
        return (permission_name, view_menu_name) in self.perms

    def has_all_dags_access(self):
        """
        Has all the dag access in any of the 3 cases:
        1. Role needs to be in (Admin, Viewer, User, Op).
        2. Has can_read permission on dags view.
        3. Has can_edit permission on dags view.
        """
        return (
            self._has_role(['Admin', 'Viewer', 'Op', 'User'])
            or self._has_perm(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)
            or self._has_perm(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG)
        )

    def clean_perms(self):
        """FAB leaves faulty permissions that need to be cleaned up"""
        self.log.debug('Cleaning faulty perms')
        sesh = self.get_session
        pvms = sesh.query(sqla_models.PermissionView).filter(
            or_(
                sqla_models.PermissionView.permission == None,  # noqa
                sqla_models.PermissionView.view_menu == None,
            )
        )
        # Since FAB doesn't define ON DELETE CASCADE on these tables, we need
        # to delete the _object_ so that SQLA knows to delete the many-to-many
        # relationship object too. :(

        deleted_count = 0
        for pvm in pvms:
            sesh.delete(pvm)
            deleted_count += 1
        sesh.commit()
        if deleted_count:
            self.log.info('Deleted %s faulty permissions', deleted_count)

    def _merge_perm(self, permission_name, view_menu_name):
        """
        Add the new (permission, view_menu) to assoc_permissionview_role if it doesn't exist.
        It will add the related entry to ab_permission
        and ab_view_menu two meta tables as well.

        :param permission_name: Name of the permission.
        :type permission_name: str
        :param view_menu_name: Name of the view-menu
        :type view_menu_name: str
        :return:
        """
        permission = self.find_permission(permission_name)
        view_menu = self.find_view_menu(view_menu_name)
        permission_view = None
        if permission and view_menu:
            permission_view = (
                self.get_session.query(self.permissionview_model)
                .filter_by(permission=permission, view_menu=view_menu)
                .first()
            )
        if not permission_view and permission_name and view_menu_name:
            self.add_permission_view_menu(permission_name, view_menu_name)

    def add_homepage_access_to_custom_roles(self):
        """
        Add Website.can_read access to all custom roles.

        :return: None.
        """
        website_permission = self.add_permission_view_menu(
            permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE
        )
        custom_roles = [role for role in self.get_all_roles() if role.name not in EXISTING_ROLES]
        for role in custom_roles:
            self.add_permission_role(role, website_permission)

        self.get_session.commit()

    def get_all_permissions(self) -> Set[Tuple[str, str]]:
        """Returns all permissions as a set of tuples with the perm name and view menu name"""
        return set(
            self.get_session.query(self.permissionview_model)
            .join(self.permission_model)
            .join(self.viewmenu_model)
            .with_entities(self.permission_model.name, self.viewmenu_model.name)
            .all()
        )

    def _get_all_non_dag_permissionviews(self) -> Dict[Tuple[str, str], PermissionView]:
        """
        Returns a dict with a key of (perm name, view menu name) and value of perm view
        with all perm views except those that are for specific DAGs.
        """
        return {
            (perm_name, viewmodel_name): viewmodel
            for perm_name, viewmodel_name, viewmodel in (
                self.get_session.query(self.permissionview_model)
                .join(self.permission_model)
                .join(self.viewmenu_model)
                .filter(~self.viewmenu_model.name.like(f"{permissions.RESOURCE_DAG_PREFIX}%"))
                .with_entities(
                    self.permission_model.name, self.viewmenu_model.name, self.permissionview_model
                )
                .all()
            )
        }

    def _get_all_roles_with_permissions(self) -> Dict[str, Role]:
        """Returns a dict with a key of role name and value of role with eagrly loaded permissions"""
        return {
            r.name: r
            for r in (
                self.get_session.query(self.role_model).options(joinedload(self.role_model.permissions)).all()
            )
        }

    def create_dag_specific_permissions(self) -> None:
        """
        Creates 'can_read' and 'can_edit' permissions for all DAGs,
        along with any `access_control` permissions provided in them.

        This does iterate through ALL the DAGs, which can be slow. See `sync_perm_for_dag`
        if you only need to sync a single DAG.

        :return: None.
        """
        perms = self.get_all_permissions()
        dagbag = DagBag(read_dags_from_db=True)
        dagbag.collect_dags_from_db()
        dags = dagbag.dags.values()

        for dag in dags:
            dag_resource_name = permissions.resource_name_for_dag(dag.dag_id)
            for perm_name in self.DAG_PERMS:
                if (perm_name, dag_resource_name) not in perms:
                    self._merge_perm(perm_name, dag_resource_name)

            if dag.access_control:
                self._sync_dag_view_permissions(dag_resource_name, dag.access_control)

    def update_admin_perm_view(self):
        """
        Admin should have all the permission-views, except the dag views.
        because Admin already has Dags permission.
        Add the missing ones to the table for admin.

        :return: None.
        """
        dag_pvs = (
            self.get_session.query(sqla_models.ViewMenu)
            .filter(sqla_models.ViewMenu.name.like(f"{permissions.RESOURCE_DAG_PREFIX}%"))
            .all()
        )
        pv_ids = [pv.id for pv in dag_pvs]
        pvms = (
            self.get_session.query(sqla_models.PermissionView)
            .filter(~sqla_models.PermissionView.view_menu_id.in_(pv_ids))
            .all()
        )

        pvms = [p for p in pvms if p.permission and p.view_menu]

        admin = self.find_role('Admin')
        admin.permissions = list(set(admin.permissions) | set(pvms))

        self.get_session.commit()

    def sync_roles(self):
        """
        1. Init the default role(Admin, Viewer, User, Op, public)
           with related permissions.
        2. Init the custom role(dag-user) with related permissions.

        :return: None.
        """
        # Create global all-dag VM
        self.create_perm_vm_for_all_dag()

        # Sync the default roles (Admin, Viewer, User, Op, public) with related permissions
        self.bulk_sync_roles(self.ROLE_CONFIGS)

        self.add_homepage_access_to_custom_roles()
        # init existing roles, the rest role could be created through UI.
        self.update_admin_perm_view()
        self.clean_perms()

    def sync_resource_permissions(self, perms=None):
        """Populates resource-based permissions."""
        if not perms:
            return

        for action, resource in perms:
            self.add_view_menu(resource)
            self.add_permission_view_menu(action, resource)

    def sync_perm_for_dag(self, dag_id, access_control=None):
        """
        Sync permissions for given dag id. The dag id surely exists in our dag bag
        as only / refresh button or DagBag will call this function

        :param dag_id: the ID of the DAG whose permissions should be updated
        :type dag_id: str
        :param access_control: a dict where each key is a rolename and
            each value is a set() of permission names (e.g.,
            {'can_read'}
        :type access_control: dict
        :return:
        """
        dag_resource_name = permissions.resource_name_for_dag(dag_id)
        for dag_perm in self.DAG_PERMS:
            self.add_permission_view_menu(dag_perm, dag_resource_name)

        if access_control:
            self._sync_dag_view_permissions(dag_resource_name, access_control)

    def _sync_dag_view_permissions(self, dag_id, access_control):
        """Set the access policy on the given DAG's ViewModel.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :type dag_id: str
        :param access_control: a dict where each key is a rolename and
            each value is a set() of permission names (e.g.,
            {'can_read'}
        :type access_control: dict
        """
        dag_resource_name = permissions.resource_name_for_dag(dag_id)

        def _get_or_create_dag_permission(perm_name):
            dag_perm = self.find_permission_view_menu(perm_name, dag_resource_name)
            if not dag_perm:
                self.log.info("Creating new permission '%s' on view '%s'", perm_name, dag_resource_name)
                dag_perm = self.add_permission_view_menu(perm_name, dag_resource_name)

            return dag_perm

        def _revoke_stale_permissions(dag_view):
            existing_dag_perms = self.find_permissions_view_menu(dag_view)
            for perm in existing_dag_perms:
                non_admin_roles = [role for role in perm.role if role.name != 'Admin']
                for role in non_admin_roles:
                    target_perms_for_role = access_control.get(role.name, {})
                    if perm.permission.name not in target_perms_for_role:
                        self.log.info(
                            "Revoking '%s' on DAG '%s' for role '%s'",
                            perm.permission,
                            dag_resource_name,
                            role.name,
                        )
                        self.del_permission_role(role, perm)

        dag_view = self.find_view_menu(dag_resource_name)
        if dag_view:
            _revoke_stale_permissions(dag_view)

        for rolename, perms in access_control.items():
            role = self.find_role(rolename)
            if not role:
                raise AirflowException(
                    "The access_control mapping for DAG '{}' includes a role "
                    "named '{}', but that role does not exist".format(dag_id, rolename)
                )

            perms = set(perms)
            invalid_perms = perms - self.DAG_PERMS
            if invalid_perms:
                raise AirflowException(
                    "The access_control map for DAG '{}' includes the following "
                    "invalid permissions: {}; The set of valid permissions "
                    "is: {}".format(dag_resource_name, invalid_perms, self.DAG_PERMS)
                )

            for perm_name in perms:
                dag_perm = _get_or_create_dag_permission(perm_name)
                self.add_permission_role(role, dag_perm)

    def create_perm_vm_for_all_dag(self):
        """Create perm-vm if not exist and insert into FAB security model for all-dags."""
        # create perm for global logical dag
        for dag_vm in self.DAG_VMS:
            for perm in self.DAG_PERMS:
                self._merge_perm(permission_name=perm, view_menu_name=dag_vm)

    def check_authorization(
        self, perms: Optional[Sequence[Tuple[str, str]]] = None, dag_id: Optional[str] = None
    ) -> bool:
        """Checks that the logged in user has the specified permissions."""
        if not perms:
            return True

        for perm in perms:
            if perm in (
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            ):
                can_access_all_dags = self.has_access(*perm)
                if can_access_all_dags:
                    continue

                action = perm[0]
                if self.can_access_some_dags(action, dag_id):
                    continue
                return False

            elif not self.has_access(*perm):
                return False

        return True


class ApplessAirflowSecurityManager(AirflowSecurityManager):
    """Security Manager that doesn't need the whole flask app"""

    def __init__(self, session=None):
        self.session = session

    @property
    def get_session(self):
        return self.session
