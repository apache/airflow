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
from typing import TYPE_CHECKING, Any, Collection, Container, Iterable, Sequence

from flask import g
from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload

from airflow.auth.managers.fab.models import Permission, Resource, Role
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
from airflow.auth.managers.models.resource_details import DagDetails
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.models import DagBag, DagModel
from airflow.security import permissions
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager
from airflow.www.fab_security.sqla.manager import SecurityManager
from airflow.www.utils import CustomSQLAInterface

EXISTING_ROLES = {
    "Admin",
    "Viewer",
    "User",
    "Op",
    "Public",
}

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.auth.managers.base_auth_manager import ResourceMethod


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
    DAG_RESOURCES = {permissions.RESOURCE_DAG}
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

    def init_role(self, role_name, perms) -> None:
        """
        Initialize the role with actions and related resources.

        :param role_name:
        :param perms:
        :return:
        """
        warnings.warn(
            "`init_role` has been deprecated. Please use `bulk_sync_roles` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        self.bulk_sync_roles([{"role": role_name, "perms": perms}])

    def bulk_sync_roles(self, roles: Iterable[dict[str, Any]]) -> None:
        """Sync the provided roles and permissions."""
        existing_roles = self._get_all_roles_with_permissions()
        non_dag_perms = self._get_all_non_dag_permissions()

        for config in roles:
            role_name = config["role"]
            perms = config["perms"]
            role = existing_roles.get(role_name) or self.add_role(role_name)

            for action_name, resource_name in perms:
                perm = non_dag_perms.get((action_name, resource_name)) or self.create_permission(
                    action_name, resource_name
                )

                if perm not in role.permissions:
                    self.add_permission_to_role(role, perm)

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

    def get_readable_dag_ids(self, user=None) -> set[str]:
        """Gets the DAG IDs readable by authenticated user."""
        return self.get_permitted_dag_ids(methods=["GET"], user=user)

    def get_editable_dag_ids(self, user=None) -> set[str]:
        """Gets the DAG IDs editable by authenticated user."""
        return self.get_permitted_dag_ids(methods=["PUT"], user=user)

    @provide_session
    def get_permitted_dag_ids(
        self,
        *,
        methods: Container[ResourceMethod] | None = None,
        user=None,
        session: Session = NEW_SESSION,
    ) -> set[str]:
        """Generic function to get readable or writable DAGs for user."""
        if not methods:
            methods = ["PUT", "GET"]

        dag_ids = {dag.dag_id for dag in session.execute(select(DagModel.dag_id))}

        if ("GET" in methods and get_auth_manager().is_authorized_dag(method="GET", user=user)) or (
            "PUT" in methods and get_auth_manager().is_authorized_dag(method="PUT", user=user)
        ):
            return dag_ids

        return {
            dag_id
            for dag_id in dag_ids
            if (
                "GET" in methods
                and get_auth_manager().is_authorized_dag(
                    method="GET", details=DagDetails(id=dag_id), user=user
                )
            )
            or (
                "PUT" in methods
                and get_auth_manager().is_authorized_dag(
                    method="PUT", details=DagDetails(id=dag_id), user=user
                )
            )
        }

    def can_access_dags(self, user) -> bool:
        """Checks if user has read access to some dags."""
        return any(self.get_readable_dag_ids(user))

    def prefixed_dag_id(self, dag_id: str) -> str:
        """Returns the permission name for a DAG id."""
        warnings.warn(
            "`prefixed_dag_id` has been deprecated. "
            "Please use `airflow.security.permissions.resource_name_for_dag` instead.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        root_dag_id = self._get_root_dag_id(dag_id)
        return permissions.resource_name_for_dag(root_dag_id)

    def is_dag_resource(self, resource_name: str) -> bool:
        """Determines if a resource belongs to a DAG or all DAGs."""
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

    def clean_perms(self) -> None:
        """FAB leaves faulty permissions that need to be cleaned up."""
        self.log.debug("Cleaning faulty perms")
        sesh = self.appbuilder.get_session
        perms = sesh.query(Permission).filter(
            or_(
                Permission.action == None,  # noqa
                Permission.resource == None,  # noqa
            )
        )
        # Since FAB doesn't define ON DELETE CASCADE on these tables, we need
        # to delete the _object_ so that SQLA knows to delete the many-to-many
        # relationship object too. :(

        deleted_count = 0
        for perm in perms:
            sesh.delete(perm)
            deleted_count += 1
        sesh.commit()
        if deleted_count:
            self.log.info("Deleted %s faulty permissions", deleted_count)

    def _merge_perm(self, action_name: str, resource_name: str) -> None:
        """
        Add the new (action, resource) to assoc_permission_role if it doesn't exist.

        It will add the related entry to ab_permission and ab_resource two meta tables as well.

        :param action_name: Name of the action
        :param resource_name: Name of the resource
        :return:
        """
        action = self.get_action(action_name)
        resource = self.get_resource(resource_name)
        perm = None
        if action and resource:
            perm = self.appbuilder.get_session.scalar(
                select(self.permission_model).filter_by(action=action, resource=resource).limit(1)
            )
        if not perm and action_name and resource_name:
            self.create_permission(action_name, resource_name)

    def add_homepage_access_to_custom_roles(self) -> None:
        """
        Add Website.can_read access to all custom roles.

        :return: None.
        """
        website_permission = self.create_permission(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)
        custom_roles = [role for role in self.get_all_roles() if role.name not in EXISTING_ROLES]
        for role in custom_roles:
            self.add_permission_to_role(role, website_permission)

        self.appbuilder.get_session.commit()

    def get_all_permissions(self) -> set[tuple[str, str]]:
        """Returns all permissions as a set of tuples with the action and resource names."""
        return set(
            self.appbuilder.get_session.execute(
                select(self.action_model.name, self.resource_model.name)
                .join(self.permission_model.action)
                .join(self.permission_model.resource)
            )
        )

    def _get_all_non_dag_permissions(self) -> dict[tuple[str, str], Permission]:
        """
        Get permissions except those that are for specific DAGs.

        Returns a dict with a key of (action_name, resource_name) and value of permission
        with all permissions except those that are for specific DAGs.
        """
        return {
            (action_name, resource_name): viewmodel
            for action_name, resource_name, viewmodel in (
                self.appbuilder.get_session.execute(
                    select(self.action_model.name, self.resource_model.name, self.permission_model)
                    .join(self.permission_model.action)
                    .join(self.permission_model.resource)
                    .where(~self.resource_model.name.like(f"{permissions.RESOURCE_DAG_PREFIX}%"))
                )
            )
        }

    def _get_all_roles_with_permissions(self) -> dict[str, Role]:
        """Returns a dict with a key of role name and value of role with early loaded permissions."""
        return {
            r.name: r
            for r in self.appbuilder.get_session.scalars(
                select(self.role_model).options(joinedload(self.role_model.permissions))
            ).unique()
        }

    def create_dag_specific_permissions(self) -> None:
        """
        Add permissions to all DAGs.

        Creates 'can_read', 'can_edit', and 'can_delete' permissions for all
        DAGs, along with any `access_control` permissions provided in them.

        This does iterate through ALL the DAGs, which can be slow. See `sync_perm_for_dag`
        if you only need to sync a single DAG.

        :return: None.
        """
        perms = self.get_all_permissions()
        dagbag = DagBag(read_dags_from_db=True)
        dagbag.collect_dags_from_db()
        dags = dagbag.dags.values()

        for dag in dags:
            root_dag_id = dag.parent_dag.dag_id if dag.parent_dag else dag.dag_id
            dag_resource_name = permissions.resource_name_for_dag(root_dag_id)
            for action_name in self.DAG_ACTIONS:
                if (action_name, dag_resource_name) not in perms:
                    self._merge_perm(action_name, dag_resource_name)

            if dag.access_control is not None:
                self.sync_perm_for_dag(dag_resource_name, dag.access_control)

    def update_admin_permission(self) -> None:
        """
        Add missing permissions to the table for admin.

        Admin should get all the permissions, except the dag permissions
        because Admin already has Dags permission.
        Add the missing ones to the table for admin.

        :return: None.
        """
        session = self.appbuilder.get_session
        dag_resources = session.scalars(
            select(Resource).where(Resource.name.like(f"{permissions.RESOURCE_DAG_PREFIX}%"))
        )
        resource_ids = [resource.id for resource in dag_resources]

        perms = session.scalars(select(Permission).where(~Permission.resource_id.in_(resource_ids)))
        perms = [p for p in perms if p.action and p.resource]

        admin = self.find_role("Admin")
        admin.permissions = list(set(admin.permissions) | set(perms))

        session.commit()

    def create_admin_standalone(self) -> tuple[str | None, str | None]:
        """Perform the required steps when initializing airflow for standalone mode.

        If necessary, returns the username and password to be printed in the console for users to log in.
        """
        return None, None

    def sync_roles(self) -> None:
        """
        Initialize default and custom roles with related permissions.

        1. Init the default role(Admin, Viewer, User, Op, public)
           with related permissions.
        2. Init the custom role(dag-user) with related permissions.

        :return: None.
        """
        # Create global all-dag permissions
        self.create_perm_vm_for_all_dag()

        # Sync the default roles (Admin, Viewer, User, Op, public) with related permissions
        self.bulk_sync_roles(self.ROLE_CONFIGS)

        self.add_homepage_access_to_custom_roles()
        # init existing roles, the rest role could be created through UI.
        self.update_admin_permission()
        self.clean_perms()

    def sync_resource_permissions(self, perms: Iterable[tuple[str, str]] | None = None) -> None:
        """Populates resource-based permissions."""
        if not perms:
            return

        for action_name, resource_name in perms:
            self.create_resource(resource_name)
            self.create_permission(action_name, resource_name)

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

    def create_perm_vm_for_all_dag(self) -> None:
        """Create perm-vm if not exist and insert into FAB security model for all-dags."""
        # create perm for global logical dag
        for resource_name in self.DAG_RESOURCES:
            for action_name in self.DAG_ACTIONS:
                self._merge_perm(action_name, resource_name)

    def check_authorization(
        self,
        perms: Sequence[tuple[str, str]] | None = None,
    ) -> bool:
        """Checks that the logged in user has the specified permissions."""
        if not perms:
            return True

        return all(self.has_access(*perm) for perm in perms)
