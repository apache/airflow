# -*- coding: utf-8 -*-
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

import logging
from flask import g
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.security.sqla.manager import SecurityManager
from sqlalchemy import or_

from airflow import models, settings
from airflow.www_rbac.app import appbuilder

###########################################################################
#                               VIEW MENUS
###########################################################################
viewer_vms = {
    'Airflow',
    'DagModelView',
    'Browse',
    'DAG Runs',
    'DagRunModelView',
    'Task Instances',
    'TaskInstanceModelView',
    'SLA Misses',
    'SlaMissModelView',
    'Jobs',
    'JobModelView',
    'Logs',
    'LogModelView',
    'Docs',
    'Documentation',
    'Github',
    'About',
    'Version',
    'VersionView',
}

user_vms = viewer_vms

op_vms = {
    'Admin',
    'Configurations',
    'ConfigurationView',
    'Connections',
    'ConnectionModelView',
    'Pools',
    'PoolModelView',
    'Variables',
    'VariableModelView',
    'XComs',
    'XComModelView',
}

###########################################################################
#                               PERMISSIONS
###########################################################################

viewer_perms = {
    'menu_access',
    'can_index',
    'can_list',
    'can_show',
    'can_chart',
    'can_dag_stats',
    'can_dag_details',
    'can_task_stats',
    'can_code',
    'can_log',
    'can_get_logs_with_metadata',
    'can_tries',
    'can_graph',
    'can_tree',
    'can_task',
    'can_task_instances',
    'can_xcom',
    'can_gantt',
    'can_landing_times',
    'can_duration',
    'can_blocked',
    'can_rendered',
    'can_pickle_info',
    'can_version',
}

user_perms = {
    'can_dagrun_clear',
    'can_run',
    'can_trigger',
    'can_add',
    'can_edit',
    'can_delete',
    'can_paused',
    'can_refresh',
    'can_success',
    'muldelete',
    'set_failed',
    'set_running',
    'set_success',
    'clear',
}

op_perms = {
    'can_conf',
    'can_varimport',
}

# global view-menu for dag-level access
dag_vms = {
    'all_dags'
}

dag_perms = {
    'can_dag_read',
    'can_dag_edit',
}

###########################################################################
#                     DEFAULT ROLE CONFIGURATIONS
###########################################################################

ROLE_CONFIGS = [
    {
        'role': 'Viewer',
        'perms': viewer_perms,
        'vms': viewer_vms | dag_vms
    },
    {
        'role': 'User',
        'perms': viewer_perms | user_perms | dag_perms,
        'vms': viewer_vms | dag_vms | user_vms,
    },
    {
        'role': 'Op',
        'perms': viewer_perms | user_perms | op_perms | dag_perms,
        'vms': viewer_vms | dag_vms | user_vms | op_vms,
    },
]

EXISTING_ROLES = {
    'Admin',
    'Viewer',
    'User',
    'Op',
    'Public',
}


class AirflowSecurityManager(SecurityManager):

    def init_role(self, role_name, role_vms, role_perms):
        """
        Initialize the role with the permissions and related view-menus.

        :param role_name:
        :param role_vms:
        :param role_perms:
        :return:
        """
        pvms = self.get_session.query(sqla_models.PermissionView).all()
        pvms = [p for p in pvms if p.permission and p.view_menu]

        role = self.find_role(role_name)
        if not role:
            role = self.add_role(role_name)

        role_pvms = []
        for pvm in pvms:
            if pvm.view_menu.name in role_vms and pvm.permission.name in role_perms:
                role_pvms.append(pvm)
        role.permissions = list(set(role_pvms))
        self.get_session.merge(role)
        self.get_session.commit()

    def get_user_roles(self, user=None):
        """
        Get all the roles associated with the user.
        """
        if user is None:
            user = g.user
        if user.is_anonymous():
            public_role = appbuilder.config.get('AUTH_ROLE_PUBLIC')
            return [appbuilder.security_manager.find_role(public_role)] \
                if public_role else []
        return user.roles

    def get_all_permissions_views(self):
        """
        Returns a set of tuples with the perm name and view menu name
        """
        perms_views = set()
        for role in self.get_user_roles():
            for perm_view in role.permissions:
                perms_views.add((perm_view.permission.name, perm_view.view_menu.name))
        return perms_views

    def get_accessible_dag_ids(self, username=None):
        """
        Return a set of dags that user has access to(either read or write).

        :param username: Name of the user.
        :return: A set of dag ids that the user could access.
        """
        if not username:
            username = g.user

        if username.is_anonymous() or 'Public' in username.roles:
            # return an empty list if the role is public
            return set()

        roles = {role.name for role in username.roles}
        if {'Admin', 'Viewer', 'User', 'Op'} & roles:
            return dag_vms

        user_perms_views = self.get_all_permissions_views()
        # return all dags that the user could access
        return set([view for perm, view in user_perms_views if perm in dag_perms])

    def has_access(self, permission, view_name, user=None):
        """
        Verify whether a given user could perform certain permission
        (e.g can_read, can_write) on the given dag_id.

        :param str permission: permission on dag_id(e.g can_read, can_edit).
        :param str view_name: name of view-menu(e.g dag id is a view-menu as well).
        :param str user: user name
        :return: a bool whether user could perform certain permission on the dag_id.
        """
        if not user:
            user = g.user
        if user.is_anonymous():
            return self.is_item_public(permission, view_name)
        return self._has_view_access(user, permission, view_name)

    def _get_and_cache_perms(self):
        """
        Cache permissions-views
        """
        self.perms = self.get_all_permissions_views()

    def _has_role(self, role_name_or_list):
        """
        Whether the user has this role name
        """
        if not isinstance(role_name_or_list, list):
            role_name_or_list = [role_name_or_list]
        return any(
            [r.name in role_name_or_list for r in self.get_user_roles()])

    def _has_perm(self, permission_name, view_menu_name):
        """
        Whether the user has this perm
        """
        if hasattr(self, 'perms'):
            if (permission_name, view_menu_name) in self.perms:
                return True
        # rebuild the permissions set
        self._get_and_cache_perms()
        return (permission_name, view_menu_name) in self.perms

    def has_all_dags_access(self):
        """
        Has all the dag access in any of the 3 cases:
        1. Role needs to be in (Admin, Viewer, User, Op).
        2. Has can_dag_read permission on all_dags view.
        3. Has can_dag_edit permission on all_dags view.
        """
        return (
            self._has_role(['Admin', 'Viewer', 'Op', 'User']) or
            self._has_perm('can_dag_read', 'all_dags') or
            self._has_perm('can_dag_edit', 'all_dags'))

    def clean_perms(self):
        """
        FAB leaves faulty permissions that need to be cleaned up
        """
        logging.info('Cleaning faulty perms')
        sesh = self.get_session
        pvms = (
            sesh.query(sqla_models.PermissionView)
            .filter(or_(
                sqla_models.PermissionView.permission == None,  # NOQA
                sqla_models.PermissionView.view_menu == None,  # NOQA
            ))
        )
        deleted_count = pvms.delete()
        sesh.commit()
        if deleted_count:
            logging.info('Deleted {} faulty permissions'.format(deleted_count))

    def _merge_perm(self, permission_name, view_menu_name):
        """
        Add the new permission , view_menu to ab_permission_view_role if not exists.
        It will add the related entry to ab_permission
        and ab_view_menu two meta tables as well.

        :param str permission_name: Name of the permission.
        :param str view_menu_name: Name of the view-menu

        :return:
        """
        permission = self.find_permission(permission_name)
        view_menu = self.find_view_menu(view_menu_name)
        pv = None
        if permission and view_menu:
            pv = self.get_session.query(self.permissionview_model).filter_by(
                permission=permission, view_menu=view_menu).first()
        if not pv and permission_name and view_menu_name:
            self.add_permission_view_menu(permission_name, view_menu_name)

    def create_custom_dag_permission_view(self):
        """
        Workflow:
        1. when scheduler found a new dag, we will create an entry in ab_view_menu
        2. we fetch all the roles associated with dag users.
        3. we join and create all the entries for ab_permission_view_menu
           (predefined permissions * dag-view_menus)
        4. Create all the missing role-permission-views for the ab_role_permission_views

        :return: None.
        """
        # todo(Tao): should we put this function here or in scheduler loop?
        logging.info('Fetching a set of all permission, view_menu from FAB meta-table')

        def merge_pv(perm, view_menu):
            """Create permission view menu only if it doesn't exist"""
            if view_menu and perm and (view_menu, perm) not in all_pvs:
                self._merge_perm(perm, view_menu)

        all_pvs = set()
        for pv in self.get_session.query(self.permissionview_model).all():
            if pv.permission and pv.view_menu:
                all_pvs.add((pv.permission.name, pv.view_menu.name))

        # create perm for global logical dag
        for dag in dag_vms:
            for perm in dag_perms:
                merge_pv(perm, dag)

        # Get all the active / paused dags and insert them into a set
        all_dags_models = settings.Session.query(models.DagModel)\
            .filter(or_(models.DagModel.is_active, models.DagModel.is_paused))\
            .filter(~models.DagModel.is_subdag).all()

        for dag in all_dags_models:
            for perm in dag_perms:
                merge_pv(perm, dag.dag_id)

        # for all the dag-level role, add the permission of viewer
        # with the dag view to ab_permission_view
        all_roles = self.get_all_roles()
        user_role = self.find_role('User')

        dag_role = [role for role in all_roles if role.name not in EXISTING_ROLES]
        update_perm_views = []

        # todo(tao) need to remove all_dag vm
        dag_vm = self.find_view_menu('all_dags')
        ab_perm_view_role = sqla_models.assoc_permissionview_role
        perm_view = self.permissionview_model
        view_menu = self.viewmenu_model

        # todo(tao) comment on the query
        all_perm_view_by_user = settings.Session.query(ab_perm_view_role)\
            .join(perm_view, perm_view.id == ab_perm_view_role
                  .columns.permission_view_id)\
            .filter(ab_perm_view_role.columns.role_id == user_role.id)\
            .join(view_menu)\
            .filter(perm_view.view_menu_id != dag_vm.id)
        all_perm_views = set([role.permission_view_id for role in all_perm_view_by_user])

        for role in dag_role:
            # Get all the perm-view of the role
            existing_perm_view_by_user = self.get_session.query(ab_perm_view_role)\
                .filter(ab_perm_view_role.columns.role_id == role.id)

            existing_perms_views = set([role.permission_view_id
                                        for role in existing_perm_view_by_user])
            missing_perm_views = all_perm_views - existing_perms_views

            for perm_view_id in missing_perm_views:
                update_perm_views.append({'permission_view_id': perm_view_id,
                                          'role_id': role.id})

        self.get_session.execute(ab_perm_view_role.insert(), update_perm_views)
        self.get_session.commit()

    def update_admin_perm_view(self):
        """
        Admin should have all the permission-views.
        Add the missing ones to the table for admin.

        :return: None.
        """
        pvms = self.get_session.query(sqla_models.PermissionView).all()
        pvms = [p for p in pvms if p.permission and p.view_menu]

        admin = self.find_role('Admin')
        existing_perms_vms = set(admin.permissions)
        for p in pvms:
            if p not in existing_perms_vms:
                existing_perms_vms.add(p)
        admin.permissions = list(existing_perms_vms)
        self.get_session.commit()

    def sync_roles(self):
        """
        1. Init the default role(Admin, Viewer, User, Op, public)
           with related permissions.
        2. Init the custom role(dag-user) with related permissions.

        :return: None.
        """
        logging.info('Start syncing user roles.')

        # Create default user role.
        for config in ROLE_CONFIGS:
            role = config['role']
            vms = config['vms']
            perms = config['perms']
            self.init_role(role, vms, perms)
        self.create_custom_dag_permission_view()

        # init existing roles, the rest role could be created through UI.
        self.update_admin_perm_view()
        self.clean_perms()

    def sync_perm_for_dag(self, dag_id):
        """
        Sync permissions for given dag id. The dag id surely exists in our dag bag
        as only /refresh button will call this function

        :param dag_id:
        :return:
        """
        for dag_perm in dag_perms:
            perm_on_dag = self.find_permission_view_menu(dag_perm, dag_id)
            if perm_on_dag is None:
                self.add_permission_view_menu(dag_perm, dag_id)
