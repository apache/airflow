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

from flask_appbuilder.security.views import AuthOAuthView as AV
from flask_login.mixins import AnonymousUserMixin
from plugins.utils.custom_log import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
from flask_login import login_user, logout_user, current_user
from flask import redirect, url_for
import logging
from pprint import pprint, pformat
from tenacity import retry, wait_exponential, retry_if_exception_type, stop_after_delay, RetryError
import requests
from typing import Optional, Dict
import json
import os
from flask import request
from flask_appbuilder.security.views import expose
from flask import g
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.security.sqla.manager import SecurityManager
from sqlalchemy import or_, and_
from flask_appbuilder.security.views import AuthDBView
from datetime import datetime

from airflow import models
from airflow.exceptions import AirflowException
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www_rbac.app import appbuilder
from airflow.www_rbac.utils import CustomSQLAInterface
from airflow.settings import TIMEZONE

EXISTING_ROLES = {
    'Admin',
    'Viewer',
    'User',
    'Op',
    'Public',
    '工段长',
    'ME工程师',
    '运维人员'
}


ENV_SUC_CRCCODE_KEY = os.getenv('ENV_SUC_CRCCODE_KEY', 'crccode')
ENV_SUC_ROOT_URL = os.getenv('ENV_SUC_ROOT_URL', 'http://localhost:8080')
ENV_PORTAL_INDEX_URL = os.getenv(
    'ENV_PORTAL_INDEX_URL', 'http://idas-pv.smicmotor.com/ids/index.html')
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')


@retry(wait=wait_exponential(multiplier=1, max=3), stop=stop_after_delay(5), retry=retry_if_exception_type())
def doGetLoginInfo(crcCode: str) -> Optional[Dict]:
    url = "{}/{}".format(ENV_SUC_ROOT_URL, "accounts/crcCodeLogin")
    try:
        resp = requests.get(url, params={'crcCode': crcCode}, timeout=5)
        if resp.status_code != 200:
            return None
        data = resp.json()
        print("Get SSO Auth Info: {}".format(pformat(data, indent=4)))
        if not data.get('success'):
            return None
        rData = data.get('data')
        if not isinstance(rData, Dict):
            rData = json.loads(rData)
        if not rData:
            return None
        username = email = rData.get('account')
        return {'username': username, 'email': email, 'last_name': username}
    except Exception as e:
        raise e


class CustomAuthDBView(AuthDBView):
    login_template = "login.html"

    @expose("/login/", methods=["GET", "POST"])
    def login(self):
        ret = super(CustomAuthDBView, self).login()
        msg = ''
        if not current_user.is_active:
            msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                           'null', 'null',
                                           CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['LOGIN'], '查看登录页面')
        else:
            msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                           current_user,  getattr(
                                               current_user, 'last_name', ''),
                                           CUSTOM_EVENT_NAME_MAP['LOGIN'], CUSTOM_PAGE_NAME_MAP['LOGIN'], '登录')
        logging.info(msg)
        return ret

    @expose("/logout/")
    def logout(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(
                                           current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['LOGOUT'], CUSTOM_PAGE_NAME_MAP['LOGOUT'], '登出')
        ret = super(CustomAuthDBView, self).logout()
        logging.info(msg)
        return ret


class SUCUser(models.User):

    def __init__(self, user):
        self.user = user

    @property
    def last_name(self):
        return self.user.last_name

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        """Access all the things"""
        return False

    def __getattribute__(self, name):
        if name == 'last_name':
            return getattr(self.user, 'last_name', '') or getattr(self.user, 'username', '')
        return super(SUCUser, self).__getattribute__(name)


class AuthOAuthView(AV):
    login_template = "airflow/login_oauth.html"

    @expose("/login/")
    @expose("/login/<provider>")
    @expose("/login/<provider>/<register>")
    @provide_session
    def login(self, provider=None, register=None, session=None):
        crc_code = request.args.get(
            'crccode', None) or request.args.get('crcCode', None)
        if not crc_code:
            return super(AuthOAuthView, self).login(provider=provider, register=register)
        userinfo = doGetLoginInfo(crc_code)
        # userinfo = {'username': "1312321", 'email': "123213", 'last_name': "12312323"}
        if not userinfo:
            return super(AuthOAuthView, self).login(provider=provider, register=register)
        user = self.appbuilder.sm.auth_user_oauth(userinfo)

        session.merge(user)
        session.commit()
        login_user(SUCUser(user))
        session.commit()
        msg = ''
        try:
            if not current_user.is_active:
                msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                               'null', 'null',
                                               CUSTOM_EVENT_NAME_MAP['VIEW'], CUSTOM_PAGE_NAME_MAP['LOGIN'], '查看登录页面')
            else:
                msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                               current_user,  getattr(
                                                   current_user, 'last_name', ''),
                                               CUSTOM_EVENT_NAME_MAP['LOGIN'], CUSTOM_PAGE_NAME_MAP['LOGIN'], '登录')
        except AttributeError as err:
            logging.error(err)
        logging.info(msg)
        return redirect(url_for('Airflow.index'))

    @expose("/logout/")
    def logout(self):
        msg = CUSTOM_LOG_FORMAT.format(datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                                       current_user, getattr(
                                           current_user, 'last_name', ''),
                                       CUSTOM_EVENT_NAME_MAP['LOGOUT'], CUSTOM_PAGE_NAME_MAP['LOGOUT'], '登出')
        logging.info(msg)
        logout_user()
        if ENV_PORTAL_INDEX_URL and RUNTIME_ENV == 'prod':
            return redirect(ENV_PORTAL_INDEX_URL)
        return redirect(self.appbuilder.get_url_for_index)


class AirflowSecurityManager(SecurityManager, LoggingMixin):
    ###########################################################################
    #                               VIEW MENUS
    ###########################################################################
    VIEWER_VMS = {
        'Airflow',
        'DagModelView',
        '浏览',
        'DAG运行',
        'DagRunModelView',
        '任务实例',
        'TaskInstanceModelView',
        'SLA Misses',
        'SlaMissModelView',
        '工作',
        'JobModelView',
        '日志',
        'LogModelView',
        '文档',
        '文档',
        'Github',
        '关于',
        '版本',
        'VersionView',
    }

    USER_VMS = VIEWER_VMS

    OP_VMS = {
        '管理',
        '配置',
        'ConfigurationView',
        '连接',
        'ConnectionModelView',
        '池',
        'PoolModelView',
        '变量',
        'VariableModelView',
        '任务间消息',
        'XComModelView',
    }

    ###########################################################################
    #                               PERMISSIONS
    ###########################################################################

    VIEWER_PERMS = {
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

    USER_PERMS = {
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
        'can_clear',
    }

    OP_PERMS = {
        'can_conf',
        'can_varimport',
    }

    # global view-menu for dag-level access
    DAG_VMS = {
        'all_dags'
    }

    WRITE_DAG_PERMS = {
        'can_dag_edit',
    }

    READ_DAG_PERMS = {
        'can_dag_read',
    }

    DAG_PERMS = WRITE_DAG_PERMS | READ_DAG_PERMS

    TIGHTENING_VMS = {
        '管理',
        '拧紧控制器',
        '变量',
        'ErrorTagModelView',
        'VariableModelView',
        'TighteningControllerView'
    }

    BASE_VMS = {
        'Airflow',
        'DagModelView',
        '浏览',
        'DAG运行',
        'DagRunModelView',
        '任务实例',
        'TaskInstanceModelView',
        'Analysis',
        '分析'
    }

    BASE_PERMS = {
        'menu_access',
        'can_index',
        'can_last_dagruns',
        'can_dag_stats',
        'can_blocked',
        'can_task_stats',
    }

    MAINTAIN_PERMS = {
        'can_list',
        'can_show',
        'can_add',
        'can_edit',
        'can_delete',
        'muldelete',
        'set_failed',
        'set_running',
        'set_success',
    }

    MAINTAIN_VMS = {
        '管理',
        '拧紧控制器',
        '变量',
        'ErrorTagModelView',
        'VariableModelView',
        'TighteningControllerView',

    }

    TIGHTENING_PERMS = {
        'menu_access',
        'can_list',
        'can_show',
        'can_view_curve_page'
    }

    VERIFY_PERMS = {
        'can_view_curves',
        'can_view_curve_template',
        'set_final_state_ok',
        'set_final_state_nok'
    }

    ###########################################################################
    #                     DEFAULT ROLE CONFIGURATIONS
    ###########################################################################

    ROLE_CONFIGS = [
        {
            'role': 'Viewer',
            'perms': VIEWER_PERMS | READ_DAG_PERMS,
            'vms': VIEWER_VMS | DAG_VMS
        },
        {
            'role': 'User',
            'perms': VIEWER_PERMS | USER_PERMS | DAG_PERMS,
            'vms': VIEWER_VMS | DAG_VMS | USER_VMS,
        },
        {
            'role': 'Op',
            'perms': VIEWER_PERMS | USER_PERMS | OP_PERMS | DAG_PERMS,
            'vms': VIEWER_VMS | DAG_VMS | USER_VMS | OP_VMS,
        },
        {
            'role': '工段长',
            'perms': BASE_PERMS | TIGHTENING_PERMS | READ_DAG_PERMS,
            'vms': BASE_VMS | DAG_VMS | TIGHTENING_VMS,
        },
        {
            'role': 'ME工程师',
            'perms': BASE_PERMS | TIGHTENING_PERMS | READ_DAG_PERMS | VERIFY_PERMS,
            'vms': BASE_VMS | DAG_VMS | TIGHTENING_VMS,
        },
        {
            'role': '运维人员',
            'perms': BASE_PERMS | MAINTAIN_PERMS | DAG_PERMS | VIEWER_PERMS | OP_PERMS | USER_PERMS,
            'vms': BASE_VMS | MAINTAIN_VMS | DAG_VMS | VIEWER_VMS | OP_VMS
        },
    ]

    authdbview = CustomAuthDBView  # RBAC override
    authoauthview = AuthOAuthView

    def __init__(self, appbuilder):
        super(AirflowSecurityManager, self).__init__(appbuilder)

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

        if len(role.permissions) == 0:
            self.log.info(
                'Initializing permissions for role:%s in the database.', role_name)
            role_pvms = set()
            for pvm in pvms:
                if pvm.view_menu.name in role_vms and pvm.permission.name in role_perms:
                    role_pvms.add(pvm)
            role.permissions = list(role_pvms)
            self.get_session.merge(role)
            self.get_session.commit()
        else:
            self.log.debug('Existing permissions for the role:%s '
                           'within the database will persist.', role_name)

    def get_user_roles(self, user=None):
        """
        Get all the roles associated with the user.

        :param user: the ab_user in FAB model.
        :return: a list of roles associated with the user.
        """
        if user is None:
            user = g.user
        if user.is_anonymous:
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
            perms_views.update({(perm_view.permission.name, perm_view.view_menu.name)
                                for perm_view in role.permissions})
        return perms_views

    def get_accessible_dag_ids(self, username=None):
        """
        Return a set of dags that user has access to(either read or write).

        :param username: Name of the user.
        :return: A set of dag ids that the user could access.
        """
        if not username:
            username = g.user

        if username.is_anonymous or 'Public' in username.roles:
            # return an empty set if the role is public
            return set()

        roles = {role.name for role in username.roles}
        if {'Admin', 'Viewer', 'User', 'Op'} & roles:
            return self.DAG_VMS

        user_perms_views = self.get_all_permissions_views()
        # return a set of all dags that the user could access
        return set([view for perm, view in user_perms_views if perm in self.DAG_PERMS])

    def has_access(self, permission, view_name, user=None):
        """
        Verify whether a given user could perform certain permission
        (e.g can_read, can_write) on the given dag_id.

        :param permission: permission on dag_id(e.g can_read, can_edit).
        :type permission: str
        :param view_name: name of view-menu(e.g dag id is a view-menu as well).
        :type permission: str
        :param user: user name
        :type permission: str
        :return: a bool whether user could perform certain permission on the dag_id.
        :rtype bool
        """
        if not user:
            user = g.user
        if user.is_anonymous:
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
        self.log.info('Cleaning faulty perms')
        sesh = self.get_session
        pvms = (
            sesh.query(sqla_models.PermissionView)
                .filter(or_(
                sqla_models.PermissionView.permission == None,  # NOQA
                sqla_models.PermissionView.view_menu == None,  # NOQA
            ))
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
        Add the new permission , view_menu to ab_permission_view_role if not exists.
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
        pv = None
        if permission and view_menu:
            pv = self.get_session.query(self.permissionview_model).filter_by(
                permission=permission, view_menu=view_menu).first()
        if not pv and permission_name and view_menu_name:
            self.add_permission_view_menu(permission_name, view_menu_name)

    @provide_session
    def create_custom_dag_permission_view(self, session=None):
        """
        Workflow:
        1. Fetch all the existing (permissions, view-menu) from Airflow DB.
        2. Fetch all the existing dag models that are either active or paused.
        3. Create both read and write permission view-menus relation for every dags from step 2
        4. Find out all the dag specific roles(excluded pubic, admin, viewer, op, user)
        5. Get all the permission-vm owned by the user role.
        6. Grant all the user role's permission-vm except the all-dag view-menus to the dag roles.
        7. Commit the updated permission-vm-role into db

        :return: None.
        """
        # todo(Tao): should we put this function here or in scheduler loop?
        self.log.info(
            'Fetching a set of all permission, view_menu from FAB meta-table')

        def merge_pv(perm, view_menu):
            """Create permission view menu only if it doesn't exist"""
            if view_menu and perm and (view_menu, perm) not in all_pvs:
                self._merge_perm(perm, view_menu)

        all_pvs = set()
        for pv in self.get_session.query(self.permissionview_model).all():
            if pv.permission and pv.view_menu:
                all_pvs.add((pv.permission.name, pv.view_menu.name))

        # Get all the active / paused dags and insert them into a set
        all_dags_models = session.query(models.DagModel) \
            .filter(or_(models.DagModel.is_active, models.DagModel.is_paused)).all()

        # create can_dag_edit and can_dag_read permissions for every dag(vm)
        for dag in all_dags_models:
            for perm in self.DAG_PERMS:
                merge_pv(perm, dag.dag_id)

        # for all the dag-level role, add the permission of viewer
        # with the dag view to ab_permission_view
        all_roles = self.get_all_roles()
        user_role = self.find_role('User')

        dag_role = [
            role for role in all_roles if role.name not in EXISTING_ROLES]
        update_perm_views = []

        # need to remove all_dag vm from all the existing view-menus
        dag_vm = self.find_view_menu('all_dags')
        ab_perm_view_role = sqla_models.assoc_permissionview_role
        perm_view = self.permissionview_model
        view_menu = self.viewmenu_model

        all_perm_view_by_user = session.query(ab_perm_view_role) \
            .join(perm_view, perm_view.id == ab_perm_view_role
                  .columns.permission_view_id) \
            .filter(ab_perm_view_role.columns.role_id == user_role.id) \
            .join(view_menu) \
            .filter(perm_view.view_menu_id != dag_vm.id)
        all_perm_views = set(
            [role.permission_view_id for role in all_perm_view_by_user])

        for role in dag_role:
            # Get all the perm-view of the role
            existing_perm_view_by_user = self.get_session.query(ab_perm_view_role) \
                .filter(ab_perm_view_role.columns.role_id == role.id)

            existing_perms_views = set([pv.permission_view_id
                                        for pv in existing_perm_view_by_user])
            missing_perm_views = all_perm_views - existing_perms_views

            for perm_view_id in missing_perm_views:
                update_perm_views.append({'permission_view_id': perm_view_id,
                                          'role_id': role.id})

        if update_perm_views:
            self.get_session.execute(
                ab_perm_view_role.insert(), update_perm_views)
        self.get_session.commit()

    def update_admin_perm_view(self):
        """
        Admin should has all the permission-views, except the dag views.
        because Admin have already have all_dags permission.
        Add the missing ones to the table for admin.

        :return: None.
        """
        all_dag_view = self.find_view_menu('all_dags')
        dag_perm_ids = [self.find_permission(
            'can_dag_edit').id, self.find_permission('can_dag_read').id]
        pvms = self.get_session.query(sqla_models.PermissionView).filter(~and_(
            sqla_models.PermissionView.permission_id.in_(dag_perm_ids),
            sqla_models.PermissionView.view_menu_id != all_dag_view.id)
        ).all()

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
        self.log.info('Start syncing user roles.')
        # Create global all-dag VM
        self.create_perm_vm_for_all_dag()

        # Create default user role.
        for config in self.ROLE_CONFIGS:
            role = config['role']
            vms = config['vms']
            perms = config['perms']
            self.init_role(role, vms, perms)
        self.create_custom_dag_permission_view()

        # init existing roles, the rest role could be created through UI.
        self.update_admin_perm_view()
        self.clean_perms()

    def sync_perm_for_dag(self, dag_id, access_control=None):
        """
        Sync permissions for given dag id. The dag id surely exists in our dag bag
        as only / refresh button or cli.sync_perm will call this function

        :param dag_id: the ID of the DAG whose permissions should be updated
        :type dag_id: str
        :param access_control: a dict where each key is a rolename and
            each value is a set() of permission names (e.g.,
            {'can_dag_read'}
        :type access_control: dict
        :return:
        """
        for dag_perm in self.DAG_PERMS:
            perm_on_dag = self.find_permission_view_menu(dag_perm, dag_id)
            if perm_on_dag is None:
                self.add_permission_view_menu(dag_perm, dag_id)

        if access_control:
            self._sync_dag_view_permissions(dag_id, access_control)

    def _sync_dag_view_permissions(self, dag_id, access_control):
        """Set the access policy on the given DAG's ViewModel.

        :param dag_id: the ID of the DAG whose permissions should be updated
        :type dag_id: str
        :param access_control: a dict where each key is a rolename and
            each value is a set() of permission names (e.g.,
            {'can_dag_read'}
        :type access_control: dict
        """

        def _get_or_create_dag_permission(perm_name):
            dag_perm = self.find_permission_view_menu(perm_name, dag_id)
            if not dag_perm:
                self.log.info("Creating new permission '{}' on view '{}'".format(
                    perm_name,
                    dag_id
                ))
                dag_perm = self.add_permission_view_menu(perm_name, dag_id)

            return dag_perm

        def _revoke_stale_permissions(dag_view):
            existing_dag_perms = self.find_permissions_view_menu(dag_view)
            for perm in existing_dag_perms:
                non_admin_roles = [role for role in perm.role
                                   if role.name != 'Admin']
                for role in non_admin_roles:
                    target_perms_for_role = access_control.get(role.name, {})
                    if perm.permission.name not in target_perms_for_role:
                        self.log.info("Revoking '{}' on DAG '{}' for role '{}'".format(
                            perm.permission,
                            dag_id,
                            role.name
                        ))
                        self.del_permission_role(role, perm)

        dag_view = self.find_view_menu(dag_id)
        if dag_view:
            _revoke_stale_permissions(dag_view)

        for rolename, perms in access_control.items():
            role = self.find_role(rolename)
            if not role:
                raise AirflowException(
                    "The access_control mapping for DAG '{}' includes a role "
                    "named '{}', but that role does not exist".format(
                        dag_id,
                        rolename))

            perms = set(perms)
            invalid_perms = perms - self.DAG_PERMS
            if invalid_perms:
                raise AirflowException(
                    "The access_control map for DAG '{}' includes the following "
                    "invalid permissions: {}; The set of valid permissions "
                    "is: {}".format(dag_id,
                                    (perms - self.DAG_PERMS),
                                    self.DAG_PERMS))

            for perm_name in perms:
                dag_perm = _get_or_create_dag_permission(perm_name)
                self.add_permission_role(role, dag_perm)

    def create_perm_vm_for_all_dag(self):
        """
        Create perm-vm if not exist and insert into FAB security model for all-dags.
        """
        # create perm for global logical dag
        for dag_vm in self.DAG_VMS:
            for perm in self.DAG_PERMS:
                self._merge_perm(permission_name=perm,
                                 view_menu_name=dag_vm)
