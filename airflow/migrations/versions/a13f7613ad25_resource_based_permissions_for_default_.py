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

"""Resource based permissions for default FAB views.

Revision ID: a13f7613ad25
Revises: e165e7455d70
Create Date: 2021-03-20 21:23:05.793378

"""
import logging

from airflow.security import permissions
from airflow.www.app import create_app

# revision identifiers, used by Alembic.
revision = 'a13f7613ad25'
down_revision = 'e165e7455d70'
branch_labels = None
depends_on = None


mapping = {
    ("PermissionModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PERMISSION),
    ],
    ("PermissionViewModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PERMISSION_VIEW),
    ],
    ("ResetMyPasswordView", "can_this_form_get"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
    ],
    ("ResetMyPasswordView", "can_this_form_post"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PASSWORD),
    ],
    ("ResetPasswordView", "can_this_form_get"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
    ],
    ("ResetPasswordView", "can_this_form_post"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_PASSWORD),
    ],
    ("RoleModelView", "can_delete"): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_download"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_show"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_edit"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_add"): [
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE),
    ],
    ("RoleModelView", "can_copyrole"): [
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE),
    ],
    ("ViewMenuModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "can_add"): [
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "can_userinfo"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserDBModelView", "can_download"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "can_show"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "can_list"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "can_edit"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserDBModelView", "resetmypassword"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
    ],
    ("UserDBModelView", "resetpasswords"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
    ],
    ("UserDBModelView", "userinfoedit"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserDBModelView", "can_delete"): [
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_VIEW_MENU),
    ],
    ("UserInfoEditView", "can_this_form_get"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserInfoEditView", "can_this_form_post"): [
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserStatsChartView", "can_chart"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER_STATS_CHART),
    ],
    ("UserLDAPModelView", "can_userinfo"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserOAuthModelView", "can_userinfo"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserOIDModelView", "can_userinfo"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("UserRemoteUserModelView", "can_userinfo"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
    ],
    ("DagRunModelView", "can_clear"): [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
        (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_TASK_INSTANCE),
    ],
}


def remap_permissions():
    """Apply Map Airflow view permissions."""
    appbuilder = create_app(config={'FAB_UPDATE_PERMS': False}).appbuilder
    for old, new in mapping.items():
        (old_view_name, old_perm_name) = old
        old_pvm = appbuilder.sm.find_permission_view_menu(old_perm_name, old_view_name)
        if not old_pvm:
            continue
        for new_perm_name, new_view_name in new:
            new_pvm = appbuilder.sm.add_permission_view_menu(new_perm_name, new_view_name)
            for role in appbuilder.sm.get_all_roles():
                if appbuilder.sm.exist_permission_on_roles(old_view_name, old_perm_name, [role.id]):
                    appbuilder.sm.add_permission_role(role, new_pvm)
                    appbuilder.sm.del_permission_role(role, old_pvm)
        appbuilder.sm.del_permission_view_menu(old_perm_name, old_view_name)

        if not appbuilder.sm.find_permission(old_perm_name):
            continue
        view_menus = appbuilder.sm.get_all_view_menu()
        if not any(appbuilder.sm.find_permission_view_menu(old_perm_name, view.name) for view in view_menus):
            appbuilder.sm.del_permission(old_perm_name)


def upgrade():
    """Apply Resource based permissions."""
    log = logging.getLogger()
    handlers = log.handlers[:]
    remap_permissions()
    log.handlers = handlers


def downgrade():
    """Unapply Resource based permissions."""
    pass
