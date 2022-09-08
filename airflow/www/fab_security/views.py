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

from flask import request
from flask_appbuilder import expose
from flask_appbuilder.security.decorators import has_access
from flask_appbuilder.security.views import (
    PermissionModelView,
    PermissionViewModelView,
    ResetMyPasswordView,
    ResetPasswordView,
    RoleModelView,
    UserDBModelView,
    UserInfoEditView,
    UserLDAPModelView,
    UserOAuthModelView,
    UserOIDModelView,
    UserRemoteUserModelView,
    UserStatsChartView,
    ViewMenuModelView,
)
from flask_babel import lazy_gettext

from airflow.security import permissions


class ActionModelView(PermissionModelView):
    """Customize permission names for FAB's builtin PermissionModelView."""

    class_permission_name = permissions.RESOURCE_ACTION
    route_base = "/actions"
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]

    list_title = lazy_gettext("List Actions")
    show_title = lazy_gettext("Show Action")
    add_title = lazy_gettext("Add Action")
    edit_title = lazy_gettext("Edit Action")

    label_columns = {"name": lazy_gettext("Name")}


class PermissionPairModelView(PermissionViewModelView):
    """Customize permission names for FAB's builtin PermissionViewModelView."""

    class_permission_name = permissions.RESOURCE_PERMISSION
    route_base = "/permissions"
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]

    list_title = lazy_gettext("List Permissions")
    show_title = lazy_gettext("Show Permission")
    add_title = lazy_gettext("Add Permission")
    edit_title = lazy_gettext("Edit Permission")

    label_columns = {
        "action": lazy_gettext("Action"),
        "resource": lazy_gettext("Resource"),
    }
    list_columns = ["action", "resource"]


class CustomResetMyPasswordView(ResetMyPasswordView):
    """Customize permission names for FAB's builtin ResetMyPasswordView."""

    class_permission_name = permissions.RESOURCE_MY_PASSWORD
    method_permission_name = {
        'this_form_get': 'read',
        'this_form_post': 'edit',
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomResetPasswordView(ResetPasswordView):
    """Customize permission names for FAB's builtin ResetPasswordView."""

    class_permission_name = permissions.RESOURCE_PASSWORD
    method_permission_name = {
        'this_form_get': 'read',
        'this_form_post': 'edit',
    }

    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomRoleModelView(RoleModelView):
    """Customize permission names for FAB's builtin RoleModelView."""

    class_permission_name = permissions.RESOURCE_ROLE
    method_permission_name = {
        'delete': 'delete',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'add': 'create',
        'copy_role': 'create',
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class ResourceModelView(ViewMenuModelView):
    """Customize permission names for FAB's builtin ViewMenuModelView."""

    class_permission_name = permissions.RESOURCE_RESOURCE
    route_base = "/resources"
    method_permission_name = {
        'list': 'read',
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]

    list_title = lazy_gettext("List Resources")
    show_title = lazy_gettext("Show Resource")
    add_title = lazy_gettext("Add Resource")
    edit_title = lazy_gettext("Edit Resource")

    label_columns = {"name": lazy_gettext("Name")}


class CustomUserInfoEditView(UserInfoEditView):
    """Customize permission names for FAB's builtin UserInfoEditView."""

    class_permission_name = permissions.RESOURCE_MY_PROFILE
    route_base = "/userinfoeditview"
    method_permission_name = {
        'this_form_get': 'edit',
        'this_form_post': 'edit',
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomUserStatsChartView(UserStatsChartView):
    """Customize permission names for FAB's builtin UserStatsChartView."""

    class_permission_name = permissions.RESOURCE_USER_STATS_CHART
    route_base = "/userstatschartview"
    method_permission_name = {
        'chart': 'read',
        'list': 'read',
    }
    base_permissions = [permissions.ACTION_CAN_READ]


class MultiResourceUserMixin:
    """Remaps UserModelView permissions to new resources and actions."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'userinfo': 'read',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'userinfoedit': 'edit',
        'delete': 'delete',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]

    @property
    def class_permission_name(self):
        """Returns appropriate permission name depending on request method name."""
        if request:
            action_name = request.view_args.get("name")
            _, method_name = request.url_rule.endpoint.rsplit(".", 1)
            if method_name == 'action' and action_name:
                return self.class_permission_name_mapping.get(action_name, self._class_permission_name)
            if method_name:
                return self.class_permission_name_mapping.get(method_name, self._class_permission_name)
        return self._class_permission_name

    @class_permission_name.setter
    def class_permission_name(self, name):
        self._class_permission_name = name

    @expose("/show/<pk>", methods=["GET"])
    @has_access
    def show(self, pk):
        pk = self._deserialize_pk_if_composite(pk)
        widgets = self._show(pk)
        widgets['show'].template_args['actions'].pop('userinfoedit')
        return self.render_template(
            self.show_template,
            pk=pk,
            title=self.show_title,
            widgets=widgets,
            related_views=self._related_views,
        )


class CustomUserDBModelView(MultiResourceUserMixin, UserDBModelView):
    """Customize permission names for FAB's builtin UserDBModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'resetmypassword': permissions.RESOURCE_MY_PASSWORD,
        'resetpasswords': permissions.RESOURCE_PASSWORD,
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'add': 'create',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'delete': 'delete',
        'resetmypassword': 'read',
        'resetpasswords': 'read',
        'userinfo': 'read',
        'userinfoedit': 'read',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class CustomUserLDAPModelView(MultiResourceUserMixin, UserLDAPModelView):
    """Customize permission names for FAB's builtin UserLDAPModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'add': 'create',
        'userinfo': 'read',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'userinfoedit': 'edit',
        'delete': 'delete',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class CustomUserOAuthModelView(MultiResourceUserMixin, UserOAuthModelView):
    """Customize permission names for FAB's builtin UserOAuthModelView."""


class CustomUserOIDModelView(MultiResourceUserMixin, UserOIDModelView):
    """Customize permission names for FAB's builtin UserOIDModelView."""


class CustomUserRemoteUserModelView(MultiResourceUserMixin, UserRemoteUserModelView):
    """Customize permission names for FAB's builtin UserRemoteUserModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        'userinfoedit': permissions.RESOURCE_MY_PROFILE,
        'userinfo': permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        'add': 'create',
        'userinfo': 'read',
        'download': 'read',
        'show': 'read',
        'list': 'read',
        'edit': 'edit',
        'userinfoedit': 'edit',
        'delete': 'delete',
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]
