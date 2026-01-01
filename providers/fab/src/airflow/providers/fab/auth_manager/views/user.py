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

from flask import request
from flask_appbuilder import expose
from flask_appbuilder.security.decorators import has_access
from flask_appbuilder.security.views import (
    UserDBModelView,
    UserLDAPModelView,
    UserOAuthModelView,
    UserRemoteUserModelView,
)
from wtforms.validators import DataRequired

from airflow.providers.fab.www.security import permissions


class MultiResourceUserMixin:
    """Remaps UserModelView permissions to new resources and actions."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        "userinfoedit": permissions.RESOURCE_MY_PROFILE,
        "userinfo": permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        "userinfo": "read",
        "download": "read",
        "show": "read",
        "list": "read",
        "edit": "edit",
        "userinfoedit": "edit",
        "delete": "delete",
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
            if method_name == "action" and action_name:
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
        widgets["show"].template_args["actions"].pop("userinfoedit", None)
        widgets["show"].template_args["actions"].pop("resetmypassword", None)
        return self.render_template(
            self.show_template,
            pk=pk,
            title=self.show_title,
            widgets=widgets,
            related_views=self._related_views,
        )


class CustomUserLDAPModelView(MultiResourceUserMixin, UserLDAPModelView):
    """Customize permission names for FAB's builtin UserLDAPModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        "userinfoedit": permissions.RESOURCE_MY_PROFILE,
        "userinfo": permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        "add": "create",
        "userinfo": "read",
        "download": "read",
        "show": "read",
        "list": "read",
        "edit": "edit",
        "userinfoedit": "edit",
        "delete": "delete",
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class CustomUserOAuthModelView(MultiResourceUserMixin, UserOAuthModelView):
    """Customize permission names for FAB's builtin UserOAuthModelView."""


class CustomUserRemoteUserModelView(MultiResourceUserMixin, UserRemoteUserModelView):
    """Customize permission names for FAB's builtin UserRemoteUserModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        "userinfoedit": permissions.RESOURCE_MY_PROFILE,
        "userinfo": permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        "add": "create",
        "userinfo": "read",
        "download": "read",
        "show": "read",
        "list": "read",
        "edit": "edit",
        "userinfoedit": "edit",
        "delete": "delete",
    }

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]


class CustomUserDBModelView(MultiResourceUserMixin, UserDBModelView):
    """Customize permission names for FAB's builtin UserDBModelView."""

    _class_permission_name = permissions.RESOURCE_USER

    class_permission_name_mapping = {
        "resetmypassword": permissions.RESOURCE_MY_PASSWORD,
        "resetpasswords": permissions.RESOURCE_PASSWORD,
        "userinfoedit": permissions.RESOURCE_MY_PROFILE,
        "userinfo": permissions.RESOURCE_MY_PROFILE,
    }

    method_permission_name = {
        "add": "create",
        "download": "read",
        "show": "read",
        "list": "read",
        "edit": "edit",
        "delete": "delete",
        "resetmypassword": "read",
        "resetpasswords": "read",
        "userinfo": "read",
        "userinfoedit": "read",
    }

    add_columns = [
        "first_name",
        "last_name",
        "username",
        "active",
        "email",
        "roles",
        "password",
        "conf_password",
    ]

    validators_columns = {"roles": [DataRequired()]}

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_DELETE,
    ]
