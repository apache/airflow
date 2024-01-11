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

from flask_appbuilder.security.views import (
    PermissionModelView,
    PermissionViewModelView,
    ViewMenuModelView,
)
from flask_babel import lazy_gettext

from airflow.security import permissions


class ActionModelView(PermissionModelView):
    """Customize permission names for FAB's builtin PermissionModelView."""

    class_permission_name = permissions.RESOURCE_ACTION
    route_base = "/actions"
    method_permission_name = {
        "list": "read",
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
        "list": "read",
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


class ResourceModelView(ViewMenuModelView):
    """Customize permission names for FAB's builtin ViewMenuModelView."""

    class_permission_name = permissions.RESOURCE_RESOURCE
    route_base = "/resources"
    method_permission_name = {
        "list": "read",
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
    ]

    list_title = lazy_gettext("List Resources")
    show_title = lazy_gettext("Show Resource")
    add_title = lazy_gettext("Add Resource")
    edit_title = lazy_gettext("Edit Resource")

    label_columns = {"name": lazy_gettext("Name")}
