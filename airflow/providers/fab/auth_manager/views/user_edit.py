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
    ResetMyPasswordView,
    ResetPasswordView,
    UserInfoEditView,
)

from airflow.security import permissions


class CustomUserInfoEditView(UserInfoEditView):
    """Customize permission names for FAB's builtin UserInfoEditView."""

    class_permission_name = permissions.RESOURCE_MY_PROFILE
    route_base = "/userinfoeditview"
    method_permission_name = {
        "this_form_get": "edit",
        "this_form_post": "edit",
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomResetMyPasswordView(ResetMyPasswordView):
    """Customize permission names for FAB's builtin ResetMyPasswordView."""

    class_permission_name = permissions.RESOURCE_MY_PASSWORD
    method_permission_name = {
        "this_form_get": "read",
        "this_form_post": "edit",
    }
    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]


class CustomResetPasswordView(ResetPasswordView):
    """Customize permission names for FAB's builtin ResetPasswordView."""

    class_permission_name = permissions.RESOURCE_PASSWORD
    method_permission_name = {
        "this_form_get": "read",
        "this_form_post": "edit",
    }

    base_permissions = [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]
