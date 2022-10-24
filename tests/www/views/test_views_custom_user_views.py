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
from __future__ import annotations

import unittest

from flask_appbuilder import SQLA
from parameterized import parameterized

from airflow import settings
from airflow.security import permissions
from airflow.www import app as application
from tests.test_utils.api_connexion_utils import create_user, delete_role
from tests.test_utils.www import check_content_in_response, check_content_not_in_response, client_with_login


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session

    def setUp(self):
        # We cannot reuse the app in tests (on class level) as in Flask 2.2 this causes
        # an exception because app context teardown is removed and if even single request is run via app
        # it cannot be re-intialized again by passing it as constructor to SQLA
        # This makes the tests slightly slower (but they work with Flask 2.1 and 2.2
        self.app = application.create_app(testing=True)
        self.appbuilder = self.app.appbuilder
        self.app.config["WTF_CSRF_ENABLED"] = False
        self.security_manager = self.appbuilder.sm
        self.delete_roles()
        self.db = SQLA(self.app)

        self.client = self.app.test_client()  # type:ignore

    def delete_roles(self):
        for role_name in ["role_edit_one_dag"]:
            delete_role(self.app, role_name)

    @parameterized.expand(
        [
            (
                "/resetpassword/form?pk={user.id}",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
                "Reset Password Form",
            ),
            (
                "/resetmypassword/form",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
                "Reset Password Form",
            ),
            (
                "/users/userinfo/",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
                "Your user information",
            ),
            (
                "/userinfoeditview/form",
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
                "Edit User",
            ),
            ("/users/add", (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER), "Add User"),
            ("/users/list/", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "List Users"),
            ("/users/show/{user.id}", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "Show User"),
            ("/users/edit/{user.id}", (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER), "Edit User"),
        ]
    )
    def test_user_model_view_with_access(self, url, permission, expected_text):
        user_without_access = create_user(
            self.app,
            username="no_access",
            role_name="role_no_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        client = client_with_login(
            self.app,
            username="no_access",
            password="no_access",
        )
        response = client.get(url.replace("{user.id}", str(user_without_access.id)), follow_redirects=True)
        check_content_not_in_response(expected_text, response)

    @parameterized.expand(
        [
            (
                "/resetpassword/form?pk={user.id}",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_PASSWORD),
                "Reset Password Form",
            ),
            (
                "/resetmypassword/form",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PASSWORD),
                "Reset Password Form",
            ),
            (
                "/users/userinfo/",
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_MY_PROFILE),
                "Your user information",
            ),
            (
                "/userinfoeditview/form",
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
                "Edit User",
            ),
            ("/users/add", (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER), "Add User"),
            ("/users/list/", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "List Users"),
            ("/users/show/{user.id}", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "Show User"),
            ("/users/edit/{user.id}", (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER), "Edit User"),
        ]
    )
    def test_user_model_view_without_access(self, url, permission, expected_text):

        user_with_access = create_user(
            self.app,
            username="has_access",
            role_name="role_has_access",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE), permission],
        )

        client = client_with_login(
            self.app,
            username="has_access",
            password="has_access",
        )
        response = client.get(url.replace("{user.id}", str(user_with_access.id)), follow_redirects=True)
        check_content_in_response(expected_text, response)

    def test_user_model_view_without_delete_access(self):

        user_to_delete = create_user(
            self.app,
            username="user_to_delete",
            role_name="user_to_delete",
        )

        create_user(
            self.app,
            username="no_access",
            role_name="role_no_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        client = client_with_login(
            self.app,
            username="no_access",
            password="no_access",
        )

        response = client.post(f"/users/delete/{user_to_delete.id}", follow_redirects=True)

        check_content_not_in_response("Deleted Row", response)
        assert bool(self.security_manager.get_user_by_id(user_to_delete.id)) is True

    def test_user_model_view_with_delete_access(self):

        user_to_delete = create_user(
            self.app,
            username="user_to_delete",
            role_name="user_to_delete",
        )

        create_user(
            self.app,
            username="has_access",
            role_name="role_has_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
            ],
        )

        client = client_with_login(
            self.app,
            username="has_access",
            password="has_access",
        )

        response = client.post(f"/users/delete/{user_to_delete.id}", follow_redirects=True)
        check_content_in_response("Deleted Row", response)
        check_content_not_in_response(user_to_delete.username, response)
        assert bool(self.security_manager.get_user_by_id(user_to_delete.id)) is False
