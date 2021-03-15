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

import unittest

from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.security import permissions
from airflow.www import app
from airflow.www.security import EXISTING_ROLES
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


class TestRoleEndpoint(unittest.TestCase):
    @classmethod
    @dont_initialize_flask_app_submodules(
        skip_all_except=["init_appbuilder", "init_api_experimental_auth", "init_api_connexion"]
    )
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        cls.security_manager = cls.appbuilder.sm  # type:ignore
        create_user(
            cls.app,  # type: ignore
            username="test",
            role_name="Test",
            permissions=[
                (permissions.ACTION_CAN_LIST, permissions.RESOURCE_ROLE_MODEL_VIEW),
                (permissions.ACTION_CAN_SHOW, permissions.RESOURCE_ROLE_MODEL_VIEW),
                (permissions.ACTION_CAN_LIST, permissions.RESOURCE_PERMISSION_MODEL_VIEW),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore


class TestGetRoleEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles/Admin", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json['name'] == "Admin"

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/roles/invalid-role", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404
        assert {
            'detail': "The Role with name `invalid-role` was not found",
            'status': 404,
            'title': 'Role not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/roles/Admin")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/roles/Admin", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetRolesEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        EXISTING_ROLES.update(['Test', 'TestNoPermissions'])
        assert response.json['total_entries'] == len(EXISTING_ROLES)
        roles = {role['name'] for role in response.json['roles']}
        assert roles == EXISTING_ROLES

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/roles")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get("/api/v1/roles", environ_overrides={'REMOTE_USER': "test_no_permissions"})
        assert response.status_code == 403


class TestGetRolesEndpointPaginationandFilter(TestRoleEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/roles?limit=1", ['Admin']),
            ("/api/v1/roles?limit=2", ['Admin', "Op"]),
            (
                "/api/v1/roles?offset=1",
                ['Op', 'Public', 'Test', 'TestNoPermissions', 'User', 'Viewer'],
            ),
            (
                "/api/v1/roles?offset=0",
                ['Admin', 'Op', 'Public', 'Test', 'TestNoPermissions', 'User', 'Viewer'],
            ),
            ("/api/v1/roles?limit=1&offset=2", ["Public"]),
            ("/api/v1/roles?limit=1&offset=1", ["Op"]),
            (
                "/api/v1/roles?limit=2&offset=2",
                ['Public', 'Test'],
            ),
        ]
    )
    def test_can_handle_limit_and_offset(self, url, expected_roles):
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        EXISTING_ROLES.update(['Test', 'TestNoPermissions'])
        assert response.json["total_entries"] == len(EXISTING_ROLES)
        roles = [role['name'] for role in response.json['roles'] if role]
        assert roles == expected_roles


class TestGetPermissionsEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/permissions", environ_overrides={'REMOTE_USER': "test"})
        actions = {i[0] for i in self.security_manager.get_all_permissions() if i}
        assert response.status_code == 200
        assert response.json['total_entries'] == len(actions)
        returned_actions = {perm['name'] for perm in response.json['actions']}
        assert actions == returned_actions

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/permissions")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/permissions", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403
