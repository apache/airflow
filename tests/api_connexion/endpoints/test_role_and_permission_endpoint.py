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

from flask import current_app
from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.www import app
from airflow.www.security import EXISTING_ROLES
from tests.test_utils.config import conf_vars


class TestRoleEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore


class TestGetRoleEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles/Admin")
        assert response.status_code == 200
        assert response.json['name'] == "Admin"

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/roles/invalid-role")
        assert response.status_code == 404
        assert {
            'detail': "The Role with name `invalid-role` was not found",
            'status': 404,
            'title': 'Role not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestGetRolesEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/roles")
        assert response.status_code == 200
        assert response.json['total_entries'] == len(EXISTING_ROLES)
        roles = {role['name'] for role in response.json['roles']}
        assert roles == EXISTING_ROLES


class TestGetRolesEndpointPaginationandFilter(TestRoleEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/roles?limit=1", ['Admin']),
            ("/api/v1/roles?limit=2", ['Admin', "Viewer"]),
            (
                "/api/v1/roles?offset=1",
                ['Viewer', 'User', 'Op', 'Public', 'Test'],
            ),
            (
                "/api/v1/roles?offset=0",
                ["Admin", 'Viewer', 'User', 'Op', 'Public', 'Test'],
            ),
            ("/api/v1/roles?limit=1&offset=2", ["User"]),
            ("/api/v1/roles?limit=1&offset=1", ["Viewer"]),
            (
                "/api/v1/roles?limit=2&offset=2",
                ["User", "Op"],
            ),
        ]
    )
    def test_can_handle_limit_and_offset(self, url, expected_roles):
        response = self.client.get(url)
        assert response.status_code == 200
        assert response.json["total_entries"] == 5
        roles = [role['name'] for role in response.json['roles'] if role]
        assert roles.sort() == expected_roles.sort()


class TestGetPermissionsEndpoint(TestRoleEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/api/v1/permissions")
        with self.app.app_context():
            sec_manager = current_app.appbuilder.sm
            actions = {i[0] for i in sec_manager.get_all_permissions() if i}
        assert response.status_code == 200
        assert response.json['total_entries'] == len(actions)
        returned_actions = {perm['name'] for perm in response.json['actions']}
        assert actions == returned_actions
