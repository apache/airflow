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

from unittest import mock

import pytest

from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.providers.fab.auth_manager.models import Role, User
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.www.security_manager import EXISTING_ROLES
from tests.test_utils.api_connexion_utils import create_role, create_user, delete_role, delete_user

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

DEFAULT_TIME = "2020-06-11T18:00:00+00:00"

EXAMPLE_USER_NAME = "example_user"

EXAMPLE_USER_EMAIL = "example_user@example.com"


def _delete_user(**filters):
    with create_session() as session:
        user = session.query(User).filter_by(**filters).first()
        if user is None:
            return
        user.roles = []
        session.delete(user)


@pytest.fixture
def autoclean_user_payload(autoclean_username, autoclean_email):
    return {
        "username": autoclean_username,
        "password": "resutsop",
        "email": autoclean_email,
        "first_name": "Tester",
        "last_name": "",
    }


@pytest.fixture
def autoclean_admin_user(configured_app, autoclean_user_payload):
    security_manager = configured_app.appbuilder.sm
    return security_manager.add_user(
        role=security_manager.find_role("Admin"),
        **autoclean_user_payload,
    )


@pytest.fixture
def autoclean_username():
    _delete_user(username=EXAMPLE_USER_NAME)
    yield EXAMPLE_USER_NAME
    _delete_user(username=EXAMPLE_USER_NAME)


@pytest.fixture
def autoclean_email():
    _delete_user(email=EXAMPLE_USER_EMAIL)
    yield EXAMPLE_USER_EMAIL
    _delete_user(email=EXAMPLE_USER_EMAIL)


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_ROLE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_ACTION),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
        ],
    )

    yield app

    delete_user(app, username="test")  # type: ignore


class TestFABforwarding:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self):
        """
        Delete all roles except these ones.
        Test and TestNoPermissions are deleted by delete_user above
        """
        session = self.app.appbuilder.get_session
        existing_roles = set(EXISTING_ROLES)
        existing_roles.update(["Test", "TestNoPermissions"])
        roles = session.query(Role).filter(~Role.name.in_(existing_roles)).all()
        for role in roles:
            delete_role(self.app, role.name)
        users = session.query(User).filter(User.changed_on == timezone.parse(DEFAULT_TIME))
        users.delete(synchronize_session=False)
        session.commit()


class TestFABRoleForwarding(TestFABforwarding):
    @mock.patch("airflow.api_connexion.endpoints.forward_to_fab_endpoint.get_auth_manager")
    def test_raises_400_if_manager_is_not_fab(self, mock_get_auth_manager):
        mock_get_auth_manager.return_value = BaseAuthManager(self.app.appbuilder)
        response = self.client.get("api/v1/roles", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 400
        assert (
            response.json["detail"]
            == "This endpoint is only available when using the default auth manager FabAuthManager."
        )

    def test_get_role_forwards_to_fab(self):
        resp = self.client.get("api/v1/roles/Test", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200

    def test_get_roles_forwards_to_fab(self):
        resp = self.client.get("api/v1/roles", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200

    def test_delete_role_forwards_to_fab(self):
        role = create_role(self.app, "mytestrole")
        resp = self.client.delete(f"api/v1/roles/{role.name}", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 204

    def test_patch_role_forwards_to_fab(self):
        role = create_role(self.app, "mytestrole")
        resp = self.client.patch(
            f"api/v1/roles/{role.name}", json={"name": "Test2"}, environ_overrides={"REMOTE_USER": "test"}
        )
        assert resp.status_code == 200

    def test_post_role_forwards_to_fab(self):
        payload = {
            "name": "Test2",
            "actions": [{"resource": {"name": "Connections"}, "action": {"name": "can_create"}}],
        }
        resp = self.client.post("api/v1/roles", json=payload, environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200

    def test_get_role_permissions_forwards_to_fab(self):
        resp = self.client.get("api/v1/permissions", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200


class TestFABUserForwarding(TestFABforwarding):
    def _create_users(self, count, roles=None):
        # create users with defined created_on and changed_on date
        # for easy testing
        if roles is None:
            roles = []
        return [
            User(
                first_name=f"test{i}",
                last_name=f"test{i}",
                username=f"TEST_USER{i}",
                email=f"mytest@test{i}.org",
                roles=roles or [],
                created_on=timezone.parse(DEFAULT_TIME),
                changed_on=timezone.parse(DEFAULT_TIME),
            )
            for i in range(1, count + 1)
        ]

    def test_get_user_forwards_to_fab(self):
        users = self._create_users(1)
        session = self.app.appbuilder.get_session
        session.add_all(users)
        session.commit()
        resp = self.client.get("api/v1/users/TEST_USER1", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200

    def test_get_users_forwards_to_fab(self):
        users = self._create_users(2)
        session = self.app.appbuilder.get_session
        session.add_all(users)
        session.commit()
        resp = self.client.get("api/v1/users", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 200

    def test_post_user_forwards_to_fab(self, autoclean_username, autoclean_user_payload):
        response = self.client.post(
            "/api/v1/users",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        security_manager = self.app.appbuilder.sm
        user = security_manager.find_user(autoclean_username)
        assert user is not None
        assert user.roles == [security_manager.find_role("Public")]

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_patch_user_forwards_to_fab(self, autoclean_username, autoclean_user_payload):
        autoclean_user_payload["first_name"] = "Changed"
        response = self.client.patch(
            f"/api/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

    def test_delete_user_forwards_to_fab(self):
        users = self._create_users(1)
        session = self.app.appbuilder.get_session
        session.add_all(users)
        session.commit()
        resp = self.client.delete("api/v1/users/TEST_USER1", environ_overrides={"REMOTE_USER": "test"})
        assert resp.status_code == 204
