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

import unittest.mock

import pytest
from sqlalchemy.sql.functions import count
from tests_common.test_utils.api_connexion_utils import assert_401
from tests_common.test_utils.compat import ignore_provider_compatibility_error
from tests_common.test_utils.config import conf_vars

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session
from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_role,
    delete_user,
)

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.models import User


pytestmark = pytest.mark.db_test


DEFAULT_TIME = "2020-06-11T18:00:00+00:00"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")
    delete_role(app, name="TestNoPermissions")


class TestUserEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.session = self.app.appbuilder.get_session

    def teardown_method(self) -> None:
        # Delete users that have our custom default time
        users = self.session.query(User).filter(User.changed_on == timezone.parse(DEFAULT_TIME))
        users.delete(synchronize_session=False)
        self.session.commit()

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
                active=True,
            )
            for i in range(1, count + 1)
        ]


class TestGetUser(TestUserEndpoint):
    def test_should_respond_200(self):
        users = self._create_users(1)
        self.session.add_all(users)
        self.session.commit()
        response = self.client.get("/auth/fab/v1/users/TEST_USER1", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "active": True,
            "changed_on": DEFAULT_TIME,
            "created_on": DEFAULT_TIME,
            "email": "mytest@test1.org",
            "fail_login_count": None,
            "first_name": "test1",
            "last_login": None,
            "last_name": "test1",
            "login_count": None,
            "roles": [],
            "username": "TEST_USER1",
        }

    def test_last_names_can_be_empty(self):
        prince = User(
            first_name="Prince",
            last_name="",
            username="prince",
            email="prince@example.org",
            roles=[],
            created_on=timezone.parse(DEFAULT_TIME),
            changed_on=timezone.parse(DEFAULT_TIME),
        )
        self.session.add_all([prince])
        self.session.commit()
        response = self.client.get("/auth/fab/v1/users/prince", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "active": True,
            "changed_on": DEFAULT_TIME,
            "created_on": DEFAULT_TIME,
            "email": "prince@example.org",
            "fail_login_count": None,
            "first_name": "Prince",
            "last_login": None,
            "last_name": "",
            "login_count": None,
            "roles": [],
            "username": "prince",
        }

    def test_first_names_can_be_empty(self):
        liberace = User(
            first_name="",
            last_name="Liberace",
            username="liberace",
            email="liberace@example.org",
            roles=[],
            created_on=timezone.parse(DEFAULT_TIME),
            changed_on=timezone.parse(DEFAULT_TIME),
        )
        self.session.add_all([liberace])
        self.session.commit()
        response = self.client.get("/auth/fab/v1/users/liberace", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "active": True,
            "changed_on": DEFAULT_TIME,
            "created_on": DEFAULT_TIME,
            "email": "liberace@example.org",
            "fail_login_count": None,
            "first_name": "",
            "last_login": None,
            "last_name": "Liberace",
            "login_count": None,
            "roles": [],
            "username": "liberace",
        }

    def test_both_first_and_last_names_can_be_empty(self):
        nameless = User(
            first_name="",
            last_name="",
            username="nameless",
            email="nameless@example.org",
            roles=[],
            created_on=timezone.parse(DEFAULT_TIME),
            changed_on=timezone.parse(DEFAULT_TIME),
        )
        self.session.add_all([nameless])
        self.session.commit()
        response = self.client.get("/auth/fab/v1/users/nameless", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json == {
            "active": True,
            "changed_on": DEFAULT_TIME,
            "created_on": DEFAULT_TIME,
            "email": "nameless@example.org",
            "fail_login_count": None,
            "first_name": "",
            "last_login": None,
            "last_name": "",
            "login_count": None,
            "roles": [],
            "username": "nameless",
        }

    def test_should_respond_404(self):
        response = self.client.get(
            "/auth/fab/v1/users/invalid-user", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404
        assert {
            "detail": "The User with username `invalid-user` was not found",
            "status": 404,
            "title": "User not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/auth/fab/v1/users/TEST_USER1")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/auth/fab/v1/users/TEST_USER1", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetUsers(TestUserEndpoint):
    def test_should_response_200(self):
        response = self.client.get("/auth/fab/v1/users", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        usernames = [user["username"] for user in response.json["users"] if user]
        assert usernames == ["test", "test_no_permissions"]

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/auth/fab/v1/users")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/auth/fab/v1/users", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetUsersPagination(TestUserEndpoint):
    @pytest.mark.parametrize(
        "url, expected_usernames",
        [
            ("/auth/fab/v1/users?limit=1", ["test"]),
            ("/auth/fab/v1/users?limit=2", ["test", "test_no_permissions"]),
            (
                "/auth/fab/v1/users?offset=5",
                [
                    "TEST_USER4",
                    "TEST_USER5",
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            (
                "/auth/fab/v1/users?offset=0",
                [
                    "test",
                    "test_no_permissions",
                    "TEST_USER1",
                    "TEST_USER2",
                    "TEST_USER3",
                    "TEST_USER4",
                    "TEST_USER5",
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            ("/auth/fab/v1/users?limit=1&offset=5", ["TEST_USER4"]),
            ("/auth/fab/v1/users?limit=1&offset=1", ["test_no_permissions"]),
            (
                "/auth/fab/v1/users?limit=2&offset=2",
                ["TEST_USER1", "TEST_USER2"],
            ),
        ],
    )
    def test_handle_limit_offset(self, url, expected_usernames):
        users = self._create_users(10)
        self.session.add_all(users)
        self.session.commit()
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 12
        usernames = [user["username"] for user in response.json["users"] if user]
        assert usernames == expected_usernames

    def test_should_respect_page_size_limit_default(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/auth/fab/v1/users", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        # Explicitly add the 2 users on setUp
        assert response.json["total_entries"] == 200 + len(["test", "test_no_permissions"])
        assert len(response.json["users"]) == 100

    def test_should_response_400_with_invalid_order_by(self):
        users = self._create_users(2)
        self.session.add_all(users)
        self.session.commit()
        response = self.client.get(
            "/auth/fab/v1/users?order_by=myname", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'myname' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_limit_of_zero_should_return_default(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/auth/fab/v1/users?limit=0", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        # Explicit add the 2 users on setUp
        assert response.json["total_entries"] == 200 + len(["test", "test_no_permissions"])
        assert len(response.json["users"]) == 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self):
        users = self._create_users(200)
        self.session.add_all(users)
        self.session.commit()

        response = self.client.get("/auth/fab/v1/users?limit=180", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert len(response.json["users"]) == 150


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
def autoclean_username():
    _delete_user(username=EXAMPLE_USER_NAME)
    yield EXAMPLE_USER_NAME
    _delete_user(username=EXAMPLE_USER_NAME)


@pytest.fixture
def autoclean_email():
    _delete_user(email=EXAMPLE_USER_EMAIL)
    yield EXAMPLE_USER_EMAIL
    _delete_user(email=EXAMPLE_USER_EMAIL)


@pytest.fixture
def user_with_same_username(configured_app, autoclean_username):
    user = create_user(
        configured_app,
        username=autoclean_username,
        email="another_user@example.com",
        role_name="TestNoPermissions",
    )
    assert user, f"failed to create user '{autoclean_username} <another_user@example.com>'"
    return user


@pytest.fixture
def user_with_same_email(configured_app, autoclean_email):
    user = create_user(
        configured_app,
        username="another_user",
        email=autoclean_email,
        role_name="TestNoPermissions",
    )
    assert user, f"failed to create user 'another_user <{autoclean_email}>'"
    return user


@pytest.fixture
def user_different(configured_app):
    username = "another_user"
    email = "another_user@example.com"

    _delete_user(username=username, email=email)
    user = create_user(configured_app, username=username, email=email, role_name="TestNoPermissions")
    assert user, "failed to create user 'another_user <another_user@example.com>'"
    yield user
    _delete_user(username=username, email=email)


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


class TestPostUser(TestUserEndpoint):
    def test_with_default_role(self, autoclean_username, autoclean_user_payload):
        self.client.application.config["AUTH_USER_REGISTRATION_ROLE"] = "Public"
        response = self.client.post(
            "/auth/fab/v1/users",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        security_manager = self.app.appbuilder.sm
        user = security_manager.find_user(autoclean_username)
        assert user is not None
        assert user.roles == [security_manager.find_role("Public")]

    def test_with_custom_roles(self, autoclean_username, autoclean_user_payload):
        response = self.client.post(
            "/auth/fab/v1/users",
            json={"roles": [{"name": "User"}, {"name": "Viewer"}], **autoclean_user_payload},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        security_manager = self.app.appbuilder.sm
        user = security_manager.find_user(autoclean_username)
        assert user is not None
        assert {r.name for r in user.roles} == {"User", "Viewer"}

    @pytest.mark.usefixtures("user_different")
    def test_with_existing_different_user(self, autoclean_user_payload):
        response = self.client.post(
            "/auth/fab/v1/users",
            json={"roles": [{"name": "User"}, {"name": "Viewer"}], **autoclean_user_payload},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

    def test_unauthenticated(self, autoclean_user_payload):
        response = self.client.post(
            "/auth/fab/v1/users",
            json=autoclean_user_payload,
        )
        assert response.status_code == 401, response.json

    def test_forbidden(self, autoclean_user_payload):
        response = self.client.post(
            "/auth/fab/v1/users",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403, response.json

    @pytest.mark.parametrize(
        "existing_user_fixture_name, error_detail_template",
        [
            ("user_with_same_username", "Username `{username}` already exists. Use PATCH to update."),
            ("user_with_same_email", "The email `{email}` is already taken."),
        ],
        ids=["username", "email"],
    )
    def test_already_exists(
        self,
        request,
        autoclean_user_payload,
        existing_user_fixture_name,
        error_detail_template,
    ):
        existing = request.getfixturevalue(existing_user_fixture_name)

        response = self.client.post(
            "/auth/fab/v1/users",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 409, response.json

        error_detail = error_detail_template.format(username=existing.username, email=existing.email)
        assert response.json["detail"] == error_detail

    @pytest.mark.parametrize(
        "payload_converter, error_message",
        [
            pytest.param(
                lambda p: {k: v for k, v in p.items() if k != "username"},
                "{'username': ['Missing data for required field.']}",
                id="missing-required",
            ),
            pytest.param(
                lambda p: {"i-am": "a typo", **p},
                "{'i-am': ['Unknown field.']}",
                id="unknown-user-field",
            ),
            pytest.param(
                lambda p: {**p, "roles": [{"also": "a typo", "name": "User"}]},
                "{'roles': {0: {'also': ['Unknown field.']}}}",
                id="unknown-role-field",
            ),
            pytest.param(
                lambda p: {**p, "roles": [{"name": "God"}, {"name": "User"}, {"name": "Overlord"}]},
                "Unknown roles: 'God', 'Overlord'",
                id="unknown-role",
            ),
        ],
    )
    def test_invalid_payload(self, autoclean_user_payload, payload_converter, error_message):
        response = self.client.post(
            "/auth/fab/v1/users",
            json=payload_converter(autoclean_user_payload),
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, response.json
        assert response.json == {
            "detail": error_message,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_internal_server_error(self, autoclean_user_payload):
        with unittest.mock.patch.object(self.app.appbuilder.sm, "add_user", return_value=None):
            response = self.client.post(
                "/auth/fab/v1/users",
                json=autoclean_user_payload,
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.json == {
                "detail": "Failed to add user `example_user`.",
                "status": 500,
                "title": "Internal Server Error",
                "type": EXCEPTIONS_LINK_MAP[500],
            }


class TestPatchUser(TestUserEndpoint):
    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_change(self, autoclean_username, autoclean_user_payload):
        autoclean_user_payload["first_name"] = "Changed"
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        # The first name is changed.
        data = response.json
        assert data["first_name"] == "Changed"
        assert data["last_name"] == ""

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_change_with_update_mask(self, autoclean_username, autoclean_user_payload):
        autoclean_user_payload["first_name"] = "Changed"
        autoclean_user_payload["last_name"] = "McTesterson"
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}?update_mask=last_name",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        # The first name is changed, but the last name isn't since we masked it.
        data = response.json
        assert data["first_name"] == "Tester"
        assert data["last_name"] == "McTesterson"

    @pytest.mark.parametrize(
        "payload, error_message",
        [
            ({"username": "another_user"}, "The username `another_user` already exists"),
            ({"email": "another_user@example.com"}, "The email `another_user@example.com` already exists"),
        ],
        ids=["username", "email"],
    )
    @pytest.mark.usefixtures("user_different")
    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_patch_already_exists(
        self,
        payload,
        error_message,
        autoclean_user_payload,
        autoclean_username,
    ):
        autoclean_user_payload.update(payload)
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 409, response.json

        assert response.json["detail"] == error_message

    @pytest.mark.parametrize(
        "field",
        ["username", "first_name", "last_name", "email"],
    )
    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_required_fields(
        self,
        field,
        autoclean_user_payload,
        autoclean_username,
    ):
        autoclean_user_payload.pop(field)
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, response.json
        assert response.json["detail"] == f"{{'{field}': ['Missing data for required field.']}}"

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_username_can_be_updated(self, autoclean_user_payload, autoclean_username):
        testusername = "testusername"
        autoclean_user_payload.update({"username": testusername})
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        _delete_user(username=testusername)
        assert response.json["username"] == testusername

    @pytest.mark.usefixtures("autoclean_admin_user")
    @unittest.mock.patch(
        "airflow.providers.fab.auth_manager.api_endpoints.user_endpoint.generate_password_hash",
        return_value="fake-hashed-pass",
    )
    def test_password_hashed(
        self,
        mock_generate_password_hash,
        autoclean_username,
        autoclean_user_payload,
    ):
        autoclean_user_payload["password"] = "new-pass"
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json
        assert "password" not in response.json

        mock_generate_password_hash.assert_called_once_with("new-pass")

        password_in_db = (
            self.session.query(User.password).filter(User.username == autoclean_username).scalar()
        )
        assert password_in_db == "fake-hashed-pass"

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_replace_roles(self, autoclean_username, autoclean_user_payload):
        # Patching a user's roles should replace the entire list.
        autoclean_user_payload["roles"] = [{"name": "User"}, {"name": "Viewer"}]
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}?update_mask=roles",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json
        assert {d["name"] for d in response.json["roles"]} == {"User", "Viewer"}

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_unchanged(self, autoclean_username, autoclean_user_payload):
        # Should allow a PATCH that changes nothing.
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.json

        expected = {k: v for k, v in autoclean_user_payload.items() if k != "password"}
        assert {k: response.json[k] for k in expected} == expected

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_unauthenticated(self, autoclean_username, autoclean_user_payload):
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
        )
        assert response.status_code == 401, response.json

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_forbidden(self, autoclean_username, autoclean_user_payload):
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403, response.json

    def test_not_found(self, autoclean_username, autoclean_user_payload):
        # This test does not populate autoclean_admin_user into the database.
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=autoclean_user_payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404, response.json

    @pytest.mark.parametrize(
        "payload_converter, error_message",
        [
            pytest.param(
                lambda p: {k: v for k, v in p.items() if k != "username"},
                "{'username': ['Missing data for required field.']}",
                id="missing-required",
            ),
            pytest.param(
                lambda p: {"i-am": "a typo", **p},
                "{'i-am': ['Unknown field.']}",
                id="unknown-user-field",
            ),
            pytest.param(
                lambda p: {**p, "roles": [{"also": "a typo", "name": "User"}]},
                "{'roles': {0: {'also': ['Unknown field.']}}}",
                id="unknown-role-field",
            ),
            pytest.param(
                lambda p: {**p, "roles": [{"name": "God"}, {"name": "User"}, {"name": "Overlord"}]},
                "Unknown roles: 'God', 'Overlord'",
                id="unknown-role",
            ),
        ],
    )
    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_invalid_payload(
        self,
        autoclean_username,
        autoclean_user_payload,
        payload_converter,
        error_message,
    ):
        response = self.client.patch(
            f"/auth/fab/v1/users/{autoclean_username}",
            json=payload_converter(autoclean_user_payload),
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, response.json
        assert response.json == {
            "detail": error_message,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }


class TestDeleteUser(TestUserEndpoint):
    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_delete(self, autoclean_username):
        response = self.client.delete(
            f"/auth/fab/v1/users/{autoclean_username}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 204, response.json  # NO CONTENT.
        assert self.session.query(count(User.id)).filter(User.username == autoclean_username).scalar() == 0

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_unauthenticated(self, autoclean_username):
        response = self.client.delete(
            f"/auth/fab/v1/users/{autoclean_username}",
        )
        assert response.status_code == 401, response.json
        assert self.session.query(count(User.id)).filter(User.username == autoclean_username).scalar() == 1

    @pytest.mark.usefixtures("autoclean_admin_user")
    def test_forbidden(self, autoclean_username):
        response = self.client.delete(
            f"/auth/fab/v1/users/{autoclean_username}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403, response.json
        assert self.session.query(count(User.id)).filter(User.username == autoclean_username).scalar() == 1

    def test_not_found(self, autoclean_username):
        # This test does not populate autoclean_admin_user into the database.
        response = self.client.delete(
            f"/auth/fab/v1/users/{autoclean_username}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404, response.json
