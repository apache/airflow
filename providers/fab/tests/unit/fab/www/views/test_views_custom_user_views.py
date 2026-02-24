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

from datetime import datetime, timedelta
from unittest import mock

import pytest
from flask.sessions import SecureCookieSessionInterface
from sqlalchemy import delete, func, select

from airflow.api_fastapi.app import get_auth_manager
from airflow.providers.fab.www import app as application
from airflow.providers.fab.www.security import permissions

from tests_common.test_utils.config import conf_vars
from unit.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_role,
    delete_user,
)
from unit.fab.utils import check_content_in_response, client_with_login

pytestmark = pytest.mark.db_test

PERMISSIONS_TESTS_PARAMS = [
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
    ("/userinfoeditview/form", (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE), "Edit User"),
    ("/users/add", (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER), "Add User"),
    ("/users/list/", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "List Users"),
    ("/users/show/{user.id}", (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER), "Show User"),
    ("/users/edit/{user.id}", (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER), "Edit User"),
]


def delete_roles(app):
    for role_name in ["role_edit_one_dag"]:
        delete_role(app, role_name)


@pytest.fixture
def app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
        app = application.create_app(enable_plugins=False)
        app.config["WTF_CSRF_ENABLED"] = False
        yield app


@pytest.fixture
def client(app):
    return app.test_client()


class TestSecurity:
    @pytest.fixture(autouse=True)
    def app_context(self, app):
        with app.app_context():
            delete_roles(app)
            yield
            delete_user(app, "no_access")
            delete_user(app, "has_access")
            delete_user(app, "test_new_user")

    @pytest.mark.parametrize(("url", "_", "expected_text"), PERMISSIONS_TESTS_PARAMS)
    def test_user_model_view_without_access(self, url, expected_text, _, app, client):
        user_without_access = create_user(
            app,
            username="no_access",
            role_name="role_no_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )
        client = client_with_login(
            app,
            username="no_access",
            password="no_access",
        )
        response = client.get(url.replace("{user.id}", str(user_without_access.id)), follow_redirects=False)
        assert response.status_code == 302
        assert response.location.startswith("/login/")

    @pytest.mark.parametrize(("url", "permission", "expected_text"), PERMISSIONS_TESTS_PARAMS)
    def test_user_model_view_with_access(self, url, permission, expected_text, app, client):
        user_with_access = create_user(
            app,
            username="has_access",
            role_name="role_has_access",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE), permission],
        )

        client = client_with_login(
            app,
            username="has_access",
            password="has_access",
        )
        response = client.get(url.replace("{user.id}", str(user_with_access.id)), follow_redirects=True)
        check_content_in_response(expected_text, response)

    def test_user_model_view_without_delete_access(self, app, client):
        user_to_delete = create_user(
            app,
            username="user_to_delete",
            role_name="user_to_delete",
        )

        create_user(
            app,
            username="no_access",
            role_name="role_no_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
            ],
        )

        client = client_with_login(
            app,
            username="no_access",
            password="no_access",
        )

        response = client.post(f"/users/delete/{user_to_delete.id}", follow_redirects=False)
        assert response.status_code == 302
        assert response.location.startswith("/login/")
        assert bool(get_auth_manager().security_manager.get_user_by_id(user_to_delete.id)) is True

    def test_user_model_view_with_delete_access(self, app, client):
        user_to_delete = create_user(
            app,
            username="user_to_delete",
            role_name="user_to_delete",
        )

        create_user(
            app,
            username="has_access",
            role_name="role_has_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_USER),
            ],
        )

        client = client_with_login(
            app,
            username="has_access",
            password="has_access",
        )

        client.post(f"/users/delete/{user_to_delete.id}", follow_redirects=False)
        assert bool(get_auth_manager().security_manager.get_user_by_id(user_to_delete.id)) is False

    def test_user_creation_without_role_shows_validation_error(self, app, client):
        """Regression test for https://github.com/apache/airflow/issues/59963"""
        create_user(
            app,
            username="has_access",
            role_name="role_has_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
            ],
        )

        client = client_with_login(
            app,
            username="has_access",
            password="has_access",
        )

        response = client.post(
            "/users/add",
            data={
                "first_name": "Test",
                "last_name": "User",
                "username": "test_new_user",
                "email": "test_new_user@example.com",
                "password": "test_password",
                "conf_password": "test_password",
                "active": "y",
            },
            follow_redirects=True,
        )

        assert response.status_code == 200
        check_content_in_response("This field is required", response)


class TestResetUserSessions:
    @pytest.fixture(autouse=True)
    def app_context(self, app):
        self.app = app
        self.session = app.appbuilder.session
        self.security_manager = app.appbuilder.sm
        self.interface = app.session_interface
        self.model = self.interface.sql_session_model
        self.serializer = self.interface.serializer
        with app.app_context():
            self.session.execute(delete(self.model))
            self.session.commit()
            self.session.flush()
            self.user_1 = create_user(
                app,
                username="user_to_delete_1",
                role_name="user_to_delete",
            )
            self.user_2 = create_user(
                app,
                username="user_to_delete_2",
                role_name="user_to_delete",
            )
            self.session.commit()
            self.session.flush()
            yield
            delete_user(app, "user_to_delete_1")
            delete_user(app, "user_to_delete_2")

    def create_user_db_session(self, session_id: str, time_delta: timedelta, user_id: int):
        self.session.add(
            self.model(
                session_id=session_id,
                data=self.serializer.encode({"_user_id": user_id}),
                expiry=datetime.now() + time_delta,
            )
        )

    @pytest.mark.parametrize(
        ("time_delta", "user_sessions_deleted"),
        [
            pytest.param(timedelta(days=-1), True, id="Both expired"),
            pytest.param(timedelta(hours=1), True, id="Both fresh"),
            pytest.param(timedelta(days=1), True, id="Both future"),
        ],
    )
    def test_reset_user_sessions_delete(self, time_delta: timedelta, user_sessions_deleted: bool):
        self.create_user_db_session("session_id_1", time_delta, self.user_1.id)
        self.create_user_db_session("session_id_2", time_delta, self.user_2.id)
        self.session.commit()
        self.session.flush()
        assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

        with self.app.app_context():
            self.security_manager.reset_password(self.user_1.id, "new_password")
        self.session.commit()
        self.session.flush()
        if user_sessions_deleted:
            assert self.session.scalar(select(func.count()).select_from(self.model)) == 1
            assert self.get_session_by_id("session_id_1") is None
        else:
            assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
            assert self.get_session_by_id("session_id_1") is not None

    def get_session_by_id(self, session_id: str):
        return self.session.scalar(select(self.model).where(self.model.session_id == session_id))

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.flash")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.has_request_context", return_value=True
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.MAX_NUM_DATABASE_USER_SESSIONS", 1
    )
    def test_refuse_delete(self, _mock_has_context, flash_mock):
        self.create_user_db_session("session_id_1", timedelta(days=1), self.user_1.id)
        self.create_user_db_session("session_id_2", timedelta(days=1), self.user_2.id)
        self.session.commit()
        self.session.flush()
        assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None
        with self.app.app_context():
            self.security_manager.reset_password(self.user_1.id, "new_password")
        assert flash_mock.called
        assert (
            "The old sessions for user user_to_delete_1 have <b>NOT</b> been deleted!"
            in flash_mock.call_args[0][0]
        )
        assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.flash")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.has_request_context", return_value=True
    )
    def test_warn_securecookie(self, _mock_has_context, flash_mock):
        self.app.session_interface = SecureCookieSessionInterface()
        with self.app.app_context():
            self.security_manager.reset_password(self.user_1.id, "new_password")
        assert flash_mock.called
        assert (
            "Since you are using `securecookie` session backend mechanism, we cannot"
            in flash_mock.call_args[0][0]
        )

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.MAX_NUM_DATABASE_USER_SESSIONS", 1
    )
    def test_refuse_delete_cli(self, log_mock):
        self.create_user_db_session("session_id_1", timedelta(days=1), self.user_1.id)
        self.create_user_db_session("session_id_2", timedelta(days=1), self.user_2.id)
        self.session.commit()
        self.session.flush()
        assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None
        with self.app.app_context():
            self.security_manager.reset_password(self.user_1.id, "new_password")
        assert log_mock.warning.called
        assert (
            "The old sessions for user user_to_delete_1 have *NOT* been deleted!\n"
            in log_mock.warning.call_args[0][0]
        )
        assert self.session.scalar(select(func.count()).select_from(self.model)) == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_warn_securecookie_cli(self, log_mock):
        self.app.session_interface = SecureCookieSessionInterface()
        with self.app.app_context():
            self.security_manager.reset_password(self.user_1.id, "new_password")
        assert log_mock.warning.called
        assert (
            "Since you are using `securecookie` session backend mechanism, we cannot"
            in log_mock.warning.call_args[0][0]
        )
