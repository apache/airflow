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
from flask_appbuilder import SQLA

from airflow import settings
from airflow.security import permissions
from airflow.www import app as application

from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_role,
)
from tests_common.test_utils.www import (
    check_content_in_response,
    check_content_not_in_response,
    client_with_login,
)

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
    (
        "/userinfoeditview/form",
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_MY_PROFILE),
        "Edit User",
    ),
    (
        "/users/add",
        (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_USER),
        "Add User",
    ),
    (
        "/users/list/",
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
        "List Users",
    ),
    (
        "/users/show/{user.id}",
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_USER),
        "Show User",
    ),
    (
        "/users/edit/{user.id}",
        (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_USER),
        "Edit User",
    ),
]


class TestSecurity:
    @classmethod
    def setup_class(cls):
        settings.configure_orm()
        cls.session = settings.Session

    def setup_method(self):
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

    @pytest.mark.parametrize("url, _, expected_text", PERMISSIONS_TESTS_PARAMS)
    def test_user_model_view_with_access(self, url, expected_text, _):
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
        response = client.get(
            url.replace("{user.id}", str(user_without_access.id)), follow_redirects=True
        )
        check_content_not_in_response(expected_text, response)

    @pytest.mark.parametrize("url, permission, expected_text", PERMISSIONS_TESTS_PARAMS)
    def test_user_model_view_without_access(self, url, permission, expected_text):
        user_with_access = create_user(
            self.app,
            username="has_access",
            role_name="role_has_access",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
                permission,
            ],
        )

        client = client_with_login(
            self.app,
            username="has_access",
            password="has_access",
        )
        response = client.get(
            url.replace("{user.id}", str(user_with_access.id)), follow_redirects=True
        )
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

        response = client.post(
            f"/users/delete/{user_to_delete.id}", follow_redirects=True
        )

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

        response = client.post(
            f"/users/delete/{user_to_delete.id}", follow_redirects=True
        )
        check_content_in_response("Deleted Row", response)
        check_content_not_in_response(user_to_delete.username, response)
        assert bool(self.security_manager.get_user_by_id(user_to_delete.id)) is False


# type: ignore[attr-defined]


class TestResetUserSessions:
    @classmethod
    def setup_class(cls):
        settings.configure_orm()

    def setup_method(self):
        # We cannot reuse the app in tests (on class level) as in Flask 2.2 this causes
        # an exception because app context teardown is removed and if even single request is run via app
        # it cannot be re-intialized again by passing it as constructor to SQLA
        # This makes the tests slightly slower (but they work with Flask 2.1 and 2.2
        self.app = application.create_app(testing=True)
        self.appbuilder = self.app.appbuilder
        self.app.config["WTF_CSRF_ENABLED"] = False
        self.security_manager = self.appbuilder.sm
        self.interface = self.app.session_interface
        self.model = self.interface.sql_session_model
        self.serializer = self.interface.serializer
        self.db = self.interface.db
        self.db.session.query(self.model).delete()
        self.db.session.commit()
        self.db.session.flush()
        self.user_1 = create_user(
            self.app,
            username="user_to_delete_1",
            role_name="user_to_delete",
        )
        self.user_2 = create_user(
            self.app,
            username="user_to_delete_2",
            role_name="user_to_delete",
        )
        self.db.session.commit()
        self.db.session.flush()

    def create_user_db_session(
        self, session_id: str, time_delta: timedelta, user_id: int
    ):
        self.db.session.add(
            self.model(
                session_id=session_id,
                data=self.serializer.dumps({"_user_id": user_id}),
                expiry=datetime.now() + time_delta,
            )
        )

    @pytest.mark.parametrize(
        "time_delta, user_sessions_deleted",
        [
            pytest.param(timedelta(days=-1), True, id="Both expired"),
            pytest.param(timedelta(hours=1), True, id="Both fresh"),
            pytest.param(timedelta(days=1), True, id="Both future"),
        ],
    )
    def test_reset_user_sessions_delete(
        self, time_delta: timedelta, user_sessions_deleted: bool
    ):
        self.create_user_db_session("session_id_1", time_delta, self.user_1.id)
        self.create_user_db_session("session_id_2", time_delta, self.user_2.id)
        self.db.session.commit()
        self.db.session.flush()
        assert self.db.session.query(self.model).count() == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

        self.security_manager.reset_password(self.user_1.id, "new_password")
        self.db.session.commit()
        self.db.session.flush()
        if user_sessions_deleted:
            assert self.db.session.query(self.model).count() == 1
            assert self.get_session_by_id("session_id_1") is None
        else:
            assert self.db.session.query(self.model).count() == 2
            assert self.get_session_by_id("session_id_1") is not None

    def get_session_by_id(self, session_id: str):
        return (
            self.db.session.query(self.model)
            .filter(self.model.session_id == session_id)
            .scalar()
        )

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.flash")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.has_request_context",
        return_value=True,
    )
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.MAX_NUM_DATABASE_USER_SESSIONS",
        1,
    )
    def test_refuse_delete(self, _mock_has_context, flash_mock):
        self.create_user_db_session("session_id_1", timedelta(days=1), self.user_1.id)
        self.create_user_db_session("session_id_2", timedelta(days=1), self.user_2.id)
        self.db.session.commit()
        self.db.session.flush()
        assert self.db.session.query(self.model).count() == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None
        self.security_manager.reset_password(self.user_1.id, "new_password")
        assert flash_mock.called
        assert (
            "The old sessions for user user_to_delete_1 have <b>NOT</b> been deleted!"
            in flash_mock.call_args[0][0]
        )
        assert self.db.session.query(self.model).count() == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.flash")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.has_request_context",
        return_value=True,
    )
    def test_warn_securecookie(self, _mock_has_context, flash_mock):
        self.app.session_interface = SecureCookieSessionInterface()
        self.security_manager.reset_password(self.user_1.id, "new_password")
        assert flash_mock.called
        assert (
            "Since you are using `securecookie` session backend mechanism, we cannot"
            in flash_mock.call_args[0][0]
        )

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    @mock.patch(
        "airflow.providers.fab.auth_manager.security_manager.override.MAX_NUM_DATABASE_USER_SESSIONS",
        1,
    )
    def test_refuse_delete_cli(self, log_mock):
        self.create_user_db_session("session_id_1", timedelta(days=1), self.user_1.id)
        self.create_user_db_session("session_id_2", timedelta(days=1), self.user_2.id)
        self.db.session.commit()
        self.db.session.flush()
        assert self.db.session.query(self.model).count() == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None
        self.security_manager.reset_password(self.user_1.id, "new_password")
        assert log_mock.warning.called
        assert (
            "The old sessions for user user_to_delete_1 have *NOT* been deleted!\n"
            in log_mock.warning.call_args[0][0]
        )
        assert self.db.session.query(self.model).count() == 2
        assert self.get_session_by_id("session_id_1") is not None
        assert self.get_session_by_id("session_id_2") is not None

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_warn_securecookie_cli(self, log_mock):
        self.app.session_interface = SecureCookieSessionInterface()
        self.security_manager.reset_password(self.user_1.id, "new_password")
        assert log_mock.warning.called
        assert (
            "Since you are using `securecookie` session backend mechanism, we cannot"
            in log_mock.warning.call_args[0][0]
        )
