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

import json
from unittest.mock import Mock, patch

import pytest
from flask import Flask, session

from airflow.auth.managers.models.resource_details import AccessView
from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.auth.managers.simple.views.auth import SimpleAuthManagerAuthenticationViews
from airflow.www.extensions.init_appbuilder import init_appbuilder


@pytest.fixture
def auth_manager():
    return SimpleAuthManager(None)


@pytest.fixture
def auth_manager_with_appbuilder():
    flask_app = Flask(__name__)
    appbuilder = init_appbuilder(flask_app)
    return SimpleAuthManager(appbuilder)


@pytest.fixture
def test_user():
    return SimpleAuthManagerUser(username="test", role="test")


class TestSimpleAuthManager:
    @pytest.mark.db_test
    def test_init_with_no_user(self, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.init()
        with open(auth_manager_with_appbuilder.get_generated_password_file()) as file:
            passwords_str = file.read().strip()
            user_passwords_from_file = json.loads(passwords_str)

            assert user_passwords_from_file == {}

    @pytest.mark.db_test
    def test_init_with_users(self, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.appbuilder.app.config[
            "SIMPLE_AUTH_MANAGER_USERS"
        ] = [
            {
                "username": "test",
                "role": "admin",
            }
        ]
        auth_manager_with_appbuilder.init()
        with open(auth_manager_with_appbuilder.get_generated_password_file()) as file:
            passwords_str = file.read().strip()
            user_passwords_from_file = json.loads(passwords_str)

            assert len(user_passwords_from_file) == 1

    @pytest.mark.db_test
    def test_is_logged_in(self, auth_manager_with_appbuilder, app, test_user):
        with app.test_request_context():
            session["user"] = test_user
            result = auth_manager_with_appbuilder.is_logged_in()
        assert result

    @pytest.mark.db_test
    def test_is_logged_in_return_false_when_no_user_in_session(
        self, auth_manager_with_appbuilder, app
    ):
        with app.test_request_context():
            result = auth_manager_with_appbuilder.is_logged_in()

        assert result is False

    @pytest.mark.db_test
    def test_is_logged_in_with_all_admins(self, auth_manager_with_appbuilder, app):
        auth_manager_with_appbuilder.appbuilder.app.config[
            "SIMPLE_AUTH_MANAGER_ALL_ADMINS"
        ] = True
        with app.test_request_context():
            result = auth_manager_with_appbuilder.is_logged_in()
        assert result

    @patch("airflow.auth.managers.simple.simple_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager):
        auth_manager.get_url_login()
        mock_url_for.assert_called_once_with("SimpleAuthManagerAuthenticationViews.login")

    @patch("airflow.auth.managers.simple.simple_auth_manager.url_for")
    def test_get_url_logout(self, mock_url_for, auth_manager):
        auth_manager.get_url_logout()
        mock_url_for.assert_called_once_with(
            "SimpleAuthManagerAuthenticationViews.logout"
        )

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user(
        self, mock_is_logged_in, auth_manager_with_appbuilder, app, test_user
    ):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["user"] = test_user
            result = auth_manager_with_appbuilder.get_user()

        assert result == test_user

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user_with_all_admins(
        self, mock_is_logged_in, auth_manager_with_appbuilder, app
    ):
        mock_is_logged_in.return_value = True

        auth_manager_with_appbuilder.appbuilder.app.config[
            "SIMPLE_AUTH_MANAGER_ALL_ADMINS"
        ] = True
        with app.test_request_context():
            result = auth_manager_with_appbuilder.get_user()

        assert result.username == "anonymous"
        assert result.role == "admin"

    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user_return_none_when_not_logged_in(
        self, mock_is_logged_in, auth_manager
    ):
        mock_is_logged_in.return_value = False
        result = auth_manager.get_user()

        assert result is None

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    @pytest.mark.parametrize(
        "api",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_dag",
            "is_authorized_asset",
            "is_authorized_pool",
            "is_authorized_variable",
        ],
    )
    @pytest.mark.parametrize(
        "is_logged_in, role, method, result",
        [
            (True, "ADMIN", "GET", True),
            (True, "ADMIN", "DELETE", True),
            (True, "VIEWER", "POST", False),
            (True, "VIEWER", "PUT", False),
            (True, "VIEWER", "DELETE", False),
            (False, "ADMIN", "GET", False),
        ],
    )
    def test_is_authorized_methods(
        self,
        mock_is_logged_in,
        auth_manager_with_appbuilder,
        app,
        api,
        is_logged_in,
        role,
        method,
        result,
    ):
        mock_is_logged_in.return_value = is_logged_in

        with app.test_request_context():
            session["user"] = SimpleAuthManagerUser(username="test", role=role)
            assert getattr(auth_manager_with_appbuilder, api)(method=method) is result

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    @pytest.mark.parametrize(
        "api, kwargs",
        [
            ("is_authorized_view", {"access_view": AccessView.CLUSTER_ACTIVITY}),
            (
                "is_authorized_custom_view",
                {
                    "method": "GET",
                    "resource_name": "test",
                },
            ),
        ],
    )
    @pytest.mark.parametrize(
        "is_logged_in, role, result",
        [
            (True, "ADMIN", True),
            (True, "VIEWER", True),
            (True, "USER", True),
            (True, "OP", True),
            (False, "ADMIN", False),
        ],
    )
    def test_is_authorized_view_methods(
        self,
        mock_is_logged_in,
        auth_manager_with_appbuilder,
        app,
        api,
        kwargs,
        is_logged_in,
        role,
        result,
    ):
        mock_is_logged_in.return_value = is_logged_in

        with app.test_request_context():
            session["user"] = SimpleAuthManagerUser(username="test", role=role)
            assert getattr(auth_manager_with_appbuilder, api)(**kwargs) is result

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    @pytest.mark.parametrize(
        "api",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_asset",
            "is_authorized_pool",
            "is_authorized_variable",
        ],
    )
    @pytest.mark.parametrize(
        "role, method, result",
        [
            ("ADMIN", "GET", True),
            ("OP", "DELETE", True),
            ("USER", "DELETE", False),
            ("VIEWER", "PUT", False),
        ],
    )
    def test_is_authorized_methods_op_role_required(
        self,
        mock_is_logged_in,
        auth_manager_with_appbuilder,
        app,
        api,
        role,
        method,
        result,
    ):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["user"] = SimpleAuthManagerUser(username="test", role=role)
            assert getattr(auth_manager_with_appbuilder, api)(method=method) is result

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    @pytest.mark.parametrize(
        "api",
        ["is_authorized_dag"],
    )
    @pytest.mark.parametrize(
        "role, method, result",
        [
            ("ADMIN", "GET", True),
            ("OP", "DELETE", True),
            ("USER", "GET", True),
            ("USER", "DELETE", True),
            ("VIEWER", "PUT", False),
        ],
    )
    def test_is_authorized_methods_user_role_required(
        self,
        mock_is_logged_in,
        auth_manager_with_appbuilder,
        app,
        api,
        role,
        method,
        result,
    ):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["user"] = SimpleAuthManagerUser(username="test", role=role)
            assert getattr(auth_manager_with_appbuilder, api)(method=method) is result

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    @pytest.mark.parametrize(
        "api",
        ["is_authorized_dag", "is_authorized_asset", "is_authorized_pool"],
    )
    @pytest.mark.parametrize(
        "role, method, result",
        [
            ("ADMIN", "GET", True),
            ("VIEWER", "GET", True),
            ("OP", "GET", True),
            ("USER", "GET", True),
            ("VIEWER", "POST", False),
        ],
    )
    def test_is_authorized_methods_viewer_role_required_for_get(
        self,
        mock_is_logged_in,
        auth_manager_with_appbuilder,
        app,
        api,
        role,
        method,
        result,
    ):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["user"] = SimpleAuthManagerUser(username="test", role=role)
            assert getattr(auth_manager_with_appbuilder, api)(method=method) is result

    @pytest.mark.db_test
    @patch(
        "airflow.providers.amazon.aws.auth_manager.views.auth.conf.get_mandatory_value",
        return_value="test",
    )
    def test_register_views(self, _, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.appbuilder.add_view_no_menu = Mock()
        auth_manager_with_appbuilder.register_views()
        auth_manager_with_appbuilder.appbuilder.add_view_no_menu.assert_called_once()
        assert isinstance(
            auth_manager_with_appbuilder.appbuilder.add_view_no_menu.call_args.args[0],
            SimpleAuthManagerAuthenticationViews,
        )
