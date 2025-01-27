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

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def auth_manager():
    return SimpleAuthManager(None)


@pytest.fixture
def auth_manager_with_appbuilder():
    flask_app = Flask(__name__)
    auth_manager = SimpleAuthManager()
    auth_manager.appbuilder = init_appbuilder(flask_app)
    return auth_manager


@pytest.fixture
def test_user():
    return SimpleAuthManagerUser(username="test", role="test")


class TestSimpleAuthManager:
    @pytest.mark.db_test
    def test_get_users(self, auth_manager):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer,test2:viewer",
            }
        ):
            users = auth_manager.get_users()
            assert users == [{"role": "viewer", "username": "test1"}, {"role": "viewer", "username": "test2"}]

    @pytest.mark.db_test
    def test_init_with_default_user(self, auth_manager):
        auth_manager.init()
        with open(auth_manager.get_generated_password_file()) as file:
            passwords_str = file.read().strip()
            user_passwords_from_file = json.loads(passwords_str)

            assert len(user_passwords_from_file) == 1

    @pytest.mark.db_test
    def test_init_with_users(self, auth_manager):
        with conf_vars(
            {
                ("core", "simple_auth_manager_users"): "test1:viewer,test2:viewer",
            }
        ):
            auth_manager.init()
            with open(auth_manager.get_generated_password_file()) as file:
                passwords_str = file.read().strip()
                user_passwords_from_file = json.loads(passwords_str)

                assert len(user_passwords_from_file) == 2

    @pytest.mark.db_test
    def test_is_logged_in(self, auth_manager, app, test_user):
        with app.test_request_context():
            session["user"] = test_user
            result = auth_manager.is_logged_in()
        assert result

    @pytest.mark.db_test
    def test_is_logged_in_return_false_when_no_user_in_session(self, auth_manager, app):
        with app.test_request_context():
            result = auth_manager.is_logged_in()

        assert result is False

    @pytest.mark.db_test
    def test_is_logged_in_with_all_admins(self, auth_manager, app):
        with conf_vars(
            {
                ("core", "simple_auth_manager_all_admins"): "True",
            }
        ):
            with app.test_request_context():
                result = auth_manager.is_logged_in()
            assert result

    @patch("airflow.auth.managers.simple.simple_auth_manager.url_for")
    def test_get_url_login(self, mock_url_for, auth_manager):
        auth_manager.get_url_login()
        mock_url_for.assert_called_once_with("SimpleAuthManagerAuthenticationViews.login", next=None)

    @patch("airflow.auth.managers.simple.simple_auth_manager.url_for")
    def test_get_url_logout(self, mock_url_for, auth_manager):
        auth_manager.get_url_logout()
        mock_url_for.assert_called_once_with("SimpleAuthManagerAuthenticationViews.logout")

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user(self, mock_is_logged_in, auth_manager, app, test_user):
        mock_is_logged_in.return_value = True

        with app.test_request_context():
            session["user"] = test_user
            result = auth_manager.get_user()

        assert result == test_user

    @pytest.mark.db_test
    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user_with_all_admins(self, mock_is_logged_in, auth_manager, app):
        mock_is_logged_in.return_value = True

        with conf_vars(
            {
                ("core", "simple_auth_manager_all_admins"): "True",
            }
        ):
            with app.test_request_context():
                result = auth_manager.get_user()

        assert result.username == "anonymous"
        assert result.role == "admin"

    @patch.object(SimpleAuthManager, "is_logged_in")
    def test_get_user_return_none_when_not_logged_in(self, mock_is_logged_in, auth_manager):
        mock_is_logged_in.return_value = False
        result = auth_manager.get_user()

        assert result is None

    def test_deserialize_user(self, auth_manager):
        result = auth_manager.deserialize_user({"username": "test", "role": "admin"})
        assert result.username == "test"
        assert result.role == "admin"

    def test_serialize_user(self, auth_manager):
        user = SimpleAuthManagerUser(username="test", role="admin")
        result = auth_manager.serialize_user(user)
        assert result == {"username": "test", "role": "admin"}

    @pytest.mark.db_test
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
        "role, method, result",
        [
            ("ADMIN", "GET", True),
            ("ADMIN", "DELETE", True),
            ("VIEWER", "POST", False),
            ("VIEWER", "PUT", False),
            ("VIEWER", "DELETE", False),
        ],
    )
    def test_is_authorized_methods(self, auth_manager, app, api, role, method, result):
        with app.test_request_context():
            assert (
                getattr(auth_manager, api)(
                    method=method, user=SimpleAuthManagerUser(username="test", role=role)
                )
                is result
            )

    @pytest.mark.db_test
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
        "role, result",
        [
            ("ADMIN", True),
            ("VIEWER", True),
            ("USER", True),
            ("OP", True),
        ],
    )
    def test_is_authorized_view_methods(self, auth_manager, app, api, kwargs, role, result):
        with app.test_request_context():
            assert (
                getattr(auth_manager, api)(**kwargs, user=SimpleAuthManagerUser(username="test", role=role))
                is result
            )

    @pytest.mark.db_test
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
    def test_is_authorized_methods_op_role_required(self, auth_manager, app, api, role, method, result):
        with app.test_request_context():
            assert (
                getattr(auth_manager, api)(
                    method=method, user=SimpleAuthManagerUser(username="test", role=role)
                )
                is result
            )

    @pytest.mark.db_test
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
    def test_is_authorized_methods_user_role_required(self, auth_manager, app, api, role, method, result):
        with app.test_request_context():
            assert (
                getattr(auth_manager, api)(
                    method=method, user=SimpleAuthManagerUser(username="test", role=role)
                )
                is result
            )

    @pytest.mark.db_test
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
        self, auth_manager, app, api, role, method, result
    ):
        with app.test_request_context():
            assert (
                getattr(auth_manager, api)(
                    method=method, user=SimpleAuthManagerUser(username="test", role=role)
                )
                is result
            )

    @pytest.mark.db_test
    def test_register_views(self, auth_manager_with_appbuilder):
        auth_manager_with_appbuilder.appbuilder.add_view_no_menu = Mock()
        auth_manager_with_appbuilder.register_views()
        auth_manager_with_appbuilder.appbuilder.add_view_no_menu.assert_called_once()
        assert isinstance(
            auth_manager_with_appbuilder.appbuilder.add_view_no_menu.call_args.args[0],
            SimpleAuthManagerAuthenticationViews,
        )
