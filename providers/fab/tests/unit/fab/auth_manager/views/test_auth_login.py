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
from flask import Flask, Response, g
from flask_appbuilder.security.views import AuthDBView, AuthLDAPView, AuthOAuthView

from airflow.providers.fab.auth_manager.views import auth_login
from airflow.providers.fab.auth_manager.views.auth_login import (
    CustomAuthDBView,
    CustomAuthLDAPView,
    redirect_if_authenticated,
)
from airflow.providers.fab.auth_manager.views.auth_oauth import CustomAuthOAuthView

MODULE = "airflow.providers.fab.auth_manager.views.auth_login"


@pytest.fixture
def flask_app():
    return Flask(__name__)


@pytest.fixture
def mock_get_safe_url():
    with mock.patch(f"{MODULE}.get_safe_url", side_effect=lambda url: f"safe:{url}") as get_safe_url:
        yield get_safe_url


@pytest.fixture
def mock_redirect():
    with mock.patch(f"{MODULE}.redirect") as redirect:
        yield redirect


def _set_user(authenticated: bool | None) -> None:
    g.user = None if authenticated is None else mock.Mock(is_authenticated=authenticated)


class TestRedirectIfAuthenticated:
    def test_redirects_authenticated_user_to_next(self, flask_app, mock_get_safe_url, mock_redirect):
        with flask_app.test_request_context("/login/?next=/dags/my_dag"):
            _set_user(authenticated=True)
            response = redirect_if_authenticated()

        mock_get_safe_url.assert_called_once_with("/dags/my_dag")
        mock_redirect.assert_called_once_with("safe:/dags/my_dag")
        assert response is mock_redirect.return_value

    def test_falls_back_to_index_without_next(self, flask_app, mock_get_safe_url, mock_redirect):
        with flask_app.test_request_context("/login/"):
            _set_user(authenticated=True)
            redirect_if_authenticated()

        mock_get_safe_url.assert_called_once_with(None)
        mock_redirect.assert_called_once_with("safe:None")

    @pytest.mark.parametrize("authenticated", [None, False])
    def test_does_nothing_for_anonymous_user(
        self, flask_app, authenticated, mock_get_safe_url, mock_redirect
    ):
        with flask_app.test_request_context("/login/?next=/dags/my_dag"):
            _set_user(authenticated=authenticated)
            assert redirect_if_authenticated() is None

        mock_redirect.assert_not_called()


class TestCustomLoginViews:
    @pytest.mark.parametrize(
        ("view_class", "routes"),
        [
            (CustomAuthDBView, {"/login/"}),
            (CustomAuthLDAPView, {"/login/"}),
            (CustomAuthOAuthView, {"/login/", "/login/<provider>"}),
        ],
    )
    def test_login_routes_are_exposed(self, view_class, routes):
        assert {route for route, _methods in view_class.login._urls} == routes

    @pytest.mark.parametrize(
        ("view_class", "parent_class", "args"),
        [
            (CustomAuthDBView, AuthDBView, ()),
            (CustomAuthLDAPView, AuthLDAPView, ()),
            (CustomAuthOAuthView, AuthOAuthView, ("azure",)),
        ],
    )
    def test_authenticated_login_redirects_to_next(self, flask_app, view_class, parent_class, args):
        view = view_class()
        redirect_response = Response(status=302, headers={"Location": "/dags/my_dag"})
        with (
            flask_app.test_request_context("/login/?next=/dags/my_dag"),
            mock.patch(f"{view_class.__module__}.redirect_if_authenticated", return_value=redirect_response),
            mock.patch.object(parent_class, "login") as parent_login,
        ):
            response = view.login(*args)

        assert response.status_code == 302
        assert response.location == "/dags/my_dag"
        parent_login.assert_not_called()

    @pytest.mark.parametrize(
        ("view_class", "parent_class", "args", "expected_args"),
        [
            (CustomAuthDBView, AuthDBView, (), ()),
            (CustomAuthLDAPView, AuthLDAPView, (), ()),
            (CustomAuthOAuthView, AuthOAuthView, ("azure",), ("azure",)),
            (CustomAuthOAuthView, AuthOAuthView, (), (None,)),
        ],
    )
    def test_anonymous_login_uses_parent(self, flask_app, view_class, parent_class, args, expected_args):
        view = view_class()
        with (
            flask_app.test_request_context("/login/"),
            mock.patch(f"{view_class.__module__}.redirect_if_authenticated", return_value=None),
            mock.patch.object(parent_class, "login", return_value=Response("login page")) as parent_login,
        ):
            response = view.login(*args)

        assert response.get_data(as_text=True) == "login page"
        parent_login.assert_called_once_with(*expected_args)


def test_module_uses_token_setting_redirect():
    from airflow.providers.fab.www import views

    assert auth_login.redirect is views.redirect
