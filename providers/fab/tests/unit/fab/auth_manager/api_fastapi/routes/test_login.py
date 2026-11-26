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

from unittest.mock import patch

import pytest
from flask_login import login_user

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse

from unit.fab.auth_manager.test_utils import create_user, delete_user

SUBPATH = "/team-a/"


@pytest.mark.db_test
class TestLogin:
    dummy_login_body = {"username": "dummy", "password": "dummy"}
    dummy_token = LoginResponse(access_token="DUMMY_TOKEN")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login._get_flask_app")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.FABAuthManagerLogin")
    def test_create_token(self, mock_fab_auth_manager_login, mock_get_flask_app, test_client):
        mock_fab_auth_manager_login.create_token.return_value = self.dummy_token
        mock_get_flask_app.return_value.app_context.return_value.__enter__.return_value = None

        response = test_client.post(
            "/token",
            json=self.dummy_login_body,
        )
        assert response.status_code == 201
        assert response.json()["access_token"] == self.dummy_token.access_token

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login._get_flask_app")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.FABAuthManagerLogin")
    def test_create_token_cli(self, mock_fab_auth_manager_login, mock_get_flask_app, test_client):
        mock_fab_auth_manager_login.create_token.return_value = LoginResponse(access_token="DUMMY_TOKEN")
        mock_get_flask_app.return_value.app_context.return_value.__enter__.return_value = None

        response = test_client.post(
            "/token/cli",
            json=self.dummy_login_body,
        )
        assert response.status_code == 201
        assert response.json()["access_token"] == self.dummy_token.access_token

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login._get_flask_app")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.get_fab_auth_manager", autospec=True)
    def test_logout(self, mock_get_fab_auth_manager, mock_get_flask_app, test_client):
        mock_get_fab_auth_manager.return_value.apiserver_endpoint = "/"
        mock_get_flask_app.return_value.app_context.return_value.__enter__.return_value = None
        response = test_client.get("/logout", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/auth/logout/"
        cookies = response.headers.get_list("set-cookie")
        assert not any(c.startswith("session=") for c in cookies)
        assert any(f"{COOKIE_NAME_JWT_TOKEN}=" in c for c in cookies)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login._get_flask_app")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.get_fab_auth_manager", autospec=True)
    @patch(
        "airflow.providers.fab.auth_manager.api_fastapi.routes.login.get_cookie_path", return_value=SUBPATH
    )
    def test_logout_cookie_uses_subpath(
        self, mock_cookie_path, mock_get_fab_auth_manager, mock_get_flask_app, test_client
    ):
        """Cookies on logout must be scoped to the configured subpath."""
        mock_get_fab_auth_manager.return_value.apiserver_endpoint = "/"
        mock_get_flask_app.return_value.app_context.return_value.__enter__.return_value = None
        response = test_client.get("/logout", follow_redirects=False)
        assert response.status_code == 307
        cookies = response.headers.get_list("set-cookie")
        token_cookie = next(c for c in cookies if f"{COOKIE_NAME_JWT_TOKEN}=" in c)
        assert f"Path={SUBPATH}" in token_cookie

    def test_logout_signs_out_the_flask_session(self, real_app_client):
        """The session cookie the browser keeps no longer signs in once the logout redirects are followed."""
        auth_manager, client = real_app_client
        flask_app = auth_manager.flask_app
        with flask_app.app_context():
            user = create_user(flask_app, "test_logout_user", role_name="test_logout_role")
        try:
            with flask_app.test_request_context():
                login_user(user)
                login_response = flask_app.process_response(flask_app.response_class())
            session_cookie = login_response.headers["Set-Cookie"].split(";")[0]

            def get(path):
                # Send only the session cookie, as the browser does after the token cookie is deleted.
                client.cookies.clear()
                return client.get(path, headers={"Cookie": session_cookie}, follow_redirects=False)

            assert get("/login/").cookies.get(COOKIE_NAME_JWT_TOKEN)

            response = get("/logout")
            while response.is_redirect and response.headers["location"].startswith("/auth/logout"):
                response = get(response.headers["location"].removeprefix("/auth"))

            login_page = get("/login/")
            assert login_page.status_code == 200
            assert not login_page.cookies.get(COOKIE_NAME_JWT_TOKEN)
        finally:
            with flask_app.app_context():
                delete_user(flask_app, "test_logout_user")
