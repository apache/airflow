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

from http.cookies import SimpleCookie
from unittest.mock import ANY, Mock, patch

import pytest

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS


class TestLoginRouter:
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login(self, mock_get_keycloak_client, client):
        redirect_url = "redirect_url"
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = redirect_url
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login", follow_redirects=False)
        assert response.status_code == 307
        assert "location" in response.headers
        assert response.headers["location"] == redirect_url

    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_callback(self, mock_get_keycloak_client, mock_get_auth_manager, client):
        code = "code"
        token = "token"
        state = "state"
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
            "id_token": "id_token",
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "sub",
            "preferred_username": "preferred_username",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = token
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + f"/login_callback?code={code}&state={state}",
            follow_redirects=False,
            cookies={"_oauth_state": state},
        )
        mock_keycloak_client.token.assert_called_once_with(
            grant_type="authorization_code",
            code=code,
            redirect_uri=ANY,
        )
        mock_keycloak_client.userinfo.assert_called_once_with("access_token")
        mock_auth_manager.generate_jwt.assert_called_once()
        user = mock_auth_manager.generate_jwt.call_args[0][0]
        assert user.get_id() == "sub"
        assert user.get_name() == "preferred_username"
        assert user.access_token == "access_token"
        assert user.refresh_token == "refresh_token"
        assert response.status_code == 303
        assert "location" in response.headers
        assert "_token" in response.cookies
        assert response.cookies["_token"] == token
        assert response.cookies["_id_token"] == "id_token"
        if AIRFLOW_V_3_3_PLUS:
            assert response.cookies["_access_token"] == "access_token"
            assert response.cookies["_refresh_token"] == "refresh_token"

    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_callback_without_refresh_token(
        self, mock_get_keycloak_client, mock_get_auth_manager, client
    ):
        code = "code"
        token = "token"
        state = "state"
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "id_token": "id_token",
            # no refresh_token key -- simulates Keycloak with "Use Refresh Tokens" disabled
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "sub",
            "preferred_username": "preferred_username",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = token
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + f"/login_callback?code={code}&state={state}",
            follow_redirects=False,
            cookies={"_oauth_state": state},
        )
        mock_auth_manager.generate_jwt.assert_called_once()
        user = mock_auth_manager.generate_jwt.call_args[0][0]
        assert user.get_id() == "sub"
        assert user.refresh_token is None
        assert response.status_code == 303
        assert "_refresh_token" not in response.cookies

    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_sets_secure_state_cookie_behind_tls_proxy(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = "redirect_url"
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(
            "https://testserver" + AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login",
            follow_redirects=False,
        )
        set_cookies = response.headers.get_list("set-cookie")
        assert any("_oauth_state" in c and "Secure" in c for c in set_cookies)

    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_callback_sets_secure_cookies_behind_tls_proxy(
        self, mock_get_keycloak_client, mock_get_auth_manager, client
    ):
        state = "state"
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
            "id_token": "id_token",
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "sub",
            "preferred_username": "preferred_username",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = "token"
        response = client.get(
            "https://testserver"
            + AUTH_MANAGER_FASTAPI_APP_PREFIX
            + f"/login_callback?code=code&state={state}",
            follow_redirects=False,
            cookies={"_oauth_state": state},
        )
        set_cookies = response.headers.get_list("set-cookie")
        assert any("_token=" in c and "Secure" in c for c in set_cookies)
        assert any("_id_token=" in c and "Secure" in c for c in set_cookies)

    @conf_vars({("api", "base_url"): "https://airflow.example.com"})
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_redirect_uri_uses_configured_base_url(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = "redirect_url"
        mock_get_keycloak_client.return_value = mock_keycloak_client
        client.get(
            "https://attacker.example.net" + AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login",
            follow_redirects=False,
        )
        redirect_uri = mock_keycloak_client.auth_url.call_args.kwargs["redirect_uri"]
        assert (
            redirect_uri
            == "https://airflow.example.com" + AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback"
        )

    @conf_vars({("api", "base_url"): "https://airflow.example.com"})
    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_callback_redirect_uri_uses_configured_base_url(
        self, mock_get_keycloak_client, mock_get_auth_manager, client
    ):
        state = "state"
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
            "id_token": "id_token",
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "sub",
            "preferred_username": "preferred_username",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = "token"
        client.get(
            "https://attacker.example.net"
            + AUTH_MANAGER_FASTAPI_APP_PREFIX
            + f"/login_callback?code=code&state={state}",
            follow_redirects=False,
            cookies={"_oauth_state": state},
        )
        redirect_uri = mock_keycloak_client.token.call_args.kwargs["redirect_uri"]
        assert (
            redirect_uri
            == "https://airflow.example.com" + AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback"
        )

    @pytest.mark.parametrize(
        "next_url",
        [
            "/dags/my_dag",
            "/dags/my_dag/runs/scheduled__2026-11-03T06:30:00+00:00/tasks/my_task?try_number=2",
            "http://testserver/dags/my_dag",
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_stores_safe_next_url(self, mock_get_keycloak_client, next_url, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = "redirect_url"
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login",
            params={"next": next_url},
            follow_redirects=False,
        )
        assert response.status_code == 307
        set_cookies = [c for c in response.headers.get_list("set-cookie") if c.startswith("_login_next_url=")]
        assert len(set_cookies) == 1
        cookie = SimpleCookie(set_cookies[0])["_login_next_url"]
        assert cookie.value == next_url
        assert cookie["httponly"]
        assert cookie["max-age"] == "300"

    @pytest.mark.parametrize(
        "next_url",
        [
            "https://attacker.example.net/dags",
            "//attacker.example.net/dags",
            "javascript:alert(1)",
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_does_not_store_unsafe_next_url(self, mock_get_keycloak_client, next_url, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = "redirect_url"
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login",
            params={"next": next_url},
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert "_login_next_url" not in response.cookies

    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_without_next_url_clears_previous_next_url(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.auth_url.return_value = "redirect_url"
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login",
            cookies={"_login_next_url": "/dags/stale_dag"},
            follow_redirects=False,
        )
        set_cookies = response.headers.get_list("set-cookie")
        assert any(c.startswith('_login_next_url=""') and "Max-Age=0" in c for c in set_cookies)

    @pytest.mark.parametrize(
        ("next_url_cookie", "expected_location"),
        [
            ("/dags/my_dag/runs/run_id/tasks/my_task", "/dags/my_dag/runs/run_id/tasks/my_task"),
            (None, "/"),
            ("https://attacker.example.net/dags", "/"),
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_login_callback_redirects_to_next_url(
        self, mock_get_keycloak_client, mock_get_auth_manager, next_url_cookie, expected_location, client
    ):
        state = "state"
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
            "id_token": "id_token",
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "sub",
            "preferred_username": "preferred_username",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = "token"
        cookies = {"_oauth_state": state}
        if next_url_cookie:
            cookies["_login_next_url"] = next_url_cookie
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + f"/login_callback?code=code&state={state}",
            follow_redirects=False,
            cookies=cookies,
        )
        assert response.status_code == 303
        assert response.headers["location"] == expected_location
        assert response.cookies["_token"] == "token"
        set_cookies = response.headers.get_list("set-cookie")
        assert any(c.startswith('_login_next_url=""') and "Max-Age=0" in c for c in set_cookies)

    def test_login_callback_without_code(self, client):
        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback")
        assert response.status_code == 400

    @pytest.mark.parametrize(
        ("id_token", "logout_callback_url"),
        [
            (None, "/auth/logout_callback"),
            (
                "id_token",
                "/auth/logout_callback",
            ),
            (
                "id_token.real.value",
                "logout_url?post_logout_redirect_uri=/auth/logout_callback&id_token_hint=id_token.real.value",
            ),
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_logout(self, mock_get_keycloak_client, id_token, logout_callback_url, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.well_known.return_value = {"end_session_endpoint": "logout_url"}
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/logout",
            cookies={"_id_token": id_token},
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert "location" in response.headers
        assert response.headers["location"] == logout_callback_url
