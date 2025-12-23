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

from unittest.mock import ANY, Mock, patch

from keycloak import KeycloakPostError

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser


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
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
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
            AUTH_MANAGER_FASTAPI_APP_PREFIX + f"/login_callback?code={code}", follow_redirects=False
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

    def test_login_callback_without_code(self, client):
        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback")
        assert response.status_code == 400

    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_logout(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.well_known.return_value = {"end_session_endpoint": "logout_url"}
        mock_keycloak_client.refresh_token.return_value = {"id_token": "id_token"}
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/logout", follow_redirects=False)
        assert response.status_code == 307
        assert "location" in response.headers
        assert (
            response.headers["location"]
            == "logout_url?post_logout_redirect_uri=http://testserver/auth/logout_callback&id_token_hint=id_token"
        )
        mock_keycloak_client.refresh_token.assert_called_once_with("refresh_token")

    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_logout_when_keycloak_client_raises_keycloak_post_error(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.well_known.return_value = {"end_session_endpoint": "logout_url"}
        mock_keycloak_client.refresh_token.side_effect = KeycloakPostError(
            response_code=400,
            response_body=b'{"error":"invalid_grant","error_description":"Token is not active"}',
        )
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/logout", follow_redirects=False)
        assert response.status_code == 307
        assert "location" in response.headers
        assert (
            response.headers["location"]
            == "logout_url?post_logout_redirect_uri=http://testserver/auth/logout_callback"
        )
        mock_keycloak_client.refresh_token.assert_called_once_with("refresh_token")

    @patch("airflow.providers.keycloak.auth_manager.routes.login.get_auth_manager")
    def test_refresh_token(self, mock_get_auth_manager, client):
        mock_auth_manager = Mock()
        mock_auth_manager.refresh_user.return_value = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token="new_access_token",
            refresh_token="new_refresh_token",
        )
        mock_auth_manager.generate_jwt.return_value = "token"
        mock_get_auth_manager.return_value = mock_auth_manager

        response = client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + "/refresh", follow_redirects=False)
        assert response.status_code == 303
        assert "location" in response.headers
        assert response.headers["location"] == "/"
        assert "_token" in response.cookies
        assert response.cookies["_token"] == "token"
        mock_auth_manager.refresh_user.assert_called_once()
        mock_auth_manager.generate_jwt.assert_called_once()
