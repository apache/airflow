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

from unittest.mock import Mock, patch

from keycloak import KeycloakAuthenticationError

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX


class TestTokenRouter:
    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_create_token(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": "access_token",
            "refresh_token": "refresh_token",
        }
        mock_keycloak_client.userinfo.return_value = {"sub": "sub", "preferred_username": "username"}
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.post(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/token",
            json={"username": "username", "password": "password"},
        )

        assert response.status_code == 201
        mock_keycloak_client.token.assert_called_once_with("username", "password")
        mock_keycloak_client.userinfo.assert_called_once_with("access_token")

    @patch("airflow.providers.keycloak.auth_manager.routes.login.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_with_invalid_creds(self, mock_get_keycloak_client, client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.side_effect = KeycloakAuthenticationError()
        mock_get_keycloak_client.return_value = mock_keycloak_client
        response = client.post(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/token",
            json={"username": "username", "password": "password"},
        )

        assert response.status_code == 401
        mock_keycloak_client.token.assert_called_once_with("username", "password")
