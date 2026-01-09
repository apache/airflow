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

import fastapi
import pytest
from keycloak import KeycloakAuthenticationError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.services.token import (
    create_client_credentials_token,
    create_token_for,
)

from tests_common.test_utils.config import conf_vars


class TestTokenService:
    token = "token"
    test_username = "test_user"
    test_password = "test_pass"
    test_access_token = "access_token"
    test_refresh_token = "refresh_token"

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token(self, mock_get_keycloak_client, mock_get_auth_manager):
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": self.test_access_token,
            "refresh_token": self.test_refresh_token,
        }
        mock_keycloak_client.userinfo.return_value = {"sub": "sub", "preferred_username": "username"}
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = self.token

        assert create_token_for(username=self.test_username, password=self.test_password) == self.token
        mock_keycloak_client.token.assert_called_once_with(self.test_username, self.test_password)
        mock_keycloak_client.userinfo.assert_called_once_with(self.test_access_token)

    @conf_vars(
        {
            ("api_auth", "jwt_cli_expiration_time"): "10",
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_with_invalid_creds(self, mock_get_keycloak_client):
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.side_effect = KeycloakAuthenticationError()
        mock_get_keycloak_client.return_value = mock_keycloak_client

        with pytest.raises(fastapi.exceptions.HTTPException):
            create_token_for(
                username=self.test_username,
                password=self.test_password,
                expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
            )

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_client_credentials(self, mock_get_keycloak_client, mock_get_auth_manager):
        test_client_id = "test_client"
        test_client_secret = "test_secret"
        test_access_token = "access_token"

        mock_keycloak_client = Mock()
        mock_keycloak_client.token.return_value = {
            "access_token": test_access_token,
        }
        mock_keycloak_client.userinfo.return_value = {
            "sub": "service-account-sub",
            "preferred_username": "service-account-test_client",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_jwt.return_value = self.token

        result = create_client_credentials_token(client_id=test_client_id, client_secret=test_client_secret)

        assert result == self.token
        mock_get_keycloak_client.assert_called_once_with(
            client_id=test_client_id, client_secret=test_client_secret
        )
        mock_keycloak_client.token.assert_called_once_with(grant_type="client_credentials")
        mock_keycloak_client.userinfo.assert_called_once_with(test_access_token)

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_client_credentials_with_invalid_credentials(self, mock_get_keycloak_client):
        test_client_id = "invalid_client"
        test_client_secret = "invalid_secret"

        mock_keycloak_client = Mock()
        mock_keycloak_client.token.side_effect = KeycloakAuthenticationError()
        mock_get_keycloak_client.return_value = mock_keycloak_client

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_client_credentials_token(client_id=test_client_id, client_secret=test_client_secret)

        assert exc_info.value.status_code == 401
        assert "Client credentials authentication failed" in exc_info.value.detail
