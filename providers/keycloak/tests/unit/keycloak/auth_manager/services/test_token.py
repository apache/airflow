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
            ("keycloak_auth_manager", "client_id"): "test_client",
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
            ("keycloak_auth_manager", "client_id"): "invalid_client",
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

        assert exc_info.value.status_code == 403
        assert "Client credentials authentication failed" in exc_info.value.detail

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_client_credentials_rejects_other_client(self, mock_get_keycloak_client):
        """Only the client Airflow is configured with may exchange credentials for a token."""
        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_client_credentials_token(
                client_id="some_other_realm_client", client_secret="its_own_valid_secret"
            )

        assert exc_info.value.status_code == 403
        # No exchange is attempted: the credentials are never sent to Keycloak, so the
        # endpoint cannot be used to test whether another client's secret is valid.
        mock_get_keycloak_client.assert_not_called()

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_client_credentials_rejection_is_indistinguishable(self, mock_get_keycloak_client):
        """A wrong client id and a wrong secret must not be tellable apart."""
        mock_keycloak_client = Mock()
        mock_keycloak_client.token.side_effect = KeycloakAuthenticationError()
        mock_get_keycloak_client.return_value = mock_keycloak_client

        with pytest.raises(fastapi.exceptions.HTTPException) as wrong_secret:
            create_client_credentials_token(client_id="airflow", client_secret="wrong")
        with pytest.raises(fastapi.exceptions.HTTPException) as wrong_client:
            create_client_credentials_token(client_id="other", client_secret="wrong")

        assert wrong_secret.value.status_code == wrong_client.value.status_code
        assert wrong_secret.value.detail == wrong_client.value.detail
