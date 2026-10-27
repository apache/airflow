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

import time
from unittest.mock import AsyncMock, Mock, patch

import fastapi
import jwt
import pytest
import time_machine
from keycloak import KeycloakAuthenticationError, KeycloakOpenID
from keycloak.exceptions import KeycloakGetError, KeycloakPostError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.keycloak.auth_manager.services.token import (
    create_client_credentials_token,
    create_jwt_federated_token,
    create_token_for,
    create_token_from_authorization_code,
    is_cli_loopback_redirect_uri,
    refresh_cli_token,
)
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

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
        mock_auth_manager.generate_api_jwt.return_value = self.token

        assert create_token_for(username=self.test_username, password=self.test_password) == self.token
        # API tokens must be minted with the Keycloak JWTs in their claims, since a
        # header-authenticated client sends no cookies to supply them.
        mock_auth_manager.generate_api_jwt.assert_called_once()
        mock_auth_manager.generate_jwt.assert_not_called()
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
        mock_auth_manager.generate_api_jwt.return_value = self.token

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


class TestCreateJwtFederatedToken:
    token = "token"
    test_assertion = "assertion"
    test_access_token = "assertion"

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
            ("keycloak_auth_manager", "realm"): "airflow",
            ("keycloak_auth_manager", "server_url"): "https://keycloak.example.com",
            ("keycloak_auth_manager", "jwt_federated_client_ids"): "team-platform-admin-sa, other-sa",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    @patch("airflow.providers.keycloak.auth_manager.services.token.jwt.decode")
    @patch("airflow.providers.keycloak.auth_manager.services.token._get_jwks_client")
    def test_create_jwt_federated_token(
        self, mock_jwks_client_cls, mock_jwt_decode, mock_get_keycloak_client, mock_get_auth_manager
    ):
        mock_jwks_client_cls.return_value.get_signing_key_from_jwt.return_value = Mock(key="fake-key")
        mock_jwt_decode.return_value = {
            "sub": "service-account-sub",
            "azp": "team-platform-admin-sa",
            "aud": ["airflow"],
        }
        mock_keycloak_client = Mock()
        mock_keycloak_client.userinfo.return_value = {
            "sub": "service-account-sub",
            "preferred_username": "service-account-team-platform-admin-sa",
        }
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_auth_manager = Mock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.generate_api_jwt.return_value = self.token

        result = create_jwt_federated_token(assertion=self.test_assertion)

        assert result == self.token
        mock_jwt_decode.assert_called_once_with(
            self.test_assertion,
            "fake-key",
            algorithms=["RS256"],
            audience="airflow",
            issuer="https://keycloak.example.com/realms/airflow",
        )
        mock_keycloak_client.userinfo.assert_called_once_with(self.test_assertion)
        mock_auth_manager.generate_api_jwt.assert_called_once()

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
            ("keycloak_auth_manager", "realm"): "airflow",
            ("keycloak_auth_manager", "server_url"): "https://keycloak.example.com",
            ("keycloak_auth_manager", "jwt_federated_client_ids"): "team-platform-admin-sa",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    @patch("airflow.providers.keycloak.auth_manager.services.token.jwt.decode")
    @patch("airflow.providers.keycloak.auth_manager.services.token._get_jwks_client")
    def test_create_jwt_federated_token_invalid_signature(
        self, mock_jwks_client_cls, mock_jwt_decode, mock_get_keycloak_client
    ):
        mock_jwks_client_cls.return_value.get_signing_key_from_jwt.return_value = Mock(key="fake-key")
        mock_jwt_decode.side_effect = jwt.InvalidSignatureError()

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_jwt_federated_token(assertion=self.test_assertion)

        assert exc_info.value.status_code == 403

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
            ("keycloak_auth_manager", "realm"): "airflow",
            ("keycloak_auth_manager", "server_url"): "https://keycloak.example.com",
            ("keycloak_auth_manager", "jwt_federated_client_ids"): "other-sa",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    @patch("airflow.providers.keycloak.auth_manager.services.token.jwt.decode")
    @patch("airflow.providers.keycloak.auth_manager.services.token._get_jwks_client")
    def test_create_jwt_federated_token_client_not_allowlisted(
        self, mock_jwks_client_cls, mock_jwt_decode, mock_get_keycloak_client
    ):
        """A validly-signed token for an un-allow-listed client must still be rejected."""
        mock_jwks_client_cls.return_value.get_signing_key_from_jwt.return_value = Mock(key="fake-key")
        mock_jwt_decode.return_value = {
            "sub": "service-account-sub",
            "azp": "team-platform-admin-sa",
            "aud": ["airflow"],
        }

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_jwt_federated_token(assertion=self.test_assertion)

        assert exc_info.value.status_code == 403
        # No userinfo call is made once the client is rejected.
        mock_get_keycloak_client.assert_not_called()

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
            ("keycloak_auth_manager", "realm"): "airflow",
            ("keycloak_auth_manager", "server_url"): "https://keycloak.example.com",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    @patch("airflow.providers.keycloak.auth_manager.services.token.jwt.decode")
    @patch("airflow.providers.keycloak.auth_manager.services.token._get_jwks_client")
    def test_create_jwt_federated_token_allowlist_unset_fails_closed(
        self, mock_jwks_client_cls, mock_jwt_decode, mock_get_keycloak_client
    ):
        """An unconfigured jwt_federated_client_ids denies every caller, not just none."""
        mock_jwks_client_cls.return_value.get_signing_key_from_jwt.return_value = Mock(key="fake-key")
        mock_jwt_decode.return_value = {
            "sub": "service-account-sub",
            "azp": "team-platform-admin-sa",
            "aud": ["airflow"],
        }

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_jwt_federated_token(assertion=self.test_assertion)

        assert exc_info.value.status_code == 403
        mock_get_keycloak_client.assert_not_called()

    @pytest.mark.parametrize(
        "userinfo_error",
        [
            KeycloakAuthenticationError(),
            KeycloakGetError(response_code=403, response_body=b""),
        ],
    )
    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
            ("keycloak_auth_manager", "client_id"): "airflow",
            ("keycloak_auth_manager", "realm"): "airflow",
            ("keycloak_auth_manager", "server_url"): "https://keycloak.example.com",
            ("keycloak_auth_manager", "jwt_federated_client_ids"): "team-platform-admin-sa",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    @patch("airflow.providers.keycloak.auth_manager.services.token.jwt.decode")
    @patch("airflow.providers.keycloak.auth_manager.services.token._get_jwks_client")
    def test_create_jwt_federated_token_userinfo_rejected(
        self, mock_jwks_client_cls, mock_jwt_decode, mock_get_keycloak_client, userinfo_error
    ):
        """A Keycloak userinfo rejection returns a generic invalid-assertion response."""
        mock_jwks_client_cls.return_value.get_signing_key_from_jwt.return_value = Mock(key="fake-key")
        mock_jwt_decode.return_value = {
            "sub": "service-account-sub",
            "azp": "team-platform-admin-sa",
            "aud": ["airflow"],
        }
        mock_keycloak_client = Mock()
        mock_keycloak_client.userinfo.side_effect = userinfo_error
        mock_get_keycloak_client.return_value = mock_keycloak_client

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_jwt_federated_token(assertion=self.test_assertion)

        assert exc_info.value.status_code == 403


class TestCliLoopbackRedirectUri:
    @pytest.mark.parametrize(
        ("redirect_uri", "expected"),
        [
            ("http://127.0.0.1:53682/callback", True),
            ("http://[::1]:53682/callback", True),
            ("http://127.0.0.1/callback", False),
            ("http://localhost:53682/callback", False),
            ("https://127.0.0.1:53682/callback", False),
            ("http://evil.example.com:53682/callback", False),
            ("http://user@127.0.0.1:53682/callback", False),
            ("http://127.0.0.1:53682/other", False),
            ("http://127.0.0.1:53682/callback?next=x", False),
            ("http://127.0.0.1:53682/callback#x", False),
            ("http://127.0.0.1:99999/callback", False),
        ],
    )
    def test_is_cli_loopback_redirect_uri(self, redirect_uri, expected):
        assert is_cli_loopback_redirect_uri(redirect_uri) is expected


class TestCliTokenService:
    token = "token"
    redirect_uri = "http://127.0.0.1:53682/callback"
    code_verifier = "v" * 43

    @conf_vars({("keycloak_auth_manager", "cli_authorization_code_enabled"): "True"})
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_from_authorization_code(self, mock_get_keycloak_client, mock_get_auth_manager):
        mock_keycloak_client = Mock(spec=KeycloakOpenID)
        mock_keycloak_client.token.return_value = {"access_token": "access", "refresh_token": "refresh"}
        mock_keycloak_client.userinfo.return_value = {"sub": "sub", "preferred_username": "username"}
        mock_get_keycloak_client.return_value = mock_keycloak_client
        mock_get_auth_manager.return_value.generate_api_jwt.return_value = self.token

        token = create_token_from_authorization_code(
            "code", self.code_verifier, self.redirect_uri, expiration_time_in_seconds=10
        )

        assert token == self.token
        mock_keycloak_client.token.assert_called_once_with(
            grant_type="authorization_code",
            code="code",
            redirect_uri=self.redirect_uri,
            code_verifier=self.code_verifier,
            scope="openid",
        )
        user = mock_get_auth_manager.return_value.generate_api_jwt.call_args.args[0]
        assert (user.get_id(), user.access_token, user.refresh_token) == ("sub", "access", "refresh")
        mock_get_auth_manager.return_value.generate_api_jwt.assert_called_once_with(
            user, expiration_time_in_seconds=10
        )

    @pytest.mark.parametrize(
        ("enabled", "redirect_uri", "expected_detail"),
        [
            (
                "False",
                "http://127.0.0.1:53682/callback",
                "The authorization_code grant is not enabled for command-line clients",
            ),
            ("True", "https://evil.example.com/callback", "Invalid redirect_uri"),
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_from_authorization_code_rejected(
        self, mock_get_keycloak_client, enabled, redirect_uri, expected_detail
    ):
        with conf_vars({("keycloak_auth_manager", "cli_authorization_code_enabled"): enabled}):
            with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
                create_token_from_authorization_code("code", self.code_verifier, redirect_uri)

        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == expected_detail
        mock_get_keycloak_client.assert_not_called()

    @conf_vars({("keycloak_auth_manager", "cli_authorization_code_enabled"): "True"})
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_create_token_from_authorization_code_keycloak_error(self, mock_get_keycloak_client):
        mock_get_keycloak_client.return_value = Mock(spec=KeycloakOpenID)
        mock_get_keycloak_client.return_value.token.side_effect = KeycloakPostError("invalid_grant")

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            create_token_from_authorization_code("code", self.code_verifier, self.redirect_uri)

        assert exc_info.value.status_code == 403

    @time_machine.travel("2026-01-01 00:00:00", tick=False)
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_refresh_cli_token_keeps_original_expiry(self, mock_get_keycloak_client, mock_get_auth_manager):
        expires_at = int(time.time()) + 600
        old_token = jwt.encode({"exp": expires_at}, "secret", algorithm="HS256")
        mock_auth_manager = mock_get_auth_manager.return_value
        mock_auth_manager.get_user_from_token = AsyncMock(
            return_value=KeycloakAuthManagerUser(
                user_id="sub", name="username", access_token="old_access", refresh_token="old_refresh"
            )
        )
        mock_auth_manager.generate_api_jwt.return_value = self.token
        mock_get_keycloak_client.return_value = Mock(spec=KeycloakOpenID)
        mock_get_keycloak_client.return_value.refresh_token.return_value = {
            "access_token": "new_access",
            "refresh_token": "new_refresh",
        }

        assert refresh_cli_token(old_token) == self.token

        mock_get_keycloak_client.return_value.refresh_token.assert_called_once_with("old_refresh")
        user = mock_auth_manager.generate_api_jwt.call_args.args[0]
        assert (user.access_token, user.refresh_token) == ("new_access", "new_refresh")
        mock_auth_manager.generate_api_jwt.assert_called_once_with(user, expiration_time_in_seconds=600)

    @pytest.mark.parametrize(
        "get_user_from_token",
        [
            pytest.param(AsyncMock(side_effect=jwt.ExpiredSignatureError()), id="expired"),
            pytest.param(
                AsyncMock(side_effect=jwt.InvalidTokenError("Token has been revoked")), id="revoked"
            ),
            pytest.param(AsyncMock(return_value=None), id="no-keycloak-tokens"),
            pytest.param(
                AsyncMock(
                    return_value=KeycloakAuthManagerUser(
                        user_id="sub", name="sa", access_token="access", refresh_token=None
                    )
                ),
                id="service-account",
            ),
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_refresh_cli_token_rejected(
        self, mock_get_keycloak_client, mock_get_auth_manager, get_user_from_token
    ):
        mock_get_auth_manager.return_value.get_user_from_token = get_user_from_token

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            refresh_cli_token(self.token)

        assert exc_info.value.status_code == 403
        mock_get_keycloak_client.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.services.token.get_auth_manager")
    @patch("airflow.providers.keycloak.auth_manager.services.token.KeycloakAuthManager.get_keycloak_client")
    def test_refresh_cli_token_keycloak_session_expired(
        self, mock_get_keycloak_client, mock_get_auth_manager
    ):
        mock_get_auth_manager.return_value.get_user_from_token = AsyncMock(
            return_value=KeycloakAuthManagerUser(
                user_id="sub", name="username", access_token="access", refresh_token="refresh"
            )
        )
        mock_get_keycloak_client.return_value = Mock(spec=KeycloakOpenID)
        mock_get_keycloak_client.return_value.refresh_token.side_effect = KeycloakPostError("invalid_grant")

        with pytest.raises(fastapi.exceptions.HTTPException) as exc_info:
            refresh_cli_token(self.token)

        assert exc_info.value.status_code == 403
        mock_get_auth_manager.return_value.generate_api_jwt.assert_not_called()
