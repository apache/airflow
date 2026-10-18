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
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlencode

import jwt
import pytest
import time_machine
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.models.crypto import _NullFernet
from airflow.models.revoked_token import RevokedToken

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_revoked_tokens

AUTH_MANAGER_LOGIN_URL = "http://some_login_url"
AUTH_MANAGER_LOGOUT_URL = "http://some_logout_url"
SUBPATH = "/team-a/"

pytestmark = pytest.mark.db_test


class TestAuthEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self, test_client) -> None:
        auth_manager_mock = MagicMock()
        auth_manager_mock.get_url_login.return_value = AUTH_MANAGER_LOGIN_URL
        auth_manager_mock.get_url_logout.return_value = AUTH_MANAGER_LOGOUT_URL
        test_client.app.state.auth_manager = auth_manager_mock


class TestGetLogin(TestAuthEndpoint):
    @pytest.mark.parametrize(
        "params",
        [
            {},
            {"next": None},
            {"next": "http://localhost:8080"},
            {"next": "http://localhost:8080", "other_param": "something_else"},
        ],
    )
    @patch("airflow.api_fastapi.core_api.routes.public.auth.is_safe_url", return_value=True)
    def test_should_respond_307(self, mock_is_safe_url, test_client, params):
        response = test_client.get("/auth/login", follow_redirects=False, params=params)

        assert response.status_code == 307
        assert (
            response.headers["location"]
            == f"{AUTH_MANAGER_LOGIN_URL}?{urlencode({'next': params.get('next')})}"
            if params.get("next")
            else AUTH_MANAGER_LOGIN_URL
        )

    @patch("airflow.api_fastapi.core_api.routes.public.auth.is_safe_url", return_value=True)
    def test_next_url_correctly_encoded(self, mock_is_safe_url, test_client):
        """Test that special characters in the next URL are correctly url encoded."""
        next_url = "http://localhost:8080/dags/my_dag/runs/manual__2026-03-02T12:45:13.113989+00:00/tasks/t"
        response = test_client.get("/auth/login", follow_redirects=False, params={"next": next_url})

        assert response.status_code == 307
        location = response.headers["location"]
        query_string = location.split("?", 1)[1]
        # Literals that have special meaning in query strings must be url encoded.
        assert "+" not in query_string
        assert ":" not in query_string
        assert "/" not in query_string
        # Decoding the query string must recover the original URL unchanged.
        assert parse_qs(query_string)["next"][0] == next_url

    @pytest.mark.parametrize(
        "params",
        [
            {"next": "http://fake_domain.com:8080"},
            {"next": "http://localhost:8080/../../up"},
        ],
    )
    @conf_vars({("api", "base_url"): "http://localhost:8080/prefix"})
    def test_should_respond_400(self, test_client, params):
        response = test_client.get("/auth/login", follow_redirects=False, params=params)

        assert response.status_code == 400


class TestLogout(TestAuthEndpoint):
    @pytest.fixture(autouse=True)
    def cleanup_revoked_tokens(self):
        clear_db_revoked_tokens()
        yield
        clear_db_revoked_tokens()

    @pytest.mark.parametrize(
        ("mock_logout_url", "expected_redirection", "delete_cookies"),
        [
            # logout_url is None, should redirect to the login page directly.
            (None, AUTH_MANAGER_LOGIN_URL, True),
            # logout_url is defined, should redirect to the logout_url.
            ("http://localhost/auth/some_logout_url", "http://localhost/auth/some_logout_url", False),
        ],
    )
    def test_should_respond_307(
        self,
        test_client,
        mock_logout_url,
        expected_redirection,
        delete_cookies,
    ):
        test_client.app.state.auth_manager.get_url_logout.return_value = mock_logout_url
        response = test_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == expected_redirection

        if delete_cookies:
            cookies = response.headers.get_list("set-cookie")
            assert any(f"{COOKIE_NAME_JWT_TOKEN}=" in c for c in cookies)

    def test_logout_with_invalid_token_does_not_raise(self, test_client):
        """Test that logout with an invalid token does not raise."""
        test_client.app.state.auth_manager.get_url_logout.return_value = None

        test_client.cookies.set(COOKIE_NAME_JWT_TOKEN, "not-a-valid-jwt")

        response = test_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307

    @patch("airflow.api_fastapi.core_api.routes.public.auth.get_cookie_path", return_value=SUBPATH)
    def test_logout_cookie_uses_subpath(self, mock_cookie_path, test_client):
        """Cookies must use the subpath so they are scoped to the correct instance."""
        test_client.app.state.auth_manager.get_url_logout.return_value = None

        response = test_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        cookies = response.headers.get_list("set-cookie")
        token_cookie = next(c for c in cookies if f"{COOKIE_NAME_JWT_TOKEN}=" in c and f"Path={SUBPATH}" in c)
        assert f"Path={SUBPATH}" in token_cookie
        # Stale root-path cookie must also be cleared
        assert any(
            f"{COOKIE_NAME_JWT_TOKEN}=" in c and "Path=/" in c and f"Path={SUBPATH}" not in c for c in cookies
        )


class TestLogoutTokenRevocation:
    """Tests for token revocation on logout, using real DB queries without mocks."""

    pytestmark = [pytest.mark.db_test]

    @pytest.fixture(autouse=True)
    def cleanup_revoked_tokens(self):
        clear_db_revoked_tokens()
        yield
        clear_db_revoked_tokens()

    @pytest.fixture
    def logout_app(self):
        from airflow.api_fastapi.app import create_app

        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
            }
        ):
            yield create_app()

    @pytest.fixture
    def logout_client(self, logout_app):
        """A test client without the is_revoked mock so revocation tests hit the real DB."""
        with TestClient(logout_app, base_url="http://testserver/api/v2") as client:
            yield client

    def test_logout_revokes_token(self, logout_client):
        """Test that logout revokes the JWT token and persists it in the database."""
        now = int(time.time())
        auth_manager = logout_client.app.state.auth_manager
        signer = auth_manager._get_token_signer()
        token_payload = {
            "sub": "admin",
            "jti": "test-jti-123",
            "exp": now + 3600,
            "iat": now,
            "nbf": now,
            "aud": "apache-airflow",
            # Include the signer's configured issuer so the validator (which
            # reads `[api_auth] jwt_issuer` from the same config) does not
            # reject the synthetic token for missing iss.
            "iss": signer.issuer,
        }
        token_str = jwt.encode(token_payload, signer._secret_key, algorithm=signer.algorithm)

        logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, token_str)
        with patch.object(auth_manager, "get_url_logout", return_value=None):
            response = logout_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert RevokedToken.is_revoked("test-jti-123") is True

    def test_logout_without_cookie_does_not_revoke(self, logout_client):
        """Test that logout without a cookie does not attempt to revoke."""
        auth_manager = logout_client.app.state.auth_manager
        with patch.object(auth_manager, "get_url_logout", return_value=None):
            response = logout_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert RevokedToken.is_revoked("nonexistent-jti") is False

    @staticmethod
    def _mint(auth_manager, jti: str) -> str:
        now = int(time.time())
        signer = auth_manager._get_token_signer()
        return jwt.encode(
            {
                "sub": "admin",
                "jti": jti,
                "exp": now + 3600,
                "iat": now,
                "nbf": now,
                "aud": "apache-airflow",
                "iss": signer.issuer,
            },
            signer._secret_key,
            algorithm=signer.algorithm,
        )

    def test_logout_revokes_a_bearer_token(self, logout_client):
        """A bearer-only logout must revoke the token it presented.

        Clients calling the API authenticate with `Authorization: Bearer`, and there is
        no cookie on such a request. Logout previously read only the cookie, so it
        returned its normal response while revoking nothing and the token stayed valid
        until it expired.
        """
        auth_manager = logout_client.app.state.auth_manager
        token_str = self._mint(auth_manager, "test-jti-bearer")

        with patch.object(auth_manager, "get_url_logout", return_value=None):
            response = logout_client.get(
                "/auth/logout",
                headers={"Authorization": f"Bearer {token_str}"},
                follow_redirects=False,
            )

        assert response.status_code == 307
        assert RevokedToken.is_revoked("test-jti-bearer") is True

    def test_logout_revokes_every_credential_presented(self, logout_client):
        """Both a bearer and a cookie presented together must both be revoked.

        Revoking only the precedence-selected one would leave the other valid after the
        caller asked to be logged out.
        """
        auth_manager = logout_client.app.state.auth_manager
        bearer_token = self._mint(auth_manager, "test-jti-both-bearer")
        cookie_token = self._mint(auth_manager, "test-jti-both-cookie")

        logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, cookie_token)
        with patch.object(auth_manager, "get_url_logout", return_value=None):
            response = logout_client.get(
                "/auth/logout",
                headers={"Authorization": f"Bearer {bearer_token}"},
                follow_redirects=False,
            )

        assert response.status_code == 307
        assert RevokedToken.is_revoked("test-jti-both-bearer") is True
        assert RevokedToken.is_revoked("test-jti-both-cookie") is True

    def test_logout_revokes_both_even_when_a_trusted_user_is_cached(self, logout_app):
        """The trusted-middleware shortcut must not change what logout revokes.

        On protected routes `get_user()` can return a user cached by JWTRefreshMiddleware
        without consulting the request's credentials at all. Logout must still act on the
        credentials actually presented, so revocation cannot be skipped by that shortcut.
        """
        from airflow.api_fastapi.core_api.security import USER_INJECTED_BY_TRUSTED_MIDDLEWARE

        auth_manager = logout_app.state.auth_manager
        bearer_token = self._mint(auth_manager, "test-jti-trusted-bearer")
        cookie_token = self._mint(auth_manager, "test-jti-trusted-cookie")

        async def _inject(request, call_next):
            request.state.user = object()
            request.state.user_authenticated_via = USER_INJECTED_BY_TRUSTED_MIDDLEWARE
            return await call_next(request)

        logout_app.middleware("http")(_inject)
        with (
            TestClient(logout_app, base_url="http://testserver/api/v2") as logout_client,
            patch.object(auth_manager, "get_url_logout", return_value=None),
        ):
            logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, cookie_token)
            response = logout_client.get(
                "/auth/logout",
                headers={"Authorization": f"Bearer {bearer_token}"},
                follow_redirects=False,
            )

        assert response.status_code == 307
        assert RevokedToken.is_revoked("test-jti-trusted-bearer") is True
        assert RevokedToken.is_revoked("test-jti-trusted-cookie") is True

    def test_logout_revokes_a_bearer_token_before_an_external_redirect(self, logout_client):
        """The bearer must be revoked even when the auth manager redirects away."""
        auth_manager = logout_client.app.state.auth_manager
        token_str = self._mint(auth_manager, "test-jti-bearer-redirect")

        with patch.object(auth_manager, "get_url_logout", return_value="https://idp.example/logout"):
            response = logout_client.get(
                "/auth/logout",
                headers={"Authorization": f"Bearer {token_str}"},
                follow_redirects=False,
            )

        assert response.status_code == 307
        assert RevokedToken.is_revoked("test-jti-bearer-redirect") is True

    def test_logout_revokes_token_when_logout_url_redirects(self, logout_client):
        """Token must be revoked before the redirect when get_url_logout returns a URL."""
        now = int(time.time())
        auth_manager = logout_client.app.state.auth_manager
        signer = auth_manager._get_token_signer()
        token_payload = {
            "sub": "admin",
            "jti": "test-jti-redirect-456",
            "exp": now + 3600,
            "iat": now,
            "nbf": now,
            "aud": "apache-airflow",
            "iss": signer.issuer,
        }
        token_str = jwt.encode(token_payload, signer._secret_key, algorithm=signer.algorithm)

        logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, token_str)
        with patch.object(auth_manager, "get_url_logout", return_value="http://external/logout"):
            response = logout_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == "http://external/logout"
        assert RevokedToken.is_revoked("test-jti-redirect-456") is True

    def test_logout_clears_cookie_when_logout_url_redirects(self, logout_client):
        """The local session cookie is cleared on the external-logout redirect too, not only revoked."""
        auth_manager = logout_client.app.state.auth_manager
        token_str = self._mint(auth_manager, "test-jti-redirect-cookie")

        logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, token_str)
        # The refresh middleware is stubbed out so that only the logout route can clear the cookie.
        with (
            patch(
                "airflow.api_fastapi.auth.middlewares.refresh_token.JWTRefreshMiddleware._refresh_user",
                new=AsyncMock(return_value=(None, None)),
            ),
            patch.object(auth_manager, "get_url_logout", return_value="http://external/logout"),
        ):
            response = logout_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == "http://external/logout"
        cookies = response.headers.get_list("set-cookie")
        assert any(c.startswith(f"{COOKIE_NAME_JWT_TOKEN}=") and "Max-Age=0" in c for c in cookies)

    def test_logout_clears_cookie_when_revocation_cannot_be_recorded(self, logout_client):
        """A failed revocation write does not leave the browser session in place."""
        from sqlalchemy.exc import SQLAlchemyError

        auth_manager = logout_client.app.state.auth_manager
        token_str = self._mint(auth_manager, "test-jti-revoke-fails")

        logout_client.cookies.set(COOKIE_NAME_JWT_TOKEN, token_str)
        with (
            patch(
                "airflow.api_fastapi.auth.middlewares.refresh_token.JWTRefreshMiddleware._refresh_user",
                new=AsyncMock(return_value=(None, None)),
            ),
            patch.object(auth_manager, "get_url_logout", return_value="http://external/logout"),
            patch("airflow.models.revoked_token.RevokedToken.revoke", side_effect=SQLAlchemyError("db down")),
        ):
            response = logout_client.get("/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        cookies = response.headers.get_list("set-cookie")
        assert any(c.startswith(f"{COOKIE_NAME_JWT_TOKEN}=") and "Max-Age=0" in c for c in cookies)


class TestCreateCliToken:
    """The exchange runs unauthenticated and records single use in the real database."""

    # Test vector from RFC 7636, appendix B.
    code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    code_challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    port = 53682

    @pytest.fixture(autouse=True)
    def cleanup_revoked_tokens(self):
        clear_db_revoked_tokens()
        yield
        clear_db_revoked_tokens()

    def mint_token(self, client) -> str:
        from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
        from airflow.configuration import conf

        return client.app.state.auth_manager.generate_api_jwt(
            SimpleAuthManagerUser(username="test", role="admin"),
            expiration_time_in_seconds=conf.getint("api_auth", "jwt_cli_expiration_time"),
        )

    def issue_code(self, client, cli_token: str | None = None, token_expires_at: int | None = None) -> str:
        from airflow.api_fastapi.auth.cli_authorization_code import (
            build_loopback_redirect_uri,
            issue_cli_authorization_code,
        )
        from airflow.configuration import conf

        return issue_cli_authorization_code(
            cli_token=cli_token or self.mint_token(client),
            token_expires_at=token_expires_at
            or int(time.time()) + conf.getint("api_auth", "jwt_cli_expiration_time"),
            user_name="test",
            code_challenge=self.code_challenge,
            redirect_uri=build_loopback_redirect_uri(self.port),
        )

    def exchange(self, client, code: str, **overrides):
        body = {"code": code, "code_verifier": self.code_verifier, "port": self.port, **overrides}
        return client.post("/auth/cli/token", json=body)

    def test_exchanges_a_code_for_the_cli_token_it_carries(self, unauthenticated_test_client):
        from airflow.configuration import conf

        with time_machine.travel(datetime.now(tz=UTC), tick=False):
            token = self.mint_token(unauthenticated_test_client)
            code = self.issue_code(unauthenticated_test_client, cli_token=token)
            response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 200
        data = response.json()
        assert data["access_token"] == token
        cli_expiration = conf.getint("api_auth", "jwt_cli_expiration_time")
        assert data["expires_in_seconds"] == cli_expiration
        auth_manager = unauthenticated_test_client.app.state.auth_manager
        claims = auth_manager._get_token_validator().validated_claims(data["access_token"])
        assert claims["sub"] == "test"
        assert claims["role"] == "admin"

    def test_reports_how_long_the_token_is_still_good_for(self, unauthenticated_test_client):
        approved_at = datetime.now(tz=UTC)
        with time_machine.travel(approved_at, tick=False):
            code = self.issue_code(
                unauthenticated_test_client, token_expires_at=int(approved_at.timestamp()) + 100
            )

        with time_machine.travel(approved_at + timedelta(seconds=40), tick=False):
            response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 200
        assert response.json()["expires_in_seconds"] == 60

    def test_rejects_a_code_whose_token_has_already_expired(self, unauthenticated_test_client):
        """A CLI lifetime shorter than the code's plus a late exchange must not report success."""
        code = self.issue_code(unauthenticated_test_client, token_expires_at=int(time.time()) - 1)

        response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 400
        assert response.json() == {"detail": "The approved token has already expired"}

    def test_code_approved_through_the_ui_endpoint_is_exchangeable(
        self, test_client, unauthenticated_test_client
    ):
        approval = test_client.post(
            "http://testserver/ui/auth/cli/authorize",
            json={"code_challenge": self.code_challenge, "code_challenge_method": "S256", "port": self.port},
        )
        assert approval.status_code == 200

        response = self.exchange(unauthenticated_test_client, approval.json()["code"])

        assert response.status_code == 200
        auth_manager = unauthenticated_test_client.app.state.auth_manager
        claims = auth_manager._get_token_validator().validated_claims(response.json()["access_token"])
        assert claims["sub"] == "test"

    def test_code_works_only_once(self, unauthenticated_test_client):
        code = self.issue_code(unauthenticated_test_client)
        assert self.exchange(unauthenticated_test_client, code).status_code == 200

        response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 400
        assert response.json() == {"detail": "The authorization code has already been used"}
        assert RevokedToken.is_revoked(jwt.decode(code, options={"verify_signature": False})["jti"])

    def test_consumption_record_outlives_the_leeway(self, unauthenticated_test_client):
        """The code stays valid for jwt_leeway past exp, so cleanup must not drop the record before then."""
        from airflow.configuration import conf
        from airflow.utils.session import create_session

        code = self.issue_code(unauthenticated_test_client)
        assert self.exchange(unauthenticated_test_client, code).status_code == 200

        claims = jwt.decode(code, options={"verify_signature": False})
        with create_session() as session:
            recorded_until = session.get(RevokedToken, claims["jti"]).exp
        assert recorded_until == datetime.fromtimestamp(claims["exp"], tz=UTC) + timedelta(
            seconds=conf.getint("api_auth", "jwt_leeway")
        )

    def test_rejects_a_verifier_that_does_not_match(self, unauthenticated_test_client):
        response = self.exchange(
            unauthenticated_test_client,
            self.issue_code(unauthenticated_test_client),
            code_verifier="not-the-verifier-not-the-verifier-not-the-verifier",
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "The code verifier does not match the authorization code"}

    def test_rejects_a_port_the_code_was_not_issued_for(self, unauthenticated_test_client):
        response = self.exchange(
            unauthenticated_test_client, self.issue_code(unauthenticated_test_client), port=self.port + 1
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "The authorization code was not issued for this port"}

    def test_rejects_an_expired_code(self, unauthenticated_test_client):
        from airflow.api_fastapi.auth.cli_authorization_code import CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS

        code = self.issue_code(unauthenticated_test_client)

        with time_machine.travel(timedelta(seconds=CLI_AUTHORIZATION_CODE_LIFETIME_SECONDS + 30), tick=False):
            response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 400
        assert response.json() == {"detail": "The authorization code has expired"}

    def test_rejects_an_api_token_presented_as_a_code(self, unauthenticated_test_client):
        auth_manager = unauthenticated_test_client.app.state.auth_manager
        api_token = auth_manager._get_token_signer().generate({"sub": "test", "role": "admin"})

        response = self.exchange(unauthenticated_test_client, api_token)

        assert response.status_code == 400
        assert response.json() == {"detail": "The authorization code is not valid"}

    def test_rejects_a_code_when_the_fernet_key_is_gone(self, unauthenticated_test_client):
        code = self.issue_code(unauthenticated_test_client)

        with patch(
            "airflow.api_fastapi.auth.cli_authorization_code.get_fernet",
            autospec=True,
            return_value=_NullFernet(),
        ):
            response = self.exchange(unauthenticated_test_client, code)

        assert response.status_code == 400
        assert "[core] fernet_key" in response.json()["detail"]

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({"code_verifier": "too-short"}, id="malformed-verifier"),
            pytest.param({"port": 80}, id="privileged-port"),
        ],
    )
    def test_rejects_requests_airflowctl_cannot_have_made(self, unauthenticated_test_client, overrides):
        response = self.exchange(
            unauthenticated_test_client, self.issue_code(unauthenticated_test_client), **overrides
        )

        assert response.status_code == 422
