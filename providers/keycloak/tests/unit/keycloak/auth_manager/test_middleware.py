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

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi import Request
from jwt import InvalidTokenError

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.keycloak.auth_manager.constants import (
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_1_7_PLUS,
    AIRFLOW_V_3_3_PLUS,
    AIRFLOW_V_3_4_PLUS,
)

if AIRFLOW_V_3_1_7_PLUS:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
else:
    AuthManagerRefreshTokenExpiredException = None  # type: ignore[assignment,misc]


def pytest_generate_tests(metafunc):
    if "secure" in metafunc.fixturenames:
        metafunc.parametrize("secure", [True, False], indirect=True)


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Requires BaseAuthManager.get_fastapi_middewares().")
@pytest.mark.asyncio
class TestKeycloakJWTMiddleware:
    @pytest.fixture
    def middleware(self):
        from airflow.providers.keycloak.auth_manager.middleware import KeycloakJWTMiddleware

        return KeycloakJWTMiddleware(app=Mock(name="app"))

    @pytest.fixture
    def mock_request(self, secure):
        request = MagicMock(spec=Request, name="request")
        request.base_url.scheme = "https" if secure else "http"
        request.cookies = {}
        request.headers = {}
        request.state = MagicMock(name="state", spec=[])
        request.state.user = None
        del request.state.user_authenticated_via
        return request

    @pytest.fixture
    def mock_user(self):
        user = Mock(name="user", spec=KeycloakAuthManagerUser)
        user.user_id = "user_id"
        user.name = "name"
        user.access_token = "access_token"
        user.refresh_token = "refresh_token"
        return user

    @pytest.fixture
    def call_next(self):
        return AsyncMock(return_value=Mock(name="response"), name="call_next")

    @pytest.fixture
    def auth_manager(self):
        return Mock(name="auth_manager")

    @pytest.fixture
    def secure(self, request):
        return request.param

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_get_keycloak_tokens_from_cookies(
        self, mock_get_auth_manager, auth_manager, call_next, mock_request, middleware, mock_user
    ):
        auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        auth_manager.refresh_user.return_value = None
        mock_get_auth_manager.return_value = auth_manager

        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "token",
            COOKIE_NAME_ACCESS_TOKEN: "access_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }

        await middleware.dispatch(mock_request, call_next)

        assert mock_request.state.user is mock_user
        assert mock_request.state.user.access_token == "access_token"
        assert mock_request.state.user.refresh_token == "refresh_token"

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert mock_request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(mock_request.state, "user_authenticated_via")

        if AIRFLOW_V_3_4_PLUS:
            auth_manager.get_user_from_token.assert_called_once_with("token")
        else:
            auth_manager.get_user_from_token.assert_called_once_with("token", "access_token", "refresh_token")
        auth_manager.refresh_user.assert_called_once_with(user=mock_user)
        call_next.assert_awaited_once_with(mock_request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_refresh_keycloak_token(
        self,
        mock_get_auth_manager,
        auth_manager,
        call_next,
        mock_request,
        middleware,
        mock_user,
        secure,
    ):
        new_user = Mock(name="user", spec=KeycloakAuthManagerUser)
        new_user.access_token = "new_access_token"
        new_user.refresh_token = "new_refresh_token"
        auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        auth_manager.refresh_user = Mock(return_value=new_user)
        auth_manager.generate_jwt.return_value = "new_token"
        mock_get_auth_manager.return_value = auth_manager

        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "token",
            COOKIE_NAME_ACCESS_TOKEN: "access_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }

        response = await middleware.dispatch(mock_request, call_next)

        assert mock_request.state.user is new_user
        assert mock_request.state.user.access_token == "new_access_token"
        assert mock_request.state.user.refresh_token == "new_refresh_token"

        response.set_cookie.assert_any_call(
            COOKIE_NAME_JWT_TOKEN,
            "new_token",
            path="/",
            secure=secure,
            samesite="lax",
            httponly=True,
            max_age=None,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_ACCESS_TOKEN,
            "new_access_token",
            path="/",
            samesite="lax",
            secure=secure,
            httponly=True,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_REFRESH_TOKEN,
            "new_refresh_token",
            path="/",
            samesite="lax",
            secure=secure,
            httponly=True,
        )

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert mock_request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(mock_request.state, "user_authenticated_via")

        if AIRFLOW_V_3_4_PLUS:
            auth_manager.get_user_from_token.assert_called_once_with("token")
        else:
            auth_manager.get_user_from_token.assert_called_once_with("token", "access_token", "refresh_token")
        auth_manager.refresh_user.assert_called_once_with(user=mock_user)
        auth_manager.generate_jwt.assert_called_once_with(new_user)
        call_next.assert_awaited_once_with(mock_request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_keycloak_token(
        self, mock_get_auth_manager, auth_manager, call_next, middleware, mock_request, secure
    ):
        mock_get_auth_manager.return_value = auth_manager

        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "token"}

        response = await middleware.dispatch(mock_request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert mock_request.state.user is None

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(mock_request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(mock_request.state, "user_authenticated_via")

        call_next.assert_awaited_with(mock_request)

        response.delete_cookie.assert_any_call(
            COOKIE_NAME_JWT_TOKEN,
            path="/",
            secure=secure,
            httponly=True,
            samesite="lax",
        )

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_airflow_jwt_token(
        self, mock_get_auth_manager, auth_manager, call_next, middleware, mock_request
    ):
        mock_get_auth_manager.return_value = auth_manager

        mock_request.cookies = {
            COOKIE_NAME_ACCESS_TOKEN: "access_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }

        response = await middleware.dispatch(mock_request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert mock_request.state.user is None

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(mock_request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(mock_request.state, "user_authenticated_via")

        call_next.assert_awaited_once_with(mock_request)
        response.set_cookie.assert_not_called()
        response.delete_cookie.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    @pytest.mark.asyncio
    async def test_dispatch_expired_token(
        self,
        mock_get_auth_manager,
        auth_manager,
        call_next,
        middleware,
        mock_request,
        secure,
    ):
        mock_get_auth_manager.return_value = auth_manager
        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "invalid_token",
            COOKIE_NAME_ACCESS_TOKEN: "access_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }
        auth_manager.get_user_from_token.side_effect = InvalidTokenError()

        response = await middleware.dispatch(mock_request, call_next)

        call_next.assert_called_once_with(mock_request)
        if AIRFLOW_V_3_4_PLUS:
            auth_manager.get_user_from_token.assert_called_once_with("invalid_token")
        else:
            auth_manager.get_user_from_token.assert_called_once_with(
                "invalid_token", "access_token", "refresh_token"
            )

        response.delete_cookie.assert_any_call(
            COOKIE_NAME_JWT_TOKEN,
            path="/",
            secure=secure,
            httponly=True,
            samesite="lax",
        )

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    @pytest.mark.asyncio
    async def test_dispatch_expired_keycloak_token(
        self,
        mock_get_auth_manager,
        auth_manager,
        call_next,
        middleware,
        mock_request,
        mock_user,
        secure,
    ):
        mock_get_auth_manager.return_value = auth_manager
        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "token",
            COOKIE_NAME_ACCESS_TOKEN: "expired_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }
        mock_user.access_token = "expired_token"
        mock_user.refresh_token = "refresh_token"
        auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        if AIRFLOW_V_3_1_7_PLUS:
            auth_manager.refresh_user.side_effect = AuthManagerRefreshTokenExpiredException()
        else:
            auth_manager.refresh_user.return_value = None

        response = await middleware.dispatch(mock_request, call_next)

        call_next.assert_called_once_with(mock_request)
        if AIRFLOW_V_3_4_PLUS:
            auth_manager.get_user_from_token.assert_called_once_with("token")
        else:
            auth_manager.get_user_from_token.assert_called_once_with(
                "token", "expired_token", "refresh_token"
            )
        auth_manager.refresh_user.assert_called_once_with(user=mock_user)

        if AIRFLOW_V_3_1_7_PLUS:
            response.delete_cookie.assert_any_call(
                COOKIE_NAME_JWT_TOKEN,
                path="/",
                secure=secure,
                httponly=True,
                samesite="lax",
            )
        auth_manager.generate_jwt.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_dispatch_does_not_clear_fresh_token_set_by_endpoint(
        self,
        mock_get_auth_manager,
        auth_manager,
        call_next,
        middleware,
        mock_request,
        mock_user,
    ):
        """
        An expired token on the request must not clear the cookie when the endpoint
        set a fresh one on the response (e.g. the login callback exchanging the
        authorization code while an expired token is still in the cookie jar).
        """
        mock_get_auth_manager.return_value = auth_manager
        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "expired",
            COOKIE_NAME_ACCESS_TOKEN: "expired_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }
        auth_manager.get_user_from_token = AsyncMock(side_effect=InvalidTokenError())

        async def call_endpoint(request):
            # The endpoint (login callback) mints a fresh JWT and signals it
            request.state.jwt_token_issued = True
            return Mock(name="response")

        call_next.side_effect = call_endpoint

        response = await middleware.dispatch(mock_request, call_next)

        response.set_cookie.assert_not_called()

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_cookie_path")
    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    @pytest.mark.asyncio
    async def test_dispatch_invalid_token_clears_root_cookie(
        self,
        mock_get_auth_manager,
        mock_get_cookie_path,
        auth_manager,
        call_next,
        middleware,
        mock_request,
        secure,
    ):
        mock_get_cookie_path.return_value = "/foo/"
        mock_get_auth_manager.return_value = auth_manager
        auth_manager.get_user_from_token.side_effect = InvalidTokenError()
        """When a stale _token exists at root path, clearing must target both the subpath and root."""
        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "stale_root_token",
            COOKIE_NAME_ACCESS_TOKEN: "access_token",
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }
        response = await middleware.dispatch(mock_request, call_next)

        # Expect two delete cookies: one at the subpath and one at root "/"
        response.delete_cookie.assert_any_call(
            "_token",
            path="/foo/",
            secure=secure,
            samesite="lax",
            httponly=True,
        )
        response.delete_cookie.assert_any_call(
            "_token",
            path="/",
            secure=secure,
            samesite="lax",
            httponly=True,
        )
