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

from unittest.mock import AsyncMock, Mock, patch

import pytest

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.keycloak.auth_manager.constants import (
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

if AIRFLOW_V_3_4_PLUS:
    from airflow.providers.keycloak.auth_manager.middleware import KeycloakJWTRefreshMiddleware
else:
    from airflow.providers.keycloak.auth_manager.middleware import (
        KeycloakJWTMiddleware as KeycloakJWTRefreshMiddleware,
    )


@pytest.mark.skipif(AIRFLOW_V_3_4_PLUS, reason="Old version of KeycloakJWTMiddleware.")
@pytest.mark.asyncio
class TestKeycloakJWTMiddleware:
    def _make_request(self, headers: dict | None = None, cookies: dict | None = None) -> Mock:
        request = Mock()
        request.headers = headers or {}
        request.cookies = cookies or {}
        request.state = Mock(spec=[])
        return request

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_get_keycloak_tokens_from_cookies(self, mock_get_auth_manager):
        user = Mock(name="user")
        user.access_token = "access_token"
        user.refresh_token = "refresh_token"
        auth_manager = Mock()
        auth_manager.get_user_from_token = AsyncMock(return_value=user)
        auth_manager.refresh_user.return_value = None
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            }
        )
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        assert request.state.user is user
        assert request.state.user.access_token == "access_token"
        assert request.state.user.refresh_token == "refresh_token"

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        auth_manager.get_user_from_token.assert_called_once_with("token", "access_token", "refresh_token")
        auth_manager.refresh_user.assert_called_once_with(user=user)
        call_next.assert_awaited_once_with(request)

    @pytest.mark.parametrize(("scheme", "secure"), [("http", False), ("https", True)])
    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_refresh_keycloak_token(self, mock_get_auth_manager, scheme, secure):
        user = Mock(name="user")
        new_user = Mock(name="user")
        new_user.access_token = "new_access_token"
        new_user.refresh_token = "new_refresh_token"
        auth_manager = Mock()
        auth_manager.get_user_from_token = AsyncMock(return_value=user)
        auth_manager.refresh_user = Mock(return_value=new_user)
        auth_manager.generate_jwt.return_value = "new_token"
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            }
        )
        request.base_url.scheme = scheme
        call_next = AsyncMock(return_value=Mock(name="response"))

        response = await middleware.dispatch(request, call_next)

        assert request.state.user is new_user
        assert request.state.user.access_token == "new_access_token"
        assert request.state.user.refresh_token == "new_refresh_token"

        response.set_cookie.assert_any_call(
            COOKIE_NAME_JWT_TOKEN,
            "new_token",
            path="/",
            secure=secure,
            httponly=True,
            max_age=None,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_ACCESS_TOKEN,
            "new_access_token",
            path="/",
            secure=secure,
            httponly=True,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_REFRESH_TOKEN,
            "new_refresh_token",
            path="/",
            secure=secure,
            httponly=True,
        )

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        auth_manager.get_user_from_token.assert_called_once_with("token", "access_token", "refresh_token")
        auth_manager.refresh_user.assert_called_once_with(user=user)
        auth_manager.generate_jwt.assert_called_once_with(new_user)
        call_next.assert_awaited_once_with(request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_keycloak_token(self, mock_get_auth_manager):
        auth_manager = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(cookies={COOKIE_NAME_JWT_TOKEN: "token"})

        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert not hasattr(request.state, "user")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        call_next.assert_awaited_once_with(request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_airflow_jwt_token(self, mock_get_auth_manager):
        auth_manager = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={COOKIE_NAME_ACCESS_TOKEN: "access_token", COOKIE_NAME_REFRESH_TOKEN: "refresh_token"}
        )

        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert not hasattr(request.state, "user")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        call_next.assert_awaited_once_with(request)


@pytest.mark.skipif(
    not AIRFLOW_V_3_4_PLUS,
    reason="KeycloakJWTRefreshMiddleware uses KeycloakAuthManager.get_jwt_refresh_middleware().",
)
@pytest.mark.asyncio
class TestKeycloakJWTRefreshMiddleware:
    def _make_request(self, headers: dict | None = None, cookies: dict | None = None) -> Mock:
        request = Mock()
        request.headers = headers or {}
        request.cookies = cookies or {}
        request.state = Mock(spec=[])
        return request

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_get_keycloak_tokens_from_cookies(self, mock_get_auth_manager):
        user = Mock(name="user")
        user.access_token = "access_token"
        user.refresh_token = "refresh_token"
        auth_manager = Mock()
        auth_manager.get_user_from_token = AsyncMock(return_value=user)
        auth_manager.refresh_user.return_value = None
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            }
        )
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        assert request.state.user is user
        assert request.state.user.access_token == "access_token"
        assert request.state.user.refresh_token == "refresh_token"

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        auth_manager.get_user_from_token.assert_called_once_with("token")
        auth_manager.refresh_user.assert_called_once_with(user=user)
        call_next.assert_awaited_once_with(request)

    @pytest.mark.parametrize(("scheme", "secure"), [("http", False), ("https", True)])
    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_refresh_keycloak_token(self, mock_get_auth_manager, scheme, secure):
        user = Mock(name="user")
        new_user = Mock(name="user")
        new_user.access_token = "new_access_token"
        new_user.refresh_token = "new_refresh_token"
        auth_manager = Mock()
        auth_manager.get_user_from_token = AsyncMock(return_value=user)
        auth_manager.refresh_user = Mock(return_value=new_user)
        auth_manager.generate_jwt.return_value = "new_token"
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            }
        )
        request.base_url.scheme = scheme
        call_next = AsyncMock(return_value=Mock(name="response"))

        response = await middleware.dispatch(request, call_next)

        assert request.state.user is new_user
        assert request.state.user.access_token == "new_access_token"
        assert request.state.user.refresh_token == "new_refresh_token"

        response.set_cookie.assert_any_call(
            COOKIE_NAME_JWT_TOKEN,
            "new_token",
            path="/",
            secure=secure,
            httponly=True,
            max_age=None,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_ACCESS_TOKEN,
            "new_access_token",
            path="/",
            secure=secure,
            httponly=True,
        )
        response.set_cookie.assert_any_call(
            COOKIE_NAME_REFRESH_TOKEN,
            "new_refresh_token",
            path="/",
            secure=secure,
            httponly=True,
        )

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert request.state.user_authenticated_via is trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        auth_manager.get_user_from_token.assert_called_once_with("token")
        auth_manager.refresh_user.assert_called_once_with(user=user)
        auth_manager.generate_jwt.assert_called_once_with(new_user)
        call_next.assert_awaited_once_with(request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_keycloak_token(self, mock_get_auth_manager):
        auth_manager = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(cookies={COOKIE_NAME_JWT_TOKEN: "token"})

        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert not hasattr(request.state, "user")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        call_next.assert_awaited_once_with(request)

    @patch("airflow.providers.keycloak.auth_manager.middleware.get_auth_manager")
    async def test_no_airflow_jwt_token(self, mock_get_auth_manager):
        auth_manager = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = KeycloakJWTRefreshMiddleware(app=Mock())
        request = self._make_request(
            cookies={COOKIE_NAME_ACCESS_TOKEN: "access_token", COOKIE_NAME_REFRESH_TOKEN: "refresh_token"}
        )

        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.get_user_from_token.assert_not_called()
        auth_manager.refresh_user.assert_not_called()

        assert not hasattr(request.state, "user")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        if trusted_marker is not None:
            assert getattr(request.state, "user_authenticated_via", None) is not trusted_marker
        else:
            assert not hasattr(request.state, "user_authenticated_via")

        call_next.assert_awaited_once_with(request)
