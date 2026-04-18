#
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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, Request, Response

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware


class TestJWTRefreshMiddleware:
    @pytest.fixture
    def middleware(self):
        return JWTRefreshMiddleware(app=MagicMock())

    @pytest.fixture
    def mock_request(self):
        request = MagicMock(spec=Request)
        request.cookies = {}
        request.state = MagicMock()
        return request

    @pytest.fixture
    def mock_user(self):
        return MagicMock(spec=BaseUser)

    @patch.object(JWTRefreshMiddleware, "_refresh_user")
    @pytest.mark.asyncio
    async def test_dispatch_no_token(self, mock_refresh_user, middleware, mock_request):
        call_next = AsyncMock(return_value=Response())

        await middleware.dispatch(mock_request, call_next)

        call_next.assert_called_once_with(mock_request)
        mock_refresh_user.assert_not_called()

    @patch.object(
        JWTRefreshMiddleware,
        "_refresh_user",
        side_effect=HTTPException(status_code=403, detail="Invalid JWT token"),
    )
    @pytest.mark.asyncio
    async def test_dispatch_invalid_token(self, mock_refresh_user, middleware, mock_request):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token"}
        call_next = AsyncMock(return_value=Response(status_code=401))

        response = await middleware.dispatch(mock_request, call_next)
        assert response.status_code == 401
        assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers.get("set-cookie")

    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_dispatch_no_refreshed_token(
        self, mock_resolve_user_from_token, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token"}
        mock_resolve_user_from_token.return_value = mock_user
        mock_auth_manager = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.refresh_user.return_value = None

        call_next = AsyncMock(return_value=Response())
        await middleware.dispatch(mock_request, call_next)

        call_next.assert_called_once_with(mock_request)
        mock_resolve_user_from_token.assert_called_once_with("valid_token")
        mock_auth_manager.generate_jwt.assert_not_called()

    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_dispatch_expired_token(self, mock_resolve_user_from_token, middleware, mock_request):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "invalid_token"}
        mock_resolve_user_from_token.side_effect = HTTPException(status_code=403)

        call_next = AsyncMock(return_value=Response())
        await middleware.dispatch(mock_request, call_next)

        call_next.assert_called_once_with(mock_request)
        mock_resolve_user_from_token.assert_called_once_with("invalid_token")

    @pytest.mark.asyncio
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.resolve_user_from_token")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.conf")
    async def test_dispatch_with_refreshed_user(
        self,
        mock_conf,
        mock_resolve_user_from_token,
        mock_get_auth_manager,
        middleware,
        mock_request,
        mock_user,
    ):
        refreshed_user = MagicMock(spec=BaseUser)
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token"}
        mock_resolve_user_from_token.return_value = mock_user
        mock_auth_manager = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        mock_conf.get.return_value = ""

        call_next = AsyncMock(return_value=Response())
        response = await middleware.dispatch(mock_request, call_next)

        assert mock_request.state.user == refreshed_user
        call_next.assert_called_once_with(mock_request)
        mock_resolve_user_from_token.assert_called_once_with("valid_token")
        mock_auth_manager.refresh_user.assert_called_once_with(user=mock_user)
        mock_auth_manager.generate_jwt.assert_called_once_with(refreshed_user)
        set_cookie_headers = response.headers.get("set-cookie", "")
        assert f"{COOKIE_NAME_JWT_TOKEN}=new_token" in set_cookie_headers

    @pytest.mark.parametrize(
        ("scheme", "ssl_cert", "expected_secure"),
        [
            pytest.param("https", "", True, id="https-no-local-ssl-cert"),
            pytest.param("http", "/etc/ssl/cert.pem", True, id="http-with-local-ssl-cert"),
            pytest.param("https", "/etc/ssl/cert.pem", True, id="https-with-local-ssl-cert"),
            pytest.param("http", "", False, id="http-no-local-ssl-cert"),
        ],
    )
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.resolve_user_from_token")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.conf")
    @pytest.mark.asyncio
    async def test_dispatch_cookie_secure_flag(
        self,
        mock_conf,
        mock_resolve_user_from_token,
        mock_get_auth_manager,
        middleware,
        mock_request,
        mock_user,
        scheme,
        ssl_cert,
        expected_secure,
    ):
        """The cookie Secure flag must follow the request scheme as well as the local ssl_cert."""
        refreshed_user = MagicMock(spec=BaseUser)
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token"}
        mock_request.base_url = MagicMock(scheme=scheme)
        mock_resolve_user_from_token.return_value = mock_user
        mock_auth_manager = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        mock_conf.get.return_value = ssl_cert

        call_next = AsyncMock(return_value=Response())
        response = await middleware.dispatch(mock_request, call_next)

        set_cookie_headers = response.headers.get("set-cookie", "")
        if expected_secure:
            assert "Secure" in set_cookie_headers
        else:
            assert "Secure" not in set_cookie_headers

    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_cookie_path", return_value="/team-a/")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.resolve_user_from_token")
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.conf")
    @pytest.mark.asyncio
    async def test_dispatch_cookie_uses_subpath(
        self,
        mock_conf,
        mock_resolve_user_from_token,
        mock_get_auth_manager,
        mock_cookie_path,
        middleware,
        mock_request,
        mock_user,
    ):
        """When a subpath is configured, set_cookie must include it as path=."""
        refreshed_user = MagicMock(spec=BaseUser)
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token"}
        mock_resolve_user_from_token.return_value = mock_user
        mock_auth_manager = MagicMock()
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        mock_conf.get.return_value = ""

        call_next = AsyncMock(return_value=Response())
        response = await middleware.dispatch(mock_request, call_next)

        set_cookie_headers = response.headers.getlist("set-cookie")
        assert any("Path=/team-a/" in h for h in set_cookie_headers)
        # Stale root-path cookie must also be cleared
        assert any(
            "Path=/" in h and "Path=/team-a/" not in h and "Max-Age=0" in h for h in set_cookie_headers
        )

    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_cookie_path", return_value="/team-a/")
    @patch.object(
        JWTRefreshMiddleware,
        "_refresh_user",
        side_effect=HTTPException(status_code=403, detail="Invalid JWT token"),
    )
    @patch("airflow.api_fastapi.auth.middlewares.refresh_token.conf")
    @pytest.mark.asyncio
    async def test_dispatch_invalid_token_clears_root_cookie(
        self,
        mock_conf,
        mock_refresh_user,
        mock_cookie_path,
        middleware,
        mock_request,
    ):
        """When a stale _token exists at root path, clearing must target both the subpath and root."""
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "stale_root_token"}
        mock_conf.get.return_value = ""

        call_next = AsyncMock(return_value=Response(status_code=401))
        response = await middleware.dispatch(mock_request, call_next)

        set_cookie_headers = response.headers.getlist("set-cookie")
        # Expect two delete cookies: one at the subpath and one at root "/"
        assert any("Path=/team-a/" in h and "Max-Age=0" in h for h in set_cookie_headers)
        assert any(
            "Path=/" in h and "Path=/team-a/" not in h and "Max-Age=0" in h for h in set_cookie_headers
        )
