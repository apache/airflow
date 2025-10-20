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
