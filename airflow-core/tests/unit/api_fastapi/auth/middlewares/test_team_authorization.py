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
from fastapi import HTTPException, Request, Response, status

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.core_api.security import USER_INJECTED_BY_TRUSTED_MIDDLEWARE

MIDDLEWARE_MODULE = "airflow.api_fastapi.auth.middlewares.team_authorization"


@pytest.fixture
def middleware():
    from airflow.api_fastapi.auth.middlewares.team_authorization import TeamAuthorizationMiddleware

    return TeamAuthorizationMiddleware(app=MagicMock(), team_name="team_a")


@pytest.fixture
def mock_request():
    request = MagicMock(spec=Request)
    request.cookies = {}
    request.headers = {}
    request.method = "GET"
    request.state = MagicMock()
    request.state.user = None
    request.state.user_authenticated_via = None
    return request


@pytest.fixture
def mock_user():
    return MagicMock(spec=BaseUser)


class TestTeamAuthorizationMiddleware:
    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_authorized_team_member_passes_through(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "a_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True
        expected = Response(status_code=200)
        call_next = AsyncMock(return_value=expected)

        response = await middleware.dispatch(mock_request, call_next)

        assert response is expected
        call_next.assert_awaited_once_with(mock_request)

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_user_outside_team_is_forbidden(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "a_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = False
        call_next = AsyncMock()

        response = await middleware.dispatch(mock_request, call_next)

        assert response.status_code == status.HTTP_403_FORBIDDEN
        call_next.assert_not_awaited()

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(
        f"{MIDDLEWARE_MODULE}.resolve_user_from_token",
        side_effect=HTTPException(status_code=401, detail="Not authenticated"),
    )
    @pytest.mark.asyncio
    async def test_unauthenticated_request_is_rejected(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request
    ):
        call_next = AsyncMock()

        response = await middleware.dispatch(mock_request, call_next)

        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        call_next.assert_not_awaited()
        # The team check is unreachable without a user.
        mock_get_auth_manager.return_value.is_authorized_team.assert_not_called()

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_authorizes_against_the_mounted_team(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "a_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True

        await middleware.dispatch(mock_request, AsyncMock(return_value=Response()))

        kwargs = mock_get_auth_manager.return_value.is_authorized_team.call_args.kwargs
        assert kwargs["user"] is mock_user
        assert kwargs["details"].name == "team_a"

    @pytest.mark.parametrize(
        ("http_method", "expected_resource_method"),
        [
            ("GET", "GET"),
            ("HEAD", "GET"),
            ("OPTIONS", "GET"),
            ("POST", "POST"),
            ("PUT", "PUT"),
            ("PATCH", "PUT"),
            ("DELETE", "DELETE"),
            ("SOMETHING_ELSE", "GET"),
        ],
    )
    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_maps_http_method_to_resource_method(
        self,
        mock_resolve_user,
        mock_get_auth_manager,
        middleware,
        mock_request,
        mock_user,
        http_method,
        expected_resource_method,
    ):
        mock_request.method = http_method
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "a_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True

        await middleware.dispatch(mock_request, AsyncMock(return_value=Response()))

        kwargs = mock_get_auth_manager.return_value.is_authorized_team.call_args.kwargs
        assert kwargs["method"] == expected_resource_method

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_prefers_bearer_token_over_cookie(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        mock_request.headers = {"Authorization": "Bearer header_token"}
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "cookie_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True

        await middleware.dispatch(mock_request, AsyncMock(return_value=Response()))

        mock_resolve_user.assert_awaited_once_with("header_token")

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_reuses_user_from_trusted_middleware(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        """A cookie request already authenticated by JWTRefreshMiddleware is not re-resolved."""
        mock_request.state.user = mock_user
        mock_request.state.user_authenticated_via = USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True

        await middleware.dispatch(mock_request, AsyncMock(return_value=Response()))

        mock_resolve_user.assert_not_awaited()
        assert mock_get_auth_manager.return_value.is_authorized_team.call_args.kwargs["user"] is mock_user

    @patch(f"{MIDDLEWARE_MODULE}.get_auth_manager")
    @patch(f"{MIDDLEWARE_MODULE}.resolve_user_from_token")
    @pytest.mark.asyncio
    async def test_ignores_untrusted_state_user(
        self, mock_resolve_user, mock_get_auth_manager, middleware, mock_request, mock_user
    ):
        """A ``state.user`` without the trust sentinel must not bypass token validation."""
        mock_request.state.user = MagicMock(spec=BaseUser)
        mock_request.state.user_authenticated_via = None
        mock_request.cookies = {COOKIE_NAME_JWT_TOKEN: "a_token"}
        mock_resolve_user.return_value = mock_user
        mock_get_auth_manager.return_value.is_authorized_team.return_value = True

        await middleware.dispatch(mock_request, AsyncMock(return_value=Response()))

        mock_resolve_user.assert_awaited_once_with("a_token")
        assert mock_get_auth_manager.return_value.is_authorized_team.call_args.kwargs["user"] is mock_user
