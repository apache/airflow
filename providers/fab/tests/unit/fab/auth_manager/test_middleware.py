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
from airflow.providers.fab.auth_manager.middleware import FabAuthRolePublicMiddleware


@pytest.mark.asyncio
class TestFabAuthRolePublicMiddleware:
    def _make_request(self, headers: dict | None = None, cookies: dict | None = None) -> Mock:
        request = Mock()
        request.headers = headers or {}
        request.cookies = cookies or {}
        request.state = Mock(spec=[])
        return request

    @patch("airflow.providers.fab.auth_manager.middleware.get_auth_manager")
    async def test_sets_public_user_when_no_auth_present(self, mock_get_auth_manager):
        public_user = Mock(name="public_user")
        auth_manager = Mock()
        auth_manager.build_public_user = Mock(return_value=public_user)
        mock_get_auth_manager.return_value = auth_manager

        middleware = FabAuthRolePublicMiddleware(app=Mock())
        request = self._make_request()
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        assert request.state.user is public_user
        auth_manager.build_public_user.assert_called_once_with()
        call_next.assert_awaited_once_with(request)

    @patch("airflow.providers.fab.auth_manager.middleware.get_auth_manager")
    async def test_skips_when_authorization_header_present(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.build_public_user = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = FabAuthRolePublicMiddleware(app=Mock())
        request = self._make_request(headers={"authorization": "Bearer real-token"})
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.build_public_user.assert_not_called()
        assert not hasattr(request.state, "user")

    @patch("airflow.providers.fab.auth_manager.middleware.get_auth_manager")
    async def test_skips_when_jwt_cookie_present(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.build_public_user = Mock()
        mock_get_auth_manager.return_value = auth_manager

        middleware = FabAuthRolePublicMiddleware(app=Mock())
        request = self._make_request(cookies={COOKIE_NAME_JWT_TOKEN: "cookie-token"})
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        auth_manager.build_public_user.assert_not_called()

    @patch("airflow.providers.fab.auth_manager.middleware.get_auth_manager")
    async def test_does_nothing_when_auth_manager_has_no_public_user(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.build_public_user = Mock(return_value=None)
        mock_get_auth_manager.return_value = auth_manager

        middleware = FabAuthRolePublicMiddleware(app=Mock())
        request = self._make_request()
        call_next = AsyncMock(return_value=Mock(name="response"))

        await middleware.dispatch(request, call_next)

        assert not hasattr(request.state, "user")
        call_next.assert_awaited_once_with(request)
