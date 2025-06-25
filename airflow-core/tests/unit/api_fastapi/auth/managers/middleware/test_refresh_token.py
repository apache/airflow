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

from asyncio import sleep
from unittest.mock import AsyncMock

import pytest

from airflow.api_fastapi.auth.managers.middleware.refresh_token import RefreshTokenMiddleware


class TestRefreshTokenMiddleware:
    """Test implementation of RefreshTokenMiddleware for unit testing."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method, path",
        [
            ("GET", "/api/v2/assets"),
            ("POST", "/api/v2/backfills"),
            ("GET", "/api/v2/dags"),
            ("POST", "/api/v2/dags/{dag_id}/clearTaskInstances"),
            ("GET", "/api/v2/dags/{dag_id}/dagRuns"),
            ("GET", "/api/v2/eventLogs"),
            ("GET", "/api/v2/jobs"),
            ("GET", "/api/v2/variables"),
            ("GET", "/api/v2/version"),
        ],
    )
    async def test_refresh_token_generic(self, test_client, method, path):
        """Test that the refresh token middleware correctly processes requests."""
        response = test_client.request(method=method, url=path)
        assert response.status_code not in {
            401,
            403,
        }, f"Unexpected status code {response.status_code} for {method} {path}"
        await sleep(1)
        response = test_client.request(method=method, url=path)
        assert response.status_code not in {
            401,
            403,
        }, f"Unexpected status code {response.status_code} for {method} {path}"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method, path",
        [
            ("GET", "/api/v2/assets"),
            ("POST", "/api/v2/backfills"),
            ("GET", "/api/v2/dags"),
            ("POST", "/api/v2/dags/{dag_id}/clearTaskInstances"),
            ("GET", "/api/v2/dags/{dag_id}/dagRuns"),
            ("GET", "/api/v2/eventLogs"),
            ("GET", "/api/v2/jobs"),
            ("GET", "/api/v2/variables"),
            ("GET", "/api/v2/version"),
        ],
    )
    async def test_refresh_token_no_auth(self, test_client, method, path):
        """Test that the refresh token middleware does not interfere with unauthenticated requests."""
        mock_auth_manager = AsyncMock()
        mock_user = AsyncMock()
        mock_auth_manager.get_user_from_token.return_value = mock_user
        mock_auth_manager.generate_jwt.return_value = "MOCK_JWT_TOKEN"
        mock_auth_manager.refresh_token.return_value = mock_user
        refresh_token = RefreshTokenMiddleware(
            app=test_client.app,
            auth_manager=mock_auth_manager,
        )

        mock_request = AsyncMock()
        mock_request.method = method
        mock_request.url.path = path
        mock_request.headers = {"authorization": "Bearer NO_TOKEN"}

        mock_call_next = AsyncMock()
        mock_response = AsyncMock()
        mock_call_next.return_value = mock_response
        mock_response.status_code = 200

        response = await refresh_token.dispatch(request=mock_request, call_next=mock_call_next)
        mock_auth_manager.get_user_from_token.assert_called_once_with("NO_TOKEN")
        mock_auth_manager.generate_jwt.assert_called_once()
        mock_auth_manager.refresh_token.assert_called_once_with(user=mock_user)
        mock_call_next.assert_called_once_with(mock_request)
        assert response.status_code == 200
