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

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import Request, Response
from starlette.datastructures import MutableHeaders

from airflow.api_fastapi.core_api.middlewares.header import StripAppDateMiddleware


class TestStripAppDateMiddleware:
    @pytest.fixture
    def middleware(self):
        return StripAppDateMiddleware(app=MagicMock())

    @pytest.fixture
    def mock_request(self):
        request = MagicMock(spec=Request)
        return request

    @pytest.fixture
    def mock_response(self):
        response = MagicMock(spec=Response)
        response.headers = MutableHeaders({"content-type": "application/json"})
        return response

    @pytest.mark.asyncio
    async def test_dispatch_removes_date_header(self, middleware, mock_request, mock_response):
        """Ensure the middleware removes the 'date' header from the response."""
        mock_response.headers["date"] = "Thu, 12 Mar 2026 12:00:00 GMT"
        call_next = AsyncMock(return_value=mock_response)
        result = await middleware.dispatch(mock_request, call_next)

        assert "date" not in result.headers
        assert result.headers["content-type"] == "application/json"
        call_next.assert_called_once_with(mock_request)

    @pytest.mark.asyncio
    async def test_dispatch_handles_missing_date_header(self, middleware, mock_request, mock_response):
        """Ensure the middleware doesn't crash if 'date' is already absent."""
        call_next = AsyncMock(return_value=mock_response)
        await middleware.dispatch(mock_request, call_next)

        assert "date" not in mock_response.headers
        assert mock_response.headers["content-type"] == "application/json"
        call_next.assert_called_once_with(mock_request)
