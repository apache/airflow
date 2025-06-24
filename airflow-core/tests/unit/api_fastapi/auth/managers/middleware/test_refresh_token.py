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

from unittest.mock import MagicMock

import pytest

from airflow.api_fastapi.auth.managers.middleware.refresh_token import (
    RefreshTokenMiddleware,
)


class TestRefreshTokenMiddleware:
    """Test class for RefreshTokenMiddleware."""

    @pytest.mark.parametrize(
        "header, expected_result",
        [
            ({"sec-fetch-mode": "cors"}, True),
            ({"sec-fetch-mode": "same-origin"}, True),
            ({"sec-fetch-mode": "no-cors"}, False),
            ({"sec-fetch-mode": "navigate"}, False),
            ({"sec-fetch-mode": "websocket"}, False),
        ],
    )
    def test_get_user_from_request(self, header, expected_result):
        """Test that get_user_from_request returns a user."""
        mock_request = MagicMock()
        mock_auth_manager = MagicMock()
        mock_app = MagicMock()
        mock_request.headers = header
        middleware = RefreshTokenMiddleware(auth_manager=mock_auth_manager, app=mock_app)
        assert middleware.check_request_cors_headers(request=mock_request) == expected_result
