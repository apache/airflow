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
from fastapi import HTTPException
from jwt import ExpiredSignatureError, InvalidTokenError

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.security import get_user, is_safe_url, requires_access_dag

from tests_common.test_utils.config import conf_vars


@pytest.mark.asyncio
class TestFastApiSecurity:
    @classmethod
    def setup_class(cls):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            create_app()

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_get_user(self, mock_get_auth_manager):
        token_str = "test-token"
        user = SimpleAuthManagerUser(username="username", role="admin")

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.return_value = user
        mock_get_auth_manager.return_value = auth_manager

        result = await get_user(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)
        assert result == user

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_get_user_wrong_token(self, mock_get_auth_manager):
        token_str = "test-token"

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.side_effect = InvalidTokenError()
        mock_get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Invalid JWT token"):
            await get_user(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_get_user_expired_token(self, mock_get_auth_manager):
        token_str = "test-token"

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.side_effect = ExpiredSignatureError()
        mock_get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Token Expired"):
            await get_user(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_requires_access_dag_authorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params.return_value = {}

        requires_access_dag("GET", DagAccessEntity.CODE)(fastapi_request, Mock())

        auth_manager.is_authorized_dag.assert_called_once()

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_requires_access_dag_unauthorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = False
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params.return_value = {}

        mock_request = Mock()
        mock_request.path_params.return_value = {}

        with pytest.raises(HTTPException, match="Forbidden"):
            requires_access_dag("GET", DagAccessEntity.CODE)(fastapi_request, Mock())

        auth_manager.is_authorized_dag.assert_called_once()

    @pytest.mark.parametrize(
        "url, expected_is_safe",
        [
            ("https://server_base_url.com/prefix/some_page?with_param=3", True),
            ("https://server_base_url.com/prefix/", True),
            ("https://server_base_url.com/prefix", True),
            ("/prefix/some_other", True),
            ("prefix/some_other", True),
            ("https://requesting_server_base_url.com/prefix2", True),  # safe in regards to the request url
            # Relative path, will go up one level escaping the prefix folder
            ("some_other", False),
            ("./some_other", False),
            # wrong scheme
            ("javascript://server_base_url.com/prefix/some_page?with_param=3", False),
            # wrong netloc
            ("https://some_netlock.com/prefix/some_page?with_param=3", False),
            # Absolute path escaping the prefix folder
            ("/some_other_page/", False),
            # traversal, escaping the `prefix` folder
            ("/../../../../some_page?with_param=3", False),
        ],
    )
    @conf_vars({("api", "base_url"): "https://server_base_url.com/prefix"})
    def test_is_safe_url(self, url, expected_is_safe):
        request = Mock()
        request.base_url = "https://requesting_server_base_url.com/prefix2"
        assert is_safe_url(url, request=request) == expected_is_safe
