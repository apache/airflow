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
from airflow.api_fastapi.auth.managers.models.resource_details import (
    ConnectionDetails,
    DagAccessEntity,
    PoolDetails,
    VariableDetails,
)
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.datamodels.common import BulkBody
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody
from airflow.api_fastapi.core_api.datamodels.pools import PoolBody
from airflow.api_fastapi.core_api.datamodels.variables import VariableBody
from airflow.api_fastapi.core_api.security import (
    is_safe_url,
    requires_access_connection,
    requires_access_connection_bulk,
    requires_access_dag,
    requires_access_pool,
    requires_access_pool_bulk,
    requires_access_variable,
    requires_access_variable_bulk,
    resolve_user_from_token,
)
from airflow.models import Connection, Pool, Variable

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
    async def test_resolve_user_from_token(self, mock_get_auth_manager):
        token_str = "test-token"
        user = SimpleAuthManagerUser(username="username", role="admin")

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.return_value = user
        mock_get_auth_manager.return_value = auth_manager

        result = await resolve_user_from_token(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)
        assert result == user

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_get_user_wrong_token(self, mock_get_auth_manager):
        token_str = "test-token"

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.side_effect = InvalidTokenError()
        mock_get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Invalid JWT token"):
            await resolve_user_from_token(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    async def test_get_user_expired_token(self, mock_get_auth_manager):
        token_str = "test-token"

        auth_manager = AsyncMock()
        auth_manager.get_user_from_token.side_effect = ExpiredSignatureError()
        mock_get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Token Expired"):
            await resolve_user_from_token(token_str)

        auth_manager.get_user_from_token.assert_called_once_with(token_str)

    @pytest.mark.db_test
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_dag_authorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params = {}

        requires_access_dag("GET", DagAccessEntity.CODE)(fastapi_request, Mock())

        auth_manager.is_authorized_dag.assert_called_once()

    @pytest.mark.db_test
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_dag_unauthorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = False
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params = {}

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

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "team_name",
        [None, "team1"],
    )
    @patch.object(Connection, "get_team_name")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_connection(self, mock_get_auth_manager, mock_get_team_name, team_name):
        auth_manager = Mock()
        auth_manager.is_authorized_connection.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params = {"connection_id": "conn_id"}
        mock_get_team_name.return_value = team_name
        user = Mock()

        requires_access_connection("GET")(fastapi_request, user)

        auth_manager.is_authorized_connection.assert_called_once_with(
            method="GET",
            details=ConnectionDetails(conn_id="conn_id", team_name=team_name),
            user=user,
        )
        mock_get_team_name.assert_called_once_with("conn_id")

    @patch.object(Connection, "get_conn_id_to_team_name_mapping")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_connection_bulk(
        self, mock_get_auth_manager, mock_get_conn_id_to_team_name_mapping
    ):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_connection.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        mock_get_conn_id_to_team_name_mapping.return_value = {"test1": "team1"}

        request = BulkBody[ConnectionBody].model_validate(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"connection_id": "test1", "conn_type": "test1"},
                            {"connection_id": "test2", "conn_type": "test2"},
                        ],
                    },
                    {
                        "action": "delete",
                        "entities": ["test3"],
                    },
                ]
            }
        )
        user = Mock()
        requires_access_connection_bulk()(request, user)

        auth_manager.batch_is_authorized_connection.assert_called_once_with(
            requests=[
                {
                    "method": "POST",
                    "details": ConnectionDetails(conn_id="test1", team_name="team1"),
                },
                {
                    "method": "POST",
                    "details": ConnectionDetails(conn_id="test2"),
                },
                {
                    "method": "DELETE",
                    "details": ConnectionDetails(conn_id="test3"),
                },
            ],
            user=user,
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "team_name",
        [None, "team1"],
    )
    @patch.object(Variable, "get_team_name")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_variable(self, mock_get_auth_manager, mock_get_team_name, team_name):
        auth_manager = Mock()
        auth_manager.is_authorized_variable.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params = {"variable_key": "var_key"}
        mock_get_team_name.return_value = team_name
        user = Mock()

        requires_access_variable("GET")(fastapi_request, user)

        auth_manager.is_authorized_variable.assert_called_once_with(
            method="GET",
            details=VariableDetails(key="var_key", team_name=team_name),
            user=user,
        )
        mock_get_team_name.assert_called_once_with("var_key")

    @patch.object(Variable, "get_key_to_team_name_mapping")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_variable_bulk(self, mock_get_auth_manager, mock_get_key_to_team_name_mapping):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_variable.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        mock_get_key_to_team_name_mapping.return_value = {"var1": "team1", "dummy": "team2"}
        request = BulkBody[VariableBody].model_validate(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"key": "var1", "value": "value1"},
                            {"key": "var2", "value": "value2"},
                        ],
                    },
                    {
                        "action": "delete",
                        "entities": ["var3"],
                    },
                ]
            }
        )
        user = Mock()
        requires_access_variable_bulk()(request, user)

        auth_manager.batch_is_authorized_variable.assert_called_once_with(
            requests=[
                {
                    "method": "POST",
                    "details": VariableDetails(key="var1", team_name="team1"),
                },
                {
                    "method": "POST",
                    "details": VariableDetails(key="var2"),
                },
                {
                    "method": "DELETE",
                    "details": VariableDetails(key="var3"),
                },
            ],
            user=user,
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "team_name",
        [None, "team1"],
    )
    @patch.object(Pool, "get_team_name")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_pool(self, mock_get_auth_manager, mock_get_team_name, team_name):
        auth_manager = Mock()
        auth_manager.is_authorized_pool.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        fastapi_request = Mock()
        fastapi_request.path_params = {"pool_name": "pool"}
        mock_get_team_name.return_value = team_name
        user = Mock()

        requires_access_pool("GET")(fastapi_request, user)

        auth_manager.is_authorized_pool.assert_called_once_with(
            method="GET",
            details=PoolDetails(name="pool", team_name=team_name),
            user=user,
        )
        mock_get_team_name.assert_called_once_with("pool")

    @patch.object(Pool, "get_name_to_team_name_mapping")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_pool_bulk(self, mock_get_auth_manager, mock_get_name_to_team_name_mapping):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_pool.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        mock_get_name_to_team_name_mapping.return_value = {"pool1": "team1"}
        request = BulkBody[PoolBody].model_validate(
            {
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"pool": "pool1", "slots": 1},
                            {"pool": "pool2", "slots": 1},
                        ],
                    },
                    {
                        "action": "delete",
                        "entities": ["pool3"],
                    },
                ]
            }
        )
        user = Mock()
        requires_access_pool_bulk()(request, user)

        auth_manager.batch_is_authorized_pool.assert_called_once_with(
            requests=[
                {
                    "method": "POST",
                    "details": PoolDetails(name="pool1", team_name="team1"),
                },
                {
                    "method": "POST",
                    "details": PoolDetails(name="pool2"),
                },
                {
                    "method": "DELETE",
                    "details": PoolDetails(name="pool3"),
                },
            ],
            user=user,
        )
