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

from unittest.mock import Mock, call, patch

import pytest

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    AssetAliasDetails,
    AssetDetails,
    BackfillDetails,
    ConfigurationDetails,
    ConnectionDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.exceptions import AirflowException
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def auth_manager():
    with conf_vars(
        {
            ("keycloak_auth_manager", "client_id"): "client_id",
            ("keycloak_auth_manager", "realm"): "realm",
            ("keycloak_auth_manager", "server_url"): "server_url",
        }
    ):
        yield KeycloakAuthManager()


@pytest.fixture
def user():
    user = Mock()
    user.access_token = "access_token"
    return user


class TestKeycloakAuthManager:
    def test_deserialize_user(self, auth_manager):
        result = auth_manager.deserialize_user(
            {
                "user_id": "user_id",
                "name": "name",
                "access_token": "access_token",
                "refresh_token": "refresh_token",
            }
        )
        assert result.user_id == "user_id"
        assert result.name == "name"
        assert result.access_token == "access_token"
        assert result.refresh_token == "refresh_token"

    def test_serialize_user(self, auth_manager):
        result = auth_manager.serialize_user(
            KeycloakAuthManagerUser(
                user_id="user_id", name="name", access_token="access_token", refresh_token="refresh_token"
            )
        )
        assert result == {
            "user_id": "user_id",
            "name": "name",
            "access_token": "access_token",
            "refresh_token": "refresh_token",
        }

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login"

    @pytest.mark.parametrize(
        "function, method, details, permission_type, permission_resource",
        [
            [
                "is_authorized_configuration",
                "GET",
                ConfigurationDetails(section="test"),
                "Configuration#GET",
                "Configuration:test#GET",
            ],
            ["is_authorized_configuration", "GET", None, "Configuration#GET", None],
            [
                "is_authorized_configuration",
                "PUT",
                ConfigurationDetails(section="test"),
                "Configuration#PUT",
                "Configuration:test#PUT",
            ],
            [
                "is_authorized_connection",
                "DELETE",
                ConnectionDetails(conn_id="test"),
                "Connection#DELETE",
                "Connection:test#DELETE",
            ],
            ["is_authorized_connection", "GET", None, "Connection#GET", None],
            ["is_authorized_backfill", "POST", BackfillDetails(id=1), "Backfill#POST", "Backfill:1#POST"],
            ["is_authorized_backfill", "GET", None, "Backfill#GET", None],
            ["is_authorized_asset", "GET", AssetDetails(id="test"), "Asset#GET", "Asset:test#GET"],
            ["is_authorized_asset", "GET", None, "Asset#GET", None],
            [
                "is_authorized_asset_alias",
                "GET",
                AssetAliasDetails(id="test"),
                "AssetAlias#GET",
                "AssetAlias:test#GET",
            ],
            ["is_authorized_asset_alias", "GET", None, "AssetAlias#GET", None],
            [
                "is_authorized_variable",
                "PUT",
                VariableDetails(key="test"),
                "Variable#PUT",
                "Variable:test#PUT",
            ],
            ["is_authorized_variable", "GET", None, "Variable#GET", None],
            ["is_authorized_pool", "POST", PoolDetails(name="test"), "Pool#POST", "Pool:test#POST"],
            ["is_authorized_pool", "GET", None, "Pool#GET", None],
        ],
    )
    @pytest.mark.parametrize(
        "status_code_type, status_code_resource, expected_one_call, expected_two_calls",
        [
            [200, 200, True, True],
            [200, 403, True, True],
            [403, 200, False, True],
            [403, 403, False, False],
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.requests")
    def test_is_authorized(
        self,
        mock_requests,
        function,
        method,
        details,
        permission_type,
        permission_resource,
        status_code_type,
        status_code_resource,
        expected_one_call,
        expected_two_calls,
        auth_manager,
        user,
    ):
        expected_num_calls = 1 if status_code_type == 200 or not permission_resource else 2
        resp1 = Mock()
        resp1.status_code = status_code_type
        resp2 = Mock()
        resp2.status_code = status_code_resource

        mock_requests.post.side_effect = [resp1, resp2]

        result = getattr(auth_manager, function)(method=method, user=user, details=details)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", permission_type)
        headers = auth_manager._get_headers("access_token")
        expected_calls = [call(token_url, data=payload, headers=headers)]
        if expected_num_calls == 2:
            expected_calls.append(
                call(token_url, data={**payload, "permission": permission_resource}, headers=headers)
            )
        mock_requests.post.assert_has_calls(expected_calls)
        expected = expected_two_calls if expected_num_calls == 2 else expected_one_call
        assert result == expected

    @pytest.mark.parametrize(
        "function",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_backfill",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_variable",
            "is_authorized_pool",
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.requests")
    def test_is_authorized_failure(self, mock_requests, function, auth_manager, user):
        resp = Mock()
        resp.status_code = 500
        mock_requests.post.return_value = resp

        with pytest.raises(AirflowException):
            getattr(auth_manager, function)(method="GET", user=user)

    @pytest.mark.parametrize(
        "status_code_type, status_code_resource, expected_one_call, expected_two_calls",
        [
            [200, 200, True, True],
            [200, 403, True, True],
            [403, 200, False, True],
            [403, 403, False, False],
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.requests")
    def test_is_authorized_view(
        self,
        mock_requests,
        status_code_type,
        status_code_resource,
        expected_one_call,
        expected_two_calls,
        auth_manager,
        user,
    ):
        expected_num_calls = 1 if status_code_type == 200 else 2
        resp1 = Mock()
        resp1.status_code = status_code_type
        resp2 = Mock()
        resp2.status_code = status_code_resource

        mock_requests.post.side_effect = [resp1, resp2]

        result = auth_manager.is_authorized_view(access_view=AccessView.CLUSTER_ACTIVITY, user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", "View#GET")
        headers = auth_manager._get_headers("access_token")
        expected_calls = [call(token_url, data=payload, headers=headers)]
        if expected_num_calls == 2:
            expected_calls.append(
                call(token_url, data={**payload, "permission": "View:CLUSTER_ACTIVITY#GET"}, headers=headers)
            )

        mock_requests.post.assert_has_calls(expected_calls)
        expected = expected_two_calls if expected_num_calls == 2 else expected_one_call
        assert result == expected

    @pytest.mark.parametrize(
        "status_code_type, status_code_resource, expected_one_call, expected_two_calls",
        [
            [200, 200, True, True],
            [200, 403, True, True],
            [403, 200, False, True],
            [403, 403, False, False],
        ],
    )
    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.requests")
    def test_is_authorized_custom_view(
        self,
        mock_requests,
        status_code_type,
        status_code_resource,
        expected_one_call,
        expected_two_calls,
        auth_manager,
        user,
    ):
        expected_num_calls = 1 if status_code_type == 200 else 2
        resp1 = Mock()
        resp1.status_code = status_code_type
        resp2 = Mock()
        resp2.status_code = status_code_resource

        mock_requests.post.side_effect = [resp1, resp2]

        result = auth_manager.is_authorized_custom_view(method="GET", resource_name="test", user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", "Custom#GET")
        headers = auth_manager._get_headers("access_token")
        expected_calls = [call(token_url, data=payload, headers=headers)]
        if expected_num_calls == 2:
            expected_calls.append(
                call(token_url, data={**payload, "permission": "Custom:test#GET"}, headers=headers)
            )

        mock_requests.post.assert_has_calls(expected_calls)
        expected = expected_two_calls if expected_num_calls == 2 else expected_one_call
        assert result == expected
