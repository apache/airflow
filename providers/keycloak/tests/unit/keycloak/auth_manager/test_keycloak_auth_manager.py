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

import json
from unittest.mock import Mock, patch

import pytest
from keycloak import KeycloakPostError

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    AssetAliasDetails,
    AssetDetails,
    BackfillDetails,
    ConfigurationDetails,
    ConnectionDetails,
    DagAccessEntity,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.api_fastapi.common.types import MenuItem
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
)
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import (
    RESOURCE_ID_ATTRIBUTE_NAME,
    KeycloakAuthManager,
)
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def auth_manager():
    with conf_vars(
        {
            (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "client_id",
            (CONF_SECTION_NAME, CONF_REALM_KEY): "realm",
            (CONF_SECTION_NAME, CONF_SERVER_URL_KEY): "server_url",
        }
    ):
        yield KeycloakAuthManager()


@pytest.fixture
def user():
    user = Mock()
    user.access_token = "access_token"
    user.refresh_token = "refresh_token"
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

    def test_get_url_logout(self, auth_manager):
        result = auth_manager.get_url_logout()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/logout"

    @patch.object(KeycloakAuthManager, "_token_expired")
    def test_refresh_user_not_expired(self, mock_token_expired, auth_manager):
        mock_token_expired.return_value = False

        result = auth_manager.refresh_user(user=Mock())

        assert result is None

    @patch.object(KeycloakAuthManager, "get_keycloak_client")
    @patch.object(KeycloakAuthManager, "_token_expired")
    def test_refresh_user_expired(self, mock_token_expired, mock_get_keycloak_client, auth_manager, user):
        mock_token_expired.return_value = True
        keycloak_client = Mock()
        keycloak_client.refresh_token.return_value = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
        }

        mock_get_keycloak_client.return_value = keycloak_client

        result = auth_manager.refresh_user(user=user)

        keycloak_client.refresh_token.assert_called_with("refresh_token")
        assert result.access_token == "new_access_token"
        assert result.refresh_token == "new_refresh_token"

    @patch.object(KeycloakAuthManager, "get_keycloak_client")
    @patch.object(KeycloakAuthManager, "_token_expired")
    def test_refresh_user_expired_with_invalid_token(self, mock_token_expired, mock_get_keycloak_client, auth_manager, user):
        mock_token_expired.return_value = True
        keycloak_client = Mock()
        keycloak_client.refresh_token.side_effect = KeycloakPostError(
            response_code=400,
            response_body= b'{"error":"invalid_grant","error_description":"Token is not active"}',
        )

        mock_get_keycloak_client.return_value = keycloak_client

        assert auth_manager.refresh_user(user=user) is None

        keycloak_client.refresh_token.assert_called_with("refresh_token")

    @pytest.mark.parametrize(
        ("function", "method", "details", "permission", "attributes"),
        [
            [
                "is_authorized_configuration",
                "GET",
                ConfigurationDetails(section="test"),
                "Configuration#GET",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_configuration", "GET", None, "Configuration#LIST", None],
            [
                "is_authorized_configuration",
                "PUT",
                ConfigurationDetails(section="test"),
                "Configuration#PUT",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            [
                "is_authorized_connection",
                "DELETE",
                ConnectionDetails(conn_id="test"),
                "Connection#DELETE",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_connection", "GET", None, "Connection#LIST", {}],
            [
                "is_authorized_backfill",
                "POST",
                BackfillDetails(id=1),
                "Backfill#POST",
                {RESOURCE_ID_ATTRIBUTE_NAME: "1"},
            ],
            ["is_authorized_backfill", "GET", None, "Backfill#LIST", {}],
            [
                "is_authorized_asset",
                "GET",
                AssetDetails(id="test"),
                "Asset#GET",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_asset", "GET", None, "Asset#LIST", {}],
            [
                "is_authorized_asset_alias",
                "GET",
                AssetAliasDetails(id="test"),
                "AssetAlias#GET",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_asset_alias", "GET", None, "AssetAlias#LIST", {}],
            [
                "is_authorized_variable",
                "PUT",
                VariableDetails(key="test"),
                "Variable#PUT",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_variable", "GET", None, "Variable#LIST", {}],
            [
                "is_authorized_pool",
                "POST",
                PoolDetails(name="test"),
                "Pool#POST",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            ["is_authorized_pool", "GET", None, "Pool#LIST", {}],
        ],
    )
    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            [200, True],
            [401, False],
            [403, False],
        ],
    )
    def test_is_authorized(
        self,
        function,
        method,
        details,
        permission,
        attributes,
        status_code,
        expected,
        auth_manager,
        user,
    ):
        mock_response = Mock()
        mock_response.status_code = status_code
        auth_manager.http_session.post = Mock(return_value=mock_response)

        result = getattr(auth_manager, function)(method=method, user=user, details=details)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", permission, attributes)
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert result == expected

    @pytest.mark.parametrize(
        "function",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_dag",
            "is_authorized_backfill",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_variable",
            "is_authorized_pool",
        ],
    )
    def test_is_authorized_failure(self, function, auth_manager, user):
        resp = Mock()
        resp.status_code = 500
        auth_manager.http_session.post = Mock(return_value=resp)

        with pytest.raises(AirflowException) as e:
            getattr(auth_manager, function)(method="GET", user=user)

        assert "Unexpected error" in str(e.value)

    @pytest.mark.parametrize(
        "function",
        [
            "is_authorized_configuration",
            "is_authorized_connection",
            "is_authorized_dag",
            "is_authorized_backfill",
            "is_authorized_asset",
            "is_authorized_asset_alias",
            "is_authorized_variable",
            "is_authorized_pool",
        ],
    )
    def test_is_authorized_invalid_request(self, function, auth_manager, user):
        resp = Mock()
        resp.status_code = 400
        resp.text = json.dumps({"error": "invalid_scope", "error_description": "Invalid scopes: GET"})
        auth_manager.http_session.post = Mock(return_value=resp)

        with pytest.raises(AirflowException) as e:
            getattr(auth_manager, function)(method="GET", user=user)

        assert "Request not recognized by Keycloak. invalid_scope. Invalid scopes: GET" in str(e.value)

    @pytest.mark.parametrize(
        ("method", "access_entity", "details", "permission", "attributes"),
        [
            [
                "GET",
                None,
                None,
                "Dag#LIST",
                {},
            ],
            [
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                DagDetails(id="test"),
                "Dag#GET",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test", "dag_entity": "TASK_INSTANCE"},
            ],
            [
                "GET",
                None,
                DagDetails(id="test"),
                "Dag#GET",
                {RESOURCE_ID_ATTRIBUTE_NAME: "test"},
            ],
            [
                "GET",
                DagAccessEntity.TASK_INSTANCE,
                None,
                "Dag#LIST",
                {"dag_entity": "TASK_INSTANCE"},
            ],
        ],
    )
    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            [200, True],
            [403, False],
        ],
    )
    def test_is_authorized_dag(
        self,
        method,
        access_entity,
        details,
        permission,
        attributes,
        status_code,
        expected,
        auth_manager,
        user,
    ):
        mock_response = Mock()
        mock_response.status_code = status_code
        auth_manager.http_session.post = Mock(return_value=mock_response)

        result = auth_manager.is_authorized_dag(
            method=method, user=user, access_entity=access_entity, details=details
        )

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", permission, attributes)
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            [200, True],
            [403, False],
        ],
    )
    def test_is_authorized_view(
        self,
        status_code,
        expected,
        auth_manager,
        user,
    ):
        mock_response = Mock()
        mock_response.status_code = status_code
        auth_manager.http_session.post = Mock(return_value=mock_response)

        result = auth_manager.is_authorized_view(access_view=AccessView.CLUSTER_ACTIVITY, user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload(
            "client_id", "View#GET", {RESOURCE_ID_ATTRIBUTE_NAME: "CLUSTER_ACTIVITY"}
        )
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            [200, True],
            [403, False],
        ],
    )
    def test_is_authorized_custom_view(
        self,
        status_code,
        expected,
        auth_manager,
        user,
    ):
        mock_response = Mock()
        mock_response.status_code = status_code
        auth_manager.http_session.post = Mock(return_value=mock_response)

        result = auth_manager.is_authorized_custom_view(method="GET", resource_name="test", user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", "Custom#GET", {RESOURCE_ID_ATTRIBUTE_NAME: "test"})
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("status_code", "response", "expected"),
        [
            [
                200,
                [{"scopes": ["MENU"], "rsname": "Assets"}, {"scopes": ["MENU"], "rsname": "Connections"}],
                {MenuItem.ASSETS, MenuItem.CONNECTIONS},
            ],
            [200, [{"scopes": ["MENU"], "rsname": "Assets"}], {MenuItem.ASSETS}],
            [200, [], set()],
            [403, [{"scopes": ["MENU"], "rsname": "Assets"}], set()],
        ],
    )
    def test_filter_authorized_menu_items(self, status_code, response, expected, auth_manager, user):
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.json.return_value = response
        auth_manager.http_session.post = Mock(return_value=mock_response)
        menu_items = [MenuItem.ASSETS, MenuItem.CONNECTIONS]

        result = auth_manager.filter_authorized_menu_items(menu_items, user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_batch_payload(
            "client_id", [("MENU", MenuItem.ASSETS.value), ("MENU", MenuItem.CONNECTIONS.value)]
        )
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert set(result) == expected

    @pytest.mark.parametrize(
        "status_code",
        [400, 500],
    )
    def test_filter_authorized_menu_items_with_failure(self, status_code, auth_manager, user):
        resp = Mock()
        resp.status_code = status_code
        resp.text = json.dumps({})
        auth_manager.http_session.post = Mock(return_value=resp)

        menu_items = [MenuItem.ASSETS, MenuItem.CONNECTIONS]

        with pytest.raises(AirflowException):
            auth_manager.filter_authorized_menu_items(menu_items, user=user)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_batch_payload(
            "client_id", [("MENU", MenuItem.ASSETS.value), ("MENU", MenuItem.CONNECTIONS.value)]
        )
        headers = auth_manager._get_headers("access_token")
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )

    def test_get_cli_commands_return_cli_commands(self, auth_manager):
        assert len(auth_manager.get_cli_commands()) == 1

    @pytest.mark.parametrize(
        ("expiration", "expected"),
        [
            (-30, True),
            (30, False),
        ],
    )
    def test_token_expired(self, auth_manager, expiration, expected):
        token = auth_manager._get_token_signer(expiration_time_in_seconds=expiration).generate({})

        assert KeycloakAuthManager._token_expired(token) is expected
