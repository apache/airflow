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

import base64
import json
from contextlib import ExitStack
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
from airflow.exceptions import AirflowProviderDeprecationWarning

try:
    from airflow.providers.common.compat.sdk import AirflowException
except ModuleNotFoundError:
    from airflow.exceptions import AirflowException
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
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
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_7_PLUS, AIRFLOW_V_3_2_PLUS


def _build_access_token(payload: dict[str, object]) -> str:
    header = {"alg": "none", "typ": "JWT"}
    header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip("=")
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return f"{header_b64}.{payload_b64}."


@pytest.fixture
def auth_manager():
    with conf_vars(
        {
            (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "client_id",
            (CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY): "client_secret",
            (CONF_SECTION_NAME, CONF_REALM_KEY): "realm",
            (CONF_SECTION_NAME, CONF_SERVER_URL_KEY): "server_url",
        }
    ):
        yield KeycloakAuthManager()


@pytest.fixture
def auth_manager_multi_team():
    with conf_vars(
        {
            ("core", "multi_team"): "True",
            (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "client_id",
            (CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY): "client_secret",
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

    def test_refresh_user_no_refresh_token(self, auth_manager):
        """Test that refresh_user returns None when refresh_token is empty (client_credentials case)."""
        user_without_refresh = Mock()
        user_without_refresh.refresh_token = None
        user_without_refresh.access_token = "access_token"

        result = auth_manager.refresh_user(user=user_without_refresh)

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
    def test_refresh_user_expired_with_invalid_token(
        self, mock_token_expired, mock_get_keycloak_client, auth_manager, user
    ):
        mock_token_expired.return_value = True
        keycloak_client = Mock()
        keycloak_client.refresh_token.side_effect = KeycloakPostError(
            response_code=400,
            response_body=b'{"error":"invalid_grant","error_description":"Token is not active"}',
        )

        mock_get_keycloak_client.return_value = keycloak_client

        if AIRFLOW_V_3_1_7_PLUS:
            from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException

            with pytest.raises(AuthManagerRefreshTokenExpiredException):
                auth_manager.refresh_user(user=user)
        else:
            auth_manager.refresh_user(user=user)

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

        with ExitStack() as stack:
            if function == "is_authorized_backfill":
                stack.enter_context(
                    pytest.warns(
                        AirflowProviderDeprecationWarning,
                        match="Use ``is_authorized_dag`` on ``DagAccessEntity.RUN`` instead for a dag level access control.",
                    )
                )

            result = getattr(auth_manager, function)(method=method, user=user, details=details)

        token_url = auth_manager._get_token_url("server_url", "realm")
        payload = auth_manager._get_payload("client_id", permission, attributes)
        headers = auth_manager._get_headers(user.access_token)
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

        with ExitStack() as stack:
            if function == "is_authorized_backfill":
                stack.enter_context(
                    pytest.warns(
                        AirflowProviderDeprecationWarning,
                        match="Use ``is_authorized_dag`` on ``DagAccessEntity.RUN`` instead for a dag level access control.",
                    )
                )

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

        with ExitStack() as stack:
            if function == "is_authorized_backfill":
                stack.enter_context(
                    pytest.warns(
                        AirflowProviderDeprecationWarning,
                        match="Use ``is_authorized_dag`` on ``DagAccessEntity.RUN`` instead for a dag level access control.",
                    )
                )

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
        headers = auth_manager._get_headers(user.access_token)
        auth_manager.http_session.post.assert_called_once_with(
            token_url, data=payload, headers=headers, timeout=5
        )
        assert result == expected

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @pytest.mark.parametrize(
        ("function", "method", "details_cls", "details_kwargs", "permission"),
        [
            ("is_authorized_dag", "GET", DagDetails, {"id": "test", "team_name": "team-a"}, "Dag#GET"),
            (
                "is_authorized_connection",
                "DELETE",
                ConnectionDetails,
                {"conn_id": "test", "team_name": "team-a"},
                "Connection#DELETE",
            ),
            (
                "is_authorized_variable",
                "PUT",
                VariableDetails,
                {"key": "test", "team_name": "team-a"},
                "Variable#PUT",
            ),
            ("is_authorized_pool", "POST", PoolDetails, {"name": "test", "team_name": "team-a"}, "Pool#POST"),
        ],
    )
    def test_team_name_ignored_when_multi_team_disabled(
        self, auth_manager, user, function, method, details_cls, details_kwargs, permission
    ):
        details = details_cls(**details_kwargs)
        mock_response = Mock()
        mock_response.status_code = 200
        auth_manager.http_session.post = Mock(return_value=mock_response)

        getattr(auth_manager, function)(method=method, user=user, details=details)

        actual_permission = auth_manager.http_session.post.call_args.kwargs["data"]["permission"]
        assert actual_permission == permission

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @pytest.mark.parametrize(
        ("function", "details_cls", "details_kwargs", "permission"),
        [
            ("is_authorized_dag", DagDetails, {"id": "test", "team_name": "team-a"}, "Dag:team-a#GET"),
            (
                "is_authorized_connection",
                ConnectionDetails,
                {"conn_id": "test", "team_name": "team-a"},
                "Connection:team-a#GET",
            ),
            (
                "is_authorized_variable",
                VariableDetails,
                {"key": "test", "team_name": "team-a"},
                "Variable:team-a#GET",
            ),
            ("is_authorized_pool", PoolDetails, {"name": "test", "team_name": "team-a"}, "Pool:team-a#GET"),
        ],
    )
    def test_with_team_name_uses_team_scoped_permission(
        self, auth_manager_multi_team, user, function, details_cls, details_kwargs, permission
    ):
        details = details_cls(**details_kwargs)
        mock_response = Mock()
        mock_response.status_code = 200
        auth_manager_multi_team.http_session.post = Mock(return_value=mock_response)

        getattr(auth_manager_multi_team, function)(method="GET", user=user, details=details)

        actual_permission = auth_manager_multi_team.http_session.post.call_args.kwargs["data"]["permission"]
        assert actual_permission == permission

    @pytest.mark.parametrize(
        ("function", "details", "permission"),
        [
            ("is_authorized_dag", DagDetails(id="test"), "Dag#GET"),
            ("is_authorized_connection", ConnectionDetails(conn_id="test"), "Connection#GET"),
            ("is_authorized_variable", VariableDetails(key="test"), "Variable#GET"),
            ("is_authorized_pool", PoolDetails(name="test"), "Pool#GET"),
        ],
    )
    def test_without_team_name_uses_global_permission(
        self, auth_manager_multi_team, user, function, details, permission
    ):
        mock_response = Mock()
        mock_response.status_code = 200
        auth_manager_multi_team.http_session.post = Mock(return_value=mock_response)

        getattr(auth_manager_multi_team, function)(method="GET", user=user, details=details)

        actual_permission = auth_manager_multi_team.http_session.post.call_args.kwargs["data"]["permission"]
        assert actual_permission == permission

    @pytest.mark.parametrize(
        ("function", "permission"),
        [
            ("is_authorized_dag", "Dag#LIST"),
            ("is_authorized_connection", "Connection#LIST"),
            ("is_authorized_variable", "Variable#LIST"),
            ("is_authorized_pool", "Pool#LIST"),
        ],
    )
    def test_list_without_team_name_uses_global_permission(
        self, auth_manager_multi_team, user, function, permission
    ):
        mock_response = Mock()
        mock_response.status_code = 200
        auth_manager_multi_team.http_session.post = Mock(return_value=mock_response)

        getattr(auth_manager_multi_team, function)(method="GET", user=user)

        actual_permission = auth_manager_multi_team.http_session.post.call_args.kwargs["data"]["permission"]
        assert actual_permission == permission

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @pytest.mark.parametrize(
        ("function", "details_cls", "details_kwargs", "permission"),
        [
            ("is_authorized_dag", DagDetails, {"team_name": "team-a"}, "Dag:team-a#LIST"),
            (
                "is_authorized_connection",
                ConnectionDetails,
                {"team_name": "team-a"},
                "Connection:team-a#LIST",
            ),
            ("is_authorized_variable", VariableDetails, {"team_name": "team-a"}, "Variable:team-a#LIST"),
            ("is_authorized_pool", PoolDetails, {"team_name": "team-a"}, "Pool:team-a#LIST"),
        ],
    )
    def test_list_with_team_name_uses_team_scoped_permission(
        self, auth_manager_multi_team, user, function, details_cls, details_kwargs, permission
    ):
        details = details_cls(**details_kwargs)
        user.access_token = _build_access_token({"groups": ["team-a"]})
        mock_response = Mock()
        mock_response.status_code = 200
        auth_manager_multi_team.http_session.post = Mock(return_value=mock_response)

        getattr(auth_manager_multi_team, function)(method="GET", user=user, details=details)

        actual_permission = auth_manager_multi_team.http_session.post.call_args.kwargs["data"]["permission"]
        assert actual_permission == permission

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @patch.object(KeycloakAuthManager, "is_authorized_dag", return_value=False)
    def test_filter_authorized_dag_ids_team_mismatch(self, mock_is_authorized, auth_manager_multi_team, user):
        result = auth_manager_multi_team.filter_authorized_dag_ids(
            dag_ids={"dag-a"}, user=user, team_name="team-b"
        )

        mock_is_authorized.assert_called_once()
        assert result == set()

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @patch.object(KeycloakAuthManager, "is_authorized_dag", return_value=True)
    def test_filter_authorized_dag_ids_team_match(self, mock_is_authorized, auth_manager_multi_team, user):
        result = auth_manager_multi_team.filter_authorized_dag_ids(
            dag_ids={"dag-a"}, user=user, team_name="team-a"
        )

        mock_is_authorized.assert_called_once()
        assert result == {"dag-a"}

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @patch.object(KeycloakAuthManager, "is_authorized_pool", return_value=False)
    def test_filter_authorized_pools_no_team_returns_empty(
        self, mock_is_authorized, auth_manager_multi_team, user
    ):
        result = auth_manager_multi_team.filter_authorized_pools(
            pool_names={"pool-a"}, user=user, team_name=None
        )

        mock_is_authorized.assert_called_once()
        assert result == set()

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team_name not supported before Airflow 3.2.0")
    @pytest.mark.parametrize(
        ("function", "details_cls", "details_kwargs"),
        [
            ("is_authorized_dag", DagDetails, {"team_name": "team-b"}),
            ("is_authorized_connection", ConnectionDetails, {"team_name": "team-b"}),
            ("is_authorized_variable", VariableDetails, {"team_name": "team-b"}),
            ("is_authorized_pool", PoolDetails, {"team_name": "team-b"}),
        ],
    )
    def test_list_with_mismatched_team_delegates_to_keycloak(
        self, auth_manager_multi_team, user, function, details_cls, details_kwargs
    ):
        details = details_cls(**details_kwargs)
        mock_response = Mock()
        mock_response.status_code = 403
        auth_manager_multi_team.http_session.post = Mock(return_value=mock_response)

        result = getattr(auth_manager_multi_team, function)(method="GET", user=user, details=details)

        auth_manager_multi_team.http_session.post.assert_called_once()
        assert result is False

    def test_filter_authorized_menu_items_with_batch_authorized(self, auth_manager, user):
        with patch.object(
            KeycloakAuthManager,
            "_is_batch_authorized",
            return_value={("MENU", menu.value) for menu in MenuItem},
        ):
            result = auth_manager.filter_authorized_menu_items(list(MenuItem), user=user)

        assert set(result) == set(MenuItem)

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
        headers = auth_manager._get_headers(user.access_token)
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
        headers = auth_manager._get_headers(user.access_token)
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
            [401, [{"scopes": ["MENU"], "rsname": "Assets"}], set()],
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
        headers = auth_manager._get_headers(user.access_token)
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
        headers = auth_manager._get_headers(user.access_token)
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

    @pytest.mark.parametrize(
        ("client_id", "client_secret"),
        [
            ("test_client", None),
            (None, "test_secret"),
        ],
    )
    def test_get_keycloak_client_with_partial_credentials_raises_error(
        self, auth_manager, client_id, client_secret
    ):
        """Test that providing only client_id or only client_secret raises ValueError."""
        with pytest.raises(
            ValueError, match="Both `client_id` and `client_secret` must be provided together"
        ):
            auth_manager.get_keycloak_client(client_id=client_id, client_secret=client_secret)

    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakOpenID")
    def test_get_keycloak_client_with_both_credentials(self, mock_keycloak_openid, auth_manager):
        """Test that providing both client_id and client_secret works correctly."""
        client = auth_manager.get_keycloak_client(client_id="test_client", client_secret="test_secret")

        mock_keycloak_openid.assert_called_once_with(
            server_url="server_url",
            realm_name="realm",
            client_id="test_client",
            client_secret_key="test_secret",
        )
        assert client == mock_keycloak_openid.return_value

    @patch("airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakOpenID")
    def test_get_keycloak_client_with_no_credentials(self, mock_keycloak_openid, auth_manager):
        """Test that providing neither credential uses config defaults."""
        client = auth_manager.get_keycloak_client()

        mock_keycloak_openid.assert_called_once_with(
            server_url="server_url",
            realm_name="realm",
            client_id="client_id",
            client_secret_key="client_secret",
        )
        assert client == mock_keycloak_openid.return_value

    @pytest.mark.parametrize(
        ("client_id", "permission", "attributes", "expected_claims"),
        [
            # Test without attributes - no claim_token should be added
            ("test_client", "DAG#GET", None, None),
            # Test with single attribute
            ("test_client", "DAG#READ", {"resource_id": "test_dag"}, {"resource_id": ["test_dag"]}),
            # Test with multiple attributes
            (
                "test_client",
                "DAG#GET",
                {"resource_id": "my_dag", "dag_entity": "RUN"},
                {"dag_entity": ["RUN"], "resource_id": ["my_dag"]},  # sorted by key
            ),
            # Test with different attribute types
            (
                "my_client",
                "Connection#POST",
                {"resource_id": "conn123", "extra": "value"},
                {"extra": ["value"], "resource_id": ["conn123"]},  # sorted by key
            ),
        ],
    )
    def test_get_payload(self, client_id, permission, attributes, expected_claims, auth_manager):
        """Test _get_payload with various attribute configurations."""
        import base64

        payload = auth_manager._get_payload(client_id, permission, attributes)

        # Verify basic payload structure
        assert payload["grant_type"] == "urn:ietf:params:oauth:grant-type:uma-ticket"
        assert payload["audience"] == client_id
        assert payload["permission"] == permission

        if attributes is None:
            # When no attributes, claim_token should not be present
            assert "claim_token" not in payload
            assert "claim_token_format" not in payload
        else:
            # When attributes are provided, claim_token should be present
            assert "claim_token" in payload
            assert "claim_token_format" in payload
            assert payload["claim_token_format"] == "urn:ietf:params:oauth:token-type:jwt"

            # Decode and verify the claim_token contains the attributes as arrays
            decoded_claim = base64.b64decode(payload["claim_token"]).decode()
            claims = json.loads(decoded_claim)
            assert claims == expected_claims

    @pytest.mark.parametrize(
        ("server_url", "expected_url"),
        [
            (
                "https://keycloak.example.com/auth",
                "https://keycloak.example.com/auth/realms/myrealm/protocol/openid-connect/token",
            ),
            (
                "https://keycloak.example.com/auth/",
                "https://keycloak.example.com/auth/realms/myrealm/protocol/openid-connect/token",
            ),
            (
                "https://keycloak.example.com/auth///",
                "https://keycloak.example.com/auth/realms/myrealm/protocol/openid-connect/token",
            ),
            (
                "https://keycloak.example.com/",
                "https://keycloak.example.com/realms/myrealm/protocol/openid-connect/token",
            ),
        ],
    )
    def test_get_token_url_normalization(self, auth_manager, server_url, expected_url):
        """Test that _get_token_url normalizes server_url by stripping trailing slashes."""
        token_url = auth_manager._get_token_url(server_url, "myrealm")
        assert token_url == expected_url
