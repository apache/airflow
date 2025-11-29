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

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from jwt import InvalidTokenError

from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager, T
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.managers.models.resource_details import (
    BackfillDetails,
    ConnectionDetails,
    DagDetails,
    PoolDetails,
    TeamDetails,
    VariableDetails,
)
from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.common.types import MenuItem

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import ResourceMethod
    from airflow.api_fastapi.auth.managers.models.resource_details import (
        AccessView,
        AssetAliasDetails,
        AssetDetails,
        ConfigurationDetails,
        DagAccessEntity,
    )


class BaseAuthManagerUserTest(BaseUser):
    def __init__(self, *, name: str) -> None:
        self.name = name

    def get_id(self) -> str:
        return self.name

    def get_name(self) -> str:
        return self.name


class EmptyAuthManager(BaseAuthManager[BaseAuthManagerUserTest]):
    def deserialize_user(self, token: dict[str, Any]) -> BaseAuthManagerUserTest:
        raise NotImplementedError()

    def serialize_user(self, user: BaseAuthManagerUserTest) -> dict[str, Any]:
        raise NotImplementedError()

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_backfill(
        self,
        *,
        method: ResourceMethod,
        details: BackfillDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_asset(
        self,
        *,
        method: ResourceMethod,
        details: AssetDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_asset_alias(
        self,
        *,
        method: ResourceMethod,
        details: AssetAliasDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_pool(
        self,
        *,
        method: ResourceMethod,
        details: PoolDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_variable(
        self,
        *,
        method: ResourceMethod,
        details: VariableDetails | None = None,
        user: BaseAuthManagerUserTest | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_view(
        self, *, access_view: AccessView, user: BaseAuthManagerUserTest | None = None
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseAuthManagerUserTest | None = None
    ):
        raise NotImplementedError()

    def filter_authorized_menu_items(self, menu_items: list[MenuItem], *, user: T) -> list[MenuItem]:
        raise NotImplementedError()

    def get_url_login(self, **kwargs) -> str:
        raise NotImplementedError()


@pytest.fixture
def auth_manager():
    return EmptyAuthManager()


class TestBaseAuthManager:
    def test_get_cli_commands_return_empty_list(self, auth_manager):
        assert auth_manager.get_cli_commands() == []

    def test_get_fastapi_app_return_none(self, auth_manager):
        assert auth_manager.get_fastapi_app() is None

    def test_refresh_user_default_returns_none(self, auth_manager):
        assert auth_manager.refresh_user(user=BaseAuthManagerUserTest(name="test")) is None

    def test_get_url_logout_return_none(self, auth_manager):
        assert auth_manager.get_url_logout() is None

    def test_get_extra_menu_items_return_empty_list(self, auth_manager):
        assert auth_manager.get_extra_menu_items(user=BaseAuthManagerUserTest(name="test")) == []

    def test_get_db_manager_return_none(self, auth_manager):
        assert auth_manager.get_db_manager() is None

    def test_is_authorized_team(self, auth_manager):
        with pytest.raises(
            NotImplementedError, match="The auth manager you are using is not compatible with multi-team"
        ):
            auth_manager.is_authorized_team(method="GET", user=BaseAuthManagerUserTest(name="test"))

    @patch.object(EmptyAuthManager, "filter_authorized_menu_items")
    def test_get_authorized_menu_items(self, mock_filter_authorized_menu_items, auth_manager):
        user = BaseAuthManagerUserTest(name="test")
        mock_filter_authorized_menu_items.return_value = []
        results = auth_manager.get_authorized_menu_items(user=user)
        mock_filter_authorized_menu_items.assert_called_once_with(list(MenuItem), user=user)
        assert results == []

    @patch(
        "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager._get_token_validator",
        autospec=True,
    )
    @patch.object(EmptyAuthManager, "deserialize_user")
    @pytest.mark.asyncio
    async def test_get_user_from_token(self, mock_deserialize_user, mock__get_token_validator, auth_manager):
        token = "token"
        payload = {}
        user = BaseAuthManagerUserTest(name="test")
        signer = AsyncMock(spec=JWTValidator)
        signer.avalidated_claims.return_value = payload
        mock__get_token_validator.return_value = signer
        mock_deserialize_user.return_value = user

        result = await auth_manager.get_user_from_token(token)

        mock_deserialize_user.assert_called_once_with(payload)
        signer.avalidated_claims.assert_called_once_with(token)
        assert result == user

    @patch(
        "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager._get_token_validator",
        autospec=True,
    )
    @patch.object(EmptyAuthManager, "deserialize_user")
    @pytest.mark.asyncio
    async def test_get_user_from_token_invalid_token_payload(
        self, mock_deserialize_user, mock__get_token_validator, auth_manager
    ):
        token = "token"
        payload = {}
        signer = AsyncMock(spec=JWTValidator)
        signer.avalidated_claims.return_value = payload
        mock__get_token_validator.return_value = signer
        mock_deserialize_user.side_effect = ValueError("Some error deserializing the user")

        with pytest.raises(InvalidTokenError, match="Some error deserializing the user"):
            await auth_manager.get_user_from_token(token)
        mock_deserialize_user.assert_called_once_with(payload)
        signer.avalidated_claims.assert_called_once_with(token)

    @patch("airflow.api_fastapi.auth.managers.base_auth_manager.JWTGenerator", autospec=True)
    @patch.object(EmptyAuthManager, "serialize_user")
    def test_generate_jwt_token(self, mock_serialize_user, mock_jwt_generator, auth_manager):
        token = "token"
        serialized_user = "serialized_user"
        signer = Mock(spec=JWTGenerator)
        signer.generate.return_value = token
        mock_jwt_generator.return_value = signer
        mock_serialize_user.return_value = {"sub": serialized_user}
        user = BaseAuthManagerUserTest(name="test")

        result = auth_manager.generate_jwt(user)

        mock_serialize_user.assert_called_once_with(user)
        signer.generate.assert_called_once_with({"sub": serialized_user})
        assert result == token

    @pytest.mark.parametrize(
        ("return_values", "expected"),
        [
            ([False, False], False),
            ([True, False], False),
            ([True, True], True),
        ],
    )
    @patch.object(EmptyAuthManager, "is_authorized_connection")
    def test_batch_is_authorized_connection(
        self, mock_is_authorized_connection, auth_manager, return_values, expected
    ):
        mock_is_authorized_connection.side_effect = return_values
        result = auth_manager.batch_is_authorized_connection(
            [
                {"method": "GET", "details": ConnectionDetails(conn_id="conn1")},
                {"method": "GET", "details": ConnectionDetails(conn_id="conn2")},
            ],
            user=Mock(),
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("return_values", "expected"),
        [
            ([False, False], False),
            ([True, False], False),
            ([True, True], True),
        ],
    )
    @patch.object(EmptyAuthManager, "is_authorized_dag")
    def test_batch_is_authorized_dag(self, mock_is_authorized_dag, auth_manager, return_values, expected):
        mock_is_authorized_dag.side_effect = return_values
        result = auth_manager.batch_is_authorized_dag(
            [
                {"method": "GET", "details": DagDetails(id="dag1")},
                {"method": "GET", "details": DagDetails(id="dag2")},
            ],
            user=Mock(),
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("return_values", "expected"),
        [
            ([False, False], False),
            ([True, False], False),
            ([True, True], True),
        ],
    )
    @patch.object(EmptyAuthManager, "is_authorized_pool")
    def test_batch_is_authorized_pool(self, mock_is_authorized_pool, auth_manager, return_values, expected):
        mock_is_authorized_pool.side_effect = return_values
        result = auth_manager.batch_is_authorized_pool(
            [
                {"method": "GET", "details": PoolDetails(name="pool1")},
                {"method": "GET", "details": PoolDetails(name="pool2")},
            ],
            user=Mock(),
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("return_values", "expected"),
        [
            ([False, False], False),
            ([True, False], False),
            ([True, True], True),
        ],
    )
    @patch.object(EmptyAuthManager, "is_authorized_variable")
    def test_batch_is_authorized_variable(
        self, mock_is_authorized_variable, auth_manager, return_values, expected
    ):
        mock_is_authorized_variable.side_effect = return_values
        result = auth_manager.batch_is_authorized_variable(
            [
                {"method": "GET", "details": VariableDetails(key="var1")},
                {"method": "GET", "details": VariableDetails(key="var2")},
            ],
            user=Mock(),
        )
        assert result == expected

    @pytest.mark.parametrize(
        ("access_per_dag", "access_per_team", "rows", "expected"),
        [
            # Without teams
            # No access to any dag
            (
                {},
                {},
                [("dag1", None), ("dag2", None)],
                set(),
            ),
            # Access to specific dags
            (
                {"dag1": True},
                {},
                [("dag1", None), ("dag2", None)],
                {"dag1"},
            ),
            # With teams
            # No access to any dag
            (
                {},
                {},
                [("dag1", "team1"), ("dag2", "team2")],
                set(),
            ),
            # Access to a specific team
            (
                {},
                {"team1": True},
                [("dag1", "team1"), ("dag2", "team1"), ("dag3", "team2")],
                {"dag1", "dag2"},
            ),
        ],
    )
    def test_get_authorized_dag_ids(
        self, auth_manager, access_per_dag: dict, access_per_team: dict, rows: list, expected: set
    ):
        def side_effect_func(
            *,
            method: ResourceMethod,
            user: BaseAuthManagerUserTest,
            access_entity: DagAccessEntity | None = None,
            details: DagDetails | None = None,
        ):
            if not details:
                return False
            return access_per_dag.get(details.id, False) or access_per_team.get(details.team_name, False)

        auth_manager.is_authorized_dag = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        session.execute.return_value.all.return_value = rows
        result = auth_manager.get_authorized_dag_ids(user=user, session=session)
        assert result == expected

    @pytest.mark.parametrize(
        ("access_per_connection", "access_per_team", "rows", "expected"),
        [
            # Without teams
            # No access to any connection
            (
                {},
                {},
                [("conn1", None), ("conn2", None)],
                set(),
            ),
            # Access to specific connections
            (
                {"conn1": True},
                {},
                [("conn1", None), ("conn2", None)],
                {"conn1"},
            ),
            # With teams
            # No access to any connection
            (
                {},
                {},
                [("conn1", "team1"), ("conn2", "team2")],
                set(),
            ),
            # Access to a specific team
            (
                {},
                {"team1": True},
                [("conn1", "team1"), ("conn2", "team1"), ("conn3", "team2")],
                {"conn1", "conn2"},
            ),
        ],
    )
    def test_get_authorized_connections(
        self, auth_manager, access_per_connection: dict, access_per_team: dict, rows: list, expected: set
    ):
        def side_effect_func(
            *,
            method: ResourceMethod,
            user: BaseAuthManagerUserTest,
            details: ConnectionDetails | None = None,
        ):
            if not details:
                return False
            return access_per_connection.get(details.conn_id, False) or access_per_team.get(
                details.team_name, False
            )

        auth_manager.is_authorized_connection = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        session.execute.return_value.all.return_value = rows
        result = auth_manager.get_authorized_connections(user=user, session=session)
        assert result == expected

    @pytest.mark.parametrize(
        ("access_per_team", "rows", "expected"),
        [
            # No access to any team
            (
                {},
                [("1", "team1"), ("2", "team2")],
                set(),
            ),
            # Access to specific teams
            (
                {"team1": True},
                [("1", "team1"), ("2", "team2")],
                {"team1"},
            ),
        ],
    )
    def test_get_authorized_teams(self, auth_manager, access_per_team: dict, rows: list, expected: set):
        def side_effect_func(
            *,
            method: ResourceMethod,
            user: BaseAuthManagerUserTest,
            details: TeamDetails | None = None,
        ):
            if not details:
                return False
            return access_per_team.get(details.name, False)

        auth_manager.is_authorized_team = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        session.execute.return_value.all.return_value = rows
        result = auth_manager.get_authorized_teams(user=user, session=session)
        assert result == expected

    @pytest.mark.parametrize(
        ("access_per_variable", "access_per_team", "rows", "expected"),
        [
            # Without teams
            # No access to any variable
            (
                {},
                {},
                [("var1", None), ("var2", None)],
                set(),
            ),
            # Access to specific variables
            (
                {"var1": True},
                {},
                [("var1", None), ("var2", None)],
                {"var1"},
            ),
            # With teams
            # No access to any variable
            (
                {},
                {},
                [("var1", "team1"), ("var2", "team2")],
                set(),
            ),
            # Access to a specific team
            (
                {},
                {"team1": True},
                [("var1", "team1"), ("var2", "team1"), ("var3", "team2")],
                {"var1", "var2"},
            ),
        ],
    )
    def test_get_authorized_variables(
        self, auth_manager, access_per_variable: dict, access_per_team: dict, rows: list, expected: set
    ):
        def side_effect_func(
            *,
            method: ResourceMethod,
            user: BaseAuthManagerUserTest,
            details: VariableDetails | None = None,
        ):
            if not details:
                return False
            return access_per_variable.get(details.key, False) or access_per_team.get(
                details.team_name, False
            )

        auth_manager.is_authorized_variable = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        session.execute.return_value.all.return_value = rows
        result = auth_manager.get_authorized_variables(user=user, session=session)
        assert result == expected

    @pytest.mark.parametrize(
        ("access_per_pool", "access_per_team", "rows", "expected"),
        [
            # Without teams
            # No access to any pool
            (
                {},
                {},
                [("pool1", None), ("pool2", None)],
                set(),
            ),
            # Access to specific pools
            (
                {"pool1": True},
                {},
                [("pool1", None), ("pool2", None)],
                {"pool1"},
            ),
            # With teams
            # No access to any pool
            (
                {},
                {},
                [("pool1", "team1"), ("pool2", "team2")],
                set(),
            ),
            # Access to a specific team
            (
                {},
                {"team1": True},
                [("pool1", "team1"), ("pool2", "team1"), ("pool3", "team2")],
                {"pool1", "pool2"},
            ),
        ],
    )
    def test_get_authorized_pools(
        self, auth_manager, access_per_pool: dict, access_per_team: dict, rows: list, expected: set
    ):
        def side_effect_func(
            *,
            method: ResourceMethod,
            user: BaseAuthManagerUserTest,
            details: PoolDetails | None = None,
        ):
            if not details:
                return False
            return access_per_pool.get(details.name, False) or access_per_team.get(details.team_name, False)

        auth_manager.is_authorized_pool = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        session.execute.return_value.all.return_value = rows
        result = auth_manager.get_authorized_pools(user=user, session=session)
        assert result == expected
