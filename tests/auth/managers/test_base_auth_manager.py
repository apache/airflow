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

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock, patch

import pytest
from flask_appbuilder.menu import Menu

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.auth.managers.models.resource_details import (
    ConnectionDetails,
    DagDetails,
    PoolDetails,
    VariableDetails,
)
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        AssetDetails,
        ConfigurationDetails,
        DagAccessEntity,
    )


class EmptyAuthManager(BaseAuthManager):
    def get_user(self) -> BaseUser:
        raise NotImplementedError()

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        access_entity: DagAccessEntity | None = None,
        details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_dataset(
        self, *, method: ResourceMethod, details: AssetDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_pool(
        self, *, method: ResourceMethod, details: PoolDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_variable(
        self, *, method: ResourceMethod, details: VariableDetails | None = None, user: BaseUser | None = None
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_view(self, *, access_view: AccessView, user: BaseUser | None = None) -> bool:
        raise NotImplementedError()

    def is_authorized_custom_view(
        self, *, method: ResourceMethod | str, resource_name: str, user: BaseUser | None = None
    ):
        raise NotImplementedError()

    def is_logged_in(self) -> bool:
        raise NotImplementedError()

    def get_url_login(self, **kwargs) -> str:
        raise NotImplementedError()

    def get_url_logout(self) -> str:
        raise NotImplementedError()


@pytest.fixture
def auth_manager():
    return EmptyAuthManager(None)


class TestBaseAuthManager:
    def test_get_cli_commands_return_empty_list(self, auth_manager):
        assert auth_manager.get_cli_commands() == []

    def test_get_api_endpoints_return_none(self, auth_manager):
        assert auth_manager.get_api_endpoints() is None

    def test_get_user_name(self, auth_manager):
        user = Mock()
        user.get_name.return_value = "test_username"
        auth_manager.get_user = MagicMock(return_value=user)
        result = auth_manager.get_user_name()
        assert result == "test_username"

    def test_get_user_name_when_not_logged_in(self, auth_manager):
        auth_manager.get_user = MagicMock(return_value=None)
        with pytest.raises(AirflowException):
            auth_manager.get_user_name()

    def test_get_user_display_name_return_user_name(self, auth_manager):
        auth_manager.get_user_name = MagicMock(return_value="test_user")
        assert auth_manager.get_user_display_name() == "test_user"

    def test_get_user_id_return_user_id(self, auth_manager):
        user = Mock()
        user.get_id = MagicMock(return_value="test_user")
        auth_manager.get_user = MagicMock(return_value=user)
        assert auth_manager.get_user_id() == "test_user"

    def test_get_user_id_raise_exception_when_no_user(self, auth_manager):
        auth_manager.get_user = MagicMock(return_value=None)
        with pytest.raises(AirflowException, match="The user must be signed in."):
            auth_manager.get_user_id()

    def test_get_url_user_profile_return_none(self, auth_manager):
        assert auth_manager.get_url_user_profile() is None

    @pytest.mark.parametrize(
        "return_values, expected",
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
            ]
        )
        assert result == expected

    @pytest.mark.parametrize(
        "return_values, expected",
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
            ]
        )
        assert result == expected

    @pytest.mark.parametrize(
        "return_values, expected",
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
            ]
        )
        assert result == expected

    @pytest.mark.parametrize(
        "return_values, expected",
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
            ]
        )
        assert result == expected

    @patch("airflow.www.security_manager.AirflowSecurityManagerV2")
    def test_security_manager_return_default_security_manager(
        self, mock_airflow_security_manager, auth_manager
    ):
        assert auth_manager.security_manager == mock_airflow_security_manager()

    @pytest.mark.parametrize(
        "access_all, access_per_dag, dag_ids, expected",
        [
            # Access to all dags
            (
                True,
                {},
                ["dag1", "dag2"],
                {"dag1", "dag2"},
            ),
            # No access to any dag
            (
                False,
                {},
                ["dag1", "dag2"],
                set(),
            ),
            # Access to specific dags
            (
                False,
                {"dag1": True},
                ["dag1", "dag2"],
                {"dag1"},
            ),
        ],
    )
    def test_get_permitted_dag_ids(
        self, auth_manager, access_all: bool, access_per_dag: dict, dag_ids: list, expected: set
    ):
        def side_effect_func(
            *,
            method: ResourceMethod,
            access_entity: DagAccessEntity | None = None,
            details: DagDetails | None = None,
            user: BaseUser | None = None,
        ):
            if not details:
                return access_all
            else:
                return access_per_dag.get(details.id, False)

        auth_manager.is_authorized_dag = MagicMock(side_effect=side_effect_func)
        user = Mock()
        session = Mock()
        dags = []
        for dag_id in dag_ids:
            mock = Mock()
            mock.dag_id = dag_id
            dags.append(mock)
        session.execute.return_value = dags
        result = auth_manager.get_permitted_dag_ids(user=user, session=session)
        assert result == expected

    @patch.object(EmptyAuthManager, "security_manager")
    def test_filter_permitted_menu_items(self, mock_security_manager, auth_manager):
        mock_security_manager.has_access.side_effect = [True, False, True, True, False]

        menu = Menu()
        menu.add_link(
            # These may not all be valid types, but it does let us check each attr is copied
            name="item1",
            href="h1",
            icon="i1",
            label="l1",
            baseview="b1",
            cond="c1",
        )
        menu.add_link("item2")
        menu.add_link("item3")
        menu.add_link("item3.1", category="item3")
        menu.add_link("item3.2", category="item3")

        result = auth_manager.filter_permitted_menu_items(menu.get_list())

        assert len(result) == 2
        assert result[0].name == "item1"
        assert result[1].name == "item3"
        assert len(result[1].childs) == 1
        assert result[1].childs[0].name == "item3.1"
        # check we've copied every attr
        assert result[0].href == "h1"
        assert result[0].icon == "i1"
        assert result[0].label == "l1"
        assert result[0].baseview == "b1"
        assert result[0].cond == "c1"

    @patch.object(EmptyAuthManager, "security_manager")
    def test_filter_permitted_menu_items_twice(self, mock_security_manager, auth_manager):
        mock_security_manager.has_access.side_effect = [
            # 1st call
            True,  # menu 1
            False,  # menu 2
            True,  # menu 3
            True,  # Item 3.1
            False,  # Item 3.2
            # 2nd call
            False,  # menu 1
            True,  # menu 2
            True,  # menu 3
            False,  # Item 3.1
            True,  # Item 3.2
        ]

        menu = Menu()
        menu.add_link("item1")
        menu.add_link("item2")
        menu.add_link("item3")
        menu.add_link("item3.1", category="item3")
        menu.add_link("item3.2", category="item3")

        result = auth_manager.filter_permitted_menu_items(menu.get_list())

        assert len(result) == 2
        assert result[0].name == "item1"
        assert result[1].name == "item3"
        assert len(result[1].childs) == 1
        assert result[1].childs[0].name == "item3.1"

        result = auth_manager.filter_permitted_menu_items(menu.get_list())

        assert len(result) == 2
        assert result[0].name == "item2"
        assert result[1].name == "item3"
        assert len(result[1].childs) == 1
        assert result[1].childs[0].name == "item3.2"
