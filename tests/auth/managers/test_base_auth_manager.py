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
from unittest.mock import MagicMock, Mock

import pytest
from flask import Flask

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.exceptions import AirflowException
from airflow.security import permissions
from airflow.www.extensions.init_appbuilder import init_appbuilder
from airflow.www.security_manager import AirflowSecurityManagerV2

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import (
        AccessView,
        ConfigurationDetails,
        ConnectionDetails,
        DagAccessEntity,
        DagDetails,
        DatasetDetails,
        PoolDetails,
        VariableDetails,
    )


class EmptyAuthManager(BaseAuthManager):
    def get_user_display_name(self) -> str:
        raise NotImplementedError()

    def get_user_name(self) -> str:
        raise NotImplementedError()

    def get_user(self) -> BaseUser:
        raise NotImplementedError()

    def get_user_id(self) -> str:
        raise NotImplementedError()

    def is_authorized_configuration(
        self,
        *,
        method: ResourceMethod,
        details: ConfigurationDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_cluster_activity(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
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
        self, *, method: ResourceMethod, details: DatasetDetails | None = None, user: BaseUser | None = None
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

    def is_logged_in(self) -> bool:
        raise NotImplementedError()

    def get_url_login(self, **kwargs) -> str:
        raise NotImplementedError()

    def get_url_logout(self) -> str:
        raise NotImplementedError()

    def get_url_user_profile(self) -> str | None:
        raise NotImplementedError()


@pytest.fixture
def auth_manager():
    return EmptyAuthManager(None, None)


@pytest.fixture
def auth_manager_with_appbuilder():
    flask_app = Flask(__name__)
    appbuilder = init_appbuilder(flask_app)
    return EmptyAuthManager(flask_app, appbuilder)


class TestBaseAuthManager:
    def test_get_cli_commands_return_empty_list(self, auth_manager):
        assert auth_manager.get_cli_commands() == []

    def test_get_api_endpoints_return_none(self, auth_manager):
        assert auth_manager.get_api_endpoints() is None

    def test_is_authorized_custom_view_throws_exception(self, auth_manager):
        with pytest.raises(AirflowException, match="The resource `.*` does not exist in the environment."):
            auth_manager.is_authorized_custom_view(
                fab_action_name=permissions.ACTION_CAN_READ,
                fab_resource_name=permissions.RESOURCE_MY_PASSWORD,
            )

    @pytest.mark.db_test
    def test_security_manager_return_default_security_manager(self, auth_manager_with_appbuilder):
        assert isinstance(auth_manager_with_appbuilder.security_manager, AirflowSecurityManagerV2)

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
