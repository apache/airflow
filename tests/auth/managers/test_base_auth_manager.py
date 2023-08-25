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

from unittest import mock

import pytest

from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.auth.managers.models.authorized_action import AuthorizedAction
from airflow.auth.managers.models.base_user import BaseUser
from airflow.auth.managers.models.resource_details import ResourceDetails
from airflow.auth.managers.models.resource_method import ResourceMethod
from airflow.auth.managers.models.resource_type import ResourceType
from airflow.exceptions import AirflowException
from airflow.www.security import ApplessAirflowSecurityManager


class EmptyAuthManager(BaseAuthManager):
    def get_user_name(self) -> str:
        raise NotImplementedError()

    def get_user(self) -> BaseUser:
        raise NotImplementedError()

    def get_user_id(self) -> str:
        raise NotImplementedError()

    def is_logged_in(self) -> bool:
        raise NotImplementedError()

    def is_authorized(
        self,
        action: ResourceMethod,
        resource_type: ResourceType,
        resource_details: ResourceDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def get_url_login(self, **kwargs) -> str:
        raise NotImplementedError()

    def get_url_logout(self) -> str:
        raise NotImplementedError()

    def get_url_user_profile(self) -> str | None:
        raise NotImplementedError()


@pytest.fixture
def auth_manager():
    return EmptyAuthManager()


class TestBaseAuthManager:
    @pytest.mark.parametrize(
        "actions, is_authorized_per_action, expected_result",
        [
            # Edge case: empty list of actions
            (
                [],
                [],
                True,
            ),
            # One action with permissions
            (
                [AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.VARIABLE)],
                [True],
                True,
            ),
            # One action without permissions
            (
                [AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.VARIABLE)],
                [False],
                False,
            ),
            # Several actions, one without permission
            (
                [
                    AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.VARIABLE),
                    AuthorizedAction(action=ResourceMethod.POST, resource_type=ResourceType.VARIABLE),
                    AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.XCOM),
                ],
                [True, True, False],
                False,
            ),
            # Several actions, all with permission
            (
                [
                    AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.VARIABLE),
                    AuthorizedAction(action=ResourceMethod.POST, resource_type=ResourceType.VARIABLE),
                    AuthorizedAction(action=ResourceMethod.GET, resource_type=ResourceType.XCOM),
                ],
                [True, True, True],
                True,
            ),
        ],
    )
    @mock.patch.object(EmptyAuthManager, "is_authorized")
    def test_is_all_authorized(
        self, mock_is_authorized, actions, is_authorized_per_action, expected_result, auth_manager
    ):
        mock_is_authorized.side_effect = is_authorized_per_action
        result = auth_manager.is_all_authorized(actions)
        assert result is expected_result

    def test_get_security_manager_override_class_return_empty_class(self, auth_manager):
        assert auth_manager.get_security_manager_override_class() is object

    def test_get_security_manager_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="Security manager not defined."):
            _security_manager = auth_manager.security_manager

    def test_get_security_manager_defined(self, auth_manager):
        auth_manager.security_manager = ApplessAirflowSecurityManager()
        _security_manager = auth_manager.security_manager
        assert type(_security_manager) is ApplessAirflowSecurityManager

    @pytest.mark.parametrize(
        "resource_type, result",
        [
            (ResourceType.DAG, True),
            (ResourceType.VARIABLE, False),
        ],
    )
    def test_is_dag_resource(self, resource_type, result):
        assert BaseAuthManager.is_dag_resource(resource_type) is result
