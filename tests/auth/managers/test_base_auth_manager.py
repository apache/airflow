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

import pytest

from airflow.auth.managers.base_auth_manager import BaseAuthManager, ResourceMethod
from airflow.exceptions import AirflowException
from airflow.www.security import ApplessAirflowSecurityManager

if TYPE_CHECKING:
    from airflow.auth.managers.models.base_user import BaseUser
    from airflow.auth.managers.models.resource_details import ConnectionDetails, DagAccessEntity, DagDetails


class EmptyAuthManager(BaseAuthManager):
    def get_user_name(self) -> str:
        raise NotImplementedError()

    def get_user(self) -> BaseUser:
        raise NotImplementedError()

    def get_user_id(self) -> str:
        raise NotImplementedError()

    def is_authorized_configuration(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        raise NotImplementedError()

    def is_authorized_cluster_activity(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        raise NotImplementedError()

    def is_authorized_connection(
        self,
        *,
        method: ResourceMethod,
        connection_details: ConnectionDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_dag(
        self,
        *,
        method: ResourceMethod,
        dag_access_entity: DagAccessEntity | None = None,
        dag_details: DagDetails | None = None,
        user: BaseUser | None = None,
    ) -> bool:
        raise NotImplementedError()

    def is_authorized_dataset(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        raise NotImplementedError()

    def is_authorized_variable(self, *, method: ResourceMethod, user: BaseUser | None = None) -> bool:
        raise NotImplementedError()

    def is_authorized_website(self, *, user: BaseUser | None = None) -> bool:
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
    return EmptyAuthManager()


class TestBaseAuthManager:
    def test_get_security_manager_override_class_return_empty_class(self, auth_manager):
        assert auth_manager.get_security_manager_override_class() is object

    def test_get_security_manager_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="Security manager not defined."):
            _security_manager = auth_manager.security_manager

    def test_get_security_manager_defined(self, auth_manager):
        auth_manager.security_manager = ApplessAirflowSecurityManager()
        _security_manager = auth_manager.security_manager
        assert type(_security_manager) is ApplessAirflowSecurityManager
