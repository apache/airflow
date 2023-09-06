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

import pytest

from airflow.auth.managers.base_auth_manager import BaseAuthManager
from airflow.exceptions import AirflowException
from airflow.www.security_appless import ApplessAirflowSecurityManager
from airflow.www.security_manager import AirflowSecurityManagerV2


@pytest.fixture
def auth_manager():
    class EmptyAuthManager(BaseAuthManager):
        def get_user_name(self) -> str:
            raise NotImplementedError()

        def is_logged_in(self) -> bool:
            raise NotImplementedError()

        def get_url_login(self, **kwargs) -> str:
            raise NotImplementedError()

    # noinspection PyTypeChecker
    return EmptyAuthManager(None)


class TestBaseAuthManager:
    def test_get_security_manager_override_class_return_empty_class(self, auth_manager):
        assert auth_manager.get_security_manager_override_class() is AirflowSecurityManagerV2

    def test_get_security_manager_not_defined(self, auth_manager):
        with pytest.raises(AirflowException, match="Security manager not defined."):
            _security_manager = auth_manager.security_manager

    def test_get_security_manager_defined(self, auth_manager):
        auth_manager.security_manager = ApplessAirflowSecurityManager()
        _security_manager = auth_manager.security_manager
        assert type(_security_manager) is ApplessAirflowSecurityManager
