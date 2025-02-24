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
from unittest.mock import MagicMock, patch

from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse

TEST_USER_1 = "test1"
TEST_ROLE_1 = "viewer"
TEST_USER_2 = "test2"
TEST_ROLE_2 = "admin"


class TestLogin:
    def setup_method(
        self,
    ):
        self.dummy_app_builder = MagicMock()
        self.dummy_app = MagicMock()

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.login.get_auth_manager")
    def test_create_token(self, get_auth_manager, fab_auth_manager):
        get_auth_manager.return_value = fab_auth_manager
        fab_auth_manager.init()

        fab_auth_manager.appbuilder = self.dummy_app_builder
        self.dummy_app_builder.app = self.dummy_app

        security_manager = MagicMock()
        fab_auth_manager.security_manager = security_manager
        security_manager.refresh_jwt_token.return_value = "DUMMY_TOKEN"

        login_response: LoginResponse = FABAuthManagerLogin.create_token(
            expiration_time_in_sec=1,
        )
        assert login_response.jwt_token == "DUMMY_TOKEN"
