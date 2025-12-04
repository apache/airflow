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

from unittest.mock import ANY, MagicMock, patch

import pytest
from flask_appbuilder.const import AUTH_DB, AUTH_LDAP
from starlette.exceptions import HTTPException

from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin


@pytest.fixture
def auth_manager():
    return MagicMock()


@pytest.fixture
def security_manager():
    return MagicMock()


@pytest.fixture
def user():
    user = MagicMock()
    user.password = "dummy"
    return user


@patch("airflow.providers.fab.auth_manager.api_fastapi.services.login.get_auth_manager")
class TestLogin:
    def setup_method(
        self,
    ):
        self.login_body = MagicMock()
        self.login_body.username = "username"
        self.login_body.password = "password"
        self.dummy_token = "DUMMY_TOKEN"

    @pytest.mark.parametrize(
        ("auth_type", "method"),
        [
            [AUTH_DB, "auth_user_db"],
            [AUTH_LDAP, "auth_user_ldap"],
        ],
    )
    def test_create_token(self, get_auth_manager, auth_type, method, auth_manager, security_manager, user):
        security_manager.auth_type = auth_type
        getattr(security_manager, method).return_value = user

        auth_manager.security_manager = security_manager
        auth_manager.generate_jwt.return_value = self.dummy_token

        get_auth_manager.return_value = auth_manager

        result = FABAuthManagerLogin.create_token(
            body=self.login_body,
        )
        assert result.access_token == self.dummy_token
        getattr(security_manager, method).assert_called_once_with(
            self.login_body.username, self.login_body.password, rotate_session_id=False
        )
        auth_manager.generate_jwt.assert_called_once_with(user=user, expiration_time_in_seconds=ANY)

    @pytest.mark.parametrize(
        ("auth_type", "methods"),
        [
            [AUTH_DB, ["auth_user_db"]],
            [AUTH_LDAP, ["auth_user_ldap", "auth_user_db"]],
        ],
    )
    def test_create_token_no_user(
        self, get_auth_manager, auth_type, methods, auth_manager, security_manager, user
    ):
        security_manager.auth_type = auth_type
        for method in methods:
            getattr(security_manager, method).return_value = None

        auth_manager.security_manager = security_manager
        get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerLogin.create_token(
                body=self.login_body,
            )
        assert ex.value.status_code == 401
