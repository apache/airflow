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
from starlette.exceptions import HTTPException

from airflow.providers.fab.auth_manager.api_fastapi.services.login import FABAuthManagerLogin


@pytest.fixture
def auth_manager():
    return MagicMock()


@pytest.fixture
def user():
    user = MagicMock()
    user.password = "dummy"
    return user


@patch("airflow.providers.fab.auth_manager.api_fastapi.services.login.get_fab_auth_manager")
class TestLogin:
    def setup_method(
        self,
    ):
        self.login_body = {"username": "username", "password": "password"}
        self.dummy_token = "DUMMY_TOKEN"

    def test_create_token(self, get_auth_manager, auth_manager, user):
        auth_manager.create_token.return_value = user
        auth_manager.generate_jwt.return_value = self.dummy_token

        get_auth_manager.return_value = auth_manager

        result = FABAuthManagerLogin.create_token(
            headers={},
            body=self.login_body,
        )
        assert result.access_token == self.dummy_token
        auth_manager.create_token.assert_called_once_with(headers={}, body=self.login_body)
        auth_manager.generate_jwt.assert_called_once_with(user=user, expiration_time_in_seconds=ANY)

    def test_create_token_no_user(self, get_auth_manager, auth_manager):
        auth_manager.create_token.return_value = None
        get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Invalid credentials") as ex:
            FABAuthManagerLogin.create_token(
                headers={},
                body=self.login_body,
            )
        assert ex.value.status_code == 401

    def test_create_token_wrong_payload(self, get_auth_manager, auth_manager):
        auth_manager.create_token.side_effect = ValueError("test")
        get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="test") as ex:
            FABAuthManagerLogin.create_token(
                headers={},
                body=self.login_body,
            )
        assert ex.value.status_code == 400
