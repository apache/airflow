#
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

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from airflow.auth.managers.simple.datamodels.login import LoginBody, LoginResponse
from airflow.auth.managers.simple.services.login import SimpleAuthManagerLogin

from tests_common.test_utils.config import conf_vars

TEST_USER_1 = "test1"
TEST_ROLE_1 = "viewer"
TEST_USER_2 = "test2"
TEST_ROLE_2 = "admin"


@pytest.mark.db_test
class TestLogin:
    @pytest.mark.parametrize(
        "test_user",
        [
            TEST_USER_1,
            TEST_USER_2,
        ],
    )
    @patch("airflow.auth.managers.simple.services.login.get_auth_manager")
    def test_create_token(self, get_auth_manager, auth_manager, test_user):
        get_auth_manager.return_value = auth_manager

        with conf_vars(
            {
                (
                    "core",
                    "simple_auth_manager_users",
                ): f"{TEST_USER_1}:{TEST_ROLE_1},{TEST_USER_2}:{TEST_ROLE_2}",
            }
        ):
            auth_manager.init()
            users = auth_manager.get_users()
            passwords = auth_manager.get_passwords(users=users)
            login_response: LoginResponse = SimpleAuthManagerLogin.create_token(
                body=LoginBody(username=test_user, password=passwords.get(test_user, "invalid_password")),
                expiration_time_in_sec=1,
            )
            assert login_response.jwt_token if test_user in [TEST_USER_1, TEST_USER_2] else True

    @pytest.mark.parametrize(
        "json_body",
        [
            {"username": "test", "password": ""},
            {"username": "", "password": "test"},
            {"username": "", "password": ""},
        ],
    )
    def test_create_token_empty_user_password(self, test_client, json_body):
        with pytest.raises(HTTPException) as ex:
            SimpleAuthManagerLogin.create_token(
                body=LoginBody(username=json_body["username"], password=json_body["password"]),
                expiration_time_in_sec=1,
            )
        assert "Username and password must be provided" in ex.value.detail
