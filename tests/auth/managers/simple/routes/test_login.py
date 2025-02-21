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

from airflow.auth.managers.simple.datamodels.login import LoginResponse

TEST_USER_1 = "test1"
TEST_USER_2 = "test2"


class TestLogin:
    @pytest.mark.parametrize(
        "test_user",
        [
            TEST_USER_1,
            TEST_USER_2,
        ],
    )
    @patch("airflow.auth.managers.simple.routes.login.SimpleAuthManagerLogin")
    def test_create_token(self, mock_simple_auth_manager_login, test_client, auth_manager, test_user):
        mock_simple_auth_manager_login.create_token.return_value = LoginResponse(jwt_token="DUMMY_TOKEN")

        response = test_client.post(
            "/token",
            json={"username": test_user, "password": "DUMMY_PASS"},
        )
        assert response.status_code == 201
        assert response.json()["jwt_token"]

    def test_create_token_invalid_user_password(self, test_client):
        response = test_client.post(
            "/token",
            json={"username": "INVALID_USER", "password": "INVALID_PASS"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid credentials"

    @pytest.mark.parametrize(
        "test_user",
        [
            TEST_USER_1,
            TEST_USER_2,
        ],
    )
    @patch("airflow.auth.managers.simple.routes.login.SimpleAuthManagerLogin")
    def test_create_token_cli(self, mock_simple_auth_manager_login, test_client, auth_manager, test_user):
        mock_simple_auth_manager_login.create_token.return_value = LoginResponse(jwt_token="DUMMY_TOKEN")

        response = test_client.post(
            "/token/cli",
            json={"username": test_user, "password": "DUMMY_PASS"},
        )
        assert response.status_code == 201
        assert response.json()["jwt_token"]

    def test_create_token_invalid_user_password_cli(self, test_client):
        response = test_client.post(
            "/token/cli",
            json={"username": "INVALID_USER", "password": "INVALID_PASS"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid credentials"
