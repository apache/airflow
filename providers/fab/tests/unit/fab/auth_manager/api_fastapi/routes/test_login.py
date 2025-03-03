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

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginBody, LoginResponse


@pytest.mark.db_test
class TestLogin:
    dummy_login_body = LoginBody(username="dummy", password="dummy")
    dummy_token = LoginResponse(jwt_token="DUMMY_TOKEN")

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.FABAuthManagerLogin")
    def test_create_token(self, mock_fab_auth_manager_login, test_client):
        mock_fab_auth_manager_login.create_token.return_value = self.dummy_token

        response = test_client.post(
            "/token",
            json=self.dummy_login_body.model_dump(),
        )
        assert response.status_code == 201
        assert response.json()["jwt_token"] == self.dummy_token.jwt_token

    @patch("airflow.providers.fab.auth_manager.api_fastapi.routes.login.FABAuthManagerLogin")
    def test_create_token_cli(self, mock_fab_auth_manager_login, test_client):
        mock_fab_auth_manager_login.create_token.return_value = LoginResponse(jwt_token="DUMMY_TOKEN")

        response = test_client.post(
            "/token/cli",
            json=self.dummy_login_body.model_dump(),
        )
        assert response.status_code == 201
        assert response.json()["jwt_token"] == self.dummy_token.jwt_token
