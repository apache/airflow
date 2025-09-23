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

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX

from tests_common.test_utils.config import conf_vars


class TestTokenRouter:
    token = "token"
    token_body_dict = {"username": "username", "password": "password"}

    @conf_vars(
        {
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.token.create_token_for")
    def test_create_token(self, mock_create_token_for, client):
        mock_create_token_for.return_value = self.token
        response = client.post(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/token",
            json=self.token_body_dict,
        )

        assert response.status_code == 201
        assert response.json() == {"access_token": self.token}

    @conf_vars(
        {
            ("api_auth", "jwt_cli_expiration_time"): "10",
            ("api_auth", "jwt_expiration_time"): "10",
        }
    )
    @patch("airflow.providers.keycloak.auth_manager.routes.token.create_token_for")
    def test_create_token_cli(self, mock_create_token_for, client):
        mock_create_token_for.return_value = self.token
        response = client.post(
            AUTH_MANAGER_FASTAPI_APP_PREFIX + "/token/cli",
            json=self.token_body_dict,
        )

        assert response.status_code == 201
        assert response.json() == {"access_token": self.token}
