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

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser


@pytest.fixture
def auth_manager():
    return KeycloakAuthManager()


class TestKeycloakAuthManager:
    def test_deserialize_user(self, auth_manager):
        result = auth_manager.deserialize_user(
            {
                "user_id": "user_id",
                "name": "name",
                "access_token": "access_token",
                "refresh_token": "refresh_token",
            }
        )
        assert result.user_id == "user_id"
        assert result.name == "name"
        assert result.access_token == "access_token"
        assert result.refresh_token == "refresh_token"

    def test_serialize_user(self, auth_manager):
        result = auth_manager.serialize_user(
            KeycloakAuthManagerUser(
                user_id="user_id", name="name", access_token="access_token", refresh_token="refresh_token"
            )
        )
        assert result == {
            "user_id": "user_id",
            "name": "name",
            "access_token": "access_token",
            "refresh_token": "refresh_token",
        }

    def test_get_url_login(self, auth_manager):
        result = auth_manager.get_url_login()
        assert result == f"{AUTH_MANAGER_FASTAPI_APP_PREFIX}/login"
