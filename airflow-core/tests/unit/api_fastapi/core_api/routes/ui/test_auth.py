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

from unittest import mock

import pytest

from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.common.types import ExtraMenuItem, MenuItem

pytestmark = pytest.mark.db_test


class TestGetAuthLinks:
    @mock.patch("airflow.api_fastapi.core_api.routes.ui.auth.get_auth_manager")
    def test_should_response_200(self, mock_get_auth_manager, test_client):
        mock_get_auth_manager.return_value.get_authorized_menu_items.return_value = [
            MenuItem.VARIABLES,
            MenuItem.CONNECTIONS,
        ]
        mock_get_auth_manager.return_value.get_extra_menu_items.return_value = [
            ExtraMenuItem(text="name1", href="path1"),
            ExtraMenuItem(text="name2", href="path2"),
        ]
        response = test_client.get("/auth/menus")

        assert response.status_code == 200
        assert response.json() == {
            "authorized_menu_items": ["Variables", "Connections"],
            "extra_menu_items": [
                {"text": "name1", "href": "path1"},
                {"text": "name2", "href": "path2"},
            ],
        }

    def test_with_unauthenticated_user(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/auth/menus")
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}

    @mock.patch.object(SimpleAuthManager, "filter_authorized_menu_items", return_value=[])
    def test_with_unauthorized_user(self, _, unauthorized_test_client):
        response = unauthorized_test_client.get("/auth/menus")
        assert response.status_code == 200
        assert response.json() == {"authorized_menu_items": [], "extra_menu_items": []}


class TestGetMeResponse:
    def test_should_response_200_with_authenticated_user(self, test_client):
        """Test /auth/me endpoint with SimpleAuthManager authenticated user."""
        response = test_client.get("/auth/me")

        assert response.status_code == 200
        assert response.json() == {
            "username": "test",
            "id": "test",
        }

    def test_with_unauthenticated_user(self, unauthenticated_test_client):
        """Test /auth/me endpoint with no authentication."""
        response = unauthenticated_test_client.get("/auth/me")
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}
