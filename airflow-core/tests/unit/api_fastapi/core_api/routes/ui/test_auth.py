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
from airflow.models.team import Team

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_teams

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
    @pytest.fixture(autouse=True)
    def clean_teams(self):
        clear_db_teams()
        yield
        clear_db_teams()

    def test_should_response_200_with_authenticated_user(self, test_client):
        """Test /auth/me endpoint with SimpleAuthManager authenticated user."""
        response = test_client.get("/auth/me")

        assert response.status_code == 200
        assert response.json() == {
            "username": "test",
            "id": "test",
            "teams": None,
        }

    @conf_vars({("core", "multi_team"): "true"})
    def test_teams_of_user_authorized_on_all_teams(self, test_client, session):
        session.add_all([Team(name="team2"), Team(name="team1")])
        session.commit()

        response = test_client.get("/auth/me")

        assert response.status_code == 200
        assert response.json()["teams"] == ["team1", "team2"]

    @conf_vars({("core", "multi_team"): "true"})
    @mock.patch.object(SimpleAuthManager, "_is_admin", return_value=False)
    def test_teams_limited_to_the_teams_the_user_belongs_to(self, _, test_client, session):
        session.add_all([Team(name="team1"), Team(name="team2")])
        session.commit()

        response = test_client.get("/auth/me")

        assert response.status_code == 200
        # The authenticated test user belongs to ``team1`` only.
        assert response.json()["teams"] == ["team1"]

    @conf_vars({("core", "multi_team"): "true"})
    @mock.patch.object(SimpleAuthManager, "_is_admin", return_value=False)
    def test_teams_empty_when_user_belongs_to_no_team(self, _, test_client, session):
        session.add(Team(name="team2"))
        session.commit()

        response = test_client.get("/auth/me")

        assert response.status_code == 200
        assert response.json()["teams"] == []

    def test_with_unauthenticated_user(self, unauthenticated_test_client):
        """Test /auth/me endpoint with no authentication."""
        response = unauthenticated_test_client.get("/auth/me")
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}


class TestGenerateToken:
    def test_generate_api_token(self, test_client):
        """Test generating an API token returns correct response shape."""
        response = test_client.post("/auth/token", json={"token_type": "api"})

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "api"
        assert data["expires_in_seconds"] == 86400  # default jwt_expiration_time

    def test_generate_cli_token(self, test_client):
        """Test generating a CLI token uses jwt_cli_expiration_time config."""
        response = test_client.post("/auth/token", json={"token_type": "cli"})

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "cli"
        # cli expiration comes from jwt_cli_expiration_time config
        assert isinstance(data["expires_in_seconds"], int)
        assert data["expires_in_seconds"] > 0

    def test_default_token_type_is_api(self, test_client):
        """Test that the default token type is API when not specified."""
        response = test_client.post("/auth/token", json={})

        assert response.status_code == 200
        data = response.json()
        assert data["token_type"] == "api"

    def test_unauthenticated_request(self, unauthenticated_test_client):
        """Test that unauthenticated requests are rejected."""
        response = unauthenticated_test_client.post("/auth/token", json={"token_type": "api"})
        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}
