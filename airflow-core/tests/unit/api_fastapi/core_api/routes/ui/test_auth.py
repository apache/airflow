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

from datetime import UTC, datetime
from unittest import mock
from unittest.mock import patch

import pytest
import time_machine

from airflow.api_fastapi.auth.cli_authorization_code import (
    build_loopback_redirect_uri,
    validate_cli_authorization_code,
)
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.common.types import ExtraMenuItem, MenuItem
from airflow.configuration import conf
from airflow.models.crypto import _NullFernet
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


class TestAuthorizeCli:
    # Test vector from RFC 7636, appendix B.
    code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    code_challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
    body = {"code_challenge": code_challenge, "code_challenge_method": "S256", "port": 53682}

    def redeem(self, code: str) -> dict:
        return validate_cli_authorization_code(
            code, code_verifier=self.code_verifier, redirect_uri=build_loopback_redirect_uri(53682)
        )

    def test_returns_a_code_carrying_a_cli_token_for_the_user(self, test_client):
        # Frozen at the present so the test client's own session token stays valid.
        approved_at = datetime.now(tz=UTC)
        with time_machine.travel(approved_at, tick=False):
            response = test_client.post("/auth/cli/authorize", json=self.body)

            assert response.status_code == 200
            claims = self.redeem(response.json()["code"])
            auth_manager = test_client.app.state.auth_manager
            token_claims = auth_manager._get_token_validator().validated_claims(claims["token"])

        assert claims["name"] == "test"
        cli_expiration = conf.getint("api_auth", "jwt_cli_expiration_time")
        assert claims["token_expires_at"] == int(approved_at.timestamp()) + cli_expiration
        assert token_claims["sub"] == "test"
        assert token_claims["role"] == "admin"
        assert token_claims["exp"] - token_claims["iat"] == cli_expiration

    def test_mints_the_token_kind_made_for_the_authorization_header(self, test_client):
        auth_manager = test_client.app.state.auth_manager
        with patch.object(
            auth_manager, "generate_api_jwt", autospec=True, return_value="API-TOKEN"
        ) as generate:
            response = test_client.post("/auth/cli/authorize", json=self.body)

        assert response.status_code == 200
        assert self.redeem(response.json()["code"])["token"] == "API-TOKEN"
        generate.assert_called_once()
        assert generate.call_args.kwargs == {
            "expiration_time_in_seconds": conf.getint("api_auth", "jwt_cli_expiration_time")
        }

    @patch(
        "airflow.api_fastapi.auth.cli_authorization_code.get_fernet",
        autospec=True,
        return_value=_NullFernet(),
    )
    def test_refuses_without_a_fernet_key(self, mock_get_fernet, test_client):
        response = test_client.post("/auth/cli/authorize", json=self.body)

        assert response.status_code == 403
        assert "[core] fernet_key" in response.json()["detail"]

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({**body, "port": 80}, id="privileged-port"),
            pytest.param({**body, "port": 65536}, id="port-too-large"),
            pytest.param({**body, "code_challenge_method": "plain"}, id="plain-method"),
            pytest.param({**body, "code_challenge": "too-short"}, id="malformed-challenge"),
        ],
    )
    def test_rejects_requests_airflowctl_cannot_have_made(self, test_client, body):
        response = test_client.post("/auth/cli/authorize", json=body)

        assert response.status_code == 422

    def test_with_unauthenticated_user(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/auth/cli/authorize", json=self.body)

        assert response.status_code == 401
        assert response.json() == {"detail": "Not authenticated"}
