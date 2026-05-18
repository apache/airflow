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

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from fastapi import APIRouter, FastAPI, Request, Security
from fastapi.testclient import TestClient

from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken, TokenScope
from airflow.api_fastapi.execution_api.security import (
    ExecutionAPIRoute,
    _jwt_bearer,
    get_team_name_dep,
    require_auth,
)


class TestTIClaims:
    def test_defaults_scope_and_retains_extra(self):
        claims = TIClaims(team="data")

        assert claims.scope == "execution"
        assert claims.team == "data"

    def test_accepts_sub_as_extra_claim(self):
        claims = TIClaims(sub="not-a-uuid")

        assert claims.sub == "not-a-uuid"


class TestExecutionAPIRoute:
    """Unit tests for ExecutionAPIRoute precomputing allowed_token_types from Security scopes."""

    def test_defaults_to_execution_only(self):
        route = ExecutionAPIRoute(
            path="/test",
            endpoint=lambda: None,
            dependencies=[Security(require_auth)],
        )
        assert route.allowed_token_types == frozenset({"execution"})

    def test_extracts_token_scopes(self):
        route = ExecutionAPIRoute(
            path="/test",
            endpoint=lambda: None,
            dependencies=[
                Security(require_auth),
                Security(require_auth, scopes=["token:execution", "token:workload"]),
            ],
        )
        assert route.allowed_token_types == frozenset({"execution", "workload"})

    def test_ignores_non_token_scopes(self):
        route = ExecutionAPIRoute(
            path="/test",
            endpoint=lambda: None,
            dependencies=[
                Security(require_auth, scopes=["ti:self", "token:execution"]),
            ],
        )
        assert route.allowed_token_types == frozenset({"execution"})

    def test_rejects_invalid_token_types(self):
        with pytest.raises(ValueError, match="Invalid token types"):
            ExecutionAPIRoute(
                path="/test",
                endpoint=lambda: None,
                dependencies=[
                    Security(require_auth, scopes=["token:bogus"]),
                ],
            )


class TestTokenTypeScopeEnforcement:
    """End-to-end: ExecutionAPIRoute + require_auth enforce token types via Security scopes."""

    @pytest.fixture
    def token_type_app(self):
        """
        Mirrors the real router structure: an authenticated_router with Security(require_auth),
        a child ti_id_router with ExecutionAPIRoute and ti:self, and a specific endpoint on that
        router opting in to workload tokens via endpoint-level Security scopes.
        """
        app = FastAPI()

        authenticated_router = APIRouter(dependencies=[Security(require_auth)])
        ti_id_router = APIRouter(
            route_class=ExecutionAPIRoute,
            dependencies=[Security(require_auth, scopes=["ti:self"])],
        )

        @ti_id_router.get("/{task_instance_id}/state")
        def default_endpoint(task_instance_id: str):
            return {"ok": True}

        @ti_id_router.get(
            "/{task_instance_id}/run",
            dependencies=[Security(require_auth, scopes=["token:execution", "token:workload"])],
        )
        def workload_endpoint(task_instance_id: str):
            return {"ok": True}

        authenticated_router.include_router(ti_id_router, prefix="/task-instances")
        app.include_router(authenticated_router)

        return app

    TI_ID = "00000000-0000-0000-0000-000000000001"

    def _override_jwt(self, app, scope: TokenScope):
        ti_id = self.TI_ID

        async def mock_jwt(request: Request):
            claims = TIClaims(scope=scope)
            return TIToken(id=UUID(ti_id), claims=claims)

        app.dependency_overrides[_jwt_bearer] = mock_jwt

    def test_workload_token_rejected_on_default_route(self, token_type_app):
        self._override_jwt(token_type_app, "workload")
        client = TestClient(token_type_app)

        resp = client.get(f"/task-instances/{self.TI_ID}/state", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 403
        assert "Token type 'workload' not allowed" in resp.json()["detail"]

    def test_workload_token_accepted_on_opted_in_route(self, token_type_app):
        self._override_jwt(token_type_app, "workload")
        client = TestClient(token_type_app)

        resp = client.get(f"/task-instances/{self.TI_ID}/run", headers={"Authorization": "Bearer fake"})
        assert resp.status_code == 200

    def test_execution_token_accepted_on_both_routes(self, token_type_app):
        self._override_jwt(token_type_app, "execution")
        client = TestClient(token_type_app)

        state = client.get(f"/task-instances/{self.TI_ID}/state", headers={"Authorization": "Bearer fake"})
        run = client.get(f"/task-instances/{self.TI_ID}/run", headers={"Authorization": "Bearer fake"})
        assert state.status_code == 200
        assert run.status_code == 200


class TestGetTeamNameDep:
    """Tests for get_team_name_dep avoiding unnecessary async sessions."""

    @pytest.mark.asyncio
    async def test_returns_none_without_session_when_multi_team_disabled(self):
        """When multi_team=False, no async session should be created."""
        token = MagicMock(spec=TIToken)

        with (
            patch("airflow.configuration.conf.getboolean", return_value=False),
            patch("airflow.utils.session.create_session_async") as mock_create_session,
        ):
            result = await get_team_name_dep(token=token)

        assert result is None
        mock_create_session.assert_not_called()
