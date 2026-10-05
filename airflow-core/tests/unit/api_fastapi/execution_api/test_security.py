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
import svcs
from fastapi import APIRouter, FastAPI, Request, Security
from fastapi.testclient import TestClient
from pydantic import ValidationError
from structlog.testing import capture_logs

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken, TokenScope
from airflow.api_fastapi.execution_api.security import (
    DAG_BUNDLE_HEADER,
    DagInGrantedBundle,
    ExecutionAPIRoute,
    ExecutionOrDagProcessorToken,
    SelectedDagBundle,
    _jwt_bearer,
    get_team_name_dep,
    require_auth,
)
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team
from airflow.models.variable import Variable

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_teams, clear_db_variables

DAG_PROCESSOR_CLAIMS = TIClaims(scope="dag_processor", dag_bundles=frozenset({"granted"}))


class TestTIClaims:
    def test_defaults_scope_and_retains_extra(self):
        claims = TIClaims(team="data")

        assert claims.scope == "execution"
        assert claims.team == "data"

    def test_accepts_sub_as_extra_claim(self):
        claims = TIClaims(sub="not-a-uuid")

        assert claims.sub == "not-a-uuid"

    @pytest.mark.parametrize("dag_bundles", [None, [], [""]])
    def test_dag_processor_scope_requires_a_bundle_grant(self, dag_bundles):
        with pytest.raises(ValidationError):
            TIClaims(scope="dag_processor", dag_bundles=dag_bundles)

    def test_dag_processor_scope_keeps_granted_bundles(self):
        claims = TIClaims.model_validate({"scope": "dag_processor", "dag_bundles": ["a", "b", "a"]})

        assert claims.dag_bundles == frozenset({"a", "b"})


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

    def test_extracts_callback_token_scope(self):
        route = ExecutionAPIRoute(
            path="/test",
            endpoint=lambda: None,
            dependencies=[
                Security(require_auth, scopes=["cb:self", "token:callback"]),
            ],
        )
        assert route.allowed_token_types == frozenset({"callback"})

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


class TestJWTBearerLogging:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        app.state.svcs_registry = svcs.Registry()

        @app.get("/protected")
        def protected(token: TIToken = Security(require_auth)):
            return {"id": str(token.id)}

        return app

    @pytest.mark.parametrize(
        "bearer_credential",
        [
            pytest.param("eyJ.invalid.jwt", id="jwt-looking-token"),
            pytest.param("opaque-token-value", id="opaque-token"),
        ],
    )
    def test_validation_failure_does_not_log_supplied_credential(self, app, bearer_credential):
        validator = MagicMock(spec=JWTValidator)
        validator.avalidated_claims.side_effect = ValueError("invalid token")
        app.state.svcs_registry.register_value(JWTValidator, validator)
        client = TestClient(app)

        with capture_logs() as logs:
            response = client.get(
                "/protected",
                headers={"Authorization": f"Bearer {bearer_credential}"},
            )

        assert response.status_code == 403
        assert response.json() == {"detail": "Invalid auth token"}
        validator.avalidated_claims.assert_awaited_once_with(bearer_credential, {})
        assert any(log["event"] == "Failed to validate JWT" for log in logs)
        assert bearer_credential not in repr(logs)
        assert "invalid token" not in response.text


class TestTiSelfScopeEnforcement:
    """Routes with the ``ti:self`` scope reject mismatched JWT subjects."""

    PATH_TI_ID = "00000000-0000-0000-0000-000000000001"
    OTHER_TI_ID = "00000000-0000-0000-0000-000000000002"

    @pytest.fixture
    def app(self):
        """One router enforces ti:self, another doesn't — to confirm enforcement is opt-in."""
        app = FastAPI()

        authenticated_router = APIRouter(dependencies=[Security(require_auth)])
        ti_self_router = APIRouter(dependencies=[Security(require_auth, scopes=["ti:self"])])

        @ti_self_router.get("/{task_instance_id}/state")
        def state_endpoint(task_instance_id: str):
            return {"ok": True}

        @authenticated_router.get("/no-scope/{task_instance_id}")
        def no_scope_endpoint(task_instance_id: str):
            return {"ok": True}

        authenticated_router.include_router(ti_self_router, prefix="/ti")
        app.include_router(authenticated_router)
        return app

    def _override_jwt(self, app: FastAPI, token_ti_id: UUID):
        async def mock_jwt(request: Request):
            return TIToken(id=token_ti_id, claims=TIClaims(scope="execution"))

        app.dependency_overrides[_jwt_bearer] = mock_jwt

    def test_matching_subject_is_accepted(self, app):
        self._override_jwt(app, self.PATH_TI_ID)
        client = TestClient(app)

        resp = client.get(
            f"/ti/{self.PATH_TI_ID}/state",
            headers={"Authorization": "Bearer fake"},
        )

        assert resp.status_code == 200

    def test_mismatched_subject_is_rejected(self, app):
        """A task cannot read or write another task's resources."""
        self._override_jwt(app, self.OTHER_TI_ID)
        client = TestClient(app)

        resp = client.get(
            f"/ti/{self.PATH_TI_ID}/state",
            headers={"Authorization": "Bearer fake"},
        )

        assert resp.status_code == 403
        assert "does not match" in resp.json()["detail"]


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

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.usefixtures("async_db_engine")
    async def test_dag_processor_token_resolves_the_team_of_its_bundle(self, session):
        clear_db_dag_bundles()
        clear_db_teams()
        bundle = DagBundleModel(name="granted")
        bundle.teams.append(Team(name="team_a"))
        session.add(bundle)
        session.commit()
        token = TIToken(id=UUID(int=1), claims=DAG_PROCESSOR_CLAIMS)

        try:
            with conf_vars({("core", "multi_team"): "True"}):
                result = await get_team_name_dep(token=token, dag_bundle="granted")
        finally:
            clear_db_dag_bundles()
            clear_db_teams()

        assert result == "team_a"


def _build_client(app: FastAPI, claims: TIClaims) -> TestClient:
    async def mock_jwt(request: Request):
        return TIToken(id=UUID(int=1), claims=claims)

    app.dependency_overrides[_jwt_bearer] = mock_jwt
    return TestClient(app, headers={"Authorization": "Bearer fake"})


class TestGetSelectedDagBundle:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        router = APIRouter(route_class=ExecutionAPIRoute, dependencies=[Security(require_auth)])

        @router.get("/selected", dependencies=[ExecutionOrDagProcessorToken])
        def endpoint(dag_bundle=SelectedDagBundle):
            return {"dag_bundle": dag_bundle}

        app.include_router(router)
        return app

    @pytest.mark.parametrize(
        ("claims", "headers", "expected_status", "expected_body"),
        [
            pytest.param(
                TIClaims(scope="execution"),
                {DAG_BUNDLE_HEADER: "granted"},
                200,
                {"dag_bundle": None},
                id="execution-token-ignores-header",
            ),
            pytest.param(
                DAG_PROCESSOR_CLAIMS,
                {DAG_BUNDLE_HEADER: "granted"},
                200,
                {"dag_bundle": "granted"},
                id="granted-bundle",
            ),
            pytest.param(DAG_PROCESSOR_CLAIMS, {}, 400, None, id="missing-header"),
            pytest.param(
                DAG_PROCESSOR_CLAIMS, {DAG_BUNDLE_HEADER: "other"}, 403, None, id="ungranted-bundle"
            ),
        ],
    )
    def test_selected_bundle(self, app, claims, headers, expected_status, expected_body):
        response = _build_client(app, claims).get("/selected", headers=headers)

        assert response.status_code == expected_status
        if expected_body is not None:
            assert response.json() == expected_body


@pytest.fixture
def granted_and_other_dags(session):
    clear_db_dags()
    clear_db_dag_bundles()
    session.add_all([DagBundleModel(name="granted"), DagBundleModel(name="other")])
    session.flush()
    session.add_all(
        [
            DagModel(dag_id="granted_dag", bundle_name="granted"),
            DagModel(dag_id="other_dag", bundle_name="other"),
        ]
    )
    session.commit()
    yield
    clear_db_dags()
    clear_db_dag_bundles()


@pytest.mark.db_test
@pytest.mark.usefixtures("granted_and_other_dags", "async_db_engine")
class TestRequireDagInGrantedBundle:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        router = APIRouter(route_class=ExecutionAPIRoute, dependencies=[Security(require_auth)])

        @router.get("/dags/{dag_id}", dependencies=[ExecutionOrDagProcessorToken, DagInGrantedBundle])
        def by_path(dag_id: str):
            return {"ok": True}

        @router.get("/dags", dependencies=[ExecutionOrDagProcessorToken, DagInGrantedBundle])
        def by_query(dag_id: str | None = None):
            return {"ok": True}

        app.include_router(router)
        return app

    @pytest.mark.parametrize(
        ("url", "expected_status"),
        [
            pytest.param("/dags/granted_dag", 200, id="path-dag-in-granted-bundle"),
            pytest.param("/dags?dag_id=granted_dag", 200, id="query-dag-in-granted-bundle"),
            pytest.param("/dags/other_dag", 403, id="dag-in-other-bundle"),
            pytest.param("/dags/missing_dag", 403, id="unknown-dag"),
            pytest.param("/dags", 403, id="no-dag"),
        ],
    )
    def test_dag_processor_token(self, app, url, expected_status):
        response = _build_client(app, DAG_PROCESSOR_CLAIMS).get(url)

        assert response.status_code == expected_status

    def test_execution_token_is_not_limited_to_bundles(self, app):
        response = _build_client(app, TIClaims(scope="execution")).get("/dags/other_dag")

        assert response.status_code == 200


@pytest.mark.db_test
@pytest.mark.usefixtures("granted_and_other_dags")
class TestDagProcessorTokenOverHTTP:
    """Signed ``dag_processor`` tokens against the real Execution API routes."""

    SECRET = "dag-processor-test-secret"
    AUDIENCE = "urn:airflow.apache.org:task"

    @pytest.fixture(autouse=True)
    def real_jwt_validation(self, exec_app):
        exec_app.dependency_overrides.pop(require_auth, None)
        lifespan.registry.register_value(
            JWTValidator, JWTValidator(secret_key=self.SECRET, audience=self.AUDIENCE)
        )

    @pytest.fixture(autouse=True)
    def variable(self):
        clear_db_variables()
        Variable.set(key="key1", value="value1")
        yield
        clear_db_variables()

    def _generate_token(self, *, secret: str = SECRET, valid_for: float = 300, **claims) -> str:
        generator = JWTGenerator(secret_key=secret, audience=self.AUDIENCE, valid_for=valid_for)
        return generator.generate({"sub": "00000000-0000-0000-0000-000000000001", **claims})

    def _generate_processor_token(self, **kwargs) -> str:
        return self._generate_token(scope="dag_processor", dag_bundles=["granted"], **kwargs)

    @pytest.mark.parametrize(
        ("method", "url", "headers", "expected_status"),
        [
            pytest.param("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "granted"}, 200, id="granted-bundle"),
            pytest.param("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "other"}, 403, id="forged-bundle"),
            pytest.param("GET", "/variables/key1", {}, 400, id="no-bundle"),
            pytest.param(
                "GET", "/task-instances/count?dag_id=granted_dag", {}, 200, id="dag-in-granted-bundle"
            ),
            pytest.param("GET", "/task-instances/count?dag_id=other_dag", {}, 403, id="dag-in-other-bundle"),
            pytest.param("POST", "/xcoms/granted_dag/run/task/key", {}, 403, id="unsupported-write"),
            pytest.param(
                "GET",
                "/task-instances/00000000-0000-0000-0000-000000000001/previous-successful-dagrun",
                {},
                403,
                id="task-instance-route",
            ),
        ],
    )
    def test_route_matrix(self, client, method, url, headers, expected_status):
        token = self._generate_processor_token()

        response = client.request(
            method, f"/execution{url}", headers={"Authorization": f"Bearer {token}", **headers}, json="value"
        )

        assert response.status_code == expected_status, response.text

    @pytest.mark.parametrize(
        "token_kwargs",
        [
            pytest.param({"scope": "dag_processor"}, id="no-bundle-grant"),
            pytest.param(
                {"scope": "dag_processor", "dag_bundles": ["granted"], "secret": "other"}, id="forged"
            ),
            pytest.param(
                {"scope": "dag_processor", "dag_bundles": ["granted"], "valid_for": -60}, id="expired"
            ),
            pytest.param({"scope": "workload"}, id="wrong-token-type"),
        ],
    )
    def test_invalid_token_is_rejected(self, client, token_kwargs):
        token = self._generate_token(**token_kwargs)

        response = client.get(
            "/execution/variables/key1",
            headers={"Authorization": f"Bearer {token}", DAG_BUNDLE_HEADER: "granted"},
        )

        assert response.status_code == 403, response.text

    def test_expiring_token_is_not_reissued(self, client):
        token = self._generate_processor_token(valid_for=30)

        response = client.get(
            "/execution/variables/key1",
            headers={"Authorization": f"Bearer {token}", DAG_BUNDLE_HEADER: "granted"},
        )

        assert response.status_code == 200, response.text
        assert "Refreshed-API-Token" not in response.headers
