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

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import anyio
import jwt
import pytest
import svcs
from fastapi import APIRouter, FastAPI, Request, Security
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import select, update
from structlog.testing import capture_logs

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api import security
from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI, lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken, TokenScope
from airflow.api_fastapi.execution_api.security import (
    DAG_BUNDLE_HEADER,
    DagInGrantedBundle,
    ExecutionAPIRoute,
    ExecutionOrProcessorSecretsToken,
    SelectedDagBundle,
    _jwt_bearer,
    get_team_name_dep,
    require_auth,
)
from airflow.jobs.job import Job, JobState
from airflow.models import DagModel
from airflow.models.callback import Callback, CallbackFetchMethod
from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState
from airflow.models.dagbundle import DagBundleModel
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance
from airflow.models.team import Team
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel, XComModelV1, XComModelV2
from airflow.sdk.api.client import XComOperations
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time import request_handlers, task_runner
from airflow.sdk.execution_time.comms import DeleteXCom, GetXCom
from airflow.utils.state import CallbackState, TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_jobs,
    clear_db_teams,
    clear_db_variables,
)

DAG_PROCESSOR_CLAIMS = TIClaims(scope="dag_processor", dag_bundles=frozenset({"granted"}), job_id=1)
SESSION_SUB = "00000000-0000-0000-0000-000000000001"
JOB_ID = 4242
REGISTRATION_ID = UUID("00000000-0000-0000-0000-00000000000a")
OTHER_REGISTRATION_ID = UUID("00000000-0000-0000-0000-00000000000b")
LONG_AGO = datetime(2020, 1, 1, tzinfo=timezone.utc)


class TestTIClaims:
    def test_defaults_scope_and_retains_extra(self):
        claims = TIClaims(team="data")

        assert claims.scope == "execution"
        assert claims.team == "data"

    def test_accepts_sub_as_extra_claim(self):
        claims = TIClaims(sub="not-a-uuid")

        assert claims.sub == "not-a-uuid"

    @pytest.mark.parametrize("scope", ["dag_processor_session", "dag_processor"])
    @pytest.mark.parametrize("dag_bundles", [None, [], [""]])
    def test_dag_processor_scopes_require_a_bundle_grant(self, scope, dag_bundles):
        with pytest.raises(ValidationError):
            TIClaims.model_validate({"scope": scope, "dag_bundles": dag_bundles, "job_id": 1})

    def test_dag_processor_scope_requires_a_job(self):
        with pytest.raises(ValidationError, match="must name the Job"):
            TIClaims.model_validate({"scope": "dag_processor", "dag_bundles": ["a"]})

    def test_dag_processor_scopes_keep_their_claims(self):
        claims = TIClaims.model_validate(
            {"scope": "dag_processor", "dag_bundles": ["a", "b", "a"], "job_id": 7}
        )

        assert (claims.dag_bundles, claims.job_id) == (frozenset({"a", "b"}), 7)


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
    @pytest.mark.parametrize("scope", ["dag_processor", "dag_parse"])
    async def test_dag_processor_token_resolves_the_team_of_its_bundle(self, session, scope):
        clear_db_dag_bundles()
        clear_db_teams()
        bundle = DagBundleModel(name="granted")
        bundle.teams.append(Team(name="team_a"))
        session.add(bundle)
        session.commit()
        claims = TIClaims(
            scope=scope,
            dag_bundles=frozenset({"granted"}),
            job_id=1,
            session_id=UUID(SESSION_SUB),
            relative_fileloc="dag.py",
        )
        token = TIToken(id=UUID(int=2), claims=claims)

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


@patch("airflow.api_fastapi.execution_api.security._require_open_dag_processor_job", autospec=True)
class TestGetSelectedDagBundle:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        router = APIRouter(route_class=ExecutionAPIRoute, dependencies=[Security(require_auth)])

        @router.get("/selected", dependencies=[ExecutionOrProcessorSecretsToken])
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
    def test_selected_bundle(self, _, app, claims, headers, expected_status, expected_body):
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
@patch("airflow.api_fastapi.execution_api.security._require_open_dag_processor_job", autospec=True)
class TestRequireDagInGrantedBundle:
    @pytest.fixture
    def app(self):
        app = FastAPI()
        router = APIRouter(route_class=ExecutionAPIRoute, dependencies=[Security(require_auth)])

        @router.get("/dags/{dag_id}", dependencies=[ExecutionOrProcessorSecretsToken, DagInGrantedBundle])
        def by_path(dag_id: str):
            return {"ok": True}

        @router.get("/dags", dependencies=[ExecutionOrProcessorSecretsToken, DagInGrantedBundle])
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
            pytest.param("/dags/missing_dag", 200, id="dag-without-model-row"),
            pytest.param("/dags", 403, id="no-dag"),
        ],
    )
    def test_dag_processor_token(self, _, app, url, expected_status):
        response = _build_client(app, DAG_PROCESSOR_CLAIMS).get(url)

        assert response.status_code == expected_status

    def test_execution_token_is_not_limited_to_bundles(self, _, app):
        response = _build_client(app, TIClaims(scope="execution")).get("/dags/other_dag")

        assert response.status_code == 200


@pytest.mark.db_test
@pytest.mark.usefixtures("granted_and_other_dags")
class TestDagProcessorTokenOverHTTP:
    """Signed Dag processor tokens against the real Execution API routes."""

    SECRET = "dag-processor-test-secret"
    AUDIENCE = "urn:airflow.apache.org:task"

    @pytest.fixture(autouse=True)
    def real_jwt_signing(self, exec_app):
        exec_app.dependency_overrides.pop(require_auth, None)
        lifespan.registry.register_value(
            JWTValidator, JWTValidator(secret_key=self.SECRET, audience=self.AUDIENCE)
        )
        lifespan.registry.register_value(
            JWTGenerator, JWTGenerator(secret_key=self.SECRET, audience=self.AUDIENCE, valid_for=300)
        )

    @pytest.fixture(autouse=True)
    def variable(self):
        clear_db_variables()
        Variable.set(key="key1", value="value1")
        yield
        clear_db_variables()

    @pytest.fixture(autouse=True)
    def clean_jobs(self):
        clear_db_jobs()
        yield
        clear_db_jobs()

    @pytest.fixture
    def open_job(self, session):
        job = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
        job.id = JOB_ID
        job.session_id = UUID(SESSION_SUB)
        session.add(job)
        session.commit()

    def _generate_token(self, *, secret: str = SECRET, valid_for: float = 300, **claims) -> str:
        generator = JWTGenerator(secret_key=secret, audience=self.AUDIENCE, valid_for=valid_for)
        return generator.generate({"sub": SESSION_SUB, **claims})

    def _generate_job_token(self, **kwargs) -> str:
        return self._generate_token(scope="dag_processor", dag_bundles=["granted"], job_id=JOB_ID, **kwargs)

    def _exchange_parse_token(self, client, **body):
        return client.post(
            f"/execution/jobs/{JOB_ID}/parse-token",
            headers={"Authorization": f"Bearer {self._generate_job_token(valid_for=30)}"},
            json={
                "attempt_id": str(REGISTRATION_ID),
                "bundle_name": "granted",
                "relative_fileloc": "folder/dag.py",
                **body,
            },
        )

    @pytest.mark.usefixtures("open_job")
    def test_exchange_binds_identity_and_expiry_to_the_file_and_job(self, client):
        response = self._exchange_parse_token(client)

        assert response.status_code == 200, response.text
        claims = jwt.decode(
            response.json()["token"], self.SECRET, algorithms=["HS512"], audience=self.AUDIENCE
        )
        assert claims["sub"] == str(REGISTRATION_ID)
        assert claims["session_id"] == SESSION_SUB
        assert claims["job_id"] == JOB_ID
        assert claims["scope"] == "dag_parse"
        assert claims["dag_bundles"] == ["granted"]
        assert claims["relative_fileloc"] == "folder/dag.py"
        assert 0 < claims["exp"] - claims["iat"] <= 30

    @pytest.mark.parametrize(
        ("body", "status"),
        [
            ({"bundle_name": "other"}, 403),
            ({"relative_fileloc": ""}, 422),
            ({"relative_fileloc": "/absolute.py"}, 422),
            ({"relative_fileloc": "../outside.py"}, 422),
            ({"relative_fileloc": "nested/../outside.py"}, 422),
            ({"relative_fileloc": "nested//dag.py"}, 422),
            ({"relative_fileloc": "nested\\dag.py"}, 422),
            ({"relative_fileloc": "dag\x00.py"}, 422),
            ({"attempt_id": "not-a-uuid"}, 422),
        ],
    )
    @pytest.mark.usefixtures("open_job")
    def test_exchange_rejects_invalid_grants_and_file_identity(self, client, body, status):
        assert self._exchange_parse_token(client, **body).status_code == status

    @pytest.mark.parametrize(
        ("method", "path", "headers", "expected"),
        [
            ("GET", "/variables/key1", {}, 200),
            ("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "granted"}, 200),
            ("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "other"}, 403),
            ("GET", "/variables/keys", {}, 200),
            ("PUT", "/variables/key1", {}, 201),
            ("GET", "/task-instances/count?dag_id=granted_dag", {}, 200),
            ("GET", "/task-instances/count?dag_id=other_dag", {}, 403),
            ("POST", f"/jobs/{JOB_ID}/heartbeat", {}, 403),
            ("POST", f"/jobs/{JOB_ID}/complete", {}, 403),
            ("POST", f"/jobs/{JOB_ID}/parse-token", {}, 403),
            ("POST", "/jobs", {}, 403),
            ("POST", "/xcoms/granted_dag/run/task/key", {}, 403),
            ("GET", f"/task-instances/{SESSION_SUB}/previous-successful-dagrun", {}, 403),
        ],
    )
    @pytest.mark.usefixtures("open_job")
    def test_parsing_token_route_boundaries(self, client, method, path, headers, expected):
        exchanged = self._exchange_parse_token(client)
        assert exchanged.status_code == 200, exchanged.text

        response = client.request(
            method,
            f"/execution{path}",
            headers={"Authorization": f"Bearer {exchanged.json()['token']}", **headers},
            json={"value": "updated"},
        )

        assert response.status_code == expected, response.text
        assert "Refreshed-API-Token" not in response.headers

    @pytest.mark.usefixtures("open_job")
    def test_parsing_token_can_query_a_dag_before_its_first_parse(self, client):
        exchanged = self._exchange_parse_token(client)
        assert exchanged.status_code == 200, exchanged.text

        response = client.get(
            "/execution/task-instances/count",
            params={"dag_id": "missing_dag"},
            headers={"Authorization": f"Bearer {exchanged.json()['token']}"},
        )

        assert response.status_code == 200, response.text
        assert response.json() == 0

    @pytest.mark.parametrize("retirement", ["completed", "replaced"])
    @pytest.mark.usefixtures("open_job")
    def test_retiring_the_job_ends_parsing_credentials(self, client, session, retirement):
        token = self._exchange_parse_token(client).json()["token"]
        values = {"end_date": LONG_AGO} if retirement == "completed" else {"session_id": None}
        session.execute(update(Job).where(Job.id == JOB_ID).values(**values))
        session.commit()

        response = client.get("/execution/variables/key1", headers={"Authorization": f"Bearer {token}"})

        assert response.status_code == 403
        assert response.json()["detail"]["reason"] == "job_closed"
        assert self._exchange_parse_token(client).status_code == 403

    @pytest.mark.parametrize(
        "claims",
        [
            {"dag_bundles": ["granted", "other"]},
            {"dag_bundles": []},
            {"relative_fileloc": None},
            {"session_id": None},
            {"job_id": None},
            {"sub": "not-a-uuid"},
            {"valid_for": -60},
            {"secret": "wrong-signing-key"},
        ],
    )
    @pytest.mark.usefixtures("open_job")
    def test_malformed_or_expired_parsing_credentials_are_rejected(self, client, claims):
        token = self._generate_token(
            **{
                "sub": str(REGISTRATION_ID),
                "scope": "dag_parse",
                "dag_bundles": ["granted"],
                "relative_fileloc": "dag.py",
                "session_id": SESSION_SUB,
                "job_id": JOB_ID,
                **claims,
            }
        )

        response = client.get("/execution/variables/key1", headers={"Authorization": f"Bearer {token}"})

        assert response.status_code == 403, response.text

    def _register(self, client, registration_id: UUID):
        session_token = self._generate_token(scope="dag_processor_session", dag_bundles=["granted"])
        return client.post(
            "/execution/jobs",
            headers={"Authorization": f"Bearer {session_token}"},
            json={"registration_id": str(registration_id), "hostname": "processor-1"},
        )

    @staticmethod
    def _get_variable_status(client, token: str) -> int:
        headers = {"Authorization": f"Bearer {token}", DAG_BUNDLE_HEADER: "granted"}
        return client.get("/execution/variables/key1", headers=headers).status_code

    @pytest.mark.parametrize(
        ("method", "url", "headers", "expected_status"),
        [
            pytest.param("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "granted"}, 200, id="granted-bundle"),
            pytest.param("GET", "/variables/key1", {DAG_BUNDLE_HEADER: "other"}, 403, id="forged-bundle"),
            pytest.param("GET", "/variables/key1", {}, 400, id="no-bundle"),
            pytest.param("GET", "/variables/keys", {DAG_BUNDLE_HEADER: "granted"}, 403, id="no-list"),
            pytest.param("PUT", "/variables/key1", {DAG_BUNDLE_HEADER: "granted"}, 403, id="no-write"),
            pytest.param("DELETE", "/variables/key1", {DAG_BUNDLE_HEADER: "granted"}, 403, id="no-delete"),
            pytest.param(
                "GET", "/task-instances/count?dag_id=granted_dag", {}, 403, id="requires-parsing-token"
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
    @pytest.mark.usefixtures("open_job")
    def test_route_matrix(self, client, method, url, headers, expected_status):
        token = self._generate_job_token()

        response = client.request(
            method, f"/execution{url}", headers={"Authorization": f"Bearer {token}", **headers}, json="value"
        )

        assert response.status_code == expected_status, response.text

    @pytest.mark.parametrize(
        "token_kwargs",
        [
            pytest.param({"scope": "dag_processor", "job_id": JOB_ID}, id="no-bundle-grant"),
            pytest.param({"scope": "dag_processor", "dag_bundles": ["granted"]}, id="no-job"),
            pytest.param(
                {"scope": "dag_processor", "dag_bundles": ["granted"], "job_id": JOB_ID, "secret": "other"},
                id="forged",
            ),
            pytest.param(
                {"scope": "dag_processor", "dag_bundles": ["granted"], "job_id": JOB_ID, "valid_for": -60},
                id="expired",
            ),
            pytest.param({"scope": "dag_processor_session", "dag_bundles": ["granted"]}, id="session-token"),
            pytest.param({"scope": "workload"}, id="wrong-token-type"),
        ],
    )
    @pytest.mark.usefixtures("open_job")
    def test_invalid_token_is_rejected(self, client, token_kwargs):
        token = self._generate_token(**token_kwargs)

        response = client.get(
            "/execution/variables/key1",
            headers={"Authorization": f"Bearer {token}", DAG_BUNDLE_HEADER: "granted"},
        )

        assert response.status_code == 403, response.text

    @pytest.mark.usefixtures("open_job")
    def test_expiring_token_is_not_reissued(self, client):
        token = self._generate_job_token(valid_for=30)

        response = client.get(
            "/execution/variables/key1",
            headers={"Authorization": f"Bearer {token}", DAG_BUNDLE_HEADER: "granted"},
        )

        assert response.status_code == 200, response.text
        assert "Refreshed-API-Token" not in response.headers

    def test_job_token_lasts_until_its_job_completes(self, client):
        registered = self._register(client, REGISTRATION_ID)
        assert registered.status_code == 201, registered.text
        job_token = registered.json()["token"]
        job_url = f"/execution/jobs/{registered.json()['job_id']}"
        auth = {"Authorization": f"Bearer {job_token}"}
        assert self._get_variable_status(client, job_token) == 200
        assert client.post(f"{job_url}/heartbeat", headers=auth).json() == {"state": "running"}

        completed = client.post(f"{job_url}/complete", headers=auth, json={"state": "success"})
        assert completed.status_code == 204, completed.text
        assert self._get_variable_status(client, job_token) == 403
        assert client.post(f"{job_url}/heartbeat", headers=auth).status_code == 403
        replayed = client.post(f"{job_url}/complete", headers=auth, json={"state": "success"})
        assert replayed.status_code == 204, replayed.text

        assert self._register(client, REGISTRATION_ID).status_code == 409
        restarted = self._register(client, OTHER_REGISTRATION_ID)
        assert restarted.status_code == 201, restarted.text
        assert self._get_variable_status(client, job_token) == 403
        assert self._get_variable_status(client, restarted.json()["token"]) == 200

    def test_replacing_a_stopped_job_ends_its_tokens(self, client, session):
        first = self._register(client, REGISTRATION_ID)
        assert first.status_code == 201, first.text
        self._mark_heartbeat_expired(session, first.json()["job_id"])

        second = self._register(client, OTHER_REGISTRATION_ID)

        assert second.status_code == 201, second.text
        closed = client.get(
            "/execution/variables/key1",
            headers={"Authorization": f"Bearer {first.json()['token']}", DAG_BUNDLE_HEADER: "granted"},
        )
        assert closed.status_code == 403
        assert closed.json()["detail"]["reason"] == "job_closed"
        assert self._get_variable_status(client, second.json()["token"]) == 200
        self._mark_heartbeat_expired(session, second.json()["job_id"])
        replaced_again = self._register(client, REGISTRATION_ID)
        assert replaced_again.status_code == 409
        assert replaced_again.json()["detail"]["reason"] == "registration_retired"

    @staticmethod
    def _mark_heartbeat_expired(session, job_id: int) -> None:
        session.execute(update(Job).where(Job.id == job_id).values(latest_heartbeat=LONG_AGO))
        session.commit()


@pytest.mark.db_test
class TestAttemptLiveness:
    @pytest.fixture
    def caller(self, client, exec_app, monkeypatch, create_task_instance, session):
        ti = create_task_instance(state=TaskInstanceState.RUNNING)
        session.commit()
        token = TIToken(id=ti.id, claims=TIClaims())

        async def authenticated_token():
            return token

        monkeypatch.delitem(exec_app.dependency_overrides, require_auth)
        monkeypatch.setitem(exec_app.dependency_overrides, _jwt_bearer, authenticated_token)
        return ti, token

    @pytest.mark.parametrize("archival", ["retry", "delete"])
    def test_archived_attempt_cannot_replace_or_delete_xcom(self, client, caller, session, archival):
        ti, token = caller
        path = f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/result"
        assert client.post(path, json="original").status_code == 201
        old_id = ti.id
        if archival == "retry":
            successor = ti.prepare_db_for_next_try(session)
            successor.state = TaskInstanceState.UP_FOR_RETRY
        else:
            session.delete(ti)
        session.commit()

        expected_status = 410 if archival == "retry" else 404
        assert client.post(path, json="stale").status_code == expected_status
        assert client.delete(path).status_code == expected_status

        if archival == "retry":
            assert (
                session.scalar(
                    select(TaskInstance.id)
                    .where(TaskInstance.id == old_id, TaskInstance.working_set.is_(None))
                    .execution_options(include_all_attempts=True)
                )
                == old_id
            )
            assert client.get(path).status_code == 410
            token.id = successor.id
            assert client.post(path, json="replacement").status_code == 201
            token.id = old_id
            assert client.delete(path).status_code == 410
            assert client.get(path).status_code == 410

    def test_unknown_execution_identity_cannot_write(self, client, caller):
        _, token = caller
        token.id = uuid4()
        key = f"attempt-liveness-{uuid4()}"
        try:
            response = client.put(f"/execution/variables/{key}", json={"value": "unowned"})
            assert response.status_code == 404
            with pytest.raises(KeyError):
                Variable.get(key)
        finally:
            Variable.delete(key)

    ARCHIVED_ROUTES = [
        ("put", "/execution/store/ti/{ti_id}/key", {"value": "stale"}),
        ("delete", "/execution/store/ti/{ti_id}/key", None),
        ("delete", "/execution/store/ti/{ti_id}", None),
        ("put", "/execution/task-instances/{ti_id}/rtif", {}),
        ("post", "/execution/hitlDetails/{ti_id}", {}),
        ("patch", "/execution/hitlDetails/{ti_id}", {}),
        ("put", "/execution/store/asset/by-name/value?name=asset&key=k", {"value": "stale"}),
        ("delete", "/execution/store/asset/by-name/clear?name=asset", None),
        ("post", "/execution/dag-runs/target/run", {}),
        ("post", "/execution/dag-runs/target/run/clear", None),
        ("put", "/execution/variables/key", {"value": "stale"}),
        ("delete", "/execution/variables/key", None),
        ("post", "/execution/xcoms/{dag_id}/{run_id}/{task_id}/key", "late"),
    ]

    @pytest.mark.parametrize(
        ("method", "path", "body"),
        [pytest.param(*route, id=f"{route[0]}:{route[1]}") for route in ARCHIVED_ROUTES],
    )
    def test_archived_attempt_rejected_before_mutation(self, client, caller, session, method, path, body):
        ti, token = caller
        path = path.format(ti_id=token.id, dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id)
        ti.prepare_db_for_next_try(session)
        session.commit()

        response = client.request(method, path, json=body)

        assert response.status_code == 410, response.text

    def test_callback_execution_token_can_mutate_variable(self, client, caller, session):
        _, token = caller
        callback = Callback()
        callback.fetch_method = CallbackFetchMethod.IMPORT_PATH
        callback.state = CallbackState.QUEUED
        session.add(callback)
        session.commit()
        token.id = callback.id
        token.claims.scope = "callback"
        key = f"attempt-liveness-{uuid4()}"
        try:
            response = client.patch(f"/execution/callbacks/{callback.id}/run")
            assert response.status_code == 204
            claims = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
            token.id = UUID(claims["sub"])
            token.claims = TIClaims(**claims)

            assert client.put(f"/execution/variables/{key}", json={"value": "callback"}).status_code == 201
            assert Variable.get(key) == "callback"
        finally:
            Variable.delete(key)
            session.delete(callback)
            session.commit()

    def test_connection_test_workload_keeps_its_own_lifecycle(self, client, caller, session):
        _, token = caller
        connection_test = ConnectionTestRequest(connection_id="liveness", conn_type="http")
        connection_test.state = ConnectionTestState.QUEUED
        session.add(connection_test)
        session.commit()
        token.id = connection_test.id
        token.claims.scope = "workload"
        try:
            assert client.get(f"/execution/connection-tests/{token.id}/connection").status_code == 200
            response = client.patch(
                f"/execution/connection-tests/{token.id}", json={"state": "success", "result_message": "ok"}
            )
            assert response.status_code == 204
            session.refresh(connection_test)
            assert connection_test.state == ConnectionTestState.SUCCESS
        finally:
            session.delete(connection_test)
            session.commit()

    def test_trusted_in_process_caller_without_ti_can_write(self):
        key = f"attempt-liveness-{uuid4()}"
        try:
            with TestClient(InProcessExecutionAPI().app) as client:
                response = client.put(f"/variables/{key}", json={"value": "watcher"})
            assert response.status_code == 201
            assert Variable.get(key) == "watcher"
        finally:
            Variable.delete(key)

    @pytest.mark.parametrize("delete_rejected", [False, True], ids=["accepted", "archived-token-rejected"])
    def test_trusted_in_process_xcom_uses_current_owner_without_attempt_token(
        self, caller, create_task_instance, session, monkeypatch, delete_rejected
    ):
        archived, _ = caller
        XComModel.set_for_attempt(
            task_instance_id=archived.id, key="key", value="archived", serialize=False, session=session
        )
        current = archived.prepare_db_for_next_try(session)
        other = create_task_instance(dag_id="other_dag", task_id="other_task")
        session.commit()
        path = f"/xcoms/{archived.dag_id}/{archived.run_id}/{archived.task_id}/key"

        with TestClient(InProcessExecutionAPI().app) as client:
            other_attempt = {"X-Airflow-In-Process-Attempt-Id": str(other.id)}
            assert client.post(path, json="other", headers=other_attempt).status_code == 201
            assert client.delete(path, headers=other_attempt).status_code == 200
            archived_header = {"X-Airflow-In-Process-Attempt-Id": str(archived.id)}
            assert client.post(path, json="stale", headers=archived_header).status_code == 410
            assert client.delete(path, headers=archived_header).status_code == 410
            assert client.post(path, json="current").status_code == 201
            session.expire_all()
            assert XComModelV2.get_for_attempt(current.id, "key", session=session).value == "current"

            class APITransport:
                def get(self, path, *, params):
                    response = client.get(f"/{path}", params=params)
                    response.raise_for_status()
                    return response

                def delete(self, path, *, params):
                    headers = (
                        {"X-Airflow-In-Process-Attempt-Id": str(archived.id)} if delete_rejected else None
                    )
                    response = client.delete(f"/{path}", params=params, headers=headers)
                    if response.status_code != 200:
                        raise RuntimeError(f"DELETE rejected: {response.status_code}")
                    return response

            api = SimpleNamespace(xcoms=XComOperations(APITransport()))

            class InlineComms:
                def send(self, message):
                    if isinstance(message, GetXCom):
                        return request_handlers.handle_get_xcom(api, message)[0]
                    if isinstance(message, DeleteXCom):
                        return request_handlers.handle_delete_xcom(api, message)[0]
                    raise AssertionError(type(message))

            monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", InlineComms(), raising=False)
            with patch.object(BaseXCom, "purge", autospec=True) as purge:
                kwargs = {
                    "key": "key",
                    "dag_id": archived.dag_id,
                    "run_id": archived.run_id,
                    "task_id": archived.task_id,
                    "map_index": archived.map_index,
                }
                if delete_rejected:
                    with pytest.raises(RuntimeError, match="DELETE rejected: 410"):
                        BaseXCom.delete(**kwargs)
                    purge.assert_not_called()
                else:
                    BaseXCom.delete(**kwargs)
                    assert purge.call_args.args[0].value == "current"

        session.expire_all()
        current_row = XComModelV2.get_for_attempt(current.id, "key", session=session)
        assert (current_row.value if current_row is not None else None) == (
            "current" if delete_rejected else None
        )
        assert XComModelV2.get_for_attempt(archived.id, "key", session=session).value == "archived"

    @pytest.mark.parametrize("operation", ["xcom_write", "xcom_delete", "rtif_write", "variable_write"])
    def test_admitted_child_mutation_stays_with_archived_uuid(
        self, client, caller, session, monkeypatch, request, operation
    ):
        attempt, _ = caller
        old_id = attempt.id
        variable_key = f"attempt-liveness-{uuid4()}"
        request.addfinalizer(lambda: Variable.delete(variable_key))
        xcom_path = f"/execution/xcoms/{attempt.dag_id}/{attempt.run_id}/{attempt.task_id}/value"
        XComModel.set_for_attempt(
            task_instance_id=old_id, key="value", value="original", serialize=False, session=session
        )
        if operation == "xcom_delete":
            session.add(
                LegacyTaskDataOwner(
                    dag_id=attempt.dag_id,
                    task_id=attempt.task_id,
                    run_id=attempt.run_id,
                    map_index=attempt.map_index,
                    task_instance_id=old_id,
                )
            )
            session.add(
                XComModelV1(
                    dag_run_id=attempt.dag_run.id,
                    dag_id=attempt.dag_id,
                    task_id=attempt.task_id,
                    run_id=attempt.run_id,
                    map_index=attempt.map_index,
                    key="value",
                    value="legacy",
                )
            )
        session.commit()
        path = {
            "rtif_write": f"/execution/task-instances/{old_id}/rtif",
            "variable_write": f"/execution/variables/{variable_key}",
        }.get(operation, xcom_path)
        method = {
            "xcom_write": "POST",
            "xcom_delete": "DELETE",
            "rtif_write": "PUT",
            "variable_write": "PUT",
        }[operation]
        body = {"rtif_write": {"field": "late"}, "variable_write": {"value": "in flight"}}.get(
            operation, "late"
        )
        admitted, finish = Event(), Event()
        check = security._require_live_attempt

        async def pause_after_check(*args, **kwargs):
            await check(*args, **kwargs)
            admitted.set()
            assert await anyio.to_thread.run_sync(finish.wait, 10)

        monkeypatch.setattr(security, "_require_live_attempt", pause_after_check)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(client.request, method, path, json=body)
            try:
                assert admitted.wait(10)
                successor = attempt.prepare_db_for_next_try(session)
                XComModel.set_for_attempt(
                    task_instance_id=successor.id,
                    key="value",
                    value="successor",
                    serialize=False,
                    session=session,
                )
                RenderedTaskInstanceFields.set_for_attempt(
                    task_instance_id=successor.id, rendered_fields={"field": "successor"}, session=session
                )
                session.commit()
            finally:
                finish.set()
            response = pending.result(timeout=10)
        expected_status = {"xcom_write": 201, "xcom_delete": 200, "rtif_write": 410, "variable_write": 201}[
            operation
        ]
        assert response.status_code == expected_status, response.text
        session.expire_all()
        assert XComModelV2.get_for_attempt(successor.id, "value", session=session).value == "successor"
        assert RenderedTaskInstanceFields.get_for_attempt(successor.id, session=session).rendered_fields == {
            "field": "successor"
        }
        if operation == "xcom_write":
            assert XComModelV2.get_for_attempt(old_id, "value", session=session).value == "late"
        elif operation == "xcom_delete":
            assert XComModelV2.get_for_attempt(old_id, "value", session=session) is None
            assert (
                session.get(XComModelV1, (attempt.dag_run.id, attempt.task_id, attempt.map_index, "value"))
                is None
            )
        elif operation == "variable_write":
            assert Variable.get(variable_key) == "in flight"
            assert client.put(path, json={"value": "too late"}).status_code == 410
            assert Variable.get(variable_key) == "in flight"
        else:
            assert RenderedTaskInstanceFields.get_for_attempt(old_id, session=session) is None

    def test_public_execution_api_ignores_in_process_attempt_header(self, client, caller, session):
        archived, _ = caller
        successor = archived.prepare_db_for_next_try(session)
        session.commit()

        response = client.post(
            f"/execution/xcoms/{archived.dag_id}/{archived.run_id}/{archived.task_id}/value",
            headers={"X-Airflow-In-Process-Attempt-Id": str(successor.id)},
            json="spoofed",
        )

        assert response.status_code == 410
        assert XComModelV2.get_for_attempt(successor.id, "value", session=session) is None
