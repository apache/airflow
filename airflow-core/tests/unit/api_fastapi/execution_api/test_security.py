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
from sqlalchemy import select
from structlog.testing import capture_logs

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api import security
from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken, TokenScope
from airflow.api_fastapi.execution_api.security import (
    ExecutionAPIRoute,
    _jwt_bearer,
    get_team_name_dep,
    require_auth,
)
from airflow.models.callback import Callback, CallbackFetchMethod
from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.taskinstance import LegacyTaskDataOwner, TaskInstance
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel, XComModelV1, XComModelV2
from airflow.sdk.api.client import XComOperations
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time import request_handlers, task_runner
from airflow.sdk.execution_time.comms import DeleteXCom, GetXCom
from airflow.utils.state import CallbackState, TaskInstanceState


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

    @pytest.mark.parametrize("retirement", ["retry", "delete"])
    def test_retired_attempt_cannot_replace_or_delete_xcom(self, client, caller, session, retirement):
        ti, token = caller
        path = f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}/result"
        assert client.post(path, json="original").status_code == 201
        old_id = ti.id
        if retirement == "retry":
            successor = ti.prepare_db_for_next_try(session)
            successor.state = TaskInstanceState.UP_FOR_RETRY
        else:
            session.delete(ti)
        session.commit()

        expected_status = 410 if retirement == "retry" else 404
        assert client.post(path, json="stale").status_code == expected_status
        assert client.delete(path).status_code == expected_status

        if retirement == "retry":
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

    RETIRED_ROUTES = [
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
        [pytest.param(*route, id=f"{route[0]}:{route[1]}") for route in RETIRED_ROUTES],
    )
    def test_retired_attempt_rejected_before_mutation(self, client, caller, session, method, path, body):
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

    @pytest.mark.parametrize("delete_rejected", [False, True], ids=["accepted", "retired-token-rejected"])
    def test_trusted_in_process_xcom_uses_current_owner_without_attempt_token(
        self, caller, create_task_instance, session, monkeypatch, delete_rejected
    ):
        retired, _ = caller
        XComModel.set_for_attempt(
            task_instance_id=retired.id, key="key", value="retired", serialize=False, session=session
        )
        current = retired.prepare_db_for_next_try(session)
        other = create_task_instance(dag_id="other_dag", task_id="other_task")
        session.commit()
        path = f"/xcoms/{retired.dag_id}/{retired.run_id}/{retired.task_id}/key"

        with TestClient(InProcessExecutionAPI().app) as client:
            other_attempt = {"X-Airflow-In-Process-Attempt-Id": str(other.id)}
            assert client.post(path, json="other", headers=other_attempt).status_code == 201
            assert client.delete(path, headers=other_attempt).status_code == 200
            retired_header = {"X-Airflow-In-Process-Attempt-Id": str(retired.id)}
            assert client.post(path, json="stale", headers=retired_header).status_code == 410
            assert client.delete(path, headers=retired_header).status_code == 410
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
                        {"X-Airflow-In-Process-Attempt-Id": str(retired.id)} if delete_rejected else None
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
                    "dag_id": retired.dag_id,
                    "run_id": retired.run_id,
                    "task_id": retired.task_id,
                    "map_index": retired.map_index,
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
        assert XComModelV2.get_for_attempt(retired.id, "key", session=session).value == "retired"

    @pytest.mark.parametrize("operation", ["xcom_write", "xcom_delete", "rtif_write", "variable_write"])
    def test_admitted_child_mutation_stays_with_retired_uuid(
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
        retired, _ = caller
        successor = retired.prepare_db_for_next_try(session)
        session.commit()

        response = client.post(
            f"/execution/xcoms/{retired.dag_id}/{retired.run_id}/{retired.task_id}/value",
            headers={"X-Airflow-In-Process-Attempt-Id": str(successor.id)},
            json="spoofed",
        )

        assert response.status_code == 410
        assert XComModelV2.get_for_attempt(successor.id, "value", session=session) is None
