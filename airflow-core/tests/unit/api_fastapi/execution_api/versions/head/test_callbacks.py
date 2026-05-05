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

from typing import Literal
from unittest import mock
from uuid import UUID, uuid4

import pytest
from fastapi import Request

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.executors.workloads.callback import CallbackFetchMethod
from airflow.models.callback import ExecutorCallback
from airflow.utils.state import CallbackState

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = pytest.mark.db_test


class _FakeCallbackDef:
    """Minimal CallbackDefinitionProtocol stand-in for tests."""

    path: str = "tests.fake.callback"
    kwargs: dict = {}
    executor: str | None = None

    def serialize(self) -> dict:
        return {"path": self.path, "kwargs": self.kwargs, "executor": self.executor}


def _make_callback(state: CallbackState, session) -> ExecutorCallback:
    cb = ExecutorCallback(callback_def=_FakeCallbackDef(), fetch_method=CallbackFetchMethod.IMPORT_PATH)
    cb.state = state
    session.add(cb)
    session.commit()
    return cb


def _override_require_auth(exec_app, scope: Literal["execution", "workload"] = "execution") -> None:
    """Override require_auth to return a token whose sub matches the path callback_id."""

    async def _token(request: Request) -> TIToken:
        path_id = request.path_params.get("callback_id")
        cb_id = UUID(path_id) if path_id else uuid4()
        return TIToken(id=cb_id, claims=TIClaims(scope=scope))

    exec_app.dependency_overrides[require_auth] = _token


@pytest.fixture
def _use_real_jwt_bearer(exec_app):
    """Remove the mock require_auth override so the real JWT validation runs end-to-end."""
    exec_app.dependency_overrides.pop(require_auth, None)


class TestCallbackRun:
    def setup_method(self):
        clear_db_callbacks()

    def teardown_method(self):
        clear_db_callbacks()

    @pytest.mark.parametrize("scope", ["workload", "execution"])
    def test_run_marks_callback_running_and_swaps_workload_token(self, client, exec_app, session, scope):
        cb = _make_callback(CallbackState.QUEUED, session)

        mock_gen = mock.MagicMock(spec=JWTGenerator)
        mock_gen.generate.return_value = "mock-execution-token"
        lifespan.registry.register_value(JWTGenerator, mock_gen)

        _override_require_auth(exec_app, scope=scope)

        response = client.post(f"/execution/callbacks/{cb.id}/run")

        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 204

        session.expire_all()
        cb_after = session.get(ExecutorCallback, cb.id)
        assert cb_after.state == CallbackState.RUNNING

        if scope == "workload":
            assert response.headers["Refreshed-API-Token"] == "mock-execution-token"
            mock_gen.generate.assert_called_once()
            extras = mock_gen.generate.call_args.kwargs["extras"]
            assert extras == {"sub": str(cb.id), "scope": "execution"}
        else:
            # Execution-scoped tokens skip the swap; the middleware handles refresh elsewhere.
            assert "Refreshed-API-Token" not in response.headers
            mock_gen.generate.assert_not_called()

    @pytest.mark.parametrize(
        "state",
        [
            CallbackState.PENDING,
            CallbackState.SCHEDULED,
            CallbackState.SUCCESS,
            CallbackState.FAILED,
        ],
    )
    def test_run_returns_409_for_non_runnable_state(self, client, exec_app, session, state):
        cb = _make_callback(state, session)
        _override_require_auth(exec_app, scope="workload")

        response = client.post(f"/execution/callbacks/{cb.id}/run")
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"
        assert response.json()["detail"]["current_state"] == state.value

    def test_run_returns_404_when_callback_missing(self, client, exec_app):
        missing_id = uuid4()

        async def _token(request: Request) -> TIToken:
            return TIToken(id=missing_id, claims=TIClaims(scope="workload"))

        exec_app.dependency_overrides[require_auth] = _token

        response = client.post(f"/execution/callbacks/{missing_id}/run")
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_run_rejects_mismatched_sub(self, client, exec_app, session):
        cb = _make_callback(CallbackState.QUEUED, session)

        async def _token(request: Request) -> TIToken:
            return TIToken(id=uuid4(), claims=TIClaims(scope="workload"))

        exec_app.dependency_overrides[require_auth] = _token

        response = client.post(f"/execution/callbacks/{cb.id}/run")
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 403
        assert response.json()["detail"] == "Token subject does not match callback id"


class TestCallbackUpdateState:
    def setup_method(self):
        clear_db_callbacks()

    def teardown_method(self):
        clear_db_callbacks()

    @pytest.mark.parametrize(
        ("payload_state", "expected_state"),
        [
            ("success", CallbackState.SUCCESS),
            ("failed", CallbackState.FAILED),
        ],
    )
    def test_update_state_writes_terminal_state(
        self, client, exec_app, session, payload_state, expected_state
    ):
        cb = _make_callback(CallbackState.RUNNING, session)
        _override_require_auth(exec_app, scope="execution")

        response = client.patch(
            f"/execution/callbacks/{cb.id}/state",
            json={"state": payload_state, "output": "an output"},
        )
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 204
        session.expire_all()
        cb_after = session.get(ExecutorCallback, cb.id)
        assert cb_after.state == expected_state
        assert cb_after.output == "an output"

    @pytest.mark.parametrize(
        "state",
        [
            CallbackState.QUEUED,
            CallbackState.PENDING,
            CallbackState.SCHEDULED,
            CallbackState.SUCCESS,
            CallbackState.FAILED,
        ],
    )
    def test_update_state_returns_409_when_not_running(self, client, exec_app, session, state):
        cb = _make_callback(state, session)
        _override_require_auth(exec_app, scope="execution")

        response = client.patch(
            f"/execution/callbacks/{cb.id}/state",
            json={"state": "success"},
        )
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"

    def test_update_state_returns_404_when_callback_missing(self, client, exec_app):
        missing_id = uuid4()
        _override_require_auth(exec_app, scope="execution")

        response = client.patch(
            f"/execution/callbacks/{missing_id}/state",
            json={"state": "success"},
        )
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 404

    def test_update_state_rejects_mismatched_sub(self, client, exec_app, session):
        cb = _make_callback(CallbackState.RUNNING, session)

        async def _token(request: Request) -> TIToken:
            return TIToken(id=uuid4(), claims=TIClaims(scope="execution"))

        exec_app.dependency_overrides[require_auth] = _token

        response = client.patch(
            f"/execution/callbacks/{cb.id}/state",
            json={"state": "success"},
        )
        exec_app.dependency_overrides.pop(require_auth, None)

        assert response.status_code == 403

    @pytest.mark.usefixtures("_use_real_jwt_bearer")
    def test_update_state_rejects_workload_scope(self, client, session):
        cb = _make_callback(CallbackState.RUNNING, session)

        validator = mock.AsyncMock(spec=JWTValidator)
        validator.avalidated_claims.return_value = {
            "sub": str(cb.id),
            "scope": "workload",
            "exp": 9999999999,
            "iat": 1000000000,
            "nbf": 1000000000,
        }
        lifespan.registry.register_value(JWTValidator, validator)

        response = client.patch(
            f"/execution/callbacks/{cb.id}/state",
            json={"state": "success"},
        )

        assert response.status_code == 403
        assert "Token type 'workload' not allowed" in response.json()["detail"]
