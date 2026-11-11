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
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import svcs
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from jwt import ExpiredSignatureError

from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import JWTReissueMiddleware, lifespan
from airflow.api_fastapi.execution_api.security import ASSET_WATCHER_ID, _jwt_bearer, require_auth
from airflow.executors.workloads.callback import CallbackFetchMethod
from airflow.models.callback import ExecutorCallback
from airflow.sdk.definitions.callback import SyncCallback
from airflow.utils.state import CallbackState, State, TaskInstanceState

from tests_common.test_utils.db import clear_db_callbacks


@pytest.fixture
def jwt_bearer_client(client, exec_app, monkeypatch):
    """Test client that exercises JWTBearer so request.scope is populated for the middleware."""
    monkeypatch.delitem(exec_app.dependency_overrides, require_auth)
    return client


def sync_callback():
    """Empty (sync) callable used for unit tests"""
    pass


@pytest.fixture
def clean_callbacks():
    clear_db_callbacks()
    yield
    clear_db_callbacks()


@pytest.fixture
def request_with_expiring_execution_token(jwt_bearer_client, time_machine):
    """Send a request whose ``execution`` token for ``sub`` is inside its refresh window."""

    def send(sub):
        moment = 1743451846
        auth = AsyncMock(spec=JWTValidator)
        auth.avalidated_claims.return_value = {
            "sub": str(sub),
            "scope": "execution",
            "iat": moment,
            "exp": moment + 60,
        }
        time_machine.move_to(moment + 31, tick=False)
        lifespan.registry.register_value(JWTValidator, auth)
        return jwt_bearer_client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    return send


@pytest.mark.parametrize(
    ("age", "validity", "expect_refreshed_token"),
    (
        pytest.param(25, 60, False, id="25s-of-60s"),
        pytest.param(31, 60, True, id="30s-of-60s"),
        # Test the "20% left" behaviour for larger validities
        pytest.param(400, 600, False, id="400s-of-600s"),
        pytest.param(480, 600, True, id="480s-of-600s"),
    ),
)
@pytest.mark.db_test
@mock.patch(
    "airflow.api_fastapi.execution_api.app.is_execution_token_renewable", autospec=True, return_value=True
)
def test_expiring_token_is_reissued(
    mock_is_renewable, jwt_bearer_client, time_machine, age, validity, expect_refreshed_token
):
    moment = 1743451846  # A "random" unix epoch timestamp.
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "iat": moment,
        "exp": moment + validity,
    }

    time_machine.move_to(moment + age, tick=False)

    # Inject our fake JWTValidator object. Can be over-ridden by tests if they want
    lifespan.registry.register_value(JWTValidator, auth)
    # In order to test this we need any endpoint to hit. The easiest one to use is variable get

    response = jwt_bearer_client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    if expect_refreshed_token:
        assert "Refreshed-API-Token" in response.headers
    else:
        assert "Refreshed-API-Token" not in response.headers
    # avalidated_claims must be called exactly once — by JWTBearer only, not by the middleware.
    auth.avalidated_claims.assert_awaited_once_with("dummy", {})
    # The lifecycle lookup is only paid for a token that is due for a refresh.
    assert mock_is_renewable.await_count == int(expect_refreshed_token)


@pytest.mark.parametrize(
    ("request_age", "expected_status"),
    [
        pytest.param(599, 200, id="expires-during-request"),
        pytest.param(601, 403, id="expired-before-request"),
    ],
)
@mock.patch(
    "airflow.api_fastapi.execution_api.app.is_execution_token_renewable", autospec=True, return_value=True
)
def test_token_expiring_mid_request_is_reissued_without_revalidation(
    mock_is_renewable, time_machine, request_age, expected_status
):
    """Middleware reissues from cached JWTBearer claims without re-validating the token.

    Regression test for the TOCTOU race in JWTReissueMiddleware: a heartbeat arrives with a
    token that has 1s left, JWTBearer validates it (still valid at that moment), the handler
    advances time past expiry, and the middleware runs. In the old code the middleware would
    call avalidated_claims a second time and get ExpiredSignatureError — no Refreshed-API-Token
    header would be set, and the task would die on the next heartbeat.

    With the fix the middleware reads claims from request.scope (set by JWTBearer) instead of
    calling avalidated_claims again, so it still issues a fresh token even when the original
    has since expired. A token that is already expired when the request arrives must still be
    rejected without a refresh.
    """
    moment = 1743451846
    secret = "jwt-refresh-test-secret" * 4
    audience = "test-jwt-refresh"
    generator = JWTGenerator(secret_key=secret, audience=audience, valid_for=600)
    validator = JWTValidator(secret_key=secret, audience=audience, leeway=0)
    claims = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "scope": "execution",
        "custom": {"key": "value"},
    }
    time_machine.move_to(moment, tick=False)
    token = generator.generate(claims)
    app = FastAPI()
    app.add_middleware(JWTReissueMiddleware)

    @app.get("/protected", dependencies=[Depends(_jwt_bearer)])
    async def protected():
        time_machine.move_to(moment + 601, tick=False)
        return {"ok": True}

    with svcs.Registry() as registry:
        registry.register_value(JWTGenerator, generator)
        registry.register_value(JWTValidator, validator)
        app.state.svcs_registry = registry
        time_machine.move_to(moment + request_age, tick=False)

        with TestClient(app) as client:
            response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == expected_status
    with pytest.raises(ExpiredSignatureError):
        validator.validated_claims(token)

    if expected_status == 200:
        assert response.json() == {"ok": True}
        refreshed_claims = validator.validated_claims(response.headers["Refreshed-API-Token"])
        assert refreshed_claims.items() >= claims.items()
        assert refreshed_claims["exp"] == moment + 1201
    else:
        assert response.json() == {"detail": "Invalid auth token"}
        assert "Refreshed-API-Token" not in response.headers


@pytest.mark.db_test
def test_expiring_dag_processor_token_is_not_reissued(client, time_machine):
    moment = 1743451846
    auth = AsyncMock(spec=JWTValidator)
    auth.avalidated_claims.return_value = {
        "sub": "edb09971-4e0e-4221-ad3f-800852d38085",
        "scope": "dag_processor",
        "dag_bundles": ["dags-folder"],
        "iat": moment,
        "exp": moment + 60,
    }
    time_machine.move_to(moment + 55, tick=False)
    lifespan.registry.register_value(JWTValidator, auth)

    response = client.get("/execution/variables/key1", headers={"Authorization": "Bearer dummy"})

    assert "Refreshed-API-Token" not in response.headers


@pytest.mark.parametrize(
    ("state", "expect_refreshed_token"),
    [
        pytest.param(state, state == TaskInstanceState.RUNNING, id=state.value if state else "no_state")
        for state in State.task_states
    ],
)
@pytest.mark.db_test
def test_token_is_reissued_only_while_task_attempt_is_running(
    request_with_expiring_execution_token, create_task_instance, session, state, expect_refreshed_token
):
    ti = create_task_instance(state=state)
    session.commit()

    response = request_with_expiring_execution_token(ti.id)

    assert ("Refreshed-API-Token" in response.headers) is expect_refreshed_token


@pytest.mark.parametrize("identity", ["archived", "unknown"])
@pytest.mark.db_test
def test_token_is_not_reissued_without_a_live_attempt(
    request_with_expiring_execution_token, create_task_instance, session, identity
):
    if identity == "archived":
        # ``TaskInstance.archive`` never leaves an attempt running, so the state check alone would
        # stop its token. Archive a running attempt by hand to show the working-set check also does.
        ti = create_task_instance(state=TaskInstanceState.RUNNING)
        ti.working_set = None
        ti.archived_reason = "superseded"
        session.commit()
        sub = ti.id
    else:
        sub = uuid4()

    response = request_with_expiring_execution_token(sub)

    assert "Refreshed-API-Token" not in response.headers


@pytest.mark.parametrize(
    ("state", "expect_refreshed_token"),
    [
        pytest.param(CallbackState.RUNNING, True, id="running"),
        pytest.param(CallbackState.SUCCESS, False, id="success"),
        pytest.param(CallbackState.FAILED, False, id="failed"),
    ],
)
@pytest.mark.db_test
@pytest.mark.usefixtures("clean_callbacks")
def test_token_reissue_stops_when_callback_finishes(
    request_with_expiring_execution_token, session, state, expect_refreshed_token
):
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.state = state
    session.add(callback)
    session.commit()

    response = request_with_expiring_execution_token(callback.id)

    assert ("Refreshed-API-Token" in response.headers) is expect_refreshed_token


@pytest.mark.db_test
def test_expiring_asset_watcher_token_is_reissued(request_with_expiring_execution_token):
    response = request_with_expiring_execution_token(ASSET_WATCHER_ID)

    assert "Refreshed-API-Token" in response.headers
