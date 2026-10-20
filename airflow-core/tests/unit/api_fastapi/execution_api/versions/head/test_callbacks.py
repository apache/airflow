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

import jwt
import pytest

from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.models.callback import CallbackFetchMethod, CallbackState, ExecutorCallback
from airflow.sdk.definitions.callback import SyncCallback

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = pytest.mark.db_test


def sync_callback():
    """Empty (sync) callable used for unit tests"""
    pass


@pytest.fixture(autouse=True)
def clean_callbacks():
    clear_db_callbacks()
    yield
    clear_db_callbacks()


@pytest.fixture
def queued_callback(session):
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.data["dag_id"] = "test_dag"
    callback.state = CallbackState.QUEUED
    session.add(callback)
    session.commit()
    return callback


class TestRunCallback:
    def test_run_is_single_use(self, client, session, queued_callback):
        """First call moves QUEUED -> RUNNING and swaps the token; a replay is rejected with 409."""
        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")

        assert response.status_code == 204
        payload = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
        assert payload["scope"] == "execution"
        assert payload["sub"] == str(queued_callback.id)

        session.expire_all()
        session.refresh(queued_callback)
        assert queued_callback.state == CallbackState.RUNNING

        second = client.patch(f"/execution/callbacks/{queued_callback.id}/run")
        assert second.status_code == 409
        detail = second.json()["detail"]
        assert detail["reason"] == "invalid_state"
        assert detail["previous_state"] == CallbackState.RUNNING

    @pytest.mark.parametrize("state", [CallbackState.SUCCESS, CallbackState.FAILED, CallbackState.PENDING])
    def test_run_rejects_non_queued_state(self, client, session, queued_callback, state):
        """The token can only be exchanged while the callback is QUEUED."""
        queued_callback.state = state
        session.commit()

        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")
        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"

    def test_run_returns_404_for_nonexistent(self, client):
        """Exchanging a token for an unknown callback returns 404."""
        response = client.patch("/execution/callbacks/00000000-0000-0000-0000-000000000000/run")
        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"


@pytest.fixture
def running_callback(session):
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.data["dag_id"] = "test_dag"
    callback.state = CallbackState.RUNNING
    session.add(callback)
    session.commit()
    return callback


@pytest.fixture
def other_callback(session):
    """A second, unrelated callback, so a token can name a real id that is not this one."""
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.data["dag_id"] = "test_dag"
    callback.state = CallbackState.RUNNING
    session.add(callback)
    session.commit()
    return callback


class TestUpdateCallbackState:
    @pytest.mark.parametrize(
        ("state", "output"),
        [
            pytest.param("success", None, id="success-without-output"),
            pytest.param("success", "all good", id="success-with-output"),
            pytest.param("failed", "exited with code 3", id="failed-with-output"),
        ],
    )
    def test_running_callback_reaches_its_terminal_state(
        self, client, session, running_callback, state, output
    ):
        body = {"state": state}
        if output is not None:
            body["output"] = output

        response = client.patch(f"/execution/callbacks/{running_callback.id}/state", json=body)

        assert response.status_code == 204
        session.expire_all()
        session.refresh(running_callback)
        assert running_callback.state == state
        assert running_callback.output == output

    def test_output_is_left_alone_when_omitted(self, client, session, running_callback):
        running_callback.output = "captured earlier"
        session.commit()

        response = client.patch(f"/execution/callbacks/{running_callback.id}/state", json={"state": "failed"})

        assert response.status_code == 204
        session.expire_all()
        session.refresh(running_callback)
        assert running_callback.output == "captured earlier"

    @pytest.mark.parametrize(
        "state",
        [CallbackState.QUEUED, CallbackState.PENDING, CallbackState.SUCCESS, CallbackState.FAILED],
    )
    def test_only_a_running_callback_can_be_reported(self, client, session, running_callback, state):
        running_callback.state = state
        session.commit()

        response = client.patch(
            f"/execution/callbacks/{running_callback.id}/state", json={"state": "success"}
        )

        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["reason"] == "invalid_state"
        assert detail["previous_state"] == state

        session.expire_all()
        session.refresh(running_callback)
        assert running_callback.state == state

    def test_returns_404_for_nonexistent(self, client):
        response = client.patch(
            "/execution/callbacks/00000000-0000-0000-0000-000000000000/state",
            json={"state": "success"},
        )
        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({"state": "running"}, id="non-terminal-state"),
            pytest.param({"state": "success", "extra": "nope"}, id="unknown-field"),
            pytest.param({}, id="missing-state"),
        ],
    )
    def test_rejects_a_malformed_payload(self, client, running_callback, body):
        response = client.patch(f"/execution/callbacks/{running_callback.id}/state", json=body)
        assert response.status_code == 422


@pytest.fixture
def _use_real_jwt_bearer(exec_app):
    """Remove the mock require_auth override so the real JWT validation runs end-to-end."""
    exec_app.dependency_overrides.pop(require_auth, None)


@pytest.mark.usefixtures("_use_real_jwt_bearer")
@pytest.mark.parametrize(
    ("token_sub", "token_scope", "expected_status"),
    [
        pytest.param("self", "callback", 204, id="callback-token-matching-sub-accepted"),
        pytest.param(
            "11111111-1111-1111-1111-111111111111",
            "callback",
            403,
            id="callback-token-for-other-callback-rejected",
        ),
        pytest.param("self", "execution", 403, id="execution-scope-token-rejected"),
    ],
)
def test_run_validates_token_scope_and_sub(client, queued_callback, token_sub, token_scope, expected_status):
    """The exchange endpoint only accepts a callback-scope JWT whose sub is this callback's id."""
    sub = str(queued_callback.id) if token_sub == "self" else token_sub
    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": sub,
        "scope": token_scope,
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.patch(f"/execution/callbacks/{queued_callback.id}/run")
    assert resp.status_code == expected_status, resp.json()


@pytest.mark.usefixtures("_use_real_jwt_bearer")
@pytest.mark.parametrize(
    ("token_sub", "token_scope", "expected_status"),
    [
        pytest.param("self", "execution", 204, id="execution-token-matching-sub-accepted"),
        pytest.param("other", "execution", 403, id="execution-token-for-other-callback-rejected"),
        pytest.param(
            "00000000-0000-0000-0000-000000000000",
            "execution",
            404,
            id="execution-token-for-unknown-subject-rejected",
        ),
        pytest.param("self", "callback", 403, id="callback-scope-token-rejected"),
    ],
)
def test_state_validates_token_scope_and_sub(
    client, running_callback, other_callback, token_sub, token_scope, expected_status
):
    """
    Reporting state needs the execution token minted by /run, not the admission ticket.

    A callback-scope token is refused here, which is the point of the swap: the single-use
    ticket buys one transition into RUNNING and nothing else.

    The two rejected execution-token cases differ by design. A sub naming another *existing*
    callback clears the live-attempt admission check and is then refused 403 by ``cb:self``,
    while a sub naming nothing at all is refused 404 by the admission check itself, which runs
    first because the parent ``authenticated_router`` declares ``require_auth`` with no scopes
    and FastAPI resolves the outermost dependency before the ``cb:self`` one.
    """
    subs = {"self": str(running_callback.id), "other": str(other_callback.id)}
    sub = subs.get(token_sub, token_sub)
    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": sub,
        "scope": token_scope,
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.patch(f"/execution/callbacks/{running_callback.id}/state", json={"state": "success"})
    assert resp.status_code == expected_status, resp.json()
