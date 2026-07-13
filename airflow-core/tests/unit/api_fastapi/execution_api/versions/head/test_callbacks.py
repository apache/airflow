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
    """Empty (sync) callable used for unit tests."""


def _make_queued_callback(session, dag_id="test_dag"):
    """Create and persist an ExecutorCallback in QUEUED state."""
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.data["dag_id"] = dag_id
    callback.state = CallbackState.QUEUED
    session.add(callback)
    session.commit()
    return callback


class TestRunCallback:
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_callbacks()
        yield
        clear_db_callbacks()

    def test_run_marks_running_and_swaps_token(self, client, session):
        """First call transitions QUEUED -> RUNNING and returns a fresh execution token."""
        callback = _make_queued_callback(session)

        response = client.patch(f"/execution/callbacks/{callback.id}/run")

        assert response.status_code == 204

        # A real execution-scope JWT is minted with the callback id as its subject.
        refreshed = response.headers["Refreshed-API-Token"]
        payload = jwt.decode(refreshed, options={"verify_signature": False})
        assert payload["scope"] == "execution"
        assert payload["sub"] == str(callback.id)

        session.expire_all()
        session.refresh(callback)
        assert callback.state == CallbackState.RUNNING

    def test_run_is_single_use(self, client, session):
        """A second exchange of the same callback token is rejected with 409."""
        callback = _make_queued_callback(session)

        first = client.patch(f"/execution/callbacks/{callback.id}/run")
        assert first.status_code == 204

        second = client.patch(f"/execution/callbacks/{callback.id}/run")
        assert second.status_code == 409
        detail = second.json()["detail"]
        assert detail["reason"] == "invalid_state"
        assert detail["previous_state"] == CallbackState.RUNNING

    @pytest.mark.parametrize(
        "state",
        [CallbackState.RUNNING, CallbackState.SUCCESS, CallbackState.FAILED, CallbackState.PENDING],
    )
    def test_run_rejects_non_queued_state(self, client, session, state):
        """The token can only be exchanged while the callback is QUEUED."""
        callback = _make_queued_callback(session)
        callback.state = state
        session.commit()

        response = client.patch(f"/execution/callbacks/{callback.id}/run")
        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"

    def test_run_returns_404_for_nonexistent(self, client):
        """Exchanging a token for an unknown callback returns 404."""
        response = client.patch("/execution/callbacks/00000000-0000-0000-0000-000000000000/run")
        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"


@pytest.fixture
def _use_real_jwt_bearer(exec_app):
    """Remove the mock require_auth override so the real scope validation runs."""
    exec_app.dependency_overrides.pop(require_auth, None)


@pytest.mark.usefixtures("_use_real_jwt_bearer")
def test_run_accepts_callback_token_matching_sub(client, session):
    """The exchange endpoint accepts a callback-scope JWT whose sub is the callback id."""
    clear_db_callbacks()
    callback = _make_queued_callback(session)

    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": str(callback.id),
        "scope": "callback",
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.patch(f"/execution/callbacks/{callback.id}/run")
    assert resp.status_code == 204, resp.json()
    clear_db_callbacks()


@pytest.mark.usefixtures("_use_real_jwt_bearer")
def test_run_rejects_callback_token_for_other_callback(client, session):
    """callback:self blocks a callback token whose sub is a different callback id."""
    clear_db_callbacks()
    callback = _make_queued_callback(session)

    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        # sub belongs to a different callback than the one in the URL.
        "sub": "11111111-1111-1111-1111-111111111111",
        "scope": "callback",
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.patch(f"/execution/callbacks/{callback.id}/run")
    assert resp.status_code == 403, resp.json()
    clear_db_callbacks()


@pytest.mark.usefixtures("_use_real_jwt_bearer")
def test_run_rejects_execution_scope_token(client, session):
    """Only callback-scope tokens are accepted on the exchange endpoint."""
    clear_db_callbacks()
    callback = _make_queued_callback(session)

    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": str(callback.id),
        "scope": "execution",
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.patch(f"/execution/callbacks/{callback.id}/run")
    assert resp.status_code == 403, resp.json()
    clear_db_callbacks()
