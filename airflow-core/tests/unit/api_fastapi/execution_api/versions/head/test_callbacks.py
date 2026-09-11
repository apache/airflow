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
