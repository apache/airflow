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

import uuid
from datetime import timedelta
from typing import Any
from unittest import mock

import jwt
import pytest
import time_machine
from pydantic import BaseModel
from sqlalchemy import update

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun as DRDataModel,
    TaskInstance as TIDataModel,
    TIRunContext,
)
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.callbacks.callback_requests import (
    DagCallbackRequest,
    DagRunContext,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.executors.workloads.callback import START_DEADLINE_KEY
from airflow.models.callback import (
    Callback,
    CallbackFetchMethod,
    CallbackState,
    CallbackType,
    DagProcessorCallback,
    ExecutorCallback,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import SyncCallback
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_callbacks, clear_db_runs

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


NOW = timezone.datetime(2026, 10, 22, 12, 0, 0)

DAG_RUN = DRDataModel(
    dag_id="test_dag",
    run_id="test_run",
    logical_date=NOW,
    data_interval_start=NOW,
    data_interval_end=NOW,
    run_after=NOW,
    start_date=NOW,
    end_date=None,
    run_type=DagRunType.MANUAL,
    state=DagRunState.FAILED,
    consumed_asset_events=[],
    partition_key=None,
)
TI = TIDataModel(
    id=uuid.UUID("01890a5d-ac70-7a5b-b7d5-0dd5b1c7be47"),
    dag_id="test_dag",
    task_id="test_task",
    run_id="test_run",
    try_number=1,
    dag_version_id=uuid.UUID("01890a5d-ac70-7a5b-b7d5-0dd5b1c7be48"),
)
TI_CONTEXT = TIRunContext(dag_run=DAG_RUN, max_tries=2, variables=[], connections=[], xcom_keys_to_clear=[])
REQUEST_FIELDS: dict[str, Any] = {"filepath": "test_dag.py", "bundle_name": "testing", "bundle_version": None}


def _add_routed_callback(session, request, state=CallbackState.RUNNING) -> Callback:
    """Store a Dag processor callback as the scheduler leaves it once routed."""
    callback = DagProcessorCallback(priority_weight=1, callback=request)
    session.add(callback)
    session.flush()
    session.execute(
        update(Callback)
        .where(Callback.id == callback.id)
        .values(type=CallbackType.EXECUTOR, state=state)
        .execution_options(synchronize_session=False)
    )
    session.commit()
    session.expunge_all()
    return session.get_one(Callback, callback.id)


class TestRunCallbackStartDeadline:
    @time_machine.travel(NOW, tick=False)
    @pytest.mark.parametrize(
        ("deadline_offset", "expected_status", "expected_state"),
        [
            pytest.param(timedelta(seconds=1), 204, CallbackState.RUNNING, id="before-deadline"),
            pytest.param(timedelta(0), 409, CallbackState.QUEUED, id="at-deadline"),
            pytest.param(timedelta(seconds=-1), 409, CallbackState.QUEUED, id="past-deadline"),
        ],
    )
    def test_run_honors_start_deadline(
        self, client, session, queued_callback, deadline_offset, expected_status, expected_state
    ):
        queued_callback.data = {
            **queued_callback.data,
            START_DEADLINE_KEY: (NOW + deadline_offset).isoformat(),
        }
        session.commit()

        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")

        assert response.status_code == expected_status
        if expected_status == 409:
            assert response.json()["detail"]["reason"] == "start_deadline_passed"
        session.expire_all()
        assert session.get_one(Callback, queued_callback.id).state == expected_state

    @time_machine.travel(NOW, tick=False)
    def test_run_records_the_start(self, client, session, queued_callback):
        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")

        assert response.status_code == 204
        session.expire_all()
        assert session.get_one(Callback, queued_callback.id).started_at == NOW


class TestRunCallbackIdentityClaims:
    @pytest.fixture(autouse=True)
    def _clean_runs(self):
        clear_db_runs()
        yield
        clear_db_runs()

    def test_run_signs_the_dag_run_and_bundle_of_the_callback(self, client, session, dag_maker):
        with dag_maker("test_dag", session=session):
            EmptyOperator(task_id="test_task")
        dag_run = dag_maker.create_dagrun(run_id="test_run")
        callback = ExecutorCallback(
            callback_def=SyncCallback(sync_callback, kwargs={}),
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
        )
        callback.data |= {"dag_id": "test_dag", "dag_run_id": dag_run.id}
        callback.bundle_name = "testing"
        callback.state = CallbackState.QUEUED
        session.add(callback)
        session.commit()

        response = client.patch(f"/execution/callbacks/{callback.id}/run")

        assert response.status_code == 204
        claims = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
        assert claims["dag_id"] == "test_dag"
        assert claims["run_id"] == "test_run"
        assert claims["bundle_name"] == "testing"

    def test_run_omits_identity_the_callback_does_not_have(self, client, queued_callback):
        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")

        assert response.status_code == 204
        claims = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
        assert claims["dag_id"] == "test_dag"
        assert "run_id" not in claims
        assert "bundle_name" not in claims


class TestGetCallbackContext:
    @pytest.mark.parametrize(
        ("request_", "expected_context"),
        [
            pytest.param(
                DagCallbackRequest(
                    **REQUEST_FIELDS,
                    dag_id="test_dag",
                    run_id="test_run",
                    context_from_server=DagRunContext(dag_run=DAG_RUN, last_ti=TI),
                    msg="task_failure",
                ),
                {
                    "type": "DagCallback",
                    "dag_id": "test_dag",
                    "run_id": "test_run",
                    "is_failure_callback": True,
                    "msg": "task_failure",
                    "dag_run": DAG_RUN,
                    "last_ti": TI,
                },
                id="dag-failure",
            ),
            pytest.param(
                DagCallbackRequest(
                    **REQUEST_FIELDS, dag_id="test_dag", run_id="test_run", is_failure_callback=False
                ),
                {
                    "type": "DagCallback",
                    "dag_id": "test_dag",
                    "run_id": "test_run",
                    "is_failure_callback": False,
                    "msg": None,
                    "dag_run": None,
                    "last_ti": None,
                },
                id="dag-success-without-run-context",
            ),
            pytest.param(
                TaskCallbackRequest(
                    **REQUEST_FIELDS,
                    ti=TI,
                    task_callback_type=TaskInstanceState.UP_FOR_RETRY,
                    context_from_server=TI_CONTEXT,
                    msg="heartbeat timeout",
                ),
                {
                    "type": "TaskCallback",
                    "ti": TI,
                    "task_callback_type": "up_for_retry",
                    "msg": "heartbeat timeout",
                    "ti_context": TI_CONTEXT,
                },
                id="task-retry",
            ),
            pytest.param(
                EmailRequest(**REQUEST_FIELDS, ti=TI, email_type="retry", context_from_server=TI_CONTEXT),
                {
                    "type": "EmailCallback",
                    "ti": TI,
                    "email_type": "retry",
                    "msg": None,
                    "ti_context": TI_CONTEXT,
                },
                id="email-retry",
            ),
        ],
    )
    def test_returns_the_context_of_the_stored_request(self, client, session, request_, expected_context):
        callback = _add_routed_callback(session, request_)

        response = client.get(f"/execution/callbacks/{callback.id}/context")

        assert response.status_code == 200
        context = response.json()["context"]
        assert context.keys() == expected_context.keys()
        for key, expected in expected_context.items():
            if isinstance(expected, BaseModel):
                assert type(expected).model_validate(context[key]) == expected
            else:
                assert context[key] == expected

    @pytest.mark.parametrize(
        "state", [CallbackState.QUEUED, CallbackState.SUCCESS, CallbackState.FAILED, CallbackState.PENDING]
    )
    def test_only_a_running_callback_reads_its_context(self, client, session, state):
        callback = _add_routed_callback(
            session, DagCallbackRequest(**REQUEST_FIELDS, dag_id="test_dag", run_id="test_run"), state=state
        )

        response = client.get(f"/execution/callbacks/{callback.id}/context")

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"

    def test_import_path_callback_has_no_context(self, client, session, queued_callback):
        queued_callback.state = CallbackState.RUNNING
        session.commit()

        response = client.get(f"/execution/callbacks/{queued_callback.id}/context")

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "no_context"

    def test_returns_404_for_nonexistent(self, client):
        response = client.get("/execution/callbacks/00000000-0000-0000-0000-000000000000/context")

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"


class TestUpdateCallbackState:
    @pytest.mark.parametrize(
        ("state", "output"),
        [
            pytest.param(CallbackState.SUCCESS, None, id="success"),
            pytest.param(CallbackState.FAILED, "on_failure: ValueError: boom", id="failed"),
        ],
    )
    def test_records_the_terminal_state_of_a_running_callback(
        self, client, session, queued_callback, state, output
    ):
        queued_callback.state = CallbackState.RUNNING
        session.commit()

        response = client.patch(
            f"/execution/callbacks/{queued_callback.id}/state", json={"state": state, "output": output}
        )

        assert response.status_code == 204
        session.expire_all()
        callback = session.get_one(Callback, queued_callback.id)
        assert callback.state == state
        assert callback.output == output

    @pytest.mark.parametrize("state", [CallbackState.QUEUED, CallbackState.SUCCESS, CallbackState.FAILED])
    def test_rejects_a_callback_that_is_not_running(self, client, session, queued_callback, state):
        queued_callback.state = state
        session.commit()

        response = client.patch(f"/execution/callbacks/{queued_callback.id}/state", json={"state": "success"})

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"
        session.expire_all()
        assert session.get_one(Callback, queued_callback.id).state == state

    @pytest.mark.parametrize("state", ["running", "queued"])
    def test_rejects_a_non_terminal_state(self, client, session, queued_callback, state):
        queued_callback.state = CallbackState.RUNNING
        session.commit()

        response = client.patch(f"/execution/callbacks/{queued_callback.id}/state", json={"state": state})

        assert response.status_code == 422

    def test_returns_404_for_nonexistent(self, client):
        response = client.patch(
            "/execution/callbacks/00000000-0000-0000-0000-000000000000/state", json={"state": "success"}
        )

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"


@pytest.mark.usefixtures("_use_real_jwt_bearer")
@pytest.mark.parametrize(
    ("token_sub", "token_scope", "expected_status"),
    [
        pytest.param("self", "execution", None, id="execution-token-matching-sub-accepted"),
        pytest.param("other", "execution", 403, id="execution-token-for-other-callback"),
        pytest.param("self", "callback", 403, id="unexchanged-callback-token-rejected"),
    ],
)
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        pytest.param("GET", "context", None, id="context"),
        pytest.param("PATCH", "state", {"state": "success"}, id="state"),
    ],
)
def test_context_and_state_require_the_exchanged_token(
    client, session, token_sub, token_scope, expected_status, method, path, body
):
    """Only the execution token a callback exchanged for reads its context or reports its state."""
    request = DagCallbackRequest(**REQUEST_FIELDS, dag_id="test_dag", run_id="test_run")
    callback = _add_routed_callback(session, request)
    other_callback = _add_routed_callback(session, request)
    validator = mock.AsyncMock(spec=JWTValidator)
    validator.avalidated_claims.return_value = {
        "sub": str(callback.id if token_sub == "self" else other_callback.id),
        "scope": token_scope,
        "exp": 9999999999,
        "iat": 1000000000,
        "nbf": 1000000000,
    }
    lifespan.registry.register_value(JWTValidator, validator)

    resp = client.request(method, f"/execution/callbacks/{callback.id}/{path}", json=body)

    if expected_status is None:
        assert resp.status_code in (200, 204), resp.json()
    else:
        assert resp.status_code == expected_status, resp.json()
