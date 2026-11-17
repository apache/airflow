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
from airflow.executors.workloads.callback import EXECUTION_TIMEOUT_KEY, START_DEADLINE_KEY
from airflow.models.callback import (
    Callback,
    CallbackFetchMethod,
    CallbackState,
    CallbackType,
    ExecutorCallback,
)
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import SyncCallback
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
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

    @pytest.mark.parametrize(
        "state",
        [CallbackState.SCHEDULED, CallbackState.SUCCESS, CallbackState.FAILED, CallbackState.PENDING],
    )
    def test_run_rejects_non_queued_state(self, client, session, queued_callback, state):
        """The token can only be exchanged while the callback is QUEUED."""
        queued_callback.state = state
        session.commit()

        response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")
        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"

    def test_run_refuses_a_callback_handed_back_to_the_dag_processor(self, client, session, queued_callback):
        """A late delivery of a handed-back workload can't start what a Dag processor now runs."""
        session.execute(
            update(Callback)
            .where(Callback.id == queued_callback.id)
            .values(type=CallbackType.DAG_PROCESSOR, state=None)
        )
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
def make_running_callback(session):
    """Build a callback already in RUNNING, the only state the state endpoint accepts."""

    def _make():
        callback = ExecutorCallback(
            callback_def=SyncCallback(sync_callback, kwargs={}),
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
        )
        callback.data["dag_id"] = "test_dag"
        callback.state = CallbackState.RUNNING
        session.add(callback)
        session.commit()
        return callback

    return _make


@pytest.fixture
def running_callback(make_running_callback):
    return make_running_callback()


@pytest.fixture
def other_callback(make_running_callback):
    """A second, unrelated callback, so a token can name a real id that is not this one."""
    return make_running_callback()


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

    @pytest.mark.parametrize("state", ["success", "failed"])
    @pytest.mark.parametrize(("length", "expected_status"), [(2000, 204), (2001, 422)])
    def test_output_length_boundary(self, client, session, running_callback, state, length, expected_status):
        running_callback.output = "captured earlier"
        session.commit()
        output = "x" * length

        response = client.patch(
            f"/execution/callbacks/{running_callback.id}/state", json={"state": state, "output": output}
        )

        assert response.status_code == expected_status
        session.expire_all()
        session.refresh(running_callback)
        if expected_status == 204:
            assert running_callback.state == state
            assert running_callback.output == output
        else:
            assert running_callback.state == CallbackState.RUNNING
            assert running_callback.output == "captured earlier"

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
        [CallbackState.QUEUED, CallbackState.PENDING, CallbackState.FAILED],
    )
    def test_only_a_running_callback_can_be_reported(self, client, session, running_callback, state):
        """A row that is neither RUNNING nor already in the reported state is refused."""
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

    @pytest.mark.parametrize("state", [CallbackState.SUCCESS, CallbackState.FAILED])
    def test_repeat_report_of_the_same_state_is_accepted(self, client, session, running_callback, state):
        """A retry after a lost response must not look like a failure to the supervisor."""
        running_callback.state = state
        running_callback.output = "reported on the first attempt"
        session.commit()

        response = client.patch(
            f"/execution/callbacks/{running_callback.id}/state",
            json={"state": str(state), "output": "reported on the first attempt"},
        )

        assert response.status_code == 204
        session.expire_all()
        session.refresh(running_callback)
        assert running_callback.state == state
        assert running_callback.output == "reported on the first attempt"

    def test_repeat_report_replaces_output_written_by_the_scheduler(self, client, session, running_callback):
        """The scheduler can reach FAILED first with a placeholder; the real output still wins."""
        running_callback.state = CallbackState.FAILED
        running_callback.output = "Execution failed"
        session.commit()

        response = client.patch(
            f"/execution/callbacks/{running_callback.id}/state",
            json={"state": "failed", "output": "exited with code 3"},
        )

        assert response.status_code == 204
        session.expire_all()
        session.refresh(running_callback)
        assert running_callback.state == CallbackState.FAILED
        assert running_callback.output == "exited with code 3"

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
    callback = ExecutorCallback.from_dag_processor_request(request)
    callback.state = state
    session.add(callback)
    session.commit()
    return callback


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

    @pytest.mark.parametrize(
        "request_",
        [
            pytest.param(
                DagCallbackRequest(**REQUEST_FIELDS, dag_id="test_dag", run_id="test_run"), id="dag"
            ),
            pytest.param(TaskCallbackRequest(**REQUEST_FIELDS, ti=TI), id="task"),
            pytest.param(EmailRequest(**REQUEST_FIELDS, ti=TI, context_from_server=TI_CONTEXT), id="email"),
        ],
    )
    @pytest.mark.parametrize(
        ("replacement_dag_id", "replacement_run_id"),
        [
            pytest.param("other_dag", "test_run", id="another-dag"),
            pytest.param("test_dag", "another_run", id="another-run-of-the-same-dag"),
        ],
    )
    @time_machine.travel(NOW, tick=False)
    def test_reused_dag_run_primary_key_does_not_change_callback_identity_or_previous_run(
        self, client, session, request_, replacement_dag_id, replacement_run_id
    ):
        previous = DagRun(
            dag_id=replacement_dag_id,
            run_id="previous_run",
            logical_date=NOW - timedelta(days=1),
            start_date=NOW - timedelta(days=1),
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
        )
        original = DagRun(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=NOW,
            run_type=DagRunType.MANUAL,
            state=DagRunState.FAILED,
        )
        session.add_all([previous, original])
        session.flush()
        original_id = original.id
        callback = _add_routed_callback(session, request_, state=CallbackState.QUEUED)
        callback.data = {**callback.data, "dag_id": "test_dag", "dag_run_id": original_id}
        session.delete(original)
        session.commit()

        replacement = DagRun(
            dag_id=replacement_dag_id,
            run_id=replacement_run_id,
            logical_date=NOW,
            run_type=DagRunType.MANUAL,
            state=DagRunState.FAILED,
        )
        # Explicit reuse exercises SQLite's allocation behavior on every supported test database.
        replacement.id = original_id
        session.add(replacement)
        session.commit()

        response = client.patch(f"/execution/callbacks/{callback.id}/run")

        assert response.status_code == 204
        claims = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
        assert claims["dag_id"] == "test_dag"
        assert "run_id" not in claims

        response = client.get(f"/execution/callbacks/{callback.id}/previous-successful-dagrun")

        assert response.status_code == 200
        assert response.json() == {
            "data_interval_start": None,
            "data_interval_end": None,
            "start_date": None,
            "end_date": None,
        }


WORKER = {"hostname": "worker-1", "pid": 4242, "attempt_id": "01890a5d-ac70-7a5b-b7d5-0dd5b1c7be49"}


class TestRunCallbackRetriedExchange:
    def test_the_same_worker_can_repeat_its_exchange(self, client, session, queued_callback):
        """A worker can retry a lost exchange response without moving the callback's original start."""
        url = f"/execution/callbacks/{queued_callback.id}/run"

        with time_machine.travel(NOW, tick=False) as clock:
            first = client.patch(url, json=WORKER)
            clock.shift(timedelta(seconds=30))
            retried = client.patch(url, json=WORKER)

        assert first.status_code == retried.status_code == 204
        payload = jwt.decode(retried.headers["Refreshed-API-Token"], options={"verify_signature": False})
        assert payload["sub"] == str(queued_callback.id)
        assert payload["scope"] == "execution"
        session.expire_all()
        callback = session.get_one(Callback, queued_callback.id)
        assert callback.state == CallbackState.RUNNING
        assert callback.started_by == WORKER
        assert callback.started_at == NOW

    @pytest.mark.parametrize(
        "retry_body",
        [
            pytest.param(None, id="unidentified"),
            pytest.param({**WORKER, "hostname": "worker-2"}, id="other-host"),
            pytest.param(
                {**WORKER, "attempt_id": "01890a5d-ac70-7a5b-b7d5-0dd5b1c7be50"},
                id="redelivered-to-the-same-process",
            ),
        ],
    )
    def test_any_other_exchange_of_a_started_callback_is_refused(self, client, queued_callback, retry_body):
        """A second delivery of the workload must not run the callback again."""
        url = f"/execution/callbacks/{queued_callback.id}/run"
        client.patch(url, json=WORKER)

        response = client.patch(url, json=retry_body) if retry_body else client.patch(url)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "invalid_state"


@pytest.mark.parametrize(
    ("data", "expected_lifetime"),
    [
        pytest.param({EXECUTION_TIMEOUT_KEY: 3600}, 3600 + 1800, id="outlasts-the-report-window"),
        pytest.param({}, 600, id="default-without-timeout"),
    ],
)
@conf_vars({("execution_api", "jwt_expiration_time"): "600"})
def test_run_token_lasts_until_the_callback_must_report(
    client, session, queued_callback, data, expected_lifetime
):
    """A callback doesn't heartbeat, so nothing refreshes its token while it runs."""
    queued_callback.data = {**queued_callback.data, **data}
    session.commit()

    response = client.patch(f"/execution/callbacks/{queued_callback.id}/run")

    assert response.status_code == 204
    payload = jwt.decode(response.headers["Refreshed-API-Token"], options={"verify_signature": False})
    assert payload["exp"] - payload["iat"] == expected_lifetime


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


class TestGetPreviousSuccessfulDagRun:
    @pytest.fixture(autouse=True)
    def _clean_runs(self):
        clear_db_runs()
        yield
        clear_db_runs()

    def test_returns_the_last_successful_run_before_the_callbacks_own(self, client, session, dag_maker):
        with dag_maker("test_dag", session=session):
            EmptyOperator(task_id="test_task")
        for day, state in ((17, DagRunState.SUCCESS), (18, DagRunState.SUCCESS), (19, DagRunState.FAILED)):
            dag_run = dag_maker.create_dagrun(
                run_id=f"run_{day}",
                logical_date=timezone.datetime(2025, 1, day),
                run_type=DagRunType.SCHEDULED,
                state=state,
                start_date=timezone.datetime(2025, 1, day, 0, 30),
                session=session,
            )
            dag_run.end_date = timezone.datetime(2025, 1, day, 1)
        callback = _add_routed_callback(
            session, DagCallbackRequest(**REQUEST_FIELDS, dag_id="test_dag", run_id="run_19")
        )
        callback.data = {**callback.data, "dag_run_id": dag_run.id}
        session.commit()

        response = client.get(f"/execution/callbacks/{callback.id}/previous-successful-dagrun")

        assert response.status_code == 200
        assert response.json() == {
            "data_interval_start": "2025-01-18T00:00:00Z",
            "data_interval_end": "2025-01-19T00:00:00Z",
            "start_date": "2025-01-18T00:30:00Z",
            "end_date": "2025-01-18T01:00:00Z",
        }

    def test_a_callback_without_a_dag_run_has_no_previous_run(self, client, queued_callback):
        response = client.get(f"/execution/callbacks/{queued_callback.id}/previous-successful-dagrun")

        assert response.status_code == 200
        assert response.json() == {
            "data_interval_start": None,
            "data_interval_end": None,
            "start_date": None,
            "end_date": None,
        }

    def test_returns_404_for_nonexistent(self, client):
        response = client.get(
            "/execution/callbacks/00000000-0000-0000-0000-000000000000/previous-successful-dagrun"
        )

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"


@pytest.mark.usefixtures("_use_real_jwt_bearer")
@pytest.mark.parametrize(
    ("token_sub", "token_scope", "expected_status"),
    [
        pytest.param("self", "execution", 200, id="execution-token-matching-sub-accepted"),
        pytest.param("other", "execution", 403, id="execution-token-for-other-callback"),
        pytest.param("self", "callback", 403, id="unexchanged-callback-token-rejected"),
    ],
)
@pytest.mark.parametrize("path", ["context", "previous-successful-dagrun"])
def test_context_requires_the_exchanged_token(client, session, token_sub, token_scope, expected_status, path):
    """Only the execution token a callback exchanged for reads its context."""
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

    resp = client.get(f"/execution/callbacks/{callback.id}/{path}")
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
