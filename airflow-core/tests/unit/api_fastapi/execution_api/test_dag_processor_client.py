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

import socket
import threading
from datetime import UTC, datetime
from unittest import mock

import httpx
import jwt
import pytest
import uvicorn
from sqlalchemy import select, update
from tenacity import wait_none
from uuid6 import uuid7

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.auth.tokens import JWTGenerator, JWTValidator
from airflow.api_fastapi.execution_api.app import lifespan
from airflow.api_fastapi.execution_api.datamodels.job import DagParseTokenBody, JobState, TerminalJobState
from airflow.dag_processing.api_client import (
    DagParseContext,
    DagProcessorAPIClient,
    DagProcessorRegistrationRetired,
)
from airflow.jobs.job import Job
from airflow.models.dagbundle import DagBundleModel
from airflow.models.variable import Variable
from airflow.sdk.api.client import Client

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_jobs,
    clear_db_variables,
)

pytestmark = pytest.mark.db_test

SECRET = "processor-client-test-secret-" * 3
AUDIENCE = "urn:airflow.apache.org:task"
SESSION_ID = "00000000-0000-0000-0000-0000000000aa"
HEARTBEAT_EXPIRED = datetime(2026, 10, 5, 11, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_jobs()
    clear_db_variables()
    yield
    clear_db_jobs()
    clear_db_variables()


@pytest.fixture(autouse=True)
def freeze_time(time_machine):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)


@pytest.fixture
def api_requests():
    return []


@pytest.fixture
def api_bind_host():
    return "127.0.0.1"


@pytest.fixture
def api_secret():
    return SECRET


@pytest.fixture
def api_url(async_db_engine, api_requests, api_bind_host, api_secret):
    ready = threading.Event()
    lifespan.registry.register_value(JWTValidator, JWTValidator(secret_key=api_secret, audience=AUDIENCE))

    class Server(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets=sockets)
            ready.set()

        async def shutdown(self, sockets=None):
            try:
                await super().shutdown(sockets=sockets)
            finally:
                await async_db_engine.dispose()

    with (
        conf_vars(
            {
                ("api_auth", "jwt_secret"): api_secret,
                ("execution_api", "jwt_audience"): AUDIENCE,
                ("execution_api", "jwt_expiration_time"): "300",
            }
        ),
        socket.socket() as listener,
    ):
        listener.bind((api_bind_host, 0))
        app = create_app(apps="execution")

        @app.middleware("http")
        async def record_request(request, call_next):
            token = request.headers.get("Authorization", "").removeprefix("Bearer ")
            claims = jwt.decode(token, options={"verify_signature": False}) if token else {}
            api_requests.append((request.url.path, claims))
            return await call_next(request)

        server = Server(uvicorn.Config(app, log_config=None, access_log=False))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            assert ready.wait(timeout=15), "Execution API did not start"
            assert server.started
            yield f"http://127.0.0.1:{listener.getsockname()[1]}/execution/"
        finally:
            server.should_exit = True
            thread.join(timeout=15)
            assert not thread.is_alive(), "Execution API did not stop"


@pytest.fixture
def provision_token(tmp_path, api_secret):
    token_file = tmp_path / "processor.jwt"

    def provision(*, session_id=SESSION_ID, valid_for=600):
        token = JWTGenerator(secret_key=api_secret, audience=AUDIENCE, valid_for=valid_for).generate(
            {"sub": session_id, "scope": "dag_processor_session", "dag_bundles": ["bundle-a"]}
        )
        pending = token_file.with_suffix(".tmp")
        pending.write_text(token)
        pending.replace(token_file)
        return token_file

    return provision


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
def test_rotation_stop_and_completion_over_http(clock, api_url, provision_token, session, time_machine):
    Variable.set("processor-client-key", "value")
    with DagProcessorAPIClient(
        base_url=api_url, token_file=provision_token(), hostname="processor-1", bundle_names=["bundle-a"]
    ) as processor:
        processor.headers["Airflow-API-Version"] = "2026-06-30"
        job_id = processor.register_job()
        job = session.get(Job, job_id)
        assert str(job.registration_id) == str(processor.registration_id)
        assert processor.heartbeat() == JobState.RUNNING
        token = processor.auth.token
        with processor.use_bundle("bundle-a"):
            assert processor.variables.get("processor-client-key").value == "value"
        context = DagParseContext(
            request=DagParseTokenBody(attempt_id=uuid7(), bundle_name="bundle-a", relative_fileloc="dag.py")
        )
        with processor.use_parse(context):
            assert processor.variables.set("processor-client-key", "updated").ok
            assert processor.variables.get("processor-client-key").value == "updated"

        time_machine.shift(60)
        provision_token()
        clock.return_value = 60
        assert processor.heartbeat() == JobState.RUNNING
        assert processor.auth.token != token
        assert processor.job_id == job_id
        assert session.scalars(select(Job.id)).all() == [job_id]

        session.execute(update(Job).where(Job.id == job_id).values(state=JobState.RESTARTING.value))
        session.commit()
        assert processor.heartbeat() == JobState.RESTARTING
        processor.complete_job(TerminalJobState.SUCCESS)
        session.refresh(job)
        assert job.state == JobState.SUCCESS.value
        assert job.end_date is not None


class LostAcknowledgmentTransport(httpx.BaseTransport):
    def __init__(self):
        self.transport = httpx.HTTPTransport()
        self.lost = set()

    def handle_request(self, request):
        response = self.transport.handle_request(request)
        if (
            request.url.path.endswith(("/jobs", "/complete", "/parse-token"))
            and request.url.path not in self.lost
            and response.is_success
        ):
            self.lost.add(request.url.path)
            response.read()
            response.close()
            raise httpx.ReadError("Lost acknowledgment after commit", request=request)
        return response

    def close(self):
        self.transport.close()


def test_lost_registration_and_completion_responses_over_http(api_url, provision_token, session, monkeypatch):
    monkeypatch.setattr(Client._request_with_retry.retry, "wait", wait_none())
    transport = LostAcknowledgmentTransport()
    session.merge(DagBundleModel(name="bundle-a"))
    session.commit()
    with DagProcessorAPIClient(
        base_url=api_url, token_file=provision_token(), hostname="processor-1", transport=transport
    ) as processor:
        job_id = processor.register_job()
        assert session.scalars(select(Job.id)).all() == [job_id]
        Variable.set("parse-token-key", "value")
        context = DagParseContext(
            request=DagParseTokenBody(attempt_id=uuid7(), bundle_name="bundle-a", relative_fileloc="dag.py")
        )
        with processor.use_parse(context):
            assert processor.variables.get("parse-token-key").value == "value"
            with pytest.raises(httpx.HTTPStatusError) as error:
                processor.post(f"jobs/{job_id}/heartbeat")
            assert error.value.response.status_code == 403
        processor.complete_job(TerminalJobState.SUCCESS)
        assert session.get(Job, job_id).state == JobState.SUCCESS.value
        assert transport.lost == {
            "/execution/jobs",
            f"/execution/jobs/{job_id}/parse-token",
            f"/execution/jobs/{job_id}/complete",
        }


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
def test_expired_credentials_fail_until_provisioning_resumes(clock, api_url, provision_token, time_machine):
    with DagProcessorAPIClient(
        base_url=api_url, token_file=provision_token(), hostname="processor-1"
    ) as processor:
        job_id = processor.register_job()
        time_machine.shift(700)
        clock.return_value = 700

        with pytest.raises(httpx.HTTPStatusError) as error:
            processor.heartbeat()
        assert error.value.response.status_code == 403

        provision_token()
        assert processor.heartbeat() == JobState.RUNNING
        assert processor.job_id == job_id
        processor.complete_job(TerminalJobState.SUCCESS)


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
def test_changed_session_signals_restart_but_allows_drain_and_completion(
    clock, api_url, provision_token, session
):
    Variable.set("processor-client-key", "value")
    with DagProcessorAPIClient(
        base_url=api_url, token_file=provision_token(), hostname="processor-1"
    ) as processor:
        job_id = processor.register_job()
        provision_token(session_id="00000000-0000-0000-0000-0000000000bb")
        clock.return_value = 30

        assert processor.heartbeat() == JobState.RUNNING
        assert processor.restart_required
        assert processor.job_id == job_id
        assert session.scalars(select(Job.id)).all() == [job_id]
        with processor.use_bundle("bundle-a"):
            assert processor.variables.get("processor-client-key").value == "value"
        processor.complete_job(TerminalJobState.SUCCESS)
        assert session.get(Job, job_id).state == JobState.SUCCESS.value


class CompletionFailureTransport(httpx.HTTPTransport):
    def __init__(self, *, commit):
        super().__init__()
        self.commit = commit
        self.fail = True

    def handle_request(self, request):
        if self.fail and request.url.path.endswith("/complete"):
            if self.commit:
                response = super().handle_request(request)
                response.read()
                response.close()
            raise httpx.ReadError("Completion acknowledgment unavailable", request=request)
        return super().handle_request(request)


@pytest.mark.parametrize("outcome", ["unaccepted", "accepted", "replaced"])
@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
def test_completion_recovery_after_token_expiry(
    clock, api_url, provision_token, session, time_machine, monkeypatch, outcome
):
    monkeypatch.setattr(Client._request_with_retry.retry, "wait", wait_none())
    token_file = provision_token()
    transport = CompletionFailureTransport(commit=outcome != "unaccepted")
    with DagProcessorAPIClient(
        base_url=api_url, token_file=token_file, hostname="processor-1", transport=transport
    ) as processor:
        job_id = processor.register_job()
        with pytest.raises(httpx.ReadError):
            processor.complete_job(TerminalJobState.SUCCESS)
        time_machine.shift(350)
        clock.return_value = 350
        transport.fail = False
        if outcome == "replaced":
            with DagProcessorAPIClient(
                base_url=api_url, token_file=token_file, hostname="processor-2"
            ) as replacement:
                replacement_id = replacement.register_job()
        if outcome == "unaccepted":
            processor.complete_job(TerminalJobState.SUCCESS)
            assert not processor.restart_required
        else:
            with pytest.raises(DagProcessorRegistrationRetired):
                processor.complete_job(TerminalJobState.SUCCESS)
            assert processor.restart_required

        assert session.get(Job, job_id).state == JobState.SUCCESS.value
        if outcome == "replaced":
            assert session.get(Job, replacement_id).state == JobState.RUNNING.value
        else:
            assert session.scalars(select(Job.id)).all() == [job_id]


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
def test_replaced_job_signals_restart(clock, api_url, provision_token, session):
    token_file = provision_token()
    with (
        DagProcessorAPIClient(base_url=api_url, token_file=token_file, hostname="processor-1") as replaced,
        DagProcessorAPIClient(base_url=api_url, token_file=token_file, hostname="processor-2") as replacement,
    ):
        job_id = replaced.register_job()
        session.execute(update(Job).where(Job.id == job_id).values(latest_heartbeat=HEARTBEAT_EXPIRED))
        session.commit()
        replacement.register_job()

        with pytest.raises(DagProcessorRegistrationRetired):
            replaced.heartbeat()

        assert replaced.restart_required
        assert replacement.heartbeat() == JobState.RUNNING
