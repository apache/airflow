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

import json
import os
import socket
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
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
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import DagParseResultBody
from airflow.api_fastapi.execution_api.datamodels.job import DagParseTokenBody, JobState, TerminalJobState
from airflow.configuration import conf
from airflow.dag_processing.api_client import (
    DagParseContext,
    DagProcessorAPIClient,
    DagProcessorRegistrationRetired,
)
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.jobs.job import Job
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.team import Team
from airflow.models.variable import Variable
from airflow.sdk import Variable as SDKVariable
from airflow.sdk.api.client import Client

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_jobs,
    clear_db_teams,
    clear_db_variables,
)

pytestmark = pytest.mark.db_test

SECRET = "processor-client-test-secret-" * 3
AUDIENCE = "urn:airflow.apache.org:task"
SESSION_ID = "00000000-0000-0000-0000-0000000000aa"
HEARTBEAT_EXPIRED = datetime(2026, 10, 5, 11, 0, tzinfo=timezone.utc)


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
            request.url.path.endswith(("/jobs", "/complete", "/parse-token", "/parse-results"))
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
            with pytest.raises(httpx.HTTPStatusError) as publication_error:
                processor.post(f"jobs/{job_id}/parse-results", json={})
            assert publication_error.value.response.status_code == 403
        body = DagParseResultBody(
            attempt_id=uuid7(),
            dispatch_sequence=1,
            bundle_name="bundle-a",
            relative_fileloc="dag.py",
            parse_duration=0.1,
            serialized_dags=[],
            source_codes={},
        )
        with pytest.raises(httpx.ReadError):
            processor.publish_parse_result(body)
        processor.heartbeat()
        receipt = processor.publish_parse_result(body)
        assert receipt.attempt_id == body.attempt_id
        assert session.scalar(select(DagParseCheckpoint)).attempt_id == body.attempt_id
        assert processor.publish_parse_result(body) == receipt
        processor.complete_job(TerminalJobState.SUCCESS)
        assert session.get(Job, job_id).state == JobState.SUCCESS.value
        assert transport.lost == {
            "/execution/jobs",
            f"/execution/jobs/{job_id}/parse-token",
            f"/execution/jobs/{job_id}/complete",
            f"/execution/jobs/{job_id}/parse-results",
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


class SecretReadingLocalBundle(LocalDagBundle):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if SDKVariable.get("processor-client-key") != "from-api":
            raise ValueError("Bundle constructor did not resolve its Variable")


@pytest.mark.parametrize("multi_team", [False, True])
@pytest.mark.parametrize("database_access", [False, True])
def test_normal_processor_command_uses_authenticated_api(
    api_url,
    api_requests,
    provision_token,
    tmp_path,
    session,
    multi_team,
    database_access,
):
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_teams()
    team_name = "processor-team" if multi_team else None
    if team_name:
        session.add(Team(name=team_name))
        session.commit()
    dag_file = tmp_path / "api_dag.py"
    dag_file.write_text(
        "from airflow.sdk import DAG, Variable\n"
        'dag = DAG("authenticated_processor", schedule=None, description=Variable.get("processor-client-key"))\n'
    )
    bundle_config = [
        {
            "name": "bundle-a",
            "classpath": f"{__name__}.SecretReadingLocalBundle",
            "kwargs": {"path": str(tmp_path)},
            **({"team_name": team_name} if team_name else {}),
        }
    ]
    config = {
        ("dag_processor", "execution_api_token_file"): str(provision_token()),
        ("core", "execution_api_server_url"): api_url,
        ("core", "load_examples"): "False",
        ("core", "multi_team"): str(multi_team),
        ("dag_processor", "dag_bundle_config_list"): json.dumps(bundle_config),
        ("scheduler", "job_heartbeat_sec"): "0",
        ("database", "sql_alchemy_conn"): conf.get("database", "sql_alchemy_conn"),
    }
    try:
        with conf_vars(config):
            Variable.set("processor-client-key", "from-api", team_name=team_name)
            DagBundlesManager().sync_bundles_to_db(include_bundle_urls=False)
            processor_config = dict(config)
            if not database_access:
                processor_config.update(
                    {
                        (
                            "database",
                            "sql_alchemy_conn",
                        ): "postgresql+psycopg://blocked:blocked@127.0.0.1:1/blocked",
                        ("core", "fernet_key"): "",
                        ("api_auth", "jwt_secret"): "",
                        ("api_auth", "jwt_private_key_path"): "",
                    }
                )
            result = subprocess.run(
                ["airflow", "dag-processor", "--num-runs", "1", "--bundle-name", "bundle-a"],
                env={
                    **os.environ,
                    **{
                        f"AIRFLOW__{section.upper()}__{key.upper()}": value
                        for (section, key), value in processor_config.items()
                    },
                    "PYTHONPATH": f"{Path(__file__).parents[3]}:{os.environ.get('PYTHONPATH', '')}",
                },
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
            assert result.returncode == 0, result.stdout + result.stderr

        session.expire_all()
        job = session.scalars(select(Job)).one()
        assert job.state == JobState.SUCCESS
        assert job.end_date is not None
        assert job.team_names == ([team_name] if team_name else [])
        assert session.get(DagModel, "authenticated_processor").description == "from-api"
        assert (
            session.scalar(
                select(SerializedDagModel).where(SerializedDagModel.dag_id == "authenticated_processor")
            )
            is not None
        )
        requests = {(path, claims.get("scope")) for path, claims in api_requests}
        assert ("/execution/jobs", "dag_processor_session") in requests
        assert (f"/execution/jobs/{job.id}/heartbeat", "dag_processor") in requests
        assert (f"/execution/jobs/{job.id}/complete", "dag_processor") in requests
        assert (f"/execution/jobs/{job.id}/parse-results", "dag_processor") in requests
        assert ("/execution/variables/processor-client-key", "dag_processor") in requests
        assert ("/execution/variables/processor-client-key", "dag_parse") in requests
        parsing = [claims for _, claims in api_requests if claims.get("scope") == "dag_parse"]
        assert all(claims["relative_fileloc"] == "api_dag.py" for claims in parsing)
    finally:
        clear_db_jobs()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_variables()
        clear_db_teams()
