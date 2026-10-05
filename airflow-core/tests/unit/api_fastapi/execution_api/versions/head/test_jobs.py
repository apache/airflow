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

from datetime import datetime, timedelta, timezone
from uuid import UUID

import jwt
import pytest
from fastapi import Request
from sqlalchemy import event, insert, select, update

from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.jobs.job import Job, JobState
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_jobs, clear_db_teams

pytestmark = pytest.mark.db_test

SESSION_ID = UUID("00000000-0000-0000-0000-0000000000aa")
OTHER_SESSION_ID = UUID("00000000-0000-0000-0000-0000000000bb")
REGISTRATION_ID = UUID("00000000-0000-0000-0000-00000000000a")
OTHER_REGISTRATION_ID = UUID("00000000-0000-0000-0000-00000000000b")
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
SESSION_EXPIRY = NOW + timedelta(hours=1)


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_jobs()
    clear_db_dag_bundles()
    clear_db_teams()
    yield
    clear_db_jobs()
    clear_db_dag_bundles()
    clear_db_teams()


@pytest.fixture(autouse=True)
def frozen_time(time_machine):
    time_machine.move_to(NOW, tick=False)


@pytest.fixture
def authenticate(exec_app):
    def _authenticate(**claims) -> None:
        async def _auth(request: Request) -> TIToken:
            return TIToken(id=SESSION_ID, claims=TIClaims(**claims))

        exec_app.dependency_overrides[require_auth] = _auth

    return _authenticate


@pytest.fixture
def as_session(authenticate):
    authenticate(
        scope="dag_processor_session",
        dag_bundles=frozenset({"bundle_a", "bundle_b"}),
        exp=SESSION_EXPIRY.timestamp(),
    )


def _create_job(
    session,
    *,
    session_id: UUID | None = SESSION_ID,
    registration_id: UUID = REGISTRATION_ID,
    state: JobState = JobState.RUNNING,
    latest_heartbeat: datetime = NOW,
    end_date: datetime | None = None,
) -> Job:
    job = Job(job_type="DagProcessorJob", state=state)
    job.session_id = session_id
    job.registration_id = registration_id
    job.hostname = "processor-1"
    job.unixname = None
    job.bundle_names = ["bundle_a", "bundle_b"]
    job.latest_heartbeat = latest_heartbeat
    job.end_date = end_date
    session.add(job)
    session.commit()
    return job


def _register(client, registration_id: UUID = REGISTRATION_ID, **body):
    return client.post(
        "/execution/jobs", json={"registration_id": str(registration_id), "hostname": "processor-1", **body}
    )


def _decode(token: str) -> dict:
    return jwt.decode(token, options={"verify_signature": False})


@pytest.mark.usefixtures("as_session")
class TestRegisterJob:
    def test_registers_a_running_job_for_every_granted_bundle(self, client, session):
        response = _register(client, unixname="airflow")

        assert response.status_code == 201, response.json()
        job = session.get(Job, response.json()["job_id"])
        assert job.job_type == "DagProcessorJob"
        assert job.state == JobState.RUNNING
        assert (job.start_date, job.latest_heartbeat) == (NOW, NOW)
        assert (job.hostname, job.unixname) == ("processor-1", "airflow")
        assert job.bundle_names == ["bundle_a", "bundle_b"]
        assert (job.session_id, job.registration_id) == (SESSION_ID, REGISTRATION_ID)

    def test_returns_a_token_for_the_job_that_never_outlives_the_session_token(self, client):
        response = _register(client, bundle_names=["bundle_b"])

        assert response.status_code == 201, response.json()
        claims = _decode(response.json()["token"])
        assert claims["scope"] == "dag_processor"
        assert claims["sub"] == str(SESSION_ID)
        assert claims["job_id"] == response.json()["job_id"]
        assert claims["dag_bundles"] == ["bundle_b"]
        assert claims["exp"] <= SESSION_EXPIRY.timestamp()

    def test_rejects_an_empty_bundle_selection(self, client, session):
        response = _register(client, bundle_names=[])

        assert response.status_code == 422
        assert session.scalars(select(Job)).all() == []

    def test_rejects_an_ungranted_bundle(self, client, session):
        response = _register(client, bundle_names=["bundle_a", "bundle_c"])

        assert response.status_code == 403
        assert response.json()["detail"]["reason"] == "bundle_not_granted"
        assert session.scalars(select(Job)).all() == []

    @conf_vars({("core", "multi_team"): "True"})
    def test_records_the_teams_of_its_bundles(self, client, session):
        team_bundle = DagBundleModel(name="bundle_a")
        team_bundle.teams.append(Team(name="team_a"))
        session.add_all([team_bundle, DagBundleModel(name="bundle_b")])
        session.commit()

        response = _register(client)

        assert response.status_code == 201, response.json()
        assert session.get(Job, response.json()["job_id"]).team_names == ["team_a"]

    def test_same_registration_returns_the_same_job_with_a_fresh_token(self, client, session):
        job = _create_job(session)

        response = _register(client)

        assert response.status_code == 201, response.json()
        assert response.json()["job_id"] == job.id
        assert _decode(response.json()["token"])["job_id"] == job.id
        assert session.scalars(select(Job.id)).all() == [job.id]

    @pytest.mark.parametrize(
        "retired_job",
        [
            pytest.param({"state": JobState.SUCCESS, "end_date": NOW}, id="completed"),
            pytest.param({"session_id": None, "latest_heartbeat": NOW - timedelta(days=1)}, id="replaced"),
        ],
    )
    def test_refuses_a_retired_registration(self, client, session, retired_job):
        retired = _create_job(session, **retired_job)

        response = _register(client)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "registration_retired"
        assert session.scalars(select(Job.id)).all() == [retired.id]

    def test_a_retry_that_loses_the_race_resumes_the_original_registration(self, client, session):
        engine = session.get_bind()
        originals = []

        def register_first_from_another_connection(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("INSERT INTO job ") and not originals:
                originals.append(statement)
                with engine.connect() as other:
                    other.execute(
                        insert(Job).values(
                            job_type="DagProcessorJob",
                            state=JobState.RUNNING,
                            start_date=NOW,
                            latest_heartbeat=NOW,
                            hostname="processor-1",
                            bundle_names=["bundle_a", "bundle_b"],
                            session_id=SESSION_ID,
                            registration_id=REGISTRATION_ID,
                        )
                    )
                    other.commit()

        event.listen(engine, "before_cursor_execute", register_first_from_another_connection)
        try:
            response = _register(client)
        finally:
            event.remove(engine, "before_cursor_execute", register_first_from_another_connection)

        assert response.status_code == 201, response.json()
        assert originals
        assert session.scalars(select(Job.id)).all() == [response.json()["job_id"]]

    def test_refuses_a_conflicting_retry(self, client, session):
        _create_job(session)

        response = _register(client, hostname="processor-2")

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "registration_conflict"

    def test_refuses_another_registration_while_the_job_is_alive(self, client, session):
        running = _create_job(session)

        response = _register(client, registration_id=OTHER_REGISTRATION_ID)

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "job_running"
        session.refresh(running)
        assert running.session_id == SESSION_ID

    @pytest.mark.parametrize(
        "previous_job",
        [
            pytest.param({"latest_heartbeat": NOW - timedelta(days=1)}, id="heartbeat-expired"),
            pytest.param({"state": JobState.SUCCESS, "end_date": NOW}, id="completed"),
        ],
    )
    def test_replaces_a_stopped_job(self, client, session, previous_job):
        previous = _create_job(session, **previous_job)

        response = _register(client, registration_id=OTHER_REGISTRATION_ID)

        assert response.status_code == 201, response.json()
        assert response.json()["job_id"] != previous.id
        session.refresh(previous)
        assert previous.session_id is None


class TestHeartbeatJob:
    def test_records_a_heartbeat(self, client, session, authenticate, time_machine):
        job = _create_job(session)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id)
        time_machine.move_to(NOW + timedelta(seconds=10), tick=False)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "running"}
        session.refresh(job)
        assert job.latest_heartbeat == NOW + timedelta(seconds=10)

    def test_returns_a_stop_request(self, client, session, authenticate):
        job = _create_job(session, state=JobState.RESTARTING)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "restarting"}

    def test_refuses_a_token_issued_for_another_job(self, client, session, authenticate):
        job = _create_job(session)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id + 1)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 404
        session.refresh(job)
        assert job.latest_heartbeat == NOW


class TestCompleteJob:
    def test_records_the_final_state(self, client, session, authenticate, time_machine):
        job = _create_job(session)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id)
        time_machine.move_to(NOW + timedelta(minutes=5), tick=False)

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "success"})

        assert response.status_code == 204, response.text
        session.refresh(job)
        assert (job.state, job.end_date) == (JobState.SUCCESS, NOW + timedelta(minutes=5))

    def test_replay_keeps_the_first_outcome(self, client, session, authenticate):
        job = _create_job(session, state=JobState.SUCCESS, end_date=NOW)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id)

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "failed"})

        assert response.status_code == 204, response.text
        session.refresh(job)
        assert (job.state, job.end_date) == (JobState.SUCCESS, NOW)

    def test_a_completion_accepted_meanwhile_is_not_overwritten(self, client, session, authenticate):
        job = _create_job(session)
        authenticate(scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id)
        engine = session.get_bind()
        competing = []

        def complete_from_another_connection(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("UPDATE job SET") and not competing:
                competing.append(statement)
                with engine.connect() as other:
                    other.execute(
                        update(Job).where(Job.id == job.id).values(state=JobState.SUCCESS, end_date=NOW)
                    )
                    other.commit()

        event.listen(engine, "before_cursor_execute", complete_from_another_connection)
        try:
            response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "failed"})
        finally:
            event.remove(engine, "before_cursor_execute", complete_from_another_connection)

        assert response.status_code == 204, response.text
        assert competing
        session.refresh(job)
        assert (job.state, job.end_date) == (JobState.SUCCESS, NOW)

    @pytest.mark.parametrize(
        ("job_session_id", "token_job_offset"),
        [
            pytest.param(OTHER_SESSION_ID, 0, id="another-sessions-job"),
            pytest.param(None, 0, id="replaced-job"),
            pytest.param(SESSION_ID, 1, id="token-for-another-job"),
        ],
    )
    def test_refuses_a_job_the_token_does_not_own(
        self, client, session, authenticate, job_session_id, token_job_offset
    ):
        job = _create_job(session, session_id=job_session_id)
        authenticate(
            scope="dag_processor", dag_bundles=frozenset({"bundle_a"}), job_id=job.id + token_job_offset
        )

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "success"})

        assert response.status_code == 404
        session.refresh(job)
        assert job.end_date is None
