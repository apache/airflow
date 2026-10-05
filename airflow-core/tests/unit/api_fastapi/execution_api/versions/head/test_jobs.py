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

import pytest
from fastapi import Request
from sqlalchemy import select

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
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


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
def as_dag_processor_session(exec_app):
    async def _auth(request: Request) -> TIToken:
        claims = TIClaims(scope="dag_processor", dag_bundles=frozenset({"bundle_a", "bundle_b"}))
        return TIToken(id=SESSION_ID, claims=claims)

    exec_app.dependency_overrides[require_auth] = _auth


def _create_job(
    session,
    *,
    session_id: UUID | None = SESSION_ID,
    state: JobState = JobState.RUNNING,
    latest_heartbeat: datetime = NOW,
    end_date: datetime | None = None,
) -> Job:
    job = Job(job_type="DagProcessorJob", state=state)
    job.session_id = session_id
    job.latest_heartbeat = latest_heartbeat
    job.end_date = end_date
    session.add(job)
    session.commit()
    return job


class TestRegisterJob:
    def test_registers_a_running_job_for_every_granted_bundle(self, client, session, time_machine):
        time_machine.move_to(NOW, tick=False)

        response = client.post("/execution/jobs", json={"hostname": "processor-1", "unixname": "airflow"})

        assert response.status_code == 201, response.json()
        job = session.get(Job, response.json()["job_id"])
        assert job.job_type == "DagProcessorJob"
        assert job.state == JobState.RUNNING
        assert (job.start_date, job.latest_heartbeat) == (NOW, NOW)
        assert (job.hostname, job.unixname) == ("processor-1", "airflow")
        assert job.bundle_names == ["bundle_a", "bundle_b"]
        assert job.session_id == SESSION_ID

    def test_registers_the_requested_bundles(self, client, session):
        response = client.post(
            "/execution/jobs", json={"hostname": "processor-1", "bundle_names": ["bundle_b"]}
        )

        assert response.status_code == 201, response.json()
        assert session.get(Job, response.json()["job_id"]).bundle_names == ["bundle_b"]

    def test_rejects_an_ungranted_bundle(self, client, session):
        response = client.post(
            "/execution/jobs", json={"hostname": "processor-1", "bundle_names": ["bundle_a", "bundle_c"]}
        )

        assert response.status_code == 403
        assert response.json()["detail"]["reason"] == "bundle_not_granted"
        assert session.scalars(select(Job)).all() == []

    @conf_vars({("core", "multi_team"): "True"})
    def test_records_the_teams_of_its_bundles(self, client, session):
        team_bundle = DagBundleModel(name="bundle_a")
        team_bundle.teams.append(Team(name="team_a"))
        session.add_all([team_bundle, DagBundleModel(name="bundle_b")])
        session.commit()

        response = client.post("/execution/jobs", json={"hostname": "processor-1"})

        assert response.status_code == 201, response.json()
        assert session.get(Job, response.json()["job_id"]).team_names == ["team_a"]

    def test_refuses_while_the_session_job_is_alive(self, client, session, time_machine):
        time_machine.move_to(NOW, tick=False)
        running = _create_job(session)

        response = client.post("/execution/jobs", json={"hostname": "processor-1"})

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
    def test_moves_the_session_from_a_stopped_job(self, client, session, time_machine, previous_job):
        time_machine.move_to(NOW, tick=False)
        previous = _create_job(session, **previous_job)

        response = client.post("/execution/jobs", json={"hostname": "processor-1"})

        assert response.status_code == 201, response.json()
        session.refresh(previous)
        assert previous.session_id is None
        assert session.get(Job, response.json()["job_id"]).session_id == SESSION_ID


class TestHeartbeatJob:
    def test_records_a_heartbeat(self, client, session, time_machine):
        job = _create_job(session)
        time_machine.move_to(NOW + timedelta(seconds=10), tick=False)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "running"}
        session.refresh(job)
        assert job.latest_heartbeat == NOW + timedelta(seconds=10)

    def test_returns_a_stop_request(self, client, session):
        job = _create_job(session, state=JobState.RESTARTING)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 200, response.json()
        assert response.json() == {"state": "restarting"}

    def test_refuses_another_sessions_job(self, client, session):
        job = _create_job(session, session_id=OTHER_SESSION_ID)

        response = client.post(f"/execution/jobs/{job.id}/heartbeat")

        assert response.status_code == 404
        session.refresh(job)
        assert job.latest_heartbeat == NOW


class TestCompleteJob:
    def test_records_the_final_state(self, client, session, time_machine):
        job = _create_job(session)
        time_machine.move_to(NOW + timedelta(minutes=5), tick=False)

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "success"})

        assert response.status_code == 204, response.text
        session.refresh(job)
        assert (job.state, job.end_date) == (JobState.SUCCESS, NOW + timedelta(minutes=5))

    def test_replay_keeps_the_first_outcome(self, client, session):
        job = _create_job(session, state=JobState.SUCCESS, end_date=NOW)

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "failed"})

        assert response.status_code == 204, response.text
        session.refresh(job)
        assert (job.state, job.end_date) == (JobState.SUCCESS, NOW)

    def test_refuses_another_sessions_job(self, client, session):
        job = _create_job(session, session_id=OTHER_SESSION_ID)

        response = client.post(f"/execution/jobs/{job.id}/complete", json={"state": "success"})

        assert response.status_code == 404
        session.refresh(job)
        assert job.end_date is None
