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

import pytest

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job, JobState

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


class TestRegisterJob:
    def setup_method(self):
        clear_db_jobs()

    def teardown_method(self):
        clear_db_jobs()

    def test_register_creates_running_job_and_returns_id(self, client, session):
        response = client.post(
            "/execution/jobs",
            json={"job_type": "TriggererJob", "hostname": "triggerer-host"},
        )

        assert response.status_code == 201
        job_id = response.json()["job_id"]
        assert isinstance(job_id, int)

        session.expire_all()
        job = session.get(Job, job_id)
        assert job is not None
        assert job.job_type == "TriggererJob"
        assert job.state == JobState.RUNNING
        # Job.__init__ stamps these so the row counts as alive immediately.
        assert job.latest_heartbeat is not None
        assert job.start_date is not None
        # The registering process's hostname is recorded, not the api-server's.
        assert job.hostname == "triggerer-host"


class TestHeartbeatJob:
    def setup_method(self):
        clear_db_jobs()

    def teardown_method(self):
        clear_db_jobs()

    def test_heartbeat_updates_latest_heartbeat(self, client, session):
        stale = timezone.datetime(2020, 1, 1)
        job = Job(job_type="TriggererJob", state=JobState.RUNNING)
        job.latest_heartbeat = stale
        session.add(job)
        session.commit()
        job_id = job.id

        response = client.post(f"/execution/jobs/{job_id}/heartbeat")

        assert response.status_code == 204

        session.expire_all()
        refreshed = session.get(Job, job_id)
        assert refreshed.latest_heartbeat > stale
        assert refreshed.end_date is None

    def test_heartbeat_unknown_job_returns_404(self, client):
        response = client.post("/execution/jobs/987654/heartbeat")
        assert response.status_code == 404


class TestCompleteJob:
    def setup_method(self):
        clear_db_jobs()

    def teardown_method(self):
        clear_db_jobs()

    def test_complete_stamps_end_date(self, client, session):
        job = Job(job_type="TriggererJob", state=JobState.RUNNING)
        session.add(job)
        session.commit()
        job_id = job.id
        assert job.end_date is None

        response = client.post(f"/execution/jobs/{job_id}/complete")

        assert response.status_code == 204

        session.expire_all()
        refreshed = session.get(Job, job_id)
        assert refreshed.end_date is not None

    def test_complete_unknown_job_returns_404(self, client):
        response = client.post("/execution/jobs/987654/complete")
        assert response.status_code == 404
