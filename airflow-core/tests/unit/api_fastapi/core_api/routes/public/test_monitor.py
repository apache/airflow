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

from datetime import timedelta
from unittest import mock

import pytest

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_jobs

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"

pytestmark = pytest.mark.db_test


class TestMonitorEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_jobs()

    def teardown_method(self):
        clear_db_jobs()


class TestGetHealth(TestMonitorEndpoint):
    @provide_session
    def test_healthy_scheduler_status(self, test_client, session):
        last_scheduler_heartbeat_for_testing_1 = timezone.utcnow()
        job = Job(state=State.RUNNING, latest_heartbeat=last_scheduler_heartbeat_for_testing_1)
        SchedulerJobRunner(job=job)
        session.add(job)
        session.commit()
        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert body["metadatabase"]["status"] == "healthy"
        assert body["scheduler"]["status"] == "healthy"
        assert (
            last_scheduler_heartbeat_for_testing_1.isoformat()
            == body["scheduler"]["latest_scheduler_heartbeat"]
        )

    @provide_session
    def test_unhealthy_scheduler_is_slow(self, test_client, session):
        last_scheduler_heartbeat_for_testing_2 = timezone.utcnow() - timedelta(minutes=1)
        job = Job(state=State.RUNNING, latest_heartbeat=last_scheduler_heartbeat_for_testing_2)
        SchedulerJobRunner(job=job)
        session.add(job)
        session.commit()
        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert body["metadatabase"]["status"] == "healthy"
        assert body["scheduler"]["status"] == "unhealthy"
        assert (
            last_scheduler_heartbeat_for_testing_2.isoformat()
            == body["scheduler"]["latest_scheduler_heartbeat"]
        )

    def test_unhealthy_scheduler_no_job(self, test_client):
        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert body["metadatabase"]["status"] == "healthy"
        assert body["scheduler"]["status"] == "unhealthy"
        assert body["scheduler"]["latest_scheduler_heartbeat"] is None

    @mock.patch.object(SchedulerJobRunner, "most_recent_job")
    def test_unhealthy_metadatabase_status(self, most_recent_job_mock, test_client):
        most_recent_job_mock.side_effect = Exception
        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert body["metadatabase"]["status"] == "unhealthy"
        assert body["scheduler"]["latest_scheduler_heartbeat"] is None

    @mock.patch("airflow.api_fastapi.core_api.routes.public.monitor.get_airflow_health")
    def test_health_with_dag_processor(self, mock_get_airflow_health, test_client):
        mock_get_airflow_health.return_value = {
            "metadatabase": {"status": HEALTHY},
            "scheduler": {
                "status": HEALTHY,
                "latest_scheduler_heartbeat": "2024-11-23T11:09:16.663124+00:00",
            },
            "triggerer": {
                "status": HEALTHY,
                "latest_triggerer_heartbeat": "2024-11-23T11:09:15.815483+00:00",
            },
            "dag_processor": {
                "status": HEALTHY,
                "latest_dag_processor_heartbeat": "2024-11-23T11:09:15.815483+00:00",
            },
        }

        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert "dag_processor" in body
        assert body["metadatabase"]["status"] == HEALTHY
        assert body["scheduler"]["status"] == HEALTHY
        assert body["triggerer"]["status"] == HEALTHY

    @mock.patch("airflow.api_fastapi.core_api.routes.public.monitor.get_airflow_health")
    def test_health_without_dag_processor(self, mock_get_airflow_health, test_client):
        mock_get_airflow_health.return_value = {
            "metadatabase": {"status": HEALTHY},
            "scheduler": {
                "status": HEALTHY,
                "latest_scheduler_heartbeat": "2024-11-23T11:09:16.663124+00:00",
            },
            "triggerer": {
                "status": HEALTHY,
                "latest_triggerer_heartbeat": "2024-11-23T11:09:15.815483+00:00",
            },
        }

        response = test_client.get("/monitor/health")

        assert response.status_code == 200
        body = response.json()

        assert "dag_processor" not in body
        assert body["metadatabase"]["status"] == HEALTHY
        assert body["scheduler"]["status"] == HEALTHY
        assert body["triggerer"]["status"] == HEALTHY
