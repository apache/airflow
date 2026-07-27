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

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from airflow._shared.timezones import timezone
from airflow.api.common.airflow_health import (
    DEGRADED,
    HEALTHY,
    UNHEALTHY,
    get_airflow_health,
    get_jobs_health,
)
from airflow.jobs.job import Job, JobState
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_only_metadatabase_healthy(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [], []]
    health_status = get_airflow_health()
    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": None,
            "detailed_status": UNHEALTHY,
            "instances": None,
        },
        "triggerer": {
            "status": None,
            "latest_triggerer_heartbeat": None,
            "detailed_status": None,
            "instances": None,
        },
        "dag_processor": {
            "status": None,
            "latest_dag_processor_heartbeat": None,
            "detailed_status": None,
            "instances": None,
        },
    }

    assert health_status == expected_status


@patch("airflow.api.common.airflow_health.get_jobs_health", side_effect=Exception)
def test_get_airflow_health_metadatabase_unhealthy(mock_get_jobs_health):
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": UNHEALTHY},
        "scheduler": {
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": None,
            "detailed_status": UNHEALTHY,
            "instances": None,
        },
        "triggerer": {
            "status": UNHEALTHY,
            "latest_triggerer_heartbeat": None,
            "detailed_status": UNHEALTHY,
            "instances": None,
        },
        "dag_processor": {
            "status": UNHEALTHY,
            "latest_dag_processor_heartbeat": None,
            "detailed_status": UNHEALTHY,
            "instances": None,
        },
    }

    assert health_status == expected_status


LATEST_SCHEDULER_JOB_MOCK = MagicMock(spec=Job)
LATEST_SCHEDULER_JOB_MOCK.hostname = "scheduler-host"
LATEST_SCHEDULER_JOB_MOCK.latest_heartbeat = datetime(2024, 1, 1)
LATEST_SCHEDULER_JOB_MOCK.is_alive = MagicMock(return_value=True)


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_scheduler_healthy_no_triggerer(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[LATEST_SCHEDULER_JOB_MOCK], [], []]
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": HEALTHY,
            "latest_scheduler_heartbeat": LATEST_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": LATEST_SCHEDULER_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_scheduler_heartbeat": LATEST_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
                }
            ],
        },
        "triggerer": {
            "status": None,
            "latest_triggerer_heartbeat": None,
            "detailed_status": None,
            "instances": None,
        },
        "dag_processor": {
            "status": None,
            "latest_dag_processor_heartbeat": None,
            "detailed_status": None,
            "instances": None,
        },
    }

    assert health_status == expected_status


LATEST_TRIGGERER_JOB_MOCK = MagicMock(spec=Job)
LATEST_TRIGGERER_JOB_MOCK.hostname = "triggerer-host"
LATEST_TRIGGERER_JOB_MOCK.team_name = "team-a"
LATEST_TRIGGERER_JOB_MOCK.latest_heartbeat = datetime(2024, 1, 2)
LATEST_TRIGGERER_JOB_MOCK.is_alive = MagicMock(return_value=True)

LATEST_DAG_PROCESSOR_JOB_MOCK = MagicMock(spec=Job)
LATEST_DAG_PROCESSOR_JOB_MOCK.hostname = "dag-processor-host"
LATEST_DAG_PROCESSOR_JOB_MOCK.bundle_names = ["bundle-a"]
LATEST_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat = datetime(2024, 1, 3)
LATEST_DAG_PROCESSOR_JOB_MOCK.is_alive = MagicMock(return_value=True)


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_triggerer_healthy_no_scheduler_job_record(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [LATEST_TRIGGERER_JOB_MOCK], [LATEST_DAG_PROCESSOR_JOB_MOCK]]
    health_status = get_airflow_health()

    expected_status = {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": None,
            "detailed_status": UNHEALTHY,
            "instances": None,
        },
        "triggerer": {
            "status": HEALTHY,
            "latest_triggerer_heartbeat": LATEST_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": LATEST_TRIGGERER_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_triggerer_heartbeat": LATEST_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
                    "team_name": LATEST_TRIGGERER_JOB_MOCK.team_name,
                }
            ],
        },
        "dag_processor": {
            "status": HEALTHY,
            "latest_dag_processor_heartbeat": LATEST_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": LATEST_DAG_PROCESSOR_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_dag_processor_heartbeat": LATEST_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
                    "bundle_names": LATEST_DAG_PROCESSOR_JOB_MOCK.bundle_names,
                }
            ],
        },
    }

    assert health_status == expected_status


ALIVE_SCHEDULER_JOB_MOCK = MagicMock(spec=Job)
ALIVE_SCHEDULER_JOB_MOCK.hostname = "scheduler-alive"
ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat = datetime(2024, 2, 1)
ALIVE_SCHEDULER_JOB_MOCK.is_alive = MagicMock(return_value=True)

DEAD_SCHEDULER_JOB_MOCK = MagicMock(spec=Job)
DEAD_SCHEDULER_JOB_MOCK.hostname = "scheduler-dead"
DEAD_SCHEDULER_JOB_MOCK.latest_heartbeat = datetime(2024, 1, 1)
DEAD_SCHEDULER_JOB_MOCK.is_alive = MagicMock(return_value=False)


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_degraded_when_some_instances_unhealthy(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [
        [ALIVE_SCHEDULER_JOB_MOCK, DEAD_SCHEDULER_JOB_MOCK],
        [],
        [],
    ]
    health_status = get_airflow_health()

    assert health_status["scheduler"]["status"] == HEALTHY
    assert health_status["scheduler"]["detailed_status"] == DEGRADED
    assert health_status["scheduler"]["instances"] == [
        {
            "hostname": ALIVE_SCHEDULER_JOB_MOCK.hostname,
            "status": HEALTHY,
            "latest_scheduler_heartbeat": ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
        {
            "hostname": DEAD_SCHEDULER_JOB_MOCK.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": DEAD_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
    ]
    assert (
        health_status["scheduler"]["latest_scheduler_heartbeat"]
        == ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat()
    )


@provide_session
def test_get_jobs_health_returns_running_jobs_ordered_by_heartbeat(session):
    clear_db_jobs()
    try:
        older = timezone.utcnow() - timedelta(minutes=5)
        newer = timezone.utcnow()
        older_job = Job(state=JobState.RUNNING, latest_heartbeat=older, hostname="older")
        newer_job = Job(state=JobState.RUNNING, latest_heartbeat=newer, hostname="newer")
        failed_job = Job(state=JobState.FAILED, latest_heartbeat=newer, hostname="failed")
        other_type_job = Job(state=JobState.RUNNING, latest_heartbeat=newer, hostname="triggerer")
        SchedulerJobRunner(job=older_job)
        SchedulerJobRunner(job=newer_job)
        SchedulerJobRunner(job=failed_job)
        TriggererJobRunner(job=other_type_job, capacity=1)
        session.add_all([older_job, newer_job, failed_job, other_type_job])
        session.commit()

        jobs = get_jobs_health(SchedulerJobRunner, session=session)

        assert [job.hostname for job in jobs] == ["newer", "older"]
    finally:
        clear_db_jobs()
