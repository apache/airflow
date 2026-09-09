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
    DOWN,
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

STALE_HEARTBEAT_AGE = timedelta(minutes=5)


def _mock_job(
    *,
    hostname: str,
    heartbeat: datetime,
    alive: bool,
    team_name: str | None = None,
    bundle_names: list[str] | None = None,
) -> MagicMock:
    job = MagicMock(spec=Job)
    job.hostname = hostname
    job.latest_heartbeat = heartbeat
    job.is_alive = MagicMock(return_value=alive)
    job.team_name = team_name
    job.bundle_names = bundle_names
    return job


def _empty_component(heartbeat_field: str) -> dict:
    return {
        "status": UNHEALTHY,
        heartbeat_field: None,
        "detailed_status": DOWN,
        "instances": None,
    }


def _create_job(
    session,
    runner_class,
    *,
    hostname: str,
    heartbeat: datetime,
    state: JobState = JobState.RUNNING,
    end_date: datetime | None = None,
    team_name: str | None = None,
    bundle_names: list[str] | None = None,
) -> Job:
    job = Job(
        state=state,
        latest_heartbeat=heartbeat,
        hostname=hostname,
        end_date=end_date,
        team_name=team_name,
        bundle_names=bundle_names,
    )
    if runner_class is TriggererJobRunner:
        runner_class(job=job, capacity=1)
    else:
        runner_class(job=job)
    session.add(job)
    return job


ALIVE_SCHEDULER_JOB_MOCK = _mock_job(hostname="scheduler-alive", heartbeat=datetime(2024, 2, 1), alive=True)
STALE_SCHEDULER_JOB_MOCK = _mock_job(hostname="scheduler-stale", heartbeat=datetime(2024, 1, 1), alive=False)
STALE_SCHEDULER_JOB_MOCK_2 = _mock_job(
    hostname="scheduler-stale-2", heartbeat=datetime(2023, 12, 1), alive=False
)
STALE_SCHEDULER_JOB_MOCK_3 = _mock_job(
    hostname="scheduler-stale-3", heartbeat=datetime(2023, 11, 1), alive=False
)

ALIVE_TRIGGERER_JOB_MOCK = _mock_job(
    hostname="triggerer-alive",
    heartbeat=datetime(2024, 2, 1),
    alive=True,
    team_name="team-a",
)
STALE_TRIGGERER_JOB_MOCK = _mock_job(
    hostname="triggerer-stale",
    heartbeat=datetime(2024, 1, 1),
    alive=False,
    team_name="team-b",
)

ALIVE_DAG_PROCESSOR_JOB_MOCK = _mock_job(
    hostname="dag-processor-host",
    heartbeat=datetime(2024, 1, 3),
    alive=True,
    bundle_names=["bundle-a"],
)


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_no_jobs(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [], []]
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": _empty_component("latest_triggerer_heartbeat"),
        "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
    }


@patch("airflow.api.common.airflow_health.get_jobs_health", side_effect=Exception)
def test_get_airflow_health_metadatabase_unhealthy(mock_get_jobs_health):
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": UNHEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": _empty_component("latest_triggerer_heartbeat"),
        "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
    }


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_one_alive_job(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[ALIVE_SCHEDULER_JOB_MOCK], [], []]
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HEALTHY},
        "scheduler": {
            "status": HEALTHY,
            "latest_scheduler_heartbeat": ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_SCHEDULER_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_scheduler_heartbeat": ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
                }
            ],
        },
        "triggerer": _empty_component("latest_triggerer_heartbeat"),
        "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
    }


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_mixed_alive_and_stale_jobs(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [
        [ALIVE_SCHEDULER_JOB_MOCK, STALE_SCHEDULER_JOB_MOCK, STALE_SCHEDULER_JOB_MOCK_2],
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
            "hostname": STALE_SCHEDULER_JOB_MOCK.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": STALE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
        {
            "hostname": STALE_SCHEDULER_JOB_MOCK_2.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": STALE_SCHEDULER_JOB_MOCK_2.latest_heartbeat.isoformat(),
        },
    ]
    assert (
        health_status["scheduler"]["latest_scheduler_heartbeat"]
        == ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat()
    )
    assert health_status["triggerer"] == _empty_component("latest_triggerer_heartbeat")
    assert health_status["dag_processor"] == _empty_component("latest_dag_processor_heartbeat")


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_all_stale_jobs(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [
        [STALE_SCHEDULER_JOB_MOCK, STALE_SCHEDULER_JOB_MOCK_2, STALE_SCHEDULER_JOB_MOCK_3],
        [],
        [],
    ]
    health_status = get_airflow_health()

    assert health_status["scheduler"]["status"] == UNHEALTHY
    assert health_status["scheduler"]["detailed_status"] == DOWN
    assert health_status["scheduler"]["instances"] == [
        {
            "hostname": STALE_SCHEDULER_JOB_MOCK.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": STALE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
        },
        {
            "hostname": STALE_SCHEDULER_JOB_MOCK_2.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": STALE_SCHEDULER_JOB_MOCK_2.latest_heartbeat.isoformat(),
        },
        {
            "hostname": STALE_SCHEDULER_JOB_MOCK_3.hostname,
            "status": UNHEALTHY,
            "latest_scheduler_heartbeat": STALE_SCHEDULER_JOB_MOCK_3.latest_heartbeat.isoformat(),
        },
    ]


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_mixed_triggerers_include_team_name(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [ALIVE_TRIGGERER_JOB_MOCK, STALE_TRIGGERER_JOB_MOCK], []]
    health_status = get_airflow_health()

    assert health_status["triggerer"]["status"] == HEALTHY
    assert health_status["triggerer"]["detailed_status"] == DEGRADED
    assert health_status["triggerer"]["instances"] == [
        {
            "hostname": ALIVE_TRIGGERER_JOB_MOCK.hostname,
            "status": HEALTHY,
            "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "team_name": ALIVE_TRIGGERER_JOB_MOCK.team_name,
        },
        {
            "hostname": STALE_TRIGGERER_JOB_MOCK.hostname,
            "status": UNHEALTHY,
            "latest_triggerer_heartbeat": STALE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "team_name": STALE_TRIGGERER_JOB_MOCK.team_name,
        },
    ]
    assert (
        health_status["triggerer"]["latest_triggerer_heartbeat"]
        == ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat()
    )


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_triggerer_and_dag_processor_healthy(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [ALIVE_TRIGGERER_JOB_MOCK], [ALIVE_DAG_PROCESSOR_JOB_MOCK]]
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": {
            "status": HEALTHY,
            "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_TRIGGERER_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
                    "team_name": ALIVE_TRIGGERER_JOB_MOCK.team_name,
                }
            ],
        },
        "dag_processor": {
            "status": HEALTHY,
            "latest_dag_processor_heartbeat": ALIVE_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_DAG_PROCESSOR_JOB_MOCK.hostname,
                    "status": HEALTHY,
                    "latest_dag_processor_heartbeat": ALIVE_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
                    "bundle_names": ALIVE_DAG_PROCESSOR_JOB_MOCK.bundle_names,
                }
            ],
        },
    }


class TestAirflowHealthFromDb:
    @pytest.fixture(autouse=True)
    def cleanup_jobs(self):
        clear_db_jobs()
        yield
        clear_db_jobs()

    @provide_session
    def test_get_jobs_health_returns_unfinished_jobs_ordered_by_heartbeat(self, *, session):
        older = timezone.utcnow() - timedelta(minutes=5)
        newer = timezone.utcnow()
        older_job = _create_job(session, SchedulerJobRunner, hostname="older", heartbeat=older)
        newer_job = _create_job(session, SchedulerJobRunner, hostname="newer", heartbeat=newer)
        _create_job(
            session,
            SchedulerJobRunner,
            hostname="ended",
            heartbeat=newer,
            end_date=newer,
        )
        unfinished_failed = _create_job(
            session,
            SchedulerJobRunner,
            hostname="failed-unfinished",
            heartbeat=older - timedelta(minutes=1),
            state=JobState.FAILED,
        )
        _create_job(session, TriggererJobRunner, hostname="triggerer", heartbeat=newer)
        session.commit()

        jobs = get_jobs_health(SchedulerJobRunner, session=session)

        assert [job.hostname for job in jobs] == [
            newer_job.hostname,
            older_job.hostname,
            unfinished_failed.hostname,
        ]

    def test_get_airflow_health_no_jobs(self):
        health_status = get_airflow_health()

        assert health_status == {
            "metadatabase": {"status": HEALTHY},
            "scheduler": _empty_component("latest_scheduler_heartbeat"),
            "triggerer": _empty_component("latest_triggerer_heartbeat"),
            "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
        }

    @provide_session
    def test_get_airflow_health_one_alive_job(self, *, session):
        heartbeat = timezone.utcnow()
        job = _create_job(session, SchedulerJobRunner, hostname="scheduler-alive", heartbeat=heartbeat)
        _create_job(
            session,
            SchedulerJobRunner,
            hostname="scheduler-ended",
            heartbeat=heartbeat,
            end_date=heartbeat,
        )
        session.commit()

        health_status = get_airflow_health()

        assert health_status["metadatabase"]["status"] == HEALTHY
        assert health_status["scheduler"]["status"] == HEALTHY
        assert health_status["scheduler"]["detailed_status"] == HEALTHY
        assert health_status["scheduler"]["instances"] == [
            {
                "hostname": job.hostname,
                "status": HEALTHY,
                "latest_scheduler_heartbeat": heartbeat.isoformat(),
            }
        ]
        assert health_status["scheduler"]["latest_scheduler_heartbeat"] == heartbeat.isoformat()
        assert health_status["triggerer"] == _empty_component("latest_triggerer_heartbeat")
        assert health_status["dag_processor"] == _empty_component("latest_dag_processor_heartbeat")

    @provide_session
    def test_get_airflow_health_mixed_alive_and_stale_jobs(self, *, session):
        alive_heartbeat = timezone.utcnow()
        stale_heartbeat = timezone.utcnow() - STALE_HEARTBEAT_AGE
        older_stale_heartbeat = stale_heartbeat - timedelta(minutes=1)
        alive = _create_job(
            session, SchedulerJobRunner, hostname="scheduler-alive", heartbeat=alive_heartbeat
        )
        stale = _create_job(
            session, SchedulerJobRunner, hostname="scheduler-stale", heartbeat=stale_heartbeat
        )
        older_stale = _create_job(
            session, SchedulerJobRunner, hostname="scheduler-stale-2", heartbeat=older_stale_heartbeat
        )
        session.commit()

        health_status = get_airflow_health()

        assert health_status["scheduler"]["status"] == HEALTHY
        assert health_status["scheduler"]["detailed_status"] == DEGRADED
        assert health_status["scheduler"]["instances"] == [
            {
                "hostname": alive.hostname,
                "status": HEALTHY,
                "latest_scheduler_heartbeat": alive_heartbeat.isoformat(),
            },
            {
                "hostname": stale.hostname,
                "status": UNHEALTHY,
                "latest_scheduler_heartbeat": stale_heartbeat.isoformat(),
            },
            {
                "hostname": older_stale.hostname,
                "status": UNHEALTHY,
                "latest_scheduler_heartbeat": older_stale_heartbeat.isoformat(),
            },
        ]
        assert health_status["scheduler"]["latest_scheduler_heartbeat"] == alive_heartbeat.isoformat()

    @provide_session
    def test_get_airflow_health_all_stale_jobs(self, *, session):
        first = timezone.utcnow() - STALE_HEARTBEAT_AGE
        second = first - timedelta(minutes=1)
        third = second - timedelta(minutes=1)
        jobs = [
            _create_job(session, SchedulerJobRunner, hostname="stale-1", heartbeat=first),
            _create_job(session, SchedulerJobRunner, hostname="stale-2", heartbeat=second),
            _create_job(session, SchedulerJobRunner, hostname="stale-3", heartbeat=third),
        ]
        session.commit()

        health_status = get_airflow_health()

        assert health_status["scheduler"]["status"] == UNHEALTHY
        assert health_status["scheduler"]["detailed_status"] == DOWN
        assert health_status["scheduler"]["instances"] == [
            {
                "hostname": job.hostname,
                "status": UNHEALTHY,
                "latest_scheduler_heartbeat": job.latest_heartbeat.isoformat(),
            }
            for job in jobs
        ]

    @provide_session
    def test_get_airflow_health_mixed_triggerers_include_team_name(self, testing_team, *, session):
        alive_heartbeat = timezone.utcnow()
        stale_heartbeat = timezone.utcnow() - STALE_HEARTBEAT_AGE
        alive = _create_job(
            session,
            TriggererJobRunner,
            hostname="triggerer-alive",
            heartbeat=alive_heartbeat,
            team_name=testing_team.name,
        )
        stale = _create_job(
            session,
            TriggererJobRunner,
            hostname="triggerer-stale",
            heartbeat=stale_heartbeat,
            team_name=testing_team.name,
        )
        session.commit()

        health_status = get_airflow_health()

        assert health_status["triggerer"]["status"] == HEALTHY
        assert health_status["triggerer"]["detailed_status"] == DEGRADED
        assert health_status["triggerer"]["instances"] == [
            {
                "hostname": alive.hostname,
                "status": HEALTHY,
                "latest_triggerer_heartbeat": alive_heartbeat.isoformat(),
                "team_name": testing_team.name,
            },
            {
                "hostname": stale.hostname,
                "status": UNHEALTHY,
                "latest_triggerer_heartbeat": stale_heartbeat.isoformat(),
                "team_name": testing_team.name,
            },
        ]
        assert health_status["triggerer"]["latest_triggerer_heartbeat"] == alive_heartbeat.isoformat()
