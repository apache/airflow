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
    DetailedHealthStatus,
    HealthStatus,
    _configured_bundle_teams,
    _dag_processor_detailed_status,
    _triggerer_detailed_status,
    get_airflow_health,
    get_jobs_health,
)
from airflow.jobs.job import Job, JobState
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils.session import provide_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test

STALE_HEARTBEAT_AGE = timedelta(minutes=5)


def _patch_bundles(bundle_teams: dict[str, str | None]):
    """Patch the declared bundle/team partition that ``detailed_status`` is measured against."""
    return patch(
        "airflow.api.common.airflow_health._configured_bundle_teams",
        return_value=bundle_teams,
    )


def _mock_job(
    *,
    hostname: str,
    heartbeat: datetime,
    alive: bool,
    team_names: list[str] | None = None,
    bundle_names: list[str] | None = None,
) -> MagicMock:
    job = MagicMock(spec=Job)
    job.hostname = hostname
    job.latest_heartbeat = heartbeat
    job.is_alive = MagicMock(return_value=alive)
    job.team_names = team_names or []
    job.bundle_names = bundle_names
    return job


def _empty_component(heartbeat_field: str) -> dict:
    return {
        "status": HealthStatus.UNHEALTHY,
        heartbeat_field: None,
        "detailed_status": DetailedHealthStatus.DOWN,
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
    team_names: list[str] | None = None,
    bundle_names: list[str] | None = None,
) -> Job:
    job = Job(
        state=state,
        latest_heartbeat=heartbeat,
        hostname=hostname,
        end_date=end_date,
        team_names=team_names or [],
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
    team_names=["team-a"],
)
STALE_TRIGGERER_JOB_MOCK = _mock_job(
    hostname="triggerer-stale",
    heartbeat=datetime(2024, 1, 1),
    alive=False,
    team_names=["team-b"],
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
        "metadatabase": {"status": HealthStatus.HEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": _empty_component("latest_triggerer_heartbeat"),
        "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
    }


@patch("airflow.api.common.airflow_health.get_jobs_health", side_effect=Exception)
def test_get_airflow_health_metadatabase_unhealthy(mock_get_jobs_health):
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HealthStatus.UNHEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": _empty_component("latest_triggerer_heartbeat"),
        "dag_processor": _empty_component("latest_dag_processor_heartbeat"),
    }


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_one_alive_job(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[ALIVE_SCHEDULER_JOB_MOCK], [], []]
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HealthStatus.HEALTHY},
        "scheduler": {
            "status": HealthStatus.HEALTHY,
            "latest_scheduler_heartbeat": ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": DetailedHealthStatus.HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_SCHEDULER_JOB_MOCK.hostname,
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

    assert health_status["scheduler"]["status"] == HealthStatus.HEALTHY
    # Schedulers are symmetric, so a stale row alongside a live one is not a partial outage.
    assert health_status["scheduler"]["detailed_status"] == DetailedHealthStatus.HEALTHY
    # The rows the two dead replicas left behind name hosts that are gone, so they are not listed.
    assert health_status["scheduler"]["instances"] == [
        {
            "hostname": ALIVE_SCHEDULER_JOB_MOCK.hostname,
            "latest_scheduler_heartbeat": ALIVE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat(),
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

    assert health_status["scheduler"]["status"] == HealthStatus.UNHEALTHY
    assert health_status["scheduler"]["detailed_status"] == DetailedHealthStatus.DOWN
    assert health_status["scheduler"]["instances"] is None
    # A component with no live replica still reports when it was last heard from.
    assert (
        health_status["scheduler"]["latest_scheduler_heartbeat"]
        == STALE_SCHEDULER_JOB_MOCK.latest_heartbeat.isoformat()
    )


@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_mixed_triggerers_include_team_names(mock_get_jobs_health):
    mock_get_jobs_health.side_effect = [[], [ALIVE_TRIGGERER_JOB_MOCK, STALE_TRIGGERER_JOB_MOCK], []]
    health_status = get_airflow_health()

    assert health_status["triggerer"]["status"] == HealthStatus.HEALTHY
    # Multi-team is off here, so every live triggerer serves every trigger regardless of team.
    assert health_status["triggerer"]["detailed_status"] == DetailedHealthStatus.HEALTHY
    assert health_status["triggerer"]["instances"] == [
        {
            "hostname": ALIVE_TRIGGERER_JOB_MOCK.hostname,
            "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "team_names": ALIVE_TRIGGERER_JOB_MOCK.team_names,
        },
    ]
    assert (
        health_status["triggerer"]["latest_triggerer_heartbeat"]
        == ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat()
    )


@_patch_bundles({"bundle-a": None})
@patch("airflow.api.common.airflow_health.get_jobs_health")
def test_get_airflow_health_triggerer_and_dag_processor_healthy(mock_get_jobs_health, _mock_bundles):
    mock_get_jobs_health.side_effect = [[], [ALIVE_TRIGGERER_JOB_MOCK], [ALIVE_DAG_PROCESSOR_JOB_MOCK]]
    health_status = get_airflow_health()

    assert health_status == {
        "metadatabase": {"status": HealthStatus.HEALTHY},
        "scheduler": _empty_component("latest_scheduler_heartbeat"),
        "triggerer": {
            "status": HealthStatus.HEALTHY,
            "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": DetailedHealthStatus.HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_TRIGGERER_JOB_MOCK.hostname,
                    "latest_triggerer_heartbeat": ALIVE_TRIGGERER_JOB_MOCK.latest_heartbeat.isoformat(),
                    "team_names": ALIVE_TRIGGERER_JOB_MOCK.team_names,
                }
            ],
        },
        "dag_processor": {
            "status": HealthStatus.HEALTHY,
            "latest_dag_processor_heartbeat": ALIVE_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
            "detailed_status": DetailedHealthStatus.HEALTHY,
            "instances": [
                {
                    "hostname": ALIVE_DAG_PROCESSOR_JOB_MOCK.hostname,
                    "latest_dag_processor_heartbeat": ALIVE_DAG_PROCESSOR_JOB_MOCK.latest_heartbeat.isoformat(),
                    "bundle_names": ALIVE_DAG_PROCESSOR_JOB_MOCK.bundle_names,
                }
            ],
        },
    }


def _processor(*, alive: bool, bundle_names: list[str] | None) -> MagicMock:
    return _mock_job(
        hostname=f"dag-processor-{'alive' if alive else 'stale'}",
        heartbeat=datetime(2024, 2, 1),
        alive=alive,
        bundle_names=bundle_names,
    )


def _triggerer(*, alive: bool, team_name: str | None) -> MagicMock:
    return _mock_job(
        hostname=f"triggerer-{team_name}-{'alive' if alive else 'stale'}",
        heartbeat=datetime(2024, 2, 1),
        alive=alive,
        team_names=[team_name] if team_name else [],
    )


class TestDagProcessorDetailedStatus:
    """``detailed_status`` measures which configured bundles a live processor is parsing."""

    BUNDLES = {"bundle-a": None, "bundle-b": None}

    @pytest.mark.parametrize(
        ("jobs", "expected"),
        [
            pytest.param(
                [_processor(alive=True, bundle_names=["bundle-a", "bundle-b"])],
                DetailedHealthStatus.HEALTHY,
                id="one_processor_parses_every_bundle",
            ),
            pytest.param(
                [
                    _processor(alive=True, bundle_names=["bundle-a"]),
                    _processor(alive=True, bundle_names=["bundle-b"]),
                ],
                DetailedHealthStatus.HEALTHY,
                id="a_processor_per_bundle",
            ),
            pytest.param(
                [_processor(alive=True, bundle_names=None)],
                DetailedHealthStatus.HEALTHY,
                id="processor_without_bundle_name_parses_all",
            ),
            pytest.param(
                [_processor(alive=True, bundle_names=[])],
                DetailedHealthStatus.HEALTHY,
                id="empty_bundle_names_parses_all",
            ),
            pytest.param(
                [_processor(alive=True, bundle_names=["bundle-a"])],
                DetailedHealthStatus.DEGRADED,
                id="one_bundle_left_unparsed",
            ),
            pytest.param(
                [_processor(alive=False, bundle_names=["bundle-a", "bundle-b"])],
                DetailedHealthStatus.DOWN,
                id="only_processor_is_stale",
            ),
            pytest.param(
                [_processor(alive=True, bundle_names=["bundle-removed-from-config"])],
                DetailedHealthStatus.DOWN,
                id="processor_parses_nothing_configured",
            ),
            pytest.param([], DetailedHealthStatus.DOWN, id="no_processor_at_all"),
        ],
    )
    def test_bundle_coverage(self, jobs, expected):
        with _patch_bundles(self.BUNDLES):
            assert _dag_processor_detailed_status(jobs) == expected

    def test_restarted_processor_leaves_the_component_healthy(self):
        """A hard-killed replica keeps an unfinished job row; its replacement must still read healthy."""
        jobs = [
            _processor(alive=True, bundle_names=["bundle-a", "bundle-b"]),
            _processor(alive=False, bundle_names=["bundle-a", "bundle-b"]),
        ]

        with _patch_bundles(self.BUNDLES):
            assert _dag_processor_detailed_status(jobs) == DetailedHealthStatus.HEALTHY

    @pytest.mark.parametrize(
        ("jobs", "expected"),
        [
            pytest.param(
                [_processor(alive=True, bundle_names=None)],
                DetailedHealthStatus.HEALTHY,
                id="a_processor_is_alive",
            ),
            pytest.param(
                [_processor(alive=False, bundle_names=None)],
                DetailedHealthStatus.DOWN,
                id="no_processor_is_alive",
            ),
        ],
    )
    def test_falls_back_to_liveness_without_configured_bundles(self, jobs, expected):
        with _patch_bundles({}):
            assert _dag_processor_detailed_status(jobs) == expected


class TestTriggererDetailedStatus:
    """Under multi-team a triggerer only serves its own team, so every team scope needs one."""

    BUNDLES = {"bundle-a": "team-a", "bundle-b": "team-b", "bundle-shared": None}

    @pytest.mark.parametrize(
        ("jobs", "expected"),
        [
            pytest.param(
                [
                    _triggerer(alive=True, team_name="team-a"),
                    _triggerer(alive=True, team_name="team-b"),
                    _triggerer(alive=True, team_name=None),
                ],
                DetailedHealthStatus.HEALTHY,
                id="every_team_scope_covered",
            ),
            pytest.param(
                [
                    _triggerer(alive=True, team_name="team-a"),
                    _triggerer(alive=True, team_name=None),
                ],
                DetailedHealthStatus.DEGRADED,
                id="one_team_has_no_triggerer",
            ),
            pytest.param(
                [
                    _triggerer(alive=True, team_name="team-a"),
                    _triggerer(alive=True, team_name="team-b"),
                ],
                DetailedHealthStatus.DEGRADED,
                id="unscoped_bundles_have_no_triggerer",
            ),
            pytest.param(
                [_triggerer(alive=False, team_name="team-a")],
                DetailedHealthStatus.DOWN,
                id="no_triggerer_is_alive",
            ),
            pytest.param(
                [_triggerer(alive=True, team_name="team-of-a-removed-bundle")],
                DetailedHealthStatus.DOWN,
                id="triggerer_serves_no_configured_team",
            ),
            pytest.param([], DetailedHealthStatus.DOWN, id="no_triggerer_at_all"),
        ],
    )
    def test_team_coverage(self, jobs, expected):
        with conf_vars({("core", "multi_team"): "True"}), _patch_bundles(self.BUNDLES):
            assert _triggerer_detailed_status(jobs) == expected

    def test_restarted_triggerer_leaves_the_component_healthy(self):
        jobs = [
            _triggerer(alive=True, team_name="team-a"),
            _triggerer(alive=False, team_name="team-a"),
        ]

        with conf_vars({("core", "multi_team"): "True"}), _patch_bundles({"bundle-a": "team-a"}):
            assert _triggerer_detailed_status(jobs) == DetailedHealthStatus.HEALTHY

    @pytest.mark.parametrize(
        ("jobs", "expected"),
        [
            pytest.param(
                [_triggerer(alive=True, team_name="team-a"), _triggerer(alive=False, team_name="team-b")],
                DetailedHealthStatus.HEALTHY,
                id="a_triggerer_is_alive",
            ),
            pytest.param(
                [_triggerer(alive=False, team_name=None)],
                DetailedHealthStatus.DOWN,
                id="no_triggerer_is_alive",
            ),
        ],
    )
    def test_ignores_teams_without_multi_team(self, jobs, expected):
        with conf_vars({("core", "multi_team"): "False"}), _patch_bundles(self.BUNDLES):
            assert _triggerer_detailed_status(jobs) == expected

    def test_falls_back_to_liveness_without_configured_bundles(self):
        with conf_vars({("core", "multi_team"): "True"}), _patch_bundles({}):
            assert (
                _triggerer_detailed_status([_triggerer(alive=True, team_name="team-a")])
                == DetailedHealthStatus.HEALTHY
            )


@conf_vars({("dag_processor", "dag_bundle_config_list"): "not json"})
def test_configured_bundle_teams_survives_unreadable_config():
    assert _configured_bundle_teams() == {}


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
            "metadatabase": {"status": HealthStatus.HEALTHY},
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

        assert health_status["metadatabase"]["status"] == HealthStatus.HEALTHY
        assert health_status["scheduler"]["status"] == HealthStatus.HEALTHY
        assert health_status["scheduler"]["detailed_status"] == DetailedHealthStatus.HEALTHY
        assert health_status["scheduler"]["instances"] == [
            {
                "hostname": job.hostname,
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
        _create_job(session, SchedulerJobRunner, hostname="scheduler-stale", heartbeat=stale_heartbeat)
        _create_job(
            session, SchedulerJobRunner, hostname="scheduler-stale-2", heartbeat=older_stale_heartbeat
        )
        session.commit()

        health_status = get_airflow_health()

        assert health_status["scheduler"]["status"] == HealthStatus.HEALTHY
        assert health_status["scheduler"]["detailed_status"] == DetailedHealthStatus.HEALTHY
        assert health_status["scheduler"]["instances"] == [
            {
                "hostname": alive.hostname,
                "latest_scheduler_heartbeat": alive_heartbeat.isoformat(),
            },
        ]
        assert health_status["scheduler"]["latest_scheduler_heartbeat"] == alive_heartbeat.isoformat()

    @provide_session
    def test_get_airflow_health_all_stale_jobs(self, *, session):
        first = timezone.utcnow() - STALE_HEARTBEAT_AGE
        second = first - timedelta(minutes=1)
        third = second - timedelta(minutes=1)
        _create_job(session, SchedulerJobRunner, hostname="stale-1", heartbeat=first)
        _create_job(session, SchedulerJobRunner, hostname="stale-2", heartbeat=second)
        _create_job(session, SchedulerJobRunner, hostname="stale-3", heartbeat=third)
        session.commit()

        health_status = get_airflow_health()

        assert health_status["scheduler"]["status"] == HealthStatus.UNHEALTHY
        assert health_status["scheduler"]["detailed_status"] == DetailedHealthStatus.DOWN
        assert health_status["scheduler"]["instances"] is None
        assert health_status["scheduler"]["latest_scheduler_heartbeat"] == first.isoformat()

    @provide_session
    def test_get_airflow_health_mixed_triggerers_include_team_names(self, testing_team, *, session):
        alive_heartbeat = timezone.utcnow()
        stale_heartbeat = timezone.utcnow() - STALE_HEARTBEAT_AGE
        alive = _create_job(
            session,
            TriggererJobRunner,
            hostname="triggerer-alive",
            heartbeat=alive_heartbeat,
            team_names=[testing_team.name],
        )
        _create_job(
            session,
            TriggererJobRunner,
            hostname="triggerer-stale",
            heartbeat=stale_heartbeat,
            team_names=[testing_team.name],
        )
        session.commit()

        health_status = get_airflow_health()

        assert health_status["triggerer"]["status"] == HealthStatus.HEALTHY
        assert health_status["triggerer"]["detailed_status"] == DetailedHealthStatus.HEALTHY
        assert health_status["triggerer"]["instances"] == [
            {
                "hostname": alive.hostname,
                "latest_triggerer_heartbeat": alive_heartbeat.isoformat(),
                "team_names": [testing_team.name],
            },
        ]
        assert health_status["triggerer"]["latest_triggerer_heartbeat"] == alive_heartbeat.isoformat()
