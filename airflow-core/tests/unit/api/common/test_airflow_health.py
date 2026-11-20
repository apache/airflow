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
import time_machine

from airflow._shared.timezones import timezone
from airflow.api.common.airflow_health import (
    DetailedHealthStatus,
    HealthStatus,
    _configured_bundle_teams,
    _configured_executor_keys,
    _configured_executor_names,
    _dag_processor_detailed_status,
    _executor_detailed_status,
    _executor_instance_health,
    _triggerer_detailed_status,
    get_airflow_health,
    get_jobs_health,
)
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_utils import ExecutorName
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import EXECUTOR_JOB_TYPE, HealthCheckResult, Job, JobState, health_check_threshold
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
    heartbeat: datetime | None,
    state: JobState = JobState.RUNNING,
    end_date: datetime | None = None,
    team_names: list[str] | None = None,
    bundle_names: list[str] | None = None,
) -> Job:
    job = Job(
        job_type=runner_class.job_type,
        state=state,
        latest_heartbeat=heartbeat,
        hostname=hostname,
        end_date=end_date,
        team_names=team_names or [],
        bundle_names=bundle_names,
    )
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
    @_patch_bundles(BUNDLES)
    def test_bundle_coverage(self, mock_bundles, jobs, expected):
        assert _dag_processor_detailed_status(jobs) == expected

    @_patch_bundles(BUNDLES)
    def test_restarted_processor_leaves_the_component_healthy(self, mock_bundles):
        """A hard-killed replica keeps an unfinished job row; its replacement must still read healthy."""
        jobs = [
            _processor(alive=True, bundle_names=["bundle-a", "bundle-b"]),
            _processor(alive=False, bundle_names=["bundle-a", "bundle-b"]),
        ]

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
    @_patch_bundles({})
    def test_falls_back_to_liveness_without_configured_bundles(self, mock_bundles, jobs, expected):
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
    @conf_vars({("core", "multi_team"): "True"})
    @_patch_bundles(BUNDLES)
    def test_team_coverage(self, mock_bundles, jobs, expected):
        assert _triggerer_detailed_status(jobs) == expected

    @conf_vars({("core", "multi_team"): "True"})
    @_patch_bundles({"bundle-a": "team-a"})
    def test_restarted_triggerer_leaves_the_component_healthy(self, mock_bundles):
        jobs = [
            _triggerer(alive=True, team_name="team-a"),
            _triggerer(alive=False, team_name="team-a"),
        ]

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
    @conf_vars({("core", "multi_team"): "False"})
    @_patch_bundles(BUNDLES)
    def test_ignores_teams_without_multi_team(self, mock_bundles, jobs, expected):
        assert _triggerer_detailed_status(jobs) == expected

    @conf_vars({("core", "multi_team"): "True"})
    @_patch_bundles({})
    def test_falls_back_to_liveness_without_configured_bundles(self, mock_bundles):
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
        health_check_threshold.cache_clear()
        yield
        clear_db_jobs()
        health_check_threshold.cache_clear()

    @pytest.mark.parametrize(
        ("runner_class", "threshold"),
        [(SchedulerJobRunner, 17), (TriggererJobRunner, 41.5), (DagProcessorJobRunner, 73)],
    )
    @pytest.mark.parametrize("latest_failed", [False, True])
    @time_machine.travel("2026-11-09T09:00:00Z", tick=False)
    @conf_vars(
        {
            ("scheduler", "scheduler_health_check_threshold"): "17",
            ("triggerer", "triggerer_health_check_threshold"): "41.5",
            ("dag_processor", "health_check_threshold"): "73",
        }
    )
    @provide_session
    def test_get_jobs_health_returns_live_jobs_and_latest_unfinished(
        self, runner_class, threshold, latest_failed, *, session
    ):
        now = timezone.utcnow()
        cutoff = now - timedelta(seconds=threshold)
        _create_job(session, runner_class, hostname="live-newer", heartbeat=now - timedelta(seconds=1))
        _create_job(
            session, runner_class, hostname="live-older", heartbeat=cutoff + timedelta(microseconds=1)
        )
        _create_job(session, runner_class, hostname="at-threshold", heartbeat=cutoff)
        _create_job(session, runner_class, hostname="stale", heartbeat=cutoff - timedelta(microseconds=1))
        _create_job(
            session,
            runner_class,
            hostname="ended",
            heartbeat=now + timedelta(seconds=1),
            end_date=now,
        )
        _create_job(
            session,
            runner_class,
            hostname="failed-unfinished",
            heartbeat=now if latest_failed else now - timedelta(seconds=2),
            state=JobState.FAILED,
        )
        other_runner = SchedulerJobRunner if runner_class is TriggererJobRunner else TriggererJobRunner
        _create_job(session, other_runner, hostname="other-component", heartbeat=now)
        session.commit()

        jobs = get_jobs_health(runner_class.job_type, session=session)

        expected = ["live-newer", "live-older"]
        if latest_failed:
            expected.insert(0, "failed-unfinished")
        assert [job.hostname for job in jobs] == expected

    @provide_session
    def test_get_jobs_health_keeps_one_job_with_null_heartbeat(self, *, session):
        _create_job(session, SchedulerJobRunner, hostname="no-heartbeat-1", heartbeat=None)
        _create_job(session, SchedulerJobRunner, hostname="no-heartbeat-2", heartbeat=None)
        session.commit()

        jobs = get_jobs_health(SchedulerJobRunner.job_type, session=session)

        assert len(jobs) == 1
        assert jobs[0].latest_heartbeat is None

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

        jobs = get_jobs_health(SchedulerJobRunner.job_type, session=session)
        assert [job.hostname for job in jobs] == ["stale-1"]

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


# -- Executor health: no configured executor overrides get_health() by default, so these tests patch
# -- _configured_executor_keys directly rather than relying on a real executor class, exactly as the
# -- bundle/team partition tests above patch _configured_bundle_teams.


def _executor(
    *,
    alive: bool,
    healthy: bool | None = True,
    team_name: str | None = None,
    executor: str | None = None,
    owner_job_id: int = 1,
) -> MagicMock:
    job = _mock_job(
        hostname=f"host-{owner_job_id}",
        heartbeat=datetime(2024, 2, 1),
        alive=alive,
        team_names=[team_name] if team_name else [],
    )
    job.executor_class = executor or f"executor-{team_name}-{'alive' if alive else 'stale'}"
    job.owner_job_id = owner_job_id
    if healthy is None:
        job.health_check_result = HealthCheckResult.UNKNOWN
    else:
        job.health_check_result = HealthCheckResult.HEALTHY if healthy else HealthCheckResult.UNHEALTHY
    return job


def _patch_executor_keys(keys: set[str]):
    return patch("airflow.api.common.airflow_health._configured_executor_keys", return_value=keys)


def _patch_live_scheduler_ids(ids: set[int]):
    """Patch which owning-scheduler ids count as alive, standing in for a real SchedulerJob query."""
    return patch("airflow.api.common.airflow_health._live_scheduler_job_ids", return_value=ids)


class _HealthCheckingExecutor(BaseExecutor):
    """A stand-in for a concrete executor (e.g. Celery) that has implemented get_health()."""

    def get_health(self, timeout: float) -> bool | None:
        return True


class TestConfiguredExecutorKeys:
    """``_configured_executor_keys`` only counts an executor that actually implements get_health()."""

    @pytest.fixture(autouse=True)
    def clear_executor_caches(self):
        _configured_executor_keys.cache_clear()
        _configured_executor_names.cache_clear()
        yield
        _configured_executor_keys.cache_clear()
        _configured_executor_names.cache_clear()

    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_empty_when_no_executor_overrides_get_health(self, mock_get_executor_names):
        mock_get_executor_names.return_value = [
            ExecutorName(module_path="airflow.executors.local_executor.LocalExecutor", alias="Local")
        ]
        assert _configured_executor_keys() == set()

    @patch("airflow.api.common.airflow_health.ExecutorLoader.import_executor_cls")
    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_includes_an_executor_that_overrides_get_health(
        self, mock_get_executor_names, mock_import_executor_cls
    ):
        names = [
            ExecutorName(
                module_path=f"{_HealthCheckingExecutor.__module__}.{_HealthCheckingExecutor.__qualname__}",
                alias="Celery",
            )
        ]
        mock_get_executor_names.return_value = names
        mock_import_executor_cls.return_value = (_HealthCheckingExecutor, None)

        assert _configured_executor_keys() == {repr(names[0])}

    @patch("airflow.api.common.airflow_health.ExecutorLoader.import_executor_cls")
    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_module_path_configured_executor_has_no_alias_but_still_matches(
        self, mock_get_executor_names, mock_import_executor_cls
    ):
        """The key must agree with the scheduler side even when the executor has no alias at all."""
        names = [
            ExecutorName(
                module_path=f"{_HealthCheckingExecutor.__module__}.{_HealthCheckingExecutor.__qualname__}",
            )
        ]
        mock_get_executor_names.return_value = names
        mock_import_executor_cls.return_value = (_HealthCheckingExecutor, None)

        assert _configured_executor_keys() == {repr(names[0])}

    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names", side_effect=Exception)
    def test_survives_unreadable_executor_config(self, mock_get_executor_names):
        assert _configured_executor_keys() == set()

    @conf_vars({("scheduler", "executor_health_check_interval"): "0"})
    @patch("airflow.api.common.airflow_health.ExecutorLoader.import_executor_cls")
    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_empty_when_interval_is_zero(self, mock_get_executor_names, mock_import_executor_cls):
        """The scheduler records no row for any executor when the check is disabled."""
        names = [
            ExecutorName(
                module_path=f"{_HealthCheckingExecutor.__module__}.{_HealthCheckingExecutor.__qualname__}",
                alias="Celery",
            )
        ]
        mock_get_executor_names.return_value = names
        mock_import_executor_cls.return_value = (_HealthCheckingExecutor, None)

        assert _configured_executor_keys() == set()


class TestExecutorInstanceHealthDisplayName:
    """The API reports the name the user configured, not the internal executor_class key."""

    @pytest.fixture(autouse=True)
    def clear_executor_caches(self):
        _configured_executor_names.cache_clear()
        yield
        _configured_executor_names.cache_clear()

    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_reports_alias_when_the_key_matches_a_configured_executor(self, mock_get_executor_names):
        name = ExecutorName(
            module_path=f"{_HealthCheckingExecutor.__module__}.{_HealthCheckingExecutor.__qualname__}",
            alias="Celery",
        )
        job = _executor(alive=True, executor=repr(name))
        mock_get_executor_names.return_value = [name]

        instance = _executor_instance_health(job)

        assert instance["executor"] == "Celery"

    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names")
    def test_reports_module_path_when_the_configured_executor_has_no_alias(self, mock_get_executor_names):
        name = ExecutorName(
            module_path=f"{_HealthCheckingExecutor.__module__}.{_HealthCheckingExecutor.__qualname__}",
        )
        job = _executor(alive=True, executor=repr(name))
        mock_get_executor_names.return_value = [name]

        instance = _executor_instance_health(job)

        assert instance["executor"] == name.module_path

    @patch("airflow.api.common.airflow_health.ExecutorLoader.get_executor_names", return_value=[])
    def test_falls_back_to_the_raw_key_when_nothing_configured_matches(self, mock_get_executor_names):
        job = _executor(alive=True, executor=":SomeExecutor:")

        instance = _executor_instance_health(job)

        assert instance["executor"] == ":SomeExecutor:"


class TestExecutorDetailedStatus:
    """Each executor that implements a health check is its own coverage partition."""

    @pytest.mark.parametrize(
        ("keys", "jobs", "expected"),
        [
            pytest.param(
                {"Celery"},
                [_executor(alive=True, executor="Celery")],
                DetailedHealthStatus.HEALTHY,
                id="the_only_configured_executor_is_alive",
            ),
            pytest.param(
                {"Celery", "Kubernetes"},
                [_executor(alive=True, executor="Celery"), _executor(alive=True, executor="Kubernetes")],
                DetailedHealthStatus.HEALTHY,
                id="both_configured_executors_are_alive",
            ),
            pytest.param(
                {"Celery", "Kubernetes"},
                [_executor(alive=True, executor="Celery")],
                DetailedHealthStatus.DEGRADED,
                id="one_of_two_configured_executors_reports",
            ),
            pytest.param(
                {"Celery"},
                [_executor(alive=False, executor="Celery")],
                DetailedHealthStatus.DOWN,
                id="the_only_configured_executor_is_stale",
            ),
            pytest.param({"Celery"}, [], DetailedHealthStatus.DOWN, id="no_executor_has_ever_reported"),
            pytest.param(
                {"Celery"},
                [_executor(alive=True, healthy=None, executor="Celery")],
                DetailedHealthStatus.HEALTHY,
                id="first_check_still_pending_is_not_a_coverage_gap",
            ),
        ],
    )
    def test_coverage(self, keys, jobs, expected):
        with _patch_executor_keys(keys), _patch_live_scheduler_ids({1}):
            assert _executor_detailed_status(jobs) == expected

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1, 2})
    def test_restart_while_another_scheduler_already_reports_healthy(self, mock_live_ids, mock_keys):
        jobs = [
            _executor(alive=True, healthy=None, executor="Celery", owner_job_id=1),
            _executor(alive=True, healthy=True, executor="Celery", owner_job_id=2),
        ]
        assert _executor_detailed_status(jobs) == DetailedHealthStatus.HEALTHY

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1, 2})
    def test_unknown_does_not_mask_a_real_unhealthy_report(self, mock_live_ids, mock_keys):
        jobs = [
            _executor(alive=True, healthy=None, executor="Celery", owner_job_id=1),
            _executor(alive=True, healthy=False, executor="Celery", owner_job_id=2),
        ]
        assert _executor_detailed_status(jobs) == DetailedHealthStatus.DOWN

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1})
    def test_restarted_executor_leaves_the_component_healthy(self, mock_live_ids, mock_keys):
        """A hard-killed replica leaves its row behind; a fresh one taking over must still read healthy."""
        jobs = [
            _executor(alive=True, executor="Celery"),
            _executor(alive=False, executor="Celery"),
        ]
        assert _executor_detailed_status(jobs) == DetailedHealthStatus.HEALTHY

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1, 2})
    def test_one_scheduler_reporting_unhealthy_is_not_masked_by_another(self, mock_live_ids, mock_keys):
        """Two schedulers checking the same broker: one healthy row must not hide another's failure."""
        jobs = [
            _executor(alive=True, healthy=True, executor="Celery", owner_job_id=1),
            _executor(alive=True, healthy=False, executor="Celery", owner_job_id=2),
        ]
        assert _executor_detailed_status(jobs) == DetailedHealthStatus.DOWN

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1})
    def test_row_left_behind_by_crashed_scheduler_is_ignored(self, mock_live_ids, mock_keys):
        """A row can still look alive right after its scheduler died; it must not count as coverage."""
        jobs = [_executor(alive=True, healthy=True, executor="Celery", owner_job_id=2)]
        assert _executor_detailed_status(jobs) == DetailedHealthStatus.DOWN


class TestAirflowHealthExecutor:
    @pytest.fixture(autouse=True)
    def cleanup_jobs(self):
        clear_db_jobs()
        _configured_executor_names.cache_clear()
        yield
        clear_db_jobs()
        _configured_executor_names.cache_clear()

    def test_executor_key_omitted_when_nothing_implements_get_health(self):
        """The default, unmodified state: no executor overrides get_health, so there's no false alarm."""
        health_status = get_airflow_health()

        assert "executor" not in health_status

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1})
    @provide_session
    def test_executor_key_present_and_healthy_when_implemented_and_reporting(
        self, mock_live_ids, mock_keys, *, session
    ):
        heartbeat = timezone.utcnow()
        job = Job(job_type=EXECUTOR_JOB_TYPE, state=JobState.RUNNING, executor_class="Celery")
        job.latest_heartbeat = heartbeat
        job.owner_job_id = 1
        job.health_check_result = HealthCheckResult.HEALTHY
        session.add(job)
        session.commit()

        health_status = get_airflow_health()

        assert health_status["executor"]["status"] == HealthStatus.HEALTHY
        assert health_status["executor"]["detailed_status"] == DetailedHealthStatus.HEALTHY
        assert health_status["executor"]["latest_executor_heartbeat"] == heartbeat.isoformat()
        assert health_status["executor"]["instances"] == [
            {
                "hostname": job.hostname,
                "executor": "Celery",
                "latest_executor_heartbeat": heartbeat.isoformat(),
                "team_names": [],
                "health_check_result": HealthCheckResult.HEALTHY,
            }
        ]

    @_patch_executor_keys({"Celery"})
    @_patch_live_scheduler_ids({1})
    @provide_session
    def test_executor_key_unhealthy_when_last_check_failed_despite_live_heartbeat(
        self, mock_live_ids, mock_keys, *, session
    ):
        """A row can be alive (heartbeat moving) while the last real check still failed."""
        heartbeat = timezone.utcnow()
        job = Job(job_type=EXECUTOR_JOB_TYPE, state=JobState.RUNNING, executor_class="Celery")
        job.latest_heartbeat = heartbeat
        job.owner_job_id = 1
        job.health_check_result = HealthCheckResult.UNHEALTHY
        session.add(job)
        session.commit()

        health_status = get_airflow_health()

        assert health_status["executor"]["status"] == HealthStatus.UNHEALTHY
        assert health_status["executor"]["detailed_status"] == DetailedHealthStatus.DOWN

    @_patch_executor_keys({"Celery"})
    def test_executor_key_present_and_down_when_implemented_but_never_reported(self, mock_keys):
        """Implemented but never seen a healthy check: a real outage, correctly reported, not omitted."""
        health_status = get_airflow_health()

        assert health_status["executor"]["status"] == HealthStatus.UNHEALTHY
        assert health_status["executor"]["detailed_status"] == DetailedHealthStatus.DOWN
        assert health_status["executor"]["instances"] is None
