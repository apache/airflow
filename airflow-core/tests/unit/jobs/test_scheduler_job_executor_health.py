from __future__ import annotations

import pytest
from sqlalchemy import select

from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_utils import ExecutorName
from airflow.jobs.job import EXECUTOR_JOB_TYPE, HealthCheckResult, Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner, _executor_health_key

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


class _StubExecutor(BaseExecutor):
    mock_module_path = "stub.executor.path"
    mock_alias = "stub_executor"

    def __init__(self, result=True, team_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = ExecutorName(module_path=self.mock_module_path, alias=self.mock_alias)
        self.team_name = team_name
        self.result = result
        self.calls: list[float] = []

    def get_health(self, timeout: float) -> bool | None:
        self.calls.append(timeout)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


@pytest.fixture(autouse=True)
def clean_jobs():
    clear_db_jobs()
    yield
    clear_db_jobs()


def _make_runner(executor, session):
    scheduler_job = Job()
    session.add(scheduler_job)
    session.commit()
    runner = SchedulerJobRunner(job=scheduler_job, executors=[executor])
    return runner


def _get_executor_job(session, owner_job_id, executor_key):
    return session.scalar(
        select(Job).where(
            Job.job_type == EXECUTOR_JOB_TYPE,
            Job.owner_job_id == owner_job_id,
            Job.executor_class == executor_key,
        )
    )


def test_touch_executor_heartbeats_creates_a_row_for_a_reporting_executor(session):
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._touch_executor_heartbeats(session=session)

    job = _get_executor_job(session, runner.job.id, key)
    assert job is not None
    assert job.health_check_result == HealthCheckResult.UNKNOWN
    assert job.latest_heartbeat is not None
    assert job.hostname != key


def test_touch_executor_heartbeats_skips_executors_that_do_not_override_get_health(session):
    executor = BaseExecutor()
    executor.name = ExecutorName(module_path="airflow.executors.local_executor.LocalExecutor", alias="Local")
    runner = _make_runner(executor, session)

    runner._touch_executor_heartbeats(session=session)

    assert session.scalar(select(Job).where(Job.job_type == EXECUTOR_JOB_TYPE)) is None


def test_touch_executor_heartbeats_moves_the_heartbeat_without_changing_the_result(session):
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._touch_executor_heartbeats(session=session)
    first = _get_executor_job(session, runner.job.id, key)
    first.health_check_result = HealthCheckResult.HEALTHY
    first_heartbeat = first.latest_heartbeat
    session.commit()

    runner._touch_executor_heartbeats(session=session)
    second = _get_executor_job(session, runner.job.id, key)

    assert second.id == first.id
    assert second.health_check_result == HealthCheckResult.HEALTHY
    assert second.latest_heartbeat >= first_heartbeat


def test_run_executor_health_check_records_healthy(session):
    executor = _StubExecutor(result=True)
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._run_executor_health_check(executor, key)

    job = _get_executor_job(session, runner.job.id, key)
    assert job.health_check_result == HealthCheckResult.HEALTHY


def test_run_executor_health_check_records_unhealthy(session):
    executor = _StubExecutor(result=False)
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._run_executor_health_check(executor, key)

    job = _get_executor_job(session, runner.job.id, key)
    assert job.health_check_result == HealthCheckResult.UNHEALTHY


def test_run_executor_health_check_records_unknown_on_none(session):
    executor = _StubExecutor(result=None)
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._run_executor_health_check(executor, key)

    job = _get_executor_job(session, runner.job.id, key)
    assert job.health_check_result == HealthCheckResult.UNKNOWN


def test_run_executor_health_check_records_unknown_and_does_not_raise_when_get_health_raises(session):
    executor = _StubExecutor(result=RuntimeError("boom"))
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._run_executor_health_check(executor, key)

    job = _get_executor_job(session, runner.job.id, key)
    assert job.health_check_result == HealthCheckResult.UNKNOWN


def test_run_executor_health_check_passes_half_the_configured_interval_as_timeout(session):
    with conf_vars({("scheduler", "executor_health_check_interval"): "10.0"}):
        executor = _StubExecutor(result=True)
        runner = _make_runner(executor, session)
        key = _executor_health_key(executor)

        runner._run_executor_health_check(executor, key)

        assert executor.calls == [5.0]


def test_get_or_create_executor_job_reuses_the_row_for_the_same_scheduler(session):
    executor = _StubExecutor(team_name="team-a")
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    first = runner._get_or_create_executor_job(key, None, session=session)
    session.commit()
    first_id = first.id
    second = runner._get_or_create_executor_job(key, None, session=session)
    session.commit()

    assert second.id == first_id


def test_get_or_create_executor_job_sets_team_names(session, testing_team):
    executor = _StubExecutor(team_name=testing_team.name)
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    job = runner._get_or_create_executor_job(key, executor.team_name, session=session)
    session.commit()

    assert job.team_names == [testing_team.name]


def test_get_or_create_executor_job_gives_each_scheduler_its_own_row(session):
    executor_a = _StubExecutor()
    executor_b = _StubExecutor()
    runner_a = _make_runner(executor_a, session)
    runner_b = _make_runner(executor_b, session)
    key = _executor_health_key(executor_a)

    job_a = runner_a._get_or_create_executor_job(key, None, session=session)
    job_b = runner_b._get_or_create_executor_job(key, None, session=session)
    session.commit()

    assert job_a.id != job_b.id
    assert job_a.owner_job_id == runner_a.job.id
    assert job_b.owner_job_id == runner_b.job.id
