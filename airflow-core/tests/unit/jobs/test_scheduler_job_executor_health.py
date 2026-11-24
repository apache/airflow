#
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

import logging
import threading
import time

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_utils import ExecutorName
from airflow.jobs.job import EXECUTOR_JOB_TYPE, HealthCheckResult, Job, JobState
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


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        pytest.param(True, HealthCheckResult.HEALTHY, id="healthy"),
        pytest.param(False, HealthCheckResult.UNHEALTHY, id="unhealthy"),
        pytest.param(None, HealthCheckResult.UNKNOWN, id="unknown_on_none"),
        pytest.param(RuntimeError("boom"), HealthCheckResult.UNHEALTHY, id="unhealthy_and_does_not_raise"),
        pytest.param(NotImplementedError(), HealthCheckResult.UNKNOWN, id="unknown_on_not_implemented"),
    ],
)
def test_run_executor_health_check_records_result(session, result, expected):
    executor = _StubExecutor(result=result)
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)
    runner._touch_executor_heartbeats(session=session)
    session.commit()

    runner._run_executor_health_check(executor, key)

    job = _get_executor_job(session, runner.job.id, key)
    assert job.health_check_result == expected


def test_run_executor_health_check_does_not_create_a_row_when_none_is_open(session):
    """The health check thread only ever updates a row; only _touch_executor_heartbeats creates one."""
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._run_executor_health_check(executor, key)

    assert _get_executor_job(session, runner.job.id, key) is None


def test_run_executor_health_check_does_not_log_reported_itself_unhealthy_when_it_raised(session, caplog):
    executor = _StubExecutor(result=RuntimeError("boom"))
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    with caplog.at_level(logging.WARNING, logger="airflow.jobs.scheduler_job_runner"):
        runner._run_executor_health_check(executor, key)

    assert "reported itself unhealthy" not in caplog.text


@conf_vars({("scheduler", "executor_health_check_interval"): "10.0"})
def test_run_executor_health_check_passes_half_the_configured_interval_as_timeout(session):
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


def test_executor_health_key_is_none_for_an_executor_with_no_name():
    executor = BaseExecutor()
    assert executor.name is None
    assert _executor_health_key(executor) is None


def test_unnamed_executor_that_overrides_get_health_gets_no_row(session):
    executor = _StubExecutor()
    executor.name = None
    runner = _make_runner(executor, session)

    assert runner._executor_health_keys == {}

    runner._touch_executor_heartbeats(session=session)

    assert session.scalar(select(Job).where(Job.job_type == EXECUTOR_JOB_TYPE)) is None


def test_check_executor_health_records_unhealthy_when_a_check_stalls(session):
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)
    runner._touch_executor_heartbeats(session=session)
    session.commit()

    block_forever = threading.Event()
    stuck = threading.Thread(target=block_forever.wait, daemon=True)
    stuck.start()
    runner._executor_health_check_interval = 0.01
    runner._executor_health_check_threads[key] = (stuck, time.monotonic() - 10)

    runner._check_executor_health()

    job = _get_executor_job(session, runner.job.id, key)
    assert job is not None
    assert job.health_check_result == HealthCheckResult.UNHEALTHY
    assert key in runner._stalled_health_checks

    block_forever.set()
    stuck.join()


def test_check_executor_health_marks_unhealthy_on_every_tick_while_stalled(session):
    """A stalled check must keep marking UNHEALTHY even after the row it wrote to gets replaced."""
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)
    runner._touch_executor_heartbeats(session=session)
    session.commit()

    block_forever = threading.Event()
    stuck = threading.Thread(target=block_forever.wait, daemon=True)
    stuck.start()
    runner._executor_health_check_interval = 0.01
    runner._executor_health_check_threads[key] = (stuck, time.monotonic() - 10)

    runner._check_executor_health()
    first_job = runner._open_executor_job(key, session=session)
    assert first_job.health_check_result == HealthCheckResult.UNHEALTHY

    first_job.end_date = timezone.utcnow()
    session.commit()
    runner._touch_executor_heartbeats(session=session)
    session.commit()
    second_job = runner._open_executor_job(key, session=session)
    assert second_job.id != first_job.id

    runner._check_executor_health()
    session.refresh(second_job)
    assert second_job.health_check_result == HealthCheckResult.UNHEALTHY

    block_forever.set()
    stuck.join()


def test_join_executor_health_check_threads_prevents_a_row_reappearing_after_shutdown(session):
    """A check still running when the scheduler shuts down must not revive a row after it is closed."""
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._touch_executor_heartbeats(session=session)
    session.commit()

    release = threading.Event()

    def delayed_check():
        release.wait()
        runner._run_executor_health_check(executor, key)

    thread = threading.Thread(target=delayed_check, daemon=True)
    thread.start()
    runner._executor_health_check_threads[key] = (thread, time.monotonic())

    threading.Timer(0.05, release.set).start()
    runner._join_executor_health_check_threads()
    runner._end_executor_jobs(JobState.SUCCESS, session=session)
    session.commit()

    rows = session.scalars(
        select(Job).where(Job.job_type == EXECUTOR_JOB_TYPE, Job.owner_job_id == runner.job.id)
    ).all()
    assert len(rows) == 1
    assert rows[0].end_date is not None


def test_join_executor_health_check_threads_skips_a_check_already_marked_stalled(session):
    """A check already known to be stalled isn't coming back; shutdown must not wait on it again."""
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    block_forever = threading.Event()
    stuck = threading.Thread(target=block_forever.wait, daemon=True)
    stuck.start()
    runner._executor_health_check_timeout = 5.0
    runner._executor_health_check_threads[key] = (stuck, time.monotonic())
    runner._stalled_health_checks.add(key)

    started = time.monotonic()
    runner._join_executor_health_check_threads()
    assert time.monotonic() - started < 1.0

    block_forever.set()
    stuck.join()


def test_end_executor_jobs_closes_this_scheduler_rows_with_the_given_state(session):
    executor = _StubExecutor()
    runner = _make_runner(executor, session)
    key = _executor_health_key(executor)

    runner._touch_executor_heartbeats(session=session)
    job = _get_executor_job(session, runner.job.id, key)
    assert job.end_date is None

    runner._end_executor_jobs(JobState.SUCCESS, session=session)
    session.commit()
    session.refresh(job)

    assert job.state == JobState.SUCCESS
    assert job.end_date is not None


def test_end_executor_jobs_does_not_touch_another_schedulers_rows(session):
    executor_a = _StubExecutor()
    executor_b = _StubExecutor()
    runner_a = _make_runner(executor_a, session)
    runner_b = _make_runner(executor_b, session)
    key = _executor_health_key(executor_a)

    runner_a._touch_executor_heartbeats(session=session)
    runner_b._touch_executor_heartbeats(session=session)

    runner_a._end_executor_jobs(JobState.SUCCESS, session=session)
    session.commit()

    job_b = _get_executor_job(session, runner_b.job.id, key)
    assert job_b.state == JobState.RUNNING
    assert job_b.end_date is None
