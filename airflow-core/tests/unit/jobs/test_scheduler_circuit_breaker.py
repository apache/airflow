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
"""Tests for circuit-breaker integration in SchedulerJobRunner."""

from __future__ import annotations

from collections.abc import Generator
from datetime import timedelta

import pytest
import time_machine

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.task_circuit_breaker import TaskCircuitBreaker
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_task_circuit_breakers

pytestmark = pytest.mark.db_test


def _clean_db():
    clear_db_runs()
    clear_db_dags()
    clear_db_task_circuit_breakers()


def _open_circuit(session, dag_id, task_id, *, reset_after=None):
    """Insert an open TaskCircuitBreaker row directly."""
    cb = TaskCircuitBreaker(
        dag_id=dag_id,
        task_id=task_id,
        is_open=True,
        opened_at=timezone.utcnow(),
        opened_reason="Opened after 3 failures in 3600s window",
        failure_count=3,
        window_start=timezone.utcnow(),
        reset_after=reset_after,
        max_failures=3,
        window_seconds=3600,
    )
    session.add(cb)
    session.flush()
    return cb


class TestSkipCircuitBreakerBlockedTis:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self) -> Generator:
        _clean_db()
        yield
        _clean_db()

    def test_scheduled_ti_skipped_when_circuit_open(self, dag_maker, session):
        with dag_maker("test_cb_dag", session=session) as dag:
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        (ti,) = dr.task_instances
        ti.state = State.SCHEDULED
        session.flush()

        _open_circuit(session, dag.dag_id, "task1")

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._skip_circuit_breaker_blocked_tis(session=session)
        session.flush()

        ti.refresh_from_db(session=session)
        assert ti.state == TaskInstanceState.SKIPPED
        assert ti.note is not None
        assert "Circuit open" in ti.note

    def test_running_ti_not_affected(self, dag_maker, session):
        with dag_maker("test_cb_dag_running", session=session) as dag:
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        (ti,) = dr.task_instances
        ti.state = State.RUNNING
        session.flush()

        _open_circuit(session, dag.dag_id, "task1")

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._skip_circuit_breaker_blocked_tis(session=session)
        session.flush()

        ti.refresh_from_db(session=session)
        assert ti.state == State.RUNNING

    def test_no_open_circuit_leaves_ti_scheduled(self, dag_maker, session):
        with dag_maker("test_cb_dag_no_circuit", session=session):
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        (ti,) = dr.task_instances
        ti.state = State.SCHEDULED
        session.flush()

        # No circuit breaker row inserted

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._skip_circuit_breaker_blocked_tis(session=session)
        session.flush()

        ti.refresh_from_db(session=session)
        assert ti.state == State.SCHEDULED

    def test_expired_open_circuit_does_not_block_ti(self, dag_maker, session):
        with dag_maker("test_cb_dag_expired", session=session) as dag:
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        (ti,) = dr.task_instances
        ti.state = State.SCHEDULED
        session.flush()

        # Circuit open but reset_after is in the past
        past = timezone.utcnow() - timedelta(hours=1)
        _open_circuit(session, dag.dag_id, "task1", reset_after=past)

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._skip_circuit_breaker_blocked_tis(session=session)
        session.flush()

        ti.refresh_from_db(session=session)
        # open_circuits_subquery excludes expired circuits, so TI stays SCHEDULED
        assert ti.state == State.SCHEDULED

    def test_skipped_note_contains_opened_reason(self, dag_maker, session):
        with dag_maker("test_cb_dag_reason", session=session) as dag:
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        (ti,) = dr.task_instances
        ti.state = State.SCHEDULED
        session.flush()

        _open_circuit(session, dag.dag_id, "task1")

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._skip_circuit_breaker_blocked_tis(session=session)
        session.flush()

        ti.refresh_from_db(session=session)
        assert "Opened after 3 failures" in ti.note


class TestResetExpiredCircuitBreakers:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self) -> Generator:
        _clean_db()
        yield
        _clean_db()

    def test_auto_reset_fires_when_reset_after_elapsed(self, session):
        delay = timedelta(minutes=30)

        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            cb = _open_circuit(session, "dag1", "task1", reset_after=timezone.utcnow() + delay)

        with time_machine.travel("2026-01-01 01:00:00", tick=False):
            job_runner = SchedulerJobRunner(job=Job())
            job_runner._reset_expired_circuit_breakers(session=session)
            session.flush()

        session.refresh(cb)
        assert cb.is_open is False
        assert cb.failure_count == 0

    def test_auto_reset_does_not_fire_before_reset_after(self, session):
        delay = timedelta(hours=2)

        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            cb = _open_circuit(session, "dag1", "task1", reset_after=timezone.utcnow() + delay)

        with time_machine.travel("2026-01-01 00:30:00", tick=False):
            job_runner = SchedulerJobRunner(job=Job())
            job_runner._reset_expired_circuit_breakers(session=session)
            session.flush()

        session.refresh(cb)
        assert cb.is_open is True

    def test_manual_only_circuit_not_auto_reset(self, session):
        cb = _open_circuit(session, "dag1", "task1", reset_after=None)

        job_runner = SchedulerJobRunner(job=Job())
        job_runner._reset_expired_circuit_breakers(session=session)
        session.flush()

        session.refresh(cb)
        assert cb.is_open is True

    def test_multiple_expired_circuits_all_reset(self, session):
        delay = timedelta(minutes=5)

        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            cbs = [
                _open_circuit(session, f"dag{i}", "task1", reset_after=timezone.utcnow() + delay)
                for i in range(4)
            ]

        with time_machine.travel("2026-01-01 01:00:00", tick=False):
            job_runner = SchedulerJobRunner(job=Job())
            job_runner._reset_expired_circuit_breakers(session=session)
            session.flush()

        for cb in cbs:
            session.refresh(cb)
            assert cb.is_open is False
