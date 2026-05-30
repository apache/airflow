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

from datetime import timedelta

import pytest
import time_machine

from airflow._shared.timezones import timezone
from airflow.models.task_circuit_breaker import TaskCircuitBreaker

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"


def _record(session, *, max_failures=3, window=None, reset_delay=None, dag_id=DAG_ID, task_id=TASK_ID):
    result = TaskCircuitBreaker.record_failure(
        dag_id=dag_id,
        task_id=task_id,
        max_failures=max_failures,
        window=window,
        reset_delay=reset_delay,
        session=session,
    )
    session.flush()
    return result


def _get(session, dag_id=DAG_ID, task_id=TASK_ID):
    return session.get(TaskCircuitBreaker, (dag_id, task_id))


class TestTaskCircuitBreakerRecordFailure:
    def test_first_failure_creates_record(self, session):
        opened = _record(session)

        cb = _get(session)
        assert cb is not None
        assert cb.failure_count == 1
        assert cb.is_open is False
        assert opened is False

    def test_second_failure_within_window_increments_count(self, session):
        _record(session)
        _record(session)

        cb = _get(session)
        assert cb.failure_count == 2
        assert cb.is_open is False

    def test_failure_after_window_resets_count(self, session):
        window = timedelta(seconds=60)
        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            _record(session, window=window)

        # Travel past the window
        with time_machine.travel("2026-01-01 00:02:00", tick=False):
            _record(session, window=window)

        cb = _get(session)
        assert cb.failure_count == 1

    def test_nth_failure_opens_circuit(self, session):
        opened = False
        for _ in range(3):
            opened = _record(session, max_failures=3)

        cb = _get(session)
        assert cb.is_open is True
        assert opened is True
        assert cb.opened_at is not None
        assert "3 failures" in cb.opened_reason

    def test_circuit_opens_with_reset_delay(self, session):
        delay = timedelta(hours=1)
        now = timezone.utcnow()
        for _ in range(2):
            _record(session, max_failures=2, reset_delay=delay)

        cb = _get(session)
        assert cb.is_open is True
        assert cb.reset_after is not None
        assert cb.reset_after > now

    def test_circuit_opens_without_reset_delay_leaves_reset_after_null(self, session):
        for _ in range(2):
            _record(session, max_failures=2, reset_delay=None)

        cb = _get(session)
        assert cb.is_open is True
        assert cb.reset_after is None

    def test_failures_beyond_threshold_do_not_reopen_already_open_circuit(self, session):
        for _ in range(3):
            _record(session, max_failures=2)

        # All calls after opening should return False (already open)
        result = _record(session, max_failures=2)
        assert result is False

    def test_config_snapshot_updated_on_each_call(self, session):
        _record(session, max_failures=3, window=timedelta(seconds=60))
        _record(session, max_failures=5, window=timedelta(seconds=120))

        cb = _get(session)
        assert cb.max_failures == 5
        assert cb.window_seconds == 120

    @pytest.mark.parametrize("max_failures", [1, 2, 10])
    def test_opens_exactly_at_threshold(self, session, max_failures):
        for _ in range(max_failures - 1):
            opened = _record(session, max_failures=max_failures)
            assert opened is False

        opened = _record(session, max_failures=max_failures)
        assert opened is True

        cb = _get(session)
        assert cb.is_open is True


class TestTaskCircuitBreakerReset:
    def test_reset_clears_all_open_state(self, session):
        for _ in range(2):
            _record(session, max_failures=2)

        cb = _get(session)
        assert cb.is_open is True

        cb.reset()
        session.flush()

        cb = _get(session)
        assert cb.is_open is False
        assert cb.opened_at is None
        assert cb.opened_reason is None
        assert cb.failure_count == 0
        assert cb.window_start is None
        assert cb.reset_after is None


class TestTaskCircuitBreakerResetExpired:
    def test_resets_circuits_past_reset_after(self, session):
        delay = timedelta(seconds=30)
        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            for _ in range(2):
                _record(session, max_failures=2, reset_delay=delay)
            session.flush()

        # Travel past the reset_delay
        with time_machine.travel("2026-01-01 00:01:00", tick=False):
            closed = TaskCircuitBreaker.reset_expired(session=session)

        assert closed == 1
        cb = _get(session)
        assert cb.is_open is False

    def test_does_not_reset_circuits_before_reset_after(self, session):
        delay = timedelta(hours=1)
        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            for _ in range(2):
                _record(session, max_failures=2, reset_delay=delay)
            session.flush()

        # Only 5 minutes passed — reset_after not reached
        with time_machine.travel("2026-01-01 00:05:00", tick=False):
            closed = TaskCircuitBreaker.reset_expired(session=session)

        assert closed == 0
        cb = _get(session)
        assert cb.is_open is True

    def test_does_not_reset_manual_only_circuits(self, session):
        for _ in range(2):
            _record(session, max_failures=2, reset_delay=None)
        session.flush()

        closed = TaskCircuitBreaker.reset_expired(session=session)

        assert closed == 0
        cb = _get(session)
        assert cb.is_open is True

    def test_resets_multiple_expired_circuits(self, session):
        delay = timedelta(seconds=10)
        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            for task_id in ("t1", "t2", "t3"):
                for _ in range(2):
                    _record(session, max_failures=2, reset_delay=delay, task_id=task_id)
            session.flush()

        with time_machine.travel("2026-01-01 00:01:00", tick=False):
            closed = TaskCircuitBreaker.reset_expired(session=session)

        assert closed == 3


class TestOpenCircuitsSubquery:
    def test_includes_open_circuits_without_reset_after(self, session):
        for _ in range(2):
            _record(session, max_failures=2, reset_delay=None)
        session.flush()

        from sqlalchemy import select

        sq = TaskCircuitBreaker.open_circuits_subquery()
        rows = session.execute(select(sq.c.dag_id, sq.c.task_id)).all()
        assert (DAG_ID, TASK_ID) in rows

    def test_includes_open_circuits_with_future_reset_after(self, session):
        delay = timedelta(hours=1)
        for _ in range(2):
            _record(session, max_failures=2, reset_delay=delay)
        session.flush()

        from sqlalchemy import select

        sq = TaskCircuitBreaker.open_circuits_subquery()
        rows = session.execute(select(sq.c.dag_id, sq.c.task_id)).all()
        assert (DAG_ID, TASK_ID) in rows

    def test_excludes_expired_open_circuits(self, session):
        delay = timedelta(seconds=5)
        with time_machine.travel("2026-01-01 00:00:00", tick=False):
            for _ in range(2):
                _record(session, max_failures=2, reset_delay=delay)
            session.flush()

        # Past reset_after — circuit should be excluded from the subquery
        with time_machine.travel("2026-01-01 00:01:00", tick=False):
            from sqlalchemy import select

            sq = TaskCircuitBreaker.open_circuits_subquery()
            rows = session.execute(select(sq.c.dag_id, sq.c.task_id)).all()

        assert (DAG_ID, TASK_ID) not in rows

    def test_excludes_closed_circuits(self, session):
        _record(session)  # Only 1 failure, circuit still closed
        session.flush()

        from sqlalchemy import select

        sq = TaskCircuitBreaker.open_circuits_subquery()
        rows = session.execute(select(sq.c.dag_id, sq.c.task_id)).all()
        assert (DAG_ID, TASK_ID) not in rows
