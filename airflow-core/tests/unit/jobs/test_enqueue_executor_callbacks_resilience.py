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
"""Error-isolation tests for SchedulerJobRunner._enqueue_executor_callbacks.

A single bad ExecutorCallback (unknown executor, workload build failure, or an
executor whose ``queue_workload`` raises) must not crash the scheduler loop or
prevent the remaining callbacks in the batch from being enqueued. This mirrors
the per-deadline SAVEPOINT isolation already applied to the
``Deadline.handle_miss`` loop.
"""

from __future__ import annotations

from unittest import mock

import pytest

from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.callback import ExecutorCallback
from airflow.models.deadline import Deadline
from airflow.sdk.definitions.callback import SyncCallback
from airflow.utils.state import CallbackState

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


def _noop_callback():
    pass


@pytest.mark.need_serialized_dag
class TestEnqueueExecutorCallbacksResilience:
    @pytest.fixture(autouse=True)
    def _clean(self):
        clear_db_runs()
        clear_db_dags()
        yield
        clear_db_runs()
        clear_db_dags()

    def _make_pending_callback(self, dag_run, session, executor_name=None):
        callback = Deadline(
            deadline_time=dag_run.logical_date,
            callback=SyncCallback(_noop_callback),
            dagrun_id=dag_run.id,
            deadline_alert_id=None,
        ).callback
        callback.state = CallbackState.PENDING
        callback.data["dag_run_id"] = dag_run.id
        callback.data["dag_id"] = dag_run.dag_id
        if executor_name is not None:
            callback.data["executor"] = executor_name
        session.add(callback)
        session.flush()
        return callback

    def _runner_with_executor(self, queue_side_effect=None):
        from tests_common.test_utils.mock_executor import MockExecutor

        executor = MockExecutor(do_update=False)
        if queue_side_effect is not None:
            executor.queue_workload = mock.MagicMock(side_effect=queue_side_effect)
        job = Job()
        runner = SchedulerJobRunner(job=job, executors=[executor])
        return runner, executor

    # ---- Scenario #4: get_executor_name on old-format data (no 'executor' key) ----
    def test_get_executor_name_missing_key_returns_none(self, dag_maker, session):
        with dag_maker(dag_id="old_format_data"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name=None)
        # Simulate an old-format row whose data dict predates the 'executor' key.
        cb.data.pop("executor", None)
        assert "executor" not in cb.data
        assert cb.get_executor_name() is None  # graceful, no KeyError

    # ---- Scenario #1: ExecutorCallback referencing a non-existent executor ----
    def test_unknown_executor_does_not_crash(self, dag_maker, session):
        with dag_maker(dag_id="unknown_exec"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name="does_not_exist_executor")
        runner, _ = self._runner_with_executor()

        # Must not raise: unknown executor should be skipped/dropped, not crash.
        runner._enqueue_executor_callbacks(session)

        # Dropped -> remains PENDING (not QUEUED), loop survived.
        assert session.get(ExecutorCallback, cb.id).state == CallbackState.PENDING

    # ---- Scenario #7: queue_workload raises -> scheduler loop must survive ----
    def test_queue_workload_raises_is_isolated(self, dag_maker, session):
        with dag_maker(dag_id="queue_raises"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name=None)
        runner, _ = self._runner_with_executor(queue_side_effect=RuntimeError("executor boom"))

        # Must not propagate: a failing enqueue must be isolated like handle_miss.
        runner._enqueue_executor_callbacks(session)

        # Failed callback should NOT be marked QUEUED (left for retry as PENDING).
        assert session.get(ExecutorCallback, cb.id).state == CallbackState.PENDING

    # ---- Scenario #3: many callbacks, one bad one must not block the rest ----
    def test_one_bad_callback_does_not_block_others(self, dag_maker, session):
        with dag_maker(dag_id="mixed_batch"):
            pass
        dr = dag_maker.create_dagrun()

        good1 = self._make_pending_callback(dr, session, executor_name=None)
        bad = self._make_pending_callback(dr, session, executor_name=None)
        good2 = self._make_pending_callback(dr, session, executor_name=None)

        bad_id = bad.id

        from tests_common.test_utils.mock_executor import MockExecutor

        executor = MockExecutor(do_update=False)
        executor.supports_callbacks = True
        real_queue = executor.queue_workload

        def selective_queue(workload, session=None):
            if str(workload.callback.id) == str(bad_id):
                raise RuntimeError("bad callback boom")
            return real_queue(workload, session=session)

        executor.queue_workload = mock.MagicMock(side_effect=selective_queue)
        job = Job()
        runner = SchedulerJobRunner(job=job, executors=[executor])

        runner._enqueue_executor_callbacks(session)

        # The good callbacks must still be QUEUED despite the bad one in the batch.
        assert session.get(ExecutorCallback, good1.id).state == CallbackState.QUEUED
        assert session.get(ExecutorCallback, good2.id).state == CallbackState.QUEUED
        # The bad one stays PENDING for retry.
        assert session.get(ExecutorCallback, bad.id).state == CallbackState.PENDING

    # ---- Fairness: equal-priority backlog drains oldest-first (no starvation) ----
    def test_backlog_drains_oldest_first(self, dag_maker, session):
        """When the PENDING backlog exceeds available slots, equal-priority callbacks must be
        selected oldest-first (stable FIFO), not in a DB-arbitrary order that could starve some
        callbacks indefinitely. All deadline callbacks default to priority_weight=1, so the
        created_at/id secondary sort is what guarantees forward progress for every callback.
        """
        import datetime

        with dag_maker(dag_id="backlog_fairness"):
            pass
        dr = dag_maker.create_dagrun()

        # Create 5 equal-priority PENDING callbacks, assigning created_at in the REVERSE of
        # insertion order so insertion/physical order != age order. This makes the test robust
        # across backends: a backend that returns rows in insertion order (e.g. SQLite) would
        # pick the WRONG (newest) callbacks without the explicit created_at sort, so the
        # assertion only holds when the secondary sort key is actually applied.
        base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        inserted = []
        for _ in range(5):
            cb = self._make_pending_callback(dr, session, executor_name=None)
            cb.priority_weight = 1
            inserted.append(cb)
        # inserted[0] is newest, inserted[4] is oldest.
        for i, cb in enumerate(inserted):
            cb.created_at = base + datetime.timedelta(minutes=(5 - i))
            session.add(cb)
        session.flush()

        # Oldest-first order = reverse of insertion order.
        ordered_ids = [cb.id for cb in reversed(inserted)]  # oldest -> newest

        # Constrain capacity so only the 2 oldest should be enqueued this loop.
        from tests_common.test_utils.mock_executor import MockExecutor

        executor = MockExecutor(do_update=False)
        executor.supports_callbacks = True
        runner = SchedulerJobRunner(job=Job(), executors=[executor])
        runner._parallelism = 2

        runner._enqueue_executor_callbacks(session)

        states = {cb.id: session.get(ExecutorCallback, cb.id).state for cb in inserted}
        queued = [cid for cid in ordered_ids if states[cid] == CallbackState.QUEUED]
        pending = [cid for cid in ordered_ids if states[cid] == CallbackState.PENDING]

        # Exactly the 2 OLDEST drained; the 3 newer ones wait (and will drain next loops).
        assert queued == ordered_ids[:2], "backlog not drained oldest-first (starvation risk)"
        assert pending == ordered_ids[2:]

    # ---- Orphaned ExecutorCallback: DagRun deleted -> terminalize FAILED (no zombie PENDING) ----
    def test_executor_callback_with_deleted_dagrun_is_failed_not_stuck(self, dag_maker, session):
        """An ExecutorCallback whose DagRun has been deleted can never run (its context is built
        from the DagRun), so it must be marked FAILED rather than left PENDING and retried (and
        re-warned) every scheduler loop forever. Unlike TriggererCallbacks — which have a
        standalone Trigger that can still terminalize them — a sync ExecutorCallback has no path
        to completion once its DagRun is gone.
        """

        with dag_maker(dag_id="orphaned_cb"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name=None)
        cb_id = cb.id
        # Point the callback at a DagRun id that does not exist (simulates deletion).
        cb.data["dag_run_id"] = 999999
        session.add(cb)
        session.flush()

        runner, _ = self._runner_with_executor()
        # Must not raise, and must not leave the callback PENDING forever.
        runner._enqueue_executor_callbacks(session)

        assert session.get(ExecutorCallback, cb_id).state == CallbackState.FAILED, (
            "orphaned ExecutorCallback (deleted DagRun) should be FAILED, not stuck PENDING"
        )

    # ---- HA safety: the PENDING-callback selection must lock rows (FOR UPDATE SKIP LOCKED) ----
    def test_pending_callback_selection_uses_row_locks(self, dag_maker, session):
        """In an HA multi-scheduler deployment, two schedulers running this loop concurrently
        must not both select the same PENDING callbacks and enqueue them to their executors
        twice (double-execution of the deadline callback). The selection query must therefore go
        through ``with_row_locks(..., skip_locked=True)`` — exactly like the task-instance
        critical section and the deadline-selection query in ``_run_scheduler_loop``. The
        ``state = QUEUED`` write alone is insufficient: ``executor.queue_workload`` (the side
        effect) runs before the conflicting write is resolved at commit.

        SQLite is a no-op for row locks, so assert on the contract — that the callback query is
        passed through ``with_row_locks`` with ``skip_locked=True`` — which holds on every backend.
        """
        import airflow.jobs.scheduler_job_runner as sjr

        with dag_maker(dag_id="ha_lock"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name=None)

        runner, executor = self._runner_with_executor()
        executor.supports_callbacks = True

        real_with_row_locks = sjr.with_row_locks
        seen = {}

        def spy(query, of=None, session=None, skip_locked=False, **kwargs):
            # Record the lock applied to the ExecutorCallback selection.
            if of is ExecutorCallback:
                seen["locked"] = True
                seen["skip_locked"] = skip_locked
            return real_with_row_locks(query, of=of, session=session, skip_locked=skip_locked, **kwargs)

        with mock.patch.object(sjr, "with_row_locks", side_effect=spy):
            runner._enqueue_executor_callbacks(session)

        assert seen.get("locked"), (
            "PENDING ExecutorCallback selection must be wrapped in with_row_locks for HA safety"
        )
        assert seen.get("skip_locked") is True, "callback selection must use SKIP LOCKED"
        # Sanity: the callback was still enqueued (lock didn't break the happy path).
        assert session.get(ExecutorCallback, cb.id).state == CallbackState.QUEUED
