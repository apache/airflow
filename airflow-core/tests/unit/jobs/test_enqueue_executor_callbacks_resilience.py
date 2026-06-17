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

    # ---- Transient context-fetch failure -> requeue (PENDING), not terminal FAILED ----
    def test_transient_failure_event_requeues_callback_as_pending(self, dag_maker, session):
        """A PENDING event from the executor (the callback hit a transient context-fetch failure)
        must reset the callback to PENDING so the next scheduler loop retries it — NOT mark it
        terminally FAILED. This keeps the executor path's transient-failure behavior in lockstep
        with the triggerer path, which re-evaluates on the next loop.
        """
        from airflow.models.callback import CallbackKey

        with dag_maker(dag_id="transient_cb"):
            pass
        dr = dag_maker.create_dagrun()
        cb = self._make_pending_callback(dr, session, executor_name=None)
        cb_id = cb.id
        # Move it out of PENDING first (as if it had been queued/run), so the requeue is observable.
        cb.state = CallbackState.RUNNING
        session.add(cb)
        session.flush()

        runner, executor = self._runner_with_executor()
        executor.get_event_buffer = mock.MagicMock(
            return_value={CallbackKey(id=str(cb_id)): (CallbackState.PENDING, "context fetch failed")}
        )

        runner._process_executor_events(executor=executor, session=session)

        assert session.get(ExecutorCallback, cb_id).state == CallbackState.PENDING, (
            "a transient (PENDING) executor event must requeue the callback, not fail it"
        )
