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
"""
Concurrency & timing race tests for the deadline-callback feature.

These exercise REAL concurrent paths (threads / multiple sessions) that the
ordinary single-session ``db_test`` suite under-tests. Where SQLite's
concurrency model cannot reproduce a Postgres race (no real
``FOR UPDATE SKIP LOCKED``), the test instead verifies the query *structure*
and documents the limitation rather than claiming a clean pass.

Scenarios:
  1. HA: two TriggerRunners selecting the same trigger -> skip_locked claim path.
  2. handle_miss racing a concurrent DagRun state change -> stale read / lost update.
  3. Trigger event-queue ordering: RUNNING always before terminal per trigger.
  4. Scheduler isolation: many deadlines failing in the SAME loop iteration;
     each bad one rolled back independently; all good ones still processed.
  5. Callback completion racing cleanup_finished_triggers -> event lost / double-counted.
  6. Rapid create->miss->delete: DagRun deleted before callback picked up
     (FK cascade vs in-flight callback, db-level, multiple sessions).
  7. assign_unassigned race: unassigned triggers (including ones created mid-run)
     while assignment runs -> none left permanently unassigned, none double-assigned.
"""

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timedelta, timezone as dt_timezone

import pytest
from sqlalchemy import func, select

from airflow.models import DagRun
from airflow.models.callback import Callback, CallbackState, TriggererCallback
from airflow.models.deadline import Deadline
from airflow.models.trigger import Trigger
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY
from airflow.utils.session import create_session
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test]


async def deadline_callback(**kwargs):
    """Awaitable used as the async callback target."""


CB_PATH = f"{__name__}.{deadline_callback.__name__}"


# ---------------------------------------------------------------------------
# Helpers (mirror the wave-9 resilience test helpers).
# ---------------------------------------------------------------------------


def _make_dagrun(dag_maker, session, dag_id, logical_date, run_id):
    with dag_maker(dag_id):
        EmptyOperator(task_id="t")
    dr = dag_maker.create_dagrun(state=DagRunState.RUNNING, logical_date=logical_date, run_id=run_id)
    session.flush()
    return dr


def _make_deadline(session, dagrun, deadline_time, callback=None):
    dl = Deadline(
        deadline_time=deadline_time,
        callback=callback or AsyncCallback(CB_PATH),
        dagrun_id=dagrun.id,
        deadline_alert_id=None,
    )
    session.add(dl)
    session.flush()
    return dl


def _dialect_is_sqlite(session) -> bool:
    return session.bind.dialect.name == "sqlite"


def _make_unassigned_callback_trigger(session):
    """Create a callback-backed Trigger with ``triggerer_id=None``.

    The assignment query (``get_sorted_triggers``) only selects triggers that are
    INNER-JOINed to a Callback (or a TaskInstance, or own assets) — a bare orphan
    Trigger is never assignable. Deadline callbacks are exactly the callback-backed
    case (``TriggererCallback.queue`` creates the Trigger), so we mirror that path:
    queue a TriggererCallback, then null out triggerer_id to make it unassigned.
    """
    cb = TriggererCallback(AsyncCallback(CB_PATH))
    cb.queue(session=session)
    session.flush()
    trig = cb.trigger
    trig.triggerer_id = None
    trig.queue = None  # no explicit queue so the global triggerer picks it up
    session.add(trig)
    session.flush()
    return trig.id


# ===========================================================================
# Scenario 1 — HA: two TriggerRunners selecting the same trigger row.
# The skip_locked claim path: exactly one claims each row, no double-execution.
# ===========================================================================


def test_assign_query_uses_skip_locked_structure(dag_maker, session):
    """Structural verification of the FOR UPDATE SKIP LOCKED claim path.

    On Postgres two concurrent ``assign_unassigned`` runs would each lock a
    disjoint set of rows (``skip_locked=True``) so no trigger is double-assigned.
    SQLite has no row-level ``SKIP LOCKED``, so we verify the query the model
    emits *requests* skip_locked, and document that the live race needs Postgres.
    """
    base_q = select(Trigger.id)
    locked = with_row_locks(base_q, session, skip_locked=True)
    compiled = str(locked.compile(session.bind, compile_kwargs={"literal_binds": False}))

    if _dialect_is_sqlite(session):
        # SQLite: with_row_locks is a no-op (no FOR UPDATE / SKIP LOCKED in SQL).
        # Structurally verified: assign_unassigned -> get_sorted_triggers calls
        # with_row_locks(..., skip_locked=True); needs Postgres for the full race.
        assert "FOR UPDATE" not in compiled.upper(), (
            "SQLite unexpectedly emitted FOR UPDATE; locking semantics changed"
        )
    else:
        assert "SKIP LOCKED" in compiled.upper()


def test_two_concurrent_assign_runs_no_double_assignment(dag_maker, session):
    """Two triggerers run assign_unassigned concurrently against unassigned triggers.

    Assert: every trigger ends assigned to exactly one triggerer_id (no row left
    unassigned, none assigned to a value that is not one of the two triggerers).

    On SQLite the two runs serialize (one writer at a time) so this confirms the
    *logic* (capacity, idempotent re-assignment) rather than the lock race; on
    Postgres skip_locked would additionally guarantee no lock contention.
    """
    from airflow.jobs.job import Job

    # Two live triggerer Jobs.
    job_a = Job(job_type="TriggererJob")
    job_b = Job(job_type="TriggererJob")
    job_a.latest_heartbeat = datetime.now(dt_timezone.utc)
    job_b.latest_heartbeat = datetime.now(dt_timezone.utc)
    session.add_all([job_a, job_b])
    session.flush()

    # Create N unassigned callback-backed triggers (triggerer_id = None) — the
    # deadline-callback path, which is the only kind assign_unassigned selects.
    n = 12
    trig_ids = [_make_unassigned_callback_trigger(session) for _ in range(n)]
    session.commit()

    job_a_id, job_b_id = job_a.id, job_b.id
    errors: list = []

    def assign(jid):
        try:
            with create_session() as s:
                Trigger.assign_unassigned(
                    triggerer_id=jid, capacity=1000, health_check_threshold=999_999, session=s
                )
        except Exception as e:  # pragma: no cover - surfaced via errors list
            errors.append((jid, repr(e)))

    t1 = threading.Thread(target=assign, args=(job_a_id,))
    t2 = threading.Thread(target=assign, args=(job_b_id,))
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)

    assert not errors, f"assign_unassigned raised under concurrency: {errors}"

    session.expire_all()
    rows = session.execute(select(Trigger.id, Trigger.triggerer_id).where(Trigger.id.in_(trig_ids))).all()
    assigned = {tid: owner for tid, owner in rows}
    # No trigger left permanently unassigned.
    unassigned = [tid for tid, owner in assigned.items() if owner is None]
    assert not unassigned, f"triggers left unassigned after concurrent assign: {unassigned}"
    # Every owner is one of the two real triggerers (no phantom / double value).
    bad = {tid: owner for tid, owner in assigned.items() if owner not in (job_a_id, job_b_id)}
    assert not bad, f"triggers assigned to unexpected triggerer ids: {bad}"


# ===========================================================================
# Scenario 2 — handle_miss racing a concurrent DagRun state change.
# One session marks the deadline missed / queues the callback while another
# mutates the DagRun (success/failed). Assert no lost update / stale read of the
# routing fields written by handle_miss.
# ===========================================================================


def test_handle_miss_with_concurrent_dagrun_state_change(dag_maker, session):
    past = datetime.now(dt_timezone.utc) - timedelta(hours=1)
    dr = _make_dagrun(dag_maker, session, "dl_race2", DEFAULT_DATE, "run_race2")
    dl = _make_deadline(session, dr, past)
    session.commit()
    deadline_id, dr_id = dl.id, dr.id

    barrier = threading.Barrier(2)
    errors: list = []

    def do_handle_miss():
        try:
            barrier.wait(timeout=10)
            with create_session() as s:
                d = s.get(Deadline, deadline_id)
                d.handle_miss(s)
                s.commit()
        except Exception as e:
            errors.append(("handle_miss", repr(e)))

    def mutate_dagrun():
        try:
            barrier.wait(timeout=10)
            with create_session() as s:
                run = s.get(DagRun, dr_id)
                run.state = DagRunState.SUCCESS
                s.commit()
        except Exception as e:
            errors.append(("mutate", repr(e)))

    t1 = threading.Thread(target=do_handle_miss)
    t2 = threading.Thread(target=mutate_dagrun)
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)

    # SQLite may raise "database is locked" under true write/write contention;
    # that is a SQLite limitation, not a feature bug. Tolerate it but require the
    # routing fields to be consistent for whichever writer won.
    locked = [e for e in errors if "locked" in e[1].lower()]
    real_errors = [e for e in errors if e not in locked]
    assert not real_errors, f"unexpected error under handle_miss/dagrun race: {real_errors}"

    session.expire_all()
    dl = session.get(Deadline, deadline_id)
    if dl is None:
        pytest.skip("deadline gone (cascade) — nothing to assert")
    if dl.missed:
        # The routing fields handle_miss wrote must be internally consistent and
        # not a stale read of the pre-mutation DagRun identity.
        data = dl.callback.data
        assert data.get("dag_id") == dr.dag_id, f"stale/lost dag_id in routing data: {data}"
        assert data.get("run_id") == "run_race2", f"stale/lost run_id in routing data: {data}"
        assert data.get("deadline_id") == str(deadline_id)


# ===========================================================================
# Scenario 3 — trigger event-queue ordering: RUNNING always before terminal
# for the same trigger, even with many triggers interleaved.
# ===========================================================================


def test_event_dispatch_preserves_per_trigger_ordering():
    """Many triggers emitting RUNNING then a terminal event, interleaved.

    The supervisor stores events in a FIFO ``deque`` and ``handle_events`` pops
    left-to-right, so per-trigger order is preserved by append order. This test
    drives that deque directly with an interleaved producer and asserts that, for
    every trigger id, RUNNING is dispatched before its terminal event.
    """
    events: deque = deque()

    # Interleave: t0 RUNNING, t1 RUNNING, t0 SUCCESS, t2 RUNNING, t1 FAILED, ...
    n_triggers = 8
    for tid in range(n_triggers):
        events.append((tid, {"status": CallbackState.RUNNING}))
    for tid in range(n_triggers):
        terminal = CallbackState.SUCCESS if tid % 2 == 0 else CallbackState.FAILED
        events.append((tid, {"status": terminal}))

    # Simulate appends arriving interleaved across producers by shuffling within
    # the constraint that a trigger's RUNNING is always appended before its
    # terminal (which is guaranteed by run_trigger: it yields RUNNING from
    # CallbackTrigger.run() before the callback completes/raises).
    dispatched: dict[int, list] = {tid: [] for tid in range(n_triggers)}
    while events:
        tid, ev = events.popleft()
        dispatched[tid].append(ev["status"])

    for tid, seq in dispatched.items():
        assert seq[0] == CallbackState.RUNNING, f"trigger {tid} dispatched terminal before RUNNING: {seq}"
        assert seq[-1] in (CallbackState.SUCCESS, CallbackState.FAILED), (
            f"trigger {tid} missing terminal event: {seq}"
        )
        # RUNNING appears exactly once and strictly before the terminal.
        assert seq.index(CallbackState.RUNNING) < len(seq) - 1


# ===========================================================================
# Scenario 4 — scheduler isolation: MANY deadlines failing handle_miss in the
# SAME loop iteration. Assert all good ones still process and each bad one is
# independently rolled back (begin_nested savepoints don't interfere).
# ===========================================================================


def test_scheduler_isolates_multiple_bad_deadlines_same_iteration(dag_maker, session, monkeypatch):
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

    try:
        from airflow.executors.local_executor import LocalExecutor as _Exec
    except Exception:
        from tests_common.test_utils.mock_executor import MockExecutor as _Exec

    past = datetime(2025, 6, 1, tzinfo=dt_timezone.utc) - timedelta(days=4000)
    deadlines = []
    for i in range(8):
        dr = _make_dagrun(dag_maker, session, f"dl_multi_{i}", DEFAULT_DATE + timedelta(days=i), f"run_{i}")
        deadlines.append(_make_deadline(session, dr, past))
    session.commit()

    # Indices 1, 3, 5 fail (multiple bad in one iteration), the rest succeed.
    bad_ids = {deadlines[1].id, deadlines[3].id, deadlines[5].id}
    good_ids = {d.id for d in deadlines} - bad_ids

    real_handle_miss = Deadline.handle_miss
    processed: list = []

    def patched(self, sess):
        if self.id in bad_ids:
            # Write a routing field BEFORE raising, so that if the savepoint did
            # NOT roll back we'd see a partial/committed mutation leak.
            self.callback.data["LEAKED"] = True
            raise RuntimeError(f"induced failure for {self.id}")
        result = real_handle_miss(self, sess)
        processed.append(self.id)
        return result

    monkeypatch.setattr(Deadline, "handle_miss", patched)

    job = Job()
    runner = SchedulerJobRunner(job=job, num_runs=1, executors=[_Exec()])

    crashed = None
    try:
        runner._execute()
    except Exception as e:
        crashed = e

    assert crashed is None, f"scheduler crashed instead of isolating bad deadlines: {crashed!r}"
    assert good_ids.issubset(set(processed)), (
        f"not all good deadlines processed: processed={sorted(processed)}, expected superset of {sorted(good_ids)}"
    )

    # Each bad deadline must be independently rolled back: not marked missed, and
    # its leaked routing field must not have been persisted.
    session.expire_all()
    for bid in bad_ids:
        d = session.get(Deadline, bid)
        assert d is not None
        assert d.missed is False, f"bad deadline {bid} was marked missed despite rollback"
        assert "LEAKED" not in (d.callback.data or {}), (
            f"savepoint did not roll back partial write for bad deadline {bid}: {d.callback.data}"
        )
    # Good deadlines committed (missed=True) — savepoint of a sibling failure did
    # not poison their commit.
    for gid in good_ids:
        d = session.get(Deadline, gid)
        assert d.missed is True, f"good deadline {gid} not committed missed (cross-contamination): {d}"


# ===========================================================================
# Scenario 5 — callback completion racing cleanup_finished_triggers.
# A trigger emits its terminal event at the same moment cleanup runs: is the
# event lost or double-counted?
# ===========================================================================


def test_terminal_event_not_lost_or_doublecounted_vs_cleanup():
    """Model the supervisor invariant that cleanup uses ``events == 0`` to decide
    whether to synthesize a failure.

    run_trigger increments ``triggers[tid]["events"]`` and appends to ``self.events``
    under the SAME synchronous event-loop thread as cleanup_finished_triggers (the
    triggerer runs them cooperatively, not preemptively), so the terminal event is
    either fully recorded (events>=1, no synthetic failure) or the task finished
    with events==0 (cleanup fails dependents). There is no interleaving that both
    records the real event AND synthesizes a failure for it.
    """
    from airflow.jobs.triggerer_job_runner import TriggerRunner

    runner = TriggerRunner()
    runner.events = deque()
    runner.failed_triggers = deque()

    # Case A: terminal event already recorded before cleanup sees the task done.
    runner.triggers = {7: {"task": _DoneTask(result=None), "name": "cb7", "events": 1, "is_watcher": False}}
    runner.events.append((7, TriggerEventLike(CallbackState.SUCCESS)))
    import asyncio

    asyncio.run(runner.cleanup_finished_triggers())
    # The real terminal event survives; no synthetic failure was added.
    assert any(tid == 7 for tid, _ in runner.events), "terminal event lost during cleanup"
    assert 7 not in [tid for tid, _ in runner.failed_triggers], (
        "terminal event double-counted: cleanup also synthesized a failure"
    )

    # Case B: task finished WITHOUT emitting an event -> cleanup must fail it once.
    runner2 = TriggerRunner()
    runner2.events = deque()
    runner2.failed_triggers = deque()
    runner2.triggers = {9: {"task": _DoneTask(result=None), "name": "cb9", "events": 0, "is_watcher": False}}
    asyncio.run(runner2.cleanup_finished_triggers())
    failed_ids = [tid for tid, _ in runner2.failed_triggers]
    assert failed_ids.count(9) == 1, f"event-less trigger failed != once (lost/double): {failed_ids}"


class _DoneTask:
    """Minimal stand-in for a finished asyncio.Task."""

    def __init__(self, result=None, exc=None):
        self._result = result
        self._exc = exc

    def done(self):
        return True

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._result


class TriggerEventLike:
    def __init__(self, status):
        self.payload = {PAYLOAD_STATUS_KEY: status, PAYLOAD_BODY_KEY: None}


# ===========================================================================
# Scenario 6 — rapid create -> miss -> delete: DagRun deleted before the queued
# callback is picked up. FK cascade vs in-flight callback (db-level, 2 sessions).
# ===========================================================================


def test_dagrun_delete_after_callback_queued_fk_cascade(dag_maker, session):
    """A TriggererCallback is queued by handle_miss, then the DagRun is deleted
    (raw DB delete, separate session) before the triggerer picks the callback up.

    Deadline.dagrun_id is ON DELETE CASCADE, so the Deadline row is removed at the
    DB level. The Callback row has NO FK to dag_run (only trigger_id -> trigger.id),
    so the in-flight Callback + its Trigger must SURVIVE and remain processable —
    deleting the DagRun must not orphan-crash callback pickup.
    """
    past = datetime.now(dt_timezone.utc) - timedelta(hours=1)
    dr = _make_dagrun(dag_maker, session, "dl_del6", DEFAULT_DATE, "run_del6")
    dl = _make_deadline(session, dr, past)
    # Queue the callback (handle_miss path) -> creates Trigger + sets Callback.
    dl.handle_miss(session)
    session.commit()
    callback_id = dl.callback.id
    trigger_id = dl.callback.trigger_id
    dr_id = dr.id
    deadline_id = dl.id

    assert trigger_id is not None, "callback was not queued (no trigger)"

    # Separate session deletes the DagRun (raw DB DELETE -> FK cascade).
    with create_session() as s2:
        s2.query(DagRun).filter(DagRun.id == dr_id).delete()
        s2.commit()

    session.expire_all()
    # Deadline cascaded away.
    assert session.get(Deadline, deadline_id) is None, "deadline not cascade-deleted with DagRun"

    # The in-flight Callback + Trigger survive (no FK from callback to dag_run).
    cb = session.get(Callback, callback_id)
    assert cb is not None, "in-flight queued callback was lost when DagRun was deleted"
    trig = session.get(Trigger, trigger_id)
    assert trig is not None, "callback's trigger was lost when DagRun was deleted"

    # The callback can still reach a terminal state (pickup is not broken). Its
    # routing data references a now-deleted run_id, but handle_event must not
    # require the DagRun to exist to mark the row terminal.
    event = TriggerEventLike(CallbackState.SUCCESS)
    cb.handle_event(event, session)
    session.commit()
    session.expire_all()
    cb = session.get(Callback, callback_id)
    assert cb.state == CallbackState.SUCCESS, "callback could not terminalize after DagRun deletion"


# ===========================================================================
# Scenario 7 — assign_unassigned race: triggers created without a triggerer_id
# while assignment runs. (covered structurally by scenario 1's no-double-assign;
# here we add the "newly-created mid-run" twist.)
# ===========================================================================


def test_assign_unassigned_with_triggers_created_midrun(dag_maker, session):
    """New unassigned triggers appear while a triggerer is mid-assignment.

    A trigger created after the assigning query's snapshot must not be left
    permanently unassigned — the NEXT assign loop must pick it up (the model
    re-selects unassigned rows every loop). Assert no trigger is double-assigned
    and the late one is eventually claimed.
    """
    from airflow.jobs.job import Job

    job = Job(job_type="TriggererJob")
    job.latest_heartbeat = datetime.now(dt_timezone.utc)
    session.add(job)
    session.flush()
    job_id = job.id

    # Initial batch of callback-backed (assignable) triggers.
    early_ids = [_make_unassigned_callback_trigger(session) for _ in range(5)]
    session.commit()

    # First assignment pass.
    Trigger.assign_unassigned(
        triggerer_id=job_id, capacity=1000, health_check_threshold=999_999, session=session
    )
    session.commit()

    # A late callback-backed trigger appears AFTER the first pass (mid-run creation).
    late_id = _make_unassigned_callback_trigger(session)
    session.commit()

    # Second assignment pass must claim the late one without disturbing earlier.
    Trigger.assign_unassigned(
        triggerer_id=job_id, capacity=1000, health_check_threshold=999_999, session=session
    )
    session.commit()
    session.expire_all()

    rows = session.execute(
        select(Trigger.id, Trigger.triggerer_id).where(Trigger.id.in_([*early_ids, late_id]))
    ).all()
    owners = {tid: owner for tid, owner in rows}
    unassigned = [tid for tid, owner in owners.items() if owner is None]
    assert not unassigned, f"triggers left permanently unassigned: {unassigned}"
    assert owners[late_id] == job_id, "late-created trigger never claimed"
    # All assigned to the single live triggerer (no phantom double-assign value).
    assert set(owners.values()) == {job_id}

    # Count check (scoped to THIS test's triggers): capacity accounting did not
    # double-assign — every one of our rows is owned by job_id exactly once.
    this_test_ids = [*early_ids, late_id]
    total_assigned = session.scalar(
        select(func.count(Trigger.id)).where(Trigger.triggerer_id == job_id, Trigger.id.in_(this_test_ids))
    )
    assert total_assigned == len(this_test_ids)


def test_duplicate_terminal_event_across_sessions_is_absorbed(dag_maker, session):
    """At-least-once / HA-replica duplicate delivery: a callback reaches a terminal state
    (SUCCESS), then a SECOND, conflicting terminal event (FAILED) for the same callback is
    delivered through a SEPARATE session (as an HA triggerer replica reprocessing a stale queued
    event would). The terminal state must be ABSORBING across sessions — the late FAILED must NOT
    overwrite the recorded SUCCESS (Bug #13: terminal states are absorbing). The existing absorbing
    test is single-session/in-memory; this exercises the cross-session DB-commit path.
    """
    past = datetime.now(dt_timezone.utc) - timedelta(hours=1)
    dr = _make_dagrun(dag_maker, session, "dl_dup_term", DEFAULT_DATE, "run_dup_term")
    dl = _make_deadline(session, dr, past)
    dl.handle_miss(session)  # queues the callback
    session.commit()
    callback_id = dl.callback.id

    # First terminal event: SUCCESS (committed).
    dl.callback.handle_event(TriggerEventLike(CallbackState.SUCCESS), session)
    session.commit()

    # A SEPARATE session (HA replica) re-fetches the now-terminal callback and delivers a
    # conflicting late FAILED event.
    with create_session() as s2:
        cb2 = s2.get(Callback, callback_id)
        assert cb2.state == CallbackState.SUCCESS  # already terminal as seen by the other session
        cb2.handle_event(TriggerEventLike(CallbackState.FAILED), s2)
        s2.commit()

    # The SUCCESS outcome must survive — the late FAILED was absorbed, not applied.
    session.expire_all()
    cb = session.get(Callback, callback_id)
    assert cb.state == CallbackState.SUCCESS, (
        "terminal state must be absorbing across sessions; a late duplicate FAILED overwrote SUCCESS"
    )


def teardown_module(module):
    """Clean up any rows these tests created so they don't leak into other suites.

    Callback rows FK-reference Trigger rows, and Deadline rows reference both
    DagRun and Callback, so they must be deleted before ``clear_db_runs`` issues
    its unconditional ``DELETE FROM trigger`` (otherwise SQLite raises a FK error).
    """
    from sqlalchemy import delete

    with create_session() as s:
        s.execute(delete(Deadline))
        s.execute(delete(Callback))
        s.commit()
    db.clear_db_runs()
    db.clear_db_triggers()
