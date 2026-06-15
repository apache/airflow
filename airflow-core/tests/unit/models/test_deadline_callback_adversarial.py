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
Adversarial QA wave 7 for the deadline-callback feature.

These tests exercise the *real* models / scheduler-query / handle_event code on
SQLite, targeting behaviours prior waves could only reason about statically:

1. ``handle_miss`` idempotency / latent fragility (duplicate Trigger creation).
2. 3+ deadlines with mixed references on one DagRun -> distinct callbacks.
3. Complex ``callback_kwargs`` round-trip through ExtendedJSON.
4. ``handle_event`` with exotic bodies (set, bytes, pydantic, circular ref).
5. Repeated non-terminal events then terminalisation.
6. ``deadline_time`` boundary semantics at the scheduler query level.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone as dt_timezone

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagRun, Trigger
from airflow.models.callback import (
    ACTIVE_STATES,
    TERMINAL_STATES,
    Callback,
    TriggererCallback,
)
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.triggers.base import TriggerEvent
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY
from airflow.utils.state import CallbackState, DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test]


async def qaw7_callback():
    """Awaitable used as the callback target for these tests."""
    return None


TEST_CALLBACK_PATH = f"{__name__}.{qaw7_callback.__name__}"
DAG_ID = "qaw7_dag"


def _clean_db():
    # Order matters for SQLite FK enforcement: a Callback may reference a Trigger
    # (callback.trigger_id -> trigger.id), and clear_db_runs() deletes Trigger rows.
    # Delete deadlines and callbacks first so no callback still references a trigger
    # when clear_db_runs() issues DELETE FROM trigger.
    db.clear_db_deadline()
    db.clear_db_callbacks()
    db.clear_db_triggers()
    db.clear_db_runs()
    db.clear_db_dags()


@pytest.fixture
def session():
    from airflow.utils.session import create_session

    with create_session() as session:
        yield session


@pytest.fixture
def dagrun(session, dag_maker):
    _clean_db()
    with dag_maker(DAG_ID):
        EmptyOperator(task_id="task")
    with time_machine.travel(DEFAULT_DATE):
        dag_maker.create_dagrun(state=DagRunState.QUEUED, logical_date=DEFAULT_DATE)
        session.commit()
        dr = session.scalars(select(DagRun)).all()
        assert len(dr) == 1
        yield dr[0]
    _clean_db()


def _make_deadline(dagrun, deadline_time, kwargs=None):
    return Deadline(
        deadline_time=deadline_time,
        callback=AsyncCallback(TEST_CALLBACK_PATH, kwargs or {"arg": "v"}),
        dagrun_id=dagrun.id,
        dag_id=dagrun.dag_id,
        deadline_alert_id=None,
    )


# ---------------------------------------------------------------------------
# 1. handle_miss idempotency / latent fragility
# ---------------------------------------------------------------------------
class TestHandleMissIdempotency:
    def test_double_handle_miss_does_not_duplicate_trigger_or_orphan_first(self, dagrun, session):
        """
        Calling ``handle_miss`` twice on the *same in-memory object* (simulating a
        re-evaluation / retry-after-flush-before-commit) must NOT double-create a
        Trigger. ``handle_miss`` carries an idempotency guard (``if self.missed:
        return``): the first call flips ``missed=True`` and queues exactly one
        Trigger, the second call short-circuits before re-running ``queue()``.

        Wave 6 predicted the latent fragility that an unguarded ``queue()`` would
        mint a fresh ``Trigger.from_object(...)`` on every call and orphan the first;
        the guard added in ``Deadline.handle_miss`` is what makes that unreachable.
        """
        deadline = _make_deadline(dagrun, DEFAULT_DATE)
        session.add(deadline)
        session.flush()

        deadline.handle_miss(session)
        session.flush()
        first_trigger_id = deadline.callback.trigger_id
        assert first_trigger_id is not None
        assert deadline.callback.state == CallbackState.QUEUED
        assert deadline.missed is True

        # Second call on the same object — the early-return guard short-circuits.
        deadline.handle_miss(session)
        session.flush()
        second_trigger_id = deadline.callback.trigger_id

        # No new Trigger was minted; the callback still points at the original.
        assert second_trigger_id is not None
        assert second_trigger_id == first_trigger_id, (
            "Idempotency guard: handle_miss must not re-queue / mint a second Trigger"
        )

        all_triggers = session.scalars(select(Trigger)).all()
        # Exactly one Trigger row exists — no orphan left behind.
        assert len(all_triggers) == 1
        assert all_triggers[0].id == first_trigger_id

    def test_scheduler_query_transaction_boundary_prevents_reselection(self, dagrun, session):
        """
        Reachability guard: the scheduler selects ``~Deadline.missed`` rows, calls
        ``handle_miss`` (which sets ``missed=True``), and commits. After commit the
        same row no longer matches the ``~missed`` predicate, so it cannot be
        re-selected and re-handled. This is the transaction boundary that makes the
        double-create above unreachable through the real scheduler loop.
        """
        deadline = _make_deadline(dagrun, DEFAULT_DATE - timedelta(minutes=5))
        session.add(deadline)
        session.commit()

        scheduler_query = (
            select(Deadline)
            .where(Deadline.deadline_time < datetime.now(dt_timezone.utc))
            .where(~Deadline.missed)
        )

        # First scheduler pass: row is selected and handled.
        selected = session.scalars(scheduler_query).all()
        assert len(selected) == 1
        selected[0].handle_miss(session)
        session.commit()

        # Second scheduler pass: same predicate no longer matches the now-missed row.
        selected_again = session.scalars(scheduler_query).all()
        assert selected_again == [], (
            "Once missed=True is committed, the ~missed predicate excludes the row, "
            "so handle_miss is never reached twice via the scheduler path."
        )
        assert len(session.scalars(select(Trigger)).all()) == 1


# ---------------------------------------------------------------------------
# 2. 3+ deadlines, mixed references, one DagRun
# ---------------------------------------------------------------------------
class TestMultipleDeadlinesOneDagRun:
    def test_three_mixed_deadlines_yield_distinct_callbacks(self, dagrun, session):
        """
        Three deadlines on one DagRun (two QUEUED_AT-like at different intervals, one
        LOGICAL_DATE-like) all miss -> three distinct callbacks with distinct
        deadline_id/deadline_time and no kwargs cross-contamination.
        """
        configs = [
            (DEFAULT_DATE + timedelta(minutes=5), {"label": "queued_5m", "n": 1}),
            (DEFAULT_DATE + timedelta(minutes=30), {"label": "queued_30m", "n": 2}),
            (DEFAULT_DATE, {"label": "logical", "n": 3}),
        ]
        deadlines = []
        for dt, kw in configs:
            d = _make_deadline(dagrun, dt, kwargs=kw)
            session.add(d)
            deadlines.append(d)
        session.flush()

        for d in deadlines:
            d.handle_miss(session)
        session.flush()

        callbacks = session.scalars(select(Callback)).all()
        assert len(callbacks) == 3

        # Distinct deadline_id and deadline_time, no kwargs bleed.
        deadline_ids = {c.data["deadline_id"] for c in callbacks}
        deadline_times = {c.data["deadline_time"] for c in callbacks}
        labels = {c.data["kwargs"]["label"] for c in callbacks}
        assert len(deadline_ids) == 3
        assert len(deadline_times) == 3
        assert labels == {"queued_5m", "queued_30m", "logical"}

        # Cross-check each callback's deadline_id/time match its own Deadline row.
        for d in deadlines:
            cb = d.callback
            assert cb.data["deadline_id"] == str(d.id)
            assert cb.data["deadline_time"] == d.deadline_time.isoformat()
            assert cb.data["run_id"] == dagrun.run_id
            assert cb.trigger_id is not None

        # Each callback has its own distinct Trigger.
        trigger_ids = {c.trigger_id for c in callbacks}
        assert len(trigger_ids) == 3


# ---------------------------------------------------------------------------
# 3. Complex callback_kwargs round-trip through ExtendedJSON
# ---------------------------------------------------------------------------
class TestCallbackKwargsRoundTrip:
    def test_complex_kwargs_survive_extendedjson_roundtrip(self, dagrun, session):
        complex_kwargs = {
            "nested": {"a": [1, 2, {"deep": True}], "b": {"c": None}},
            "ts": datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt_timezone.utc),
            "none_val": None,
            "flag_true": True,
            "flag_false": False,
            "big_int": 2**62,
        }
        cb = TriggererCallback(AsyncCallback(TEST_CALLBACK_PATH, kwargs=complex_kwargs))
        session.add(cb)
        session.commit()
        cb_id = cb.id
        session.expunge_all()

        reloaded = session.get(Callback, cb_id)
        round_tripped = reloaded.data["kwargs"]

        assert round_tripped["nested"] == {"a": [1, 2, {"deep": True}], "b": {"c": None}}
        assert round_tripped["none_val"] is None
        assert round_tripped["flag_true"] is True
        assert round_tripped["flag_false"] is False
        assert round_tripped["big_int"] == 2**62
        # ExtendedJSON encodes datetime; it must round-trip back to the same instant.
        ts = round_tripped["ts"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        assert ts == complex_kwargs["ts"]


# ---------------------------------------------------------------------------
# 4. handle_event exotic bodies (exercises the json-coercion + uncoercible guard)
# ---------------------------------------------------------------------------
class _Pydantic:
    """Stand-in for a pydantic-like object (no special json support)."""

    def __init__(self, x):
        self.x = x

    def __repr__(self):
        return f"_Pydantic(x={self.x!r})"


class TestHandleEventExoticBodies:
    @pytest.mark.parametrize(
        "make_body",
        [
            pytest.param(lambda: {1, 2, 3}, id="set"),
            pytest.param(lambda: b"raw-bytes", id="bytes"),
            pytest.param(lambda: _Pydantic(42), id="pydantic_like"),
            pytest.param(lambda: _circular_dict(), id="circular_ref"),
            pytest.param(lambda: _Uncoercible(), id="uncoercible_str_raises"),
        ],
    )
    def test_exotic_success_body_always_yields_string_and_never_raises(self, dagrun, session, make_body):
        body = make_body()
        cb = TriggererCallback(AsyncCallback(TEST_CALLBACK_PATH, kwargs={}))
        cb.queue(session=session)
        session.add(cb)
        session.flush()

        event = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: body})
        # Must NOT raise — the triggerer run loop has no surrounding guard.
        cb.handle_event(event, session)

        assert cb.state == CallbackState.SUCCESS
        assert isinstance(cb.output, str), f"output must be a string, got {type(cb.output)}"
        # Must persist to the Text column without adapter errors.
        session.commit()
        assert isinstance(session.get(Callback, cb.id).output, str)


def _circular_dict():
    d: dict = {"self": None}
    d["self"] = d
    return d


class _Uncoercible:
    """A value whose str()/repr() raise — forces the final fallback branch."""

    def __repr__(self):
        raise RuntimeError("repr boom")

    def __str__(self):
        raise RuntimeError("str boom")


# ---------------------------------------------------------------------------
# 5. Repeated non-terminal events then terminalisation
# ---------------------------------------------------------------------------
class TestRepeatedEvents:
    def test_repeated_running_then_success_terminalises_once(self, dagrun, session):
        cb = TriggererCallback(AsyncCallback(TEST_CALLBACK_PATH, kwargs={}))
        cb.queue(session=session)
        session.add(cb)
        session.flush()
        assert cb.state == CallbackState.QUEUED
        assert cb.trigger is not None

        running = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
        cb.handle_event(running, session)
        assert cb.state == CallbackState.RUNNING
        assert cb.trigger is not None  # non-terminal: trigger retained
        assert cb.output is None

        cb.handle_event(running, session)
        assert cb.state == CallbackState.RUNNING
        assert cb.trigger is not None
        assert cb.output is None

        success = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: "done"})
        cb.handle_event(success, session)
        assert cb.state == CallbackState.SUCCESS
        assert cb.trigger is None  # terminal: trigger detached exactly once
        assert cb.output == "done"
        session.commit()

    def test_active_and_terminal_state_sets_are_disjoint(self):
        """Guard: no state is simultaneously active and terminal (would corrupt
        the terminalisation branch in handle_event)."""
        assert frozenset() == ACTIVE_STATES & TERMINAL_STATES


# ---------------------------------------------------------------------------
# 6. deadline_time boundary at the scheduler query level
# ---------------------------------------------------------------------------
class TestDeadlineTimeBoundary:
    def test_strict_less_than_boundary_and_future_exclusion(self, dagrun, session):
        """
        The scheduler uses ``deadline_time < now`` (strict). A deadline exactly equal
        to ``now`` must NOT be selected; one in the past must; one in the far future
        must not.
        """
        now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=dt_timezone.utc)

        past = _make_deadline(dagrun, now - timedelta(seconds=1))
        equal = _make_deadline(dagrun, now)
        future = _make_deadline(dagrun, now + timedelta(days=365))
        for d in (past, equal, future):
            session.add(d)
        session.commit()

        query = select(Deadline).where(Deadline.deadline_time < now).where(~Deadline.missed)
        selected = session.scalars(query).all()
        selected_ids = {d.id for d in selected}

        assert past.id in selected_ids, "past deadline must be selected"
        assert equal.id not in selected_ids, "deadline == now must NOT be selected (strict <)"
        assert future.id not in selected_ids, "future deadline must NOT be selected"
        assert len(selected_ids) == 1
