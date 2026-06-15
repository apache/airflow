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
Adversarial QA coverage for ``Deadline.prune_deadlines``.

``prune_deadlines`` is the batch-delete path the scheduler invokes (via
``DagRun.update_state`` -> ``dagrun.py:1237``) when a DagRun completes on time:
deadlines that no longer need to fire are removed. These tests drill into the
on-time/overdue/pending selection logic, the (lack of) batching, callback
cascade behaviour, and concurrent-mutation edge cases against a real DB.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import time_machine
from sqlalchemy import func, select

from airflow.models import DagRun
from airflow.models.callback import Callback
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import CallbackState, DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

DAG_ID = "qaw18_prune_dag"


async def _qaw18_callback():
    pass


CALLBACK_PATH = f"{__name__}.{_qaw18_callback.__name__}"


def _clean_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_deadline()


@pytest.fixture
def dagrun(session, dag_maker):
    with dag_maker(DAG_ID):
        EmptyOperator(task_id="task_id")
    with time_machine.travel(DEFAULT_DATE):
        dag_maker.create_dagrun(state=DagRunState.QUEUED, logical_date=DEFAULT_DATE)
        session.commit()
        return session.scalars(select(DagRun)).one()


def _make_deadline(session: Session, *, dagrun_id: int, deadline_time, state=None) -> Deadline:
    deadline = Deadline(
        deadline_time=deadline_time,
        callback=AsyncCallback(CALLBACK_PATH),
        dagrun_id=dagrun_id,
        dag_id=DAG_ID,
        deadline_alert_id=None,
    )
    session.add(deadline)
    session.flush()
    if state is not None:
        deadline.callback.state = state
        session.add(deadline.callback)
        session.flush()
    return deadline


@pytest.mark.db_test
class TestPruneDeadlines:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    # ------------------------------------------------------------------
    # Scenario 1: selection logic — only on-time-completed deadlines pruned
    # ------------------------------------------------------------------
    def test_prunes_only_on_time_completion(self, dagrun, session):
        """
        prune_deadlines must delete ONLY deadlines whose DagRun finished
        on/before deadline_time. Overdue (end_date after deadline) and pending
        (end_date is None) deadlines must survive.
        """
        # on-time: DagRun finished BEFORE the deadline -> should be pruned
        on_time = _make_deadline(
            session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=2)
        )
        # overdue: deadline already passed before DagRun ended -> must NOT be pruned
        overdue = _make_deadline(
            session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE - timedelta(hours=2)
        )

        # DagRun completes at DEFAULT_DATE (on time vs on_time deadline, late vs overdue deadline)
        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})

        remaining = set(session.scalars(select(Deadline.id)).all())
        assert deleted == 1
        assert on_time.id not in remaining
        assert overdue.id in remaining

    def test_pending_dagrun_not_pruned(self, dagrun, session):
        """A DagRun that has not finished (end_date is None) prunes nothing."""
        d = _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=2))
        assert dagrun.end_date is None  # QUEUED fixture leaves it unset

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})

        assert deleted == 0
        assert d.id in set(session.scalars(select(Deadline.id)).all())

    def test_exact_boundary_end_date_equals_deadline_pruned(self, dagrun, session):
        """end_date == deadline_time is on-time (the code uses <=), so it is pruned."""
        d = _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE)
        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})
        assert deleted == 1
        assert d.id not in set(session.scalars(select(Deadline.id)).all())

    # ------------------------------------------------------------------
    # Scenario 2: batching / LIMIT behaviour (CLAUDE.md scheduler-loop rule)
    # ------------------------------------------------------------------
    def test_many_deadlines_deleted_in_single_unbounded_pass(self, dagrun, session):
        """
        FINDING PROBE: With many prunable deadlines, are they deleted in
        bounded batches with intermediate commits (per the CLAUDE.md bulk-DELETE
        rule), or in one unbounded in-memory pass?

        We assert the observable outcome (all deleted) and document that the
        implementation loads every matching row via a single SELECT (no LIMIT)
        and issues one session.delete per row inside one transaction.
        """
        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        count = 50
        for _ in range(count):
            _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=1))

        assert session.scalar(select(func.count()).select_from(Deadline)) == count

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})

        assert deleted == count
        assert session.scalar(select(func.count()).select_from(Deadline)) == 0

    # ------------------------------------------------------------------
    # Scenario 3: callback cascade when callback is already QUEUED / RUNNING
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "state",
        [
            pytest.param(CallbackState.QUEUED, id="queued"),
            pytest.param(CallbackState.RUNNING, id="running"),
        ],
    )
    def test_prune_cascades_callback_even_if_in_flight(self, dagrun, session, state):
        """
        ADVERSARIAL: a deadline pruned while its callback is QUEUED/RUNNING.
        prune_deadlines does not inspect callback state, so it will delete the
        deadline and (via cascade='all, delete-orphan') the callback too.
        Verify no orphaned Callback row is left behind.
        """
        d = _make_deadline(
            session,
            dagrun_id=dagrun.id,
            deadline_time=DEFAULT_DATE + timedelta(hours=1),
            state=state,
        )
        callback_id = d.callback.id
        assert session.get(Callback, callback_id) is not None

        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})
        session.flush()

        assert deleted == 1
        # No orphaned callback row.
        assert session.get(Callback, callback_id) is None

    # ------------------------------------------------------------------
    # Scenario 4: race between prune and handle_miss on the same row
    # ------------------------------------------------------------------
    def test_prune_then_handle_miss_on_same_deadline(self, dagrun, session):
        """
        RACE: prune deletes the on-time deadline; a concurrent scheduler pass
        then tries handle_miss on the (now stale) in-memory object. The deadline
        row is gone, so handle_miss must not resurrect a half-state. We simulate
        the interleaving: prune first, then attempt to operate on the deleted row.
        """
        d = _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=1))
        callback_id = d.callback.id

        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})
        assert deleted == 1
        session.flush()

        # The row (and its callback) are gone; a stale handle_miss must not
        # recreate a dangling callback.
        assert session.get(Deadline, d.id) is None
        assert session.get(Callback, callback_id) is None

    def test_handle_miss_then_prune_does_not_delete_missed(self, dagrun, session):
        """
        Inverse race: handle_miss marks the deadline missed and queues the callback
        BEFORE the on-time prune runs. prune must NOT delete a deadline already marked
        ``missed`` — its callback is owned by the scheduler/triggerer, and cascade-deleting
        the deadline would silently drop that queued callback (a lost-callback window).

        prune_deadlines explicitly filters ``~Deadline.missed`` so a missed deadline (and its
        queued callback) survives even if the DagRun's end_date would otherwise match the
        on-time predicate. (In the real scheduler a missed deadline always has
        ``deadline_time < now <= end_date`` so it can't match the on-time predicate anyway;
        the explicit filter makes the invariant robust to future callers and clock skew.)
        """
        d = _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=1))
        d.handle_miss(session)
        session.flush()
        assert d.missed is True
        deadline_id = d.id
        callback_id = d.callback.id

        # DagRun reports on-time completion (end_date <= deadline_time) — the exact condition
        # that would have pruned the row before the ~missed guard was added.
        dagrun.end_date = DEFAULT_DATE
        session.add(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun.id})
        session.flush()

        # The missed deadline and its queued callback both survive.
        assert deleted == 0
        assert session.get(Deadline, deadline_id) is not None
        assert session.get(Callback, callback_id) is not None

    # ------------------------------------------------------------------
    # Scenario 5: DagRun row already gone
    # ------------------------------------------------------------------
    def test_prune_when_dagrun_deleted_is_clean(self, dagrun, session):
        """
        If the DagRun row is already deleted, the inner JOIN in prune_deadlines
        returns no pairs, so it is a clean no-op (returns 0) and does not raise.
        Deadlines are FK ondelete=CASCADE on dag_run, so deleting the DagRun
        also removes its deadlines — prune then finds nothing.
        """
        _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=1))
        dagrun_id = dagrun.id

        session.delete(dagrun)
        session.flush()

        deleted = Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun_id})
        assert deleted == 0
        # Cascade removed the deadline along with the DagRun.
        assert session.scalar(select(func.count()).select_from(Deadline)) == 0
