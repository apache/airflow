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
SQLite, targeting ``handle_miss`` idempotency / latent fragility (duplicate
Trigger creation).
"""

from __future__ import annotations

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagRun, Trigger
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
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
