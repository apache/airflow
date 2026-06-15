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
"""Tests for Deadline.handle_miss re-entrancy and pre-populated routing data (PR #66608)."""

from __future__ import annotations

from unittest import mock

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagRun
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "handle_miss_reentrancy_dag"
TEST_CALLBACK_PATH = "unit.models.test_deadline_handle_miss_reentrancy.callback_for_deadline"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}


async def callback_for_deadline():
    pass


def _clean_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_deadline()


@pytest.fixture
def dagrun(session, dag_maker):
    with dag_maker(DAG_ID):
        EmptyOperator(task_id="TASK_ID")
    with time_machine.travel(DEFAULT_DATE):
        dag_maker.create_dagrun(state=DagRunState.QUEUED, logical_date=DEFAULT_DATE)
        session.commit()
        return session.scalars(select(DagRun)).all()[0]


@pytest.mark.db_test
class TestHandleMissReentrancy:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    def _make_deadline(self, dagrun, session, *, preset_data=None):
        deadline = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
        )
        if preset_data:
            deadline.callback.data.update(preset_data)
        session.add(deadline)
        session.flush()
        return deadline

    def test_empty_data_handle_miss_sets_correct_top_level_fields(self, dagrun, session):
        d = self._make_deadline(dagrun, session)
        with mock.patch.object(d.callback, "queue"):
            d.handle_miss(session)
            session.flush()
        assert d.callback.data["dag_id"] == dagrun.dag_id
        assert d.callback.data["run_id"] == dagrun.run_id
        assert d.callback.data["deadline_id"] == str(d.id)
        assert d.callback.data["deadline_time"] == d.deadline_time.isoformat()
        assert d.callback.data["kwargs"] == TEST_CALLBACK_KWARGS

    def test_prepopulated_stale_routing_data_is_overwritten(self, dagrun, session):
        """callback.data already carries stale dag_id/run_id -> handle_miss overwrites
        them with the current dagrun's values (not the stale ones)."""
        d = self._make_deadline(
            dagrun,
            session,
            preset_data={"dag_id": "STALE_DAG", "run_id": "STALE_RUN"},
        )
        with mock.patch.object(d.callback, "queue"):
            d.handle_miss(session)
            session.flush()
        # Must reflect the real dagrun, not the stale preset
        assert d.callback.data["dag_id"] == dagrun.dag_id
        assert d.callback.data["run_id"] == dagrun.run_id
        assert d.callback.data["dag_id"] != "STALE_DAG"
        assert d.callback.data["run_id"] != "STALE_RUN"

    def test_double_handle_miss_is_idempotent_on_routing_fields(self, dagrun, session):
        """Calling handle_miss twice (re-entrancy) keeps correct top-level fields and
        flips missed=True without corrupting routing data."""
        d = self._make_deadline(dagrun, session)
        with mock.patch.object(d.callback, "queue") as mock_queue:
            d.handle_miss(session)
            session.flush()
            first_deadline_id = d.callback.data["deadline_id"]
            d.handle_miss(session)
            session.flush()
        assert d.missed is True
        assert d.callback.data["dag_id"] == dagrun.dag_id
        assert d.callback.data["run_id"] == dagrun.run_id
        # Routing identifiers stay stable across the (guarded) re-entrant call.
        assert d.callback.data["deadline_id"] == first_deadline_id
        # The idempotency guard means the second call short-circuits before re-queueing.
        assert mock_queue.call_count == 1

    def test_second_handle_miss_does_not_recreate_trigger_or_reset_state(self, dagrun, session):
        """The ``if self.missed: return`` guard makes handle_miss self-protecting. With the REAL
        (un-mocked) ``queue()``, a second call would otherwise replace ``self.trigger`` with a
        brand-new Trigger (orphaning the first) and reset the callback state — resurrecting a
        callback that may already be progressing. The guard must short-circuit the second call so
        the Trigger identity and state are preserved.
        """
        from airflow.models import Trigger

        d = self._make_deadline(dagrun, session)

        d.handle_miss(session)
        session.flush()
        first_trigger = d.callback.trigger
        assert first_trigger is not None
        first_trigger_id = first_trigger.id
        assert d.missed is True

        trigger_count_after_first = len(session.scalars(select(Trigger)).all())

        # Second call must be a no-op (guard returns early) — no new Trigger, same identity.
        d.handle_miss(session)
        session.flush()

        assert d.callback.trigger is not None
        assert d.callback.trigger.id == first_trigger_id, "guard must not replace the Trigger"
        assert len(session.scalars(select(Trigger)).all()) == trigger_count_after_first, (
            "second handle_miss must not create a duplicate Trigger"
        )

    def test_routing_data_persists_across_commit_and_refetch_executor(self, dagrun, session):
        """handle_miss must ``flag_modified`` the JSON ``data`` column so the routing keys it
        writes actually persist. The other tests assert only the in-memory dict (which holds the
        mutation regardless of flag_modified); this one commits, expires, and RE-FETCHES the row
        so it would fail if flag_modified were missing and the JSON mutation were silently dropped.
        Uses an ExecutorCallback because its ``dag_run_id`` routing key is required by
        ``_enqueue_executor_callbacks`` at pickup — losing it would break context-building.
        """
        from airflow.models.callback import Callback, ExecutorCallback
        from airflow.sdk.definitions.callback import SyncCallback

        deadline = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=SyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
        )
        session.add(deadline)
        session.flush()
        callback_id = deadline.callback.id
        assert isinstance(deadline.callback, ExecutorCallback)

        deadline.handle_miss(session)
        session.commit()
        session.expire_all()  # drop all in-memory state — force a fresh DB read

        refetched = session.get(Callback, callback_id)
        # These routing keys must have survived the JSON-column persist (flag_modified working).
        assert refetched.data["dag_id"] == dagrun.dag_id
        assert refetched.data["run_id"] == dagrun.run_id
        assert refetched.data["deadline_id"] == str(deadline.id)
        assert refetched.data["deadline_time"] == DEFAULT_DATE.isoformat()
        # dag_run_id is the executor-specific routing key (consumed by _enqueue_executor_callbacks).
        assert refetched.data["dag_run_id"] == str(dagrun.id)
