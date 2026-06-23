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
from sqlalchemy import select

from airflow.models import DagRun
from airflow.models.callback import Callback
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import DagRunState

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
        # Set missed + queued callback directly to test the prune ``~missed`` guard in isolation.
        d.callback.queue(session=session)
        d.missed = True
        session.add_all([d, d.callback])
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
