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
"""Coverage for ``Deadline.prune_deadlines``: on-time/overdue/pending selection,
callback cascade behaviour, and concurrent-mutation edge cases."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
import time_machine
from sqlalchemy import event, select

from airflow.models import DagRun
from airflow.models.callback import Callback
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from tests_common.test_utils.asserts import assert_queries_count
from unit.models import DEFAULT_DATE

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

DAG_ID = "prune_deadlines_test_dag"


async def _prune_test_callback():
    pass


CALLBACK_PATH = f"{__name__}.{_prune_test_callback.__name__}"


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
    def teardown_method():
        _clean_db()

    @pytest.mark.backend("sqlite")
    def test_prune_uses_unmissed_dagrun_index(self, dagrun, session):
        _make_deadline(session, dagrun_id=dagrun.id, deadline_time=DEFAULT_DATE + timedelta(hours=1))
        dagrun_id = dagrun.id
        statements = []

        def capture(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith("SELECT") and "FROM deadline" in statement:
                statements.append((statement, parameters))

        connection = session.connection()
        event.listen(connection, "before_cursor_execute", capture)
        try:
            with assert_queries_count(1, session=session):
                Deadline.prune_deadlines(session=session, conditions={DagRun.id: dagrun_id})
        finally:
            event.remove(connection, "before_cursor_execute", capture)

        assert len(statements) == 1
        statement, parameters = statements[0]
        plan = connection.exec_driver_sql(f"EXPLAIN QUERY PLAN {statement}", parameters).all()
        assert any("deadline_dagrun_id_idx" in row[3] for row in plan), plan

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
