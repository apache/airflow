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

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagRun
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
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
