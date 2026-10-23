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
from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from airflow.models.dag import DagModel
from airflow.models.dag_cancel_condition import DAG_LEVEL_TASK_ID, DagCancelCondition

from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test

DAG_ID = "cancellable"


def build_condition(**overrides: Any) -> DagCancelCondition:
    fields: dict[str, Any] = {
        "dag_id": DAG_ID,
        "task_id": DAG_LEVEL_TASK_ID,
        "webhook": "github",
        "condition_digest": "c" * 64,
        "events": ["pull_request"],
        "match": {"action": ["closed"]},
    }
    return DagCancelCondition(**(fields | overrides))


def count_conditions(session) -> int:
    return session.scalar(select(func.count()).select_from(DagCancelCondition))


class TestDagCancelCondition:
    @pytest.fixture(autouse=True)
    def dag_model(self, testing_dag_bundle, session):
        clear_db_dags()
        session.add(DagModel(dag_id=DAG_ID, bundle_name="testing"))
        session.flush()
        yield
        clear_db_dags()

    def test_a_condition_key_is_stored_once(self, session):
        session.add(build_condition())
        session.flush()
        session.expunge_all()

        session.add(build_condition(events=["push"]))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

    def test_explicit_none_events_are_stored_as_sql_null(self, session):
        session.add(build_condition(events=None))
        session.flush()

        query = (
            select(func.count()).select_from(DagCancelCondition).where(DagCancelCondition.events.is_(None))
        )
        assert session.scalar(query) == 1

    def test_deleting_a_dag_deletes_its_conditions(self, session):
        session.add_all([build_condition(), build_condition(task_id="task", cancel_state="failed")])
        session.flush()

        session.execute(delete(DagModel).where(DagModel.dag_id == DAG_ID))

        assert count_conditions(session) == 0
