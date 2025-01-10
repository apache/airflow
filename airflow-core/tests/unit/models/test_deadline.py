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

import json
from typing import Callable

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagRun
from airflow.models.deadline import Deadline, DeadlineAlert
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "dag_id_1"
RUN_ID = 1

TEST_CALLBACK_KWARGS = {"to": "the_boss@work.com"}
TEST_CALLBACK_PATH = f"{__name__}.test_callback"


def test_callback():
    """An empty Callable to use for the callback tests in this suite."""
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
        assert session.query(DagRun).count() == 1

        return session.query(DagRun).one()


@pytest.mark.db_test
class TestDeadline:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    def test_add_deadline(self, dagrun, session):
        assert session.query(Deadline).count() == 0
        deadline_orm = Deadline(
            deadline=DEFAULT_DATE,
            callback=TEST_CALLBACK_PATH,
            callback_kwargs=TEST_CALLBACK_KWARGS,
            dag_id=DAG_ID,
            dagrun_id=dagrun.id,
        )

        Deadline.add_deadline(deadline_orm)

        assert session.query(Deadline).count() == 1

        result = session.scalars(select(Deadline)).first()
        assert result.dag_id == deadline_orm.dag_id
        assert result.dagrun_id == deadline_orm.dagrun_id
        assert result.deadline == deadline_orm.deadline
        assert result.callback == deadline_orm.callback
        assert result.callback_kwargs == deadline_orm.callback_kwargs

    def test_orm(self):
        deadline_orm = Deadline(
            deadline=DEFAULT_DATE,
            callback=TEST_CALLBACK_PATH,
            callback_kwargs=TEST_CALLBACK_KWARGS,
            dag_id=DAG_ID,
            dagrun_id=RUN_ID,
        )

        assert deadline_orm.deadline == DEFAULT_DATE
        assert deadline_orm.callback == TEST_CALLBACK_PATH
        assert deadline_orm.callback_kwargs == TEST_CALLBACK_KWARGS
        assert deadline_orm.dag_id == DAG_ID
        assert deadline_orm.dagrun_id == RUN_ID

    def test_repr_with_callback_kwargs(self):
        deadline_orm = Deadline(
            deadline=DEFAULT_DATE,
            callback=TEST_CALLBACK_PATH,
            callback_kwargs=TEST_CALLBACK_KWARGS,
            dag_id=DAG_ID,
            dagrun_id=RUN_ID,
        )

        assert (
            repr(deadline_orm)
            == f"[DagRun Deadline] Dag: {deadline_orm.dag_id} Run: {deadline_orm.dagrun_id} needed by "
            f"{deadline_orm.deadline} or run: {TEST_CALLBACK_PATH}({json.dumps(deadline_orm.callback_kwargs)})"
        )

    def test_repr_without_callback_kwargs(self):
        deadline_orm = Deadline(
            deadline=DEFAULT_DATE,
            callback=TEST_CALLBACK_PATH,
            dag_id=DAG_ID,
            dagrun_id=RUN_ID,
        )

        assert deadline_orm.callback_kwargs is None
        assert (
            repr(deadline_orm)
            == f"[DagRun Deadline] Dag: {deadline_orm.dag_id} Run: {deadline_orm.dagrun_id} needed by "
            f"{deadline_orm.deadline} or run: {TEST_CALLBACK_PATH}()"
        )


class TestDeadlineAlert:
    @pytest.mark.parametrize(
        "callback_value, expect_success",
        [
            pytest.param(test_callback, True, id="valid_callable"),
            pytest.param(TEST_CALLBACK_PATH, True, id="valid_path"),
            pytest.param("bad.path.to.some.callback", False, id="invalid_path"),
            pytest.param(42, False, id="not_even_a_path"),
        ],
    )
    def test_get_callback_path(self, callback_value: Callable | str, expect_success: bool):
        if expect_success:
            path = DeadlineAlert.get_callback_path(callback_value)

            assert path == TEST_CALLBACK_PATH
        else:
            with pytest.raises(ValueError, match="callback is not a path to a callable"):
                DeadlineAlert.get_callback_path(callback_value)
