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
from datetime import datetime
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from airflow.models import DagRun
from airflow.models.deadline import Deadline, _fetch_from_db
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.deadline import DeadlineReference
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "dag_id_1"
RUN_ID = 1

TEST_CALLBACK_KWARGS = {"to": "the_boss@work.com"}
TEST_CALLBACK_PATH = f"{__name__}.test_callback"


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


@pytest.mark.db_test
class TestCalculatedDeadlineDatabaseCalls:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    @pytest.mark.parametrize(
        "column, conditions, expected_query",
        [
            pytest.param(
                DagRun.logical_date,
                {"dag_id": DAG_ID},
                "SELECT dag_run.logical_date \nFROM dag_run \nWHERE dag_run.dag_id = :dag_id_1",
                id="single_condition_logical_date",
            ),
            pytest.param(
                DagRun.queued_at,
                {"dag_id": DAG_ID},
                "SELECT dag_run.queued_at \nFROM dag_run \nWHERE dag_run.dag_id = :dag_id_1",
                id="single_condition_queued_at",
            ),
            pytest.param(
                DagRun.logical_date,
                {"dag_id": DAG_ID, "state": "running"},
                "SELECT dag_run.logical_date \nFROM dag_run \nWHERE dag_run.dag_id = :dag_id_1 AND dag_run.state = :state_1",
                id="multiple_conditions",
            ),
        ],
    )
    @mock.patch("sqlalchemy.orm.Session")
    def test_fetch_from_db_success(self, mock_session, column, conditions, expected_query):
        """Test successful database queries."""
        mock_session.scalar.return_value = DEFAULT_DATE

        result = _fetch_from_db(column, session=mock_session, **conditions)

        assert isinstance(result, datetime)
        mock_session.scalar.assert_called_once()

        # Check that the correct query was constructed
        call_args = mock_session.scalar.call_args[0][0]
        assert str(call_args) == expected_query

        # Verify the actual parameter values
        compiled = call_args.compile()
        for key, value in conditions.items():
            # Note that SQLAlchemy appends the _1 to ensure unique template field names
            assert compiled.params[f"{key}_1"] == value

    @pytest.mark.parametrize(
        "use_valid_conditions, scalar_side_effect, expected_error, expected_message",
        [
            pytest.param(
                False,
                mock.DEFAULT,  # This will allow the call to pass through
                AttributeError,
                None,
                id="invalid_attribute",
            ),
            pytest.param(
                True,
                SQLAlchemyError("Database connection failed"),
                SQLAlchemyError,
                "Database connection failed",
                id="database_error",
            ),
            pytest.param(
                True, lambda x: None, ValueError, "No matching record found in the database", id="no_results"
            ),
        ],
    )
    @mock.patch("sqlalchemy.orm.Session")
    def test_fetch_from_db_error_cases(
        self, mock_session, use_valid_conditions, scalar_side_effect, expected_error, expected_message
    ):
        """Test database access error handling."""
        model_reference = DagRun.logical_date
        conditions = {"dag_id": "test_dag"} if use_valid_conditions else {"non_existent_column": "some_value"}

        # Configure mock session
        mock_session.scalar.side_effect = scalar_side_effect

        with pytest.raises(expected_error, match=expected_message):
            _fetch_from_db(model_reference, session=mock_session, **conditions)

    @pytest.mark.parametrize(
        "reference, expected_column",
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, DagRun.logical_date, id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, DagRun.queued_at, id="queued_at"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), None, id="fixed_deadline"),
        ],
    )
    def test_deadline_database_integration(self, reference, expected_column):
        """
        Test database integration for all deadline types.

        Verifies:
        1. Calculated deadlines call _fetch_from_db with correct column.
        2. Fixed deadlines do not interact with database.
        """
        conditions = {"dag_id": DAG_ID}

        with mock.patch("airflow.models.deadline._fetch_from_db") as mock_fetch:
            mock_fetch.return_value = DEFAULT_DATE

            if expected_column is not None:
                result = reference.evaluate_with(**conditions)
                mock_fetch.assert_called_once_with(expected_column, **conditions)
            else:
                result = reference.evaluate_with(**conditions)
                mock_fetch.assert_not_called()

            assert result == DEFAULT_DATE
