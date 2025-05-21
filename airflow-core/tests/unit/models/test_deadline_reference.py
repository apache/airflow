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

import logging
from datetime import datetime
from unittest import mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from airflow.models import DagRun
from airflow.models.deadline_reference import CalculatedDeadline, DeadlineReference, FixedDatetimeDeadline

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "dag_id_1"


def _clean_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_deadline()


@pytest.mark.db_test
class TestDeadlineReference:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    def test_deadline_reference_creation(self):
        """Test that DeadlineReference provides consistent interface and types."""
        # Fixed deadline creation
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)
        assert isinstance(fixed_reference, FixedDatetimeDeadline)
        assert fixed_reference._datetime == DEFAULT_DATE

        # Calculated deadline creation
        calculated_reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        assert isinstance(calculated_reference, CalculatedDeadline)

    @pytest.mark.parametrize(
        "reference, conditions",
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, {"dag_id": DAG_ID}, id="calculated_deadline"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), {}, id="fixed_deadline"),
        ],
    )
    def test_deadline_evaluation(self, reference, conditions):
        """Test that all deadline types evaluate correctly with their required conditions."""
        if isinstance(reference, CalculatedDeadline):
            with mock.patch.object(reference, reference.value) as mock_method:
                mock_method.return_value = DEFAULT_DATE
                result = reference.evaluate_with(**conditions)
        else:
            result = reference.evaluate_with(**conditions)

        assert result == DEFAULT_DATE

    def test_deadline_evaluate_defers_to_evaluate_with(self):
        """Test that evaluate() just calls evaluate_with() for all deadline types."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)
        with mock.patch.object(fixed_reference, "evaluate_with") as mock_evaluate_with:
            mock_evaluate_with.return_value = DEFAULT_DATE
            result = fixed_reference.evaluate()

            mock_evaluate_with.assert_called_once_with()
            assert result == DEFAULT_DATE

        _reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        with pytest.raises(TypeError, match="missing 1 required positional argument"):
            _reference.evaluate()

    @pytest.mark.parametrize(
        "reference",
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
        ],
    )
    def test_calculated_deadline_requires_conditions(self, reference):
        """Test that calculated deadlines raise an appropriate error when called without required conditions."""
        with pytest.raises(TypeError, match="missing 1 required positional argument"):
            reference.evaluate()

    def test_fixed_deadline_ignores_unexpected_kwargs(self, caplog):
        """Test that fixed deadlines ignore unexpected kwargs in evaluate_with and log appropriately."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)

        with caplog.at_level(logging.DEBUG):
            # Should not raise an error and should return the same value
            with_kwargs = fixed_reference.evaluate_with(unexpected="kwargs")
            without_kwargs = fixed_reference.evaluate()

            assert with_kwargs == without_kwargs == DEFAULT_DATE
            assert "Fixed Datetime Deadlines do not accept conditions, ignoring kwargs" in caplog.text


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
        calculated_reference = DeadlineReference.DAGRUN_LOGICAL_DATE

        result = calculated_reference._fetch_from_db(column, session=mock_session, **conditions)

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
        self, mock_session, use_valid_conditions, scalar_side_effect, expected_error, expected_message, caplog
    ):
        """Test database error handling."""
        calculated_reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        model_reference = DagRun.logical_date
        conditions = {"dag_id": "test_dag"} if use_valid_conditions else {"non_existent_column": "some_value"}

        # Configure mock session
        mock_session.scalar.side_effect = scalar_side_effect

        with caplog.at_level(logging.ERROR):
            with pytest.raises(expected_error, match=expected_message):
                calculated_reference._fetch_from_db(model_reference, session=mock_session, **conditions)
            if expected_message:
                assert expected_message in caplog.text
