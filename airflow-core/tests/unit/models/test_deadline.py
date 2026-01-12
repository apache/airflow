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

import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.models import DagRun
from airflow.models.deadline import Deadline, ReferenceModels, _fetch_from_db
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import timezone
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import DeadlineReference, deadline_reference
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

DAG_ID = "dag_id_1"
INVALID_DAG_ID = "invalid_dag_id"
INVALID_RUN_ID = -1

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
    pytest.param(DeadlineReference.AVERAGE_RUNTIME(), id="average_runtime"),
]


async def callback_for_deadline():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


TEST_CALLBACK_PATH = f"{__name__}.{callback_for_deadline.__name__}"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_ASYNC_CALLBACK = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)
TEST_SYNC_CALLBACK = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)

ORIGINAL_DAGRUN_QUEUED = frozenset(DeadlineReference.TYPES.DAGRUN_QUEUED)
ORIGINAL_DAGRUN_CREATED = frozenset(DeadlineReference.TYPES.DAGRUN_CREATED)


def _clean_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_deadline()


def assert_correct_timing(reference, expected_timing):
    assert reference in DeadlineReference.TYPES.DAGRUN
    if expected_timing == DeadlineReference.TYPES.DAGRUN_CREATED:
        assert reference in DeadlineReference.TYPES.DAGRUN_CREATED
        assert reference not in DeadlineReference.TYPES.DAGRUN_QUEUED
    elif expected_timing == DeadlineReference.TYPES.DAGRUN_QUEUED:
        assert reference in DeadlineReference.TYPES.DAGRUN_QUEUED
        assert reference not in DeadlineReference.TYPES.DAGRUN_CREATED


def assert_builtin_types_unchanged(current_queued, current_created):
    for builtin_type in ORIGINAL_DAGRUN_CREATED:
        assert builtin_type in current_created
    for builtin_type in ORIGINAL_DAGRUN_QUEUED:
        assert builtin_type in current_queued


@pytest.fixture
def dagrun(session, dag_maker):
    with dag_maker(DAG_ID):
        EmptyOperator(task_id="TASK_ID")
    with time_machine.travel(DEFAULT_DATE):
        dag_maker.create_dagrun(state=DagRunState.QUEUED, logical_date=DEFAULT_DATE)

        session.commit()
        dag_runs = session.scalars(select(DagRun)).all()
        assert len(dag_runs) == 1
        return dag_runs[0]


@pytest.fixture
def deadline_orm(dagrun, session):
    deadline = Deadline(
        deadline_time=DEFAULT_DATE,
        callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
        dagrun_id=dagrun.id,
    )
    session.add(deadline)
    session.flush()
    return deadline


@pytest.mark.db_test
class TestDeadline:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    @pytest.mark.parametrize(
        "conditions",
        [
            pytest.param({}, id="empty_conditions"),
            pytest.param({Deadline.dagrun_id: -1}, id="no_matches"),
            pytest.param({Deadline.dagrun_id: "valid_placeholder"}, id="single_condition"),
            pytest.param(
                {
                    Deadline.dagrun_id: "valid_placeholder",
                    Deadline.deadline_time: datetime.now() + timedelta(days=365),
                },
                id="multiple_conditions",
            ),
            pytest.param(
                {Deadline.dagrun_id: "valid_placeholder", Deadline.callback: None},
                id="mixed_conditions",
            ),
        ],
    )
    @mock.patch("sqlalchemy.orm.Session")
    def test_prune_deadlines(self, mock_session, conditions, dagrun):
        """Test deadline resolution with various conditions."""
        if Deadline.dagrun_id in conditions:
            if conditions[Deadline.dagrun_id] == "valid_placeholder":
                conditions[Deadline.dagrun_id] = dagrun.id

        expected_result = 1 if conditions else 0
        # Set up the query chain to return a list of (Deadline, DagRun) pairs
        mock_dagrun = mock.Mock(spec=DagRun, end_date=datetime.now())
        mock_deadline = mock.Mock(spec=Deadline, deadline_time=mock_dagrun.end_date + timedelta(days=365))
        mock_query = mock_session.execute.return_value
        mock_query.all.return_value = [(mock_deadline, mock_dagrun)] if conditions else []

        result = Deadline.prune_deadlines(conditions=conditions, session=mock_session)
        assert result == expected_result
        if conditions:
            mock_session.execute.return_value.all.assert_called_once()
            mock_session.delete.assert_called_once_with(mock_deadline)
        else:
            mock_session.query.assert_not_called()

    def test_repr(self, deadline_orm, dagrun):
        assert (
            repr(deadline_orm) == f"[DagRun Deadline] Dag: {DAG_ID} Run: {dagrun.id} needed by "
            f"{DEFAULT_DATE} or run: {deadline_orm.callback}"
        )

    @pytest.mark.db_test
    def test_handle_miss(self, dagrun, session):
        deadline_orm = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
        )
        session.add(deadline_orm)
        session.flush()
        assert not deadline_orm.missed

        with mock.patch.object(deadline_orm.callback, "queue") as mock_queue:
            deadline_orm.handle_miss(session)
            session.flush()
            mock_queue.assert_called_once()

        assert deadline_orm.missed

        callback_kwargs = deadline_orm.callback.data["kwargs"]
        context = callback_kwargs.pop("context")
        assert callback_kwargs == TEST_CALLBACK_KWARGS

        assert context["deadline"]["id"] == deadline_orm.id
        assert context["deadline"]["deadline_time"].timestamp() == deadline_orm.deadline_time.timestamp()
        assert context["dag_run"] == DAGRunResponse.model_validate(dagrun).model_dump(mode="json")


@pytest.mark.db_test
class TestCalculatedDeadlineDatabaseCalls:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    @pytest.mark.parametrize(
        ("column", "conditions", "expected_query"),
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
        ("use_valid_conditions", "scalar_side_effect", "expected_error", "expected_message"),
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
        ("reference", "expected_column"),
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, DagRun.logical_date, id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, DagRun.queued_at, id="queued_at"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), None, id="fixed_deadline"),
            pytest.param(DeadlineReference.AVERAGE_RUNTIME(), None, id="average_runtime"),
        ],
    )
    def test_deadline_database_integration(self, reference, expected_column, session):
        """
        Test database integration for all deadline types.

        Verifies:
        1. Calculated deadlines call _fetch_from_db with correct column.
        2. Fixed deadlines do not interact with database.
        3. Intervals are added to reference times.
        """
        conditions = {"dag_id": DAG_ID, "run_id": "dagrun_1"}
        interval = timedelta(hours=1)
        with mock.patch("airflow.models.deadline._fetch_from_db") as mock_fetch:
            mock_fetch.return_value = DEFAULT_DATE

            if expected_column is not None:
                result = reference.evaluate_with(session=session, interval=interval, **conditions)
                mock_fetch.assert_called_once_with(expected_column, session=session, **conditions)
            elif reference == DeadlineReference.AVERAGE_RUNTIME():
                with mock.patch("airflow._shared.timezones.timezone.utcnow") as mock_utcnow:
                    mock_utcnow.return_value = DEFAULT_DATE
                    # No DAG runs exist, so it should use 24-hour default
                    result = reference.evaluate_with(session=session, interval=interval, dag_id=DAG_ID)
                    mock_fetch.assert_not_called()
                    # Should return None when no DAG runs exist
                    assert result is None
            else:
                result = reference.evaluate_with(session=session, interval=interval)
                mock_fetch.assert_not_called()
                assert result == DEFAULT_DATE + interval

    def test_average_runtime_with_sufficient_history(self, session, dag_maker):
        """Test AverageRuntimeDeadline when enough historical data exists."""
        with dag_maker(DAG_ID):
            EmptyOperator(task_id="test_task")

        # Create 10 completed DAG runs with known durations
        base_time = DEFAULT_DATE
        durations = [3600, 7200, 1800, 5400, 2700, 4500, 3300, 6000, 2400, 4200]

        for i, duration in enumerate(durations):
            logical_date = base_time + timedelta(days=i)
            start_time = logical_date + timedelta(minutes=5)
            end_time = start_time + timedelta(seconds=duration)

            dagrun = dag_maker.create_dagrun(
                logical_date=logical_date, run_id=f"test_run_{i}", state=DagRunState.SUCCESS
            )
            # Manually set start and end times
            dagrun.start_date = start_time
            dagrun.end_date = end_time

        session.commit()

        # Test with default max_runs (10)
        reference = DeadlineReference.AVERAGE_RUNTIME()
        interval = timedelta(hours=1)

        with mock.patch("airflow._shared.timezones.timezone.utcnow") as mock_utcnow:
            mock_utcnow.return_value = DEFAULT_DATE
            result = reference.evaluate_with(session=session, interval=interval, dag_id=DAG_ID)

            # Calculate expected average: sum(durations) / len(durations)
            expected_avg_seconds = sum(durations) / len(durations)
            expected = DEFAULT_DATE + timedelta(seconds=expected_avg_seconds) + interval

            # Compare only up to minutes to avoid sub-second timing issues in CI
            assert result.replace(second=0, microsecond=0) == expected.replace(second=0, microsecond=0)

    def test_average_runtime_with_insufficient_history(self, session, dag_maker):
        """Test AverageRuntimeDeadline when insufficient historical data exists."""
        with dag_maker(DAG_ID):
            EmptyOperator(task_id="test_task")

        # Create only 5 completed DAG runs (less than default max_runs of 10)
        base_time = DEFAULT_DATE
        durations = [3600, 7200, 1800, 5400, 2700]

        for i, duration in enumerate(durations):
            logical_date = base_time + timedelta(days=i)
            start_time = logical_date + timedelta(minutes=5)
            end_time = start_time + timedelta(seconds=duration)

            dagrun = dag_maker.create_dagrun(
                logical_date=logical_date, run_id=f"insufficient_run_{i}", state=DagRunState.SUCCESS
            )
            # Manually set start and end times
            dagrun.start_date = start_time
            dagrun.end_date = end_time

        session.commit()

        reference = DeadlineReference.AVERAGE_RUNTIME()
        interval = timedelta(hours=1)

        with mock.patch("airflow._shared.timezones.timezone.utcnow") as mock_utcnow:
            mock_utcnow.return_value = DEFAULT_DATE
            result = reference.evaluate_with(session=session, interval=interval, dag_id=DAG_ID)

            # Should return None since insufficient runs
            assert result is None

    def test_average_runtime_with_min_runs(self, session, dag_maker):
        """Test AverageRuntimeDeadline with min_runs parameter allowing calculation with fewer runs."""
        with dag_maker(DAG_ID):
            EmptyOperator(task_id="test_task")

        # Create only 3 completed DAG runs
        base_time = DEFAULT_DATE
        durations = [3600, 7200, 1800]  # 1h, 2h, 30min

        for i, duration in enumerate(durations):
            logical_date = base_time + timedelta(days=i)
            start_time = logical_date + timedelta(minutes=5)
            end_time = start_time + timedelta(seconds=duration)

            dagrun = dag_maker.create_dagrun(
                logical_date=logical_date, run_id=f"min_runs_test_{i}", state=DagRunState.SUCCESS
            )
            # Manually set start and end times
            dagrun.start_date = start_time
            dagrun.end_date = end_time

        session.commit()

        # Test with min_runs=2, should work with 3 runs
        reference = DeadlineReference.AVERAGE_RUNTIME(max_runs=10, min_runs=2)
        interval = timedelta(hours=1)

        with mock.patch("airflow._shared.timezones.timezone.utcnow") as mock_utcnow:
            mock_utcnow.return_value = DEFAULT_DATE
            result = reference.evaluate_with(session=session, interval=interval, dag_id=DAG_ID)

            # Should calculate average from 3 runs
            expected_avg_seconds = sum(durations) / len(durations)  # 4200 seconds
            expected = DEFAULT_DATE + timedelta(seconds=expected_avg_seconds) + interval
            # Compare only up to minutes to avoid sub-second timing issues in CI
            assert result.replace(second=0, microsecond=0) == expected.replace(second=0, microsecond=0)

        # Test with min_runs=5, should return None with only 3 runs
        reference = DeadlineReference.AVERAGE_RUNTIME(max_runs=10, min_runs=5)

        with mock.patch("airflow._shared.timezones.timezone.utcnow") as mock_utcnow:
            mock_utcnow.return_value = DEFAULT_DATE
            result = reference.evaluate_with(session=session, interval=interval, dag_id=DAG_ID)
            assert result is None

    def test_average_runtime_min_runs_validation(self):
        """Test that min_runs must be at least 1."""
        with pytest.raises(ValueError, match="min_runs must be at least 1"):
            DeadlineReference.AVERAGE_RUNTIME(max_runs=10, min_runs=0)

        with pytest.raises(ValueError, match="min_runs must be at least 1"):
            DeadlineReference.AVERAGE_RUNTIME(max_runs=10, min_runs=-1)


class TestDeadlineReference:
    """DeadlineReference lives in definitions/deadlines.py but properly testing them requires DB access."""

    DEFAULT_INTERVAL = timedelta(hours=1)
    DEFAULT_ARGS = {"interval": DEFAULT_INTERVAL}

    @pytest.mark.parametrize("reference", REFERENCE_TYPES)
    @pytest.mark.db_test
    def test_deadline_evaluate_with(self, reference, session):
        """Test that all deadline types evaluate correctly with their required conditions."""
        conditions = {
            "dag_id": DAG_ID,
            "run_id": "dagrun_1",
            "unexpected": "param",  # Add an unexpected parameter.
            "extra": "kwarg",  # Add another unexpected parameter.
        }

        with mock.patch.object(reference, "_evaluate_with") as mock_evaluate:
            mock_evaluate.return_value = DEFAULT_DATE

            if reference.required_kwargs:
                result = reference.evaluate_with(**self.DEFAULT_ARGS, session=session, **conditions)
            else:
                result = reference.evaluate_with(**self.DEFAULT_ARGS, session=session)

            # Verify only expected kwargs are passed through.
            expected_kwargs = {k: conditions[k] for k in reference.required_kwargs if k in conditions}
            expected_kwargs["session"] = session

            mock_evaluate.assert_called_once_with(**expected_kwargs)

            assert result == DEFAULT_DATE + self.DEFAULT_INTERVAL

    @pytest.mark.parametrize("reference", REFERENCE_TYPES)
    @pytest.mark.db_test
    def test_deadline_missing_required_kwargs(self, reference, session):
        """Test that deadlines raise appropriate errors for missing required parameters."""
        if reference.required_kwargs:
            with pytest.raises(
                ValueError, match=re.escape(f"{reference.__class__.__name__} is missing required parameters:")
            ) as raised_exception:
                reference.evaluate_with(session=session, **self.DEFAULT_ARGS)

            assert all(substring in str(raised_exception.value) for substring in reference.required_kwargs)
        else:
            # Let the lack of an exception here effectively assert that no exception is raised.
            reference.evaluate_with(session=session, **self.DEFAULT_ARGS)

        for required_param in reference.required_kwargs:
            assert required_param in str(raised_exception.value)

    def test_deadline_reference_creation(self):
        """Test that DeadlineReference provides consistent interface and types."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)
        assert isinstance(fixed_reference, ReferenceModels.FixedDatetimeDeadline)
        assert fixed_reference._datetime == DEFAULT_DATE

        logical_date_reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        assert isinstance(logical_date_reference, ReferenceModels.DagRunLogicalDateDeadline)

        queued_reference = DeadlineReference.DAGRUN_QUEUED_AT
        assert isinstance(queued_reference, ReferenceModels.DagRunQueuedAtDeadline)

        average_runtime_reference = DeadlineReference.AVERAGE_RUNTIME()
        assert isinstance(average_runtime_reference, ReferenceModels.AverageRuntimeDeadline)
        assert average_runtime_reference.max_runs == 10
        assert average_runtime_reference.min_runs == 10

        # Test with custom parameters
        custom_reference = DeadlineReference.AVERAGE_RUNTIME(max_runs=5, min_runs=3)
        assert custom_reference.max_runs == 5
        assert custom_reference.min_runs == 3


class TestCustomDeadlineReference:
    class MyCustomRef(ReferenceModels.BaseDeadlineReference):
        def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
            return timezone.datetime(DEFAULT_DATE)

    class MyInvalidCustomRef:
        pass

    class MyCustomRefWithKwargs(ReferenceModels.BaseDeadlineReference):
        required_kwargs = {"custom_id"}

        def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
            return timezone.datetime(DEFAULT_DATE)

    def setup_method(self):
        self.original_dagrun_created = DeadlineReference.TYPES.DAGRUN_CREATED
        self.original_dagrun_queued = DeadlineReference.TYPES.DAGRUN_QUEUED
        self.original_dagrun = DeadlineReference.TYPES.DAGRUN
        self.original_attrs = set(dir(ReferenceModels))
        self.original_deadline_attrs = set(dir(DeadlineReference))

    def teardown_method(self):
        DeadlineReference.TYPES.DAGRUN_CREATED = self.original_dagrun_created
        DeadlineReference.TYPES.DAGRUN_QUEUED = self.original_dagrun_queued
        DeadlineReference.TYPES.DAGRUN = self.original_dagrun

        for attr in set(dir(ReferenceModels)):
            if attr not in self.original_attrs:
                delattr(ReferenceModels, attr)

        for attr in set(dir(DeadlineReference)):
            if attr not in self.original_deadline_attrs:
                delattr(DeadlineReference, attr)

    @pytest.mark.parametrize(
        "reference",
        [
            pytest.param(MyCustomRef, id="basic_custom_reference"),
            pytest.param(MyCustomRefWithKwargs, id="custom_reference_with_kwargs"),
        ],
    )
    @pytest.mark.parametrize(
        "timing",
        [
            pytest.param(None, id="default_timing"),
            pytest.param(DeadlineReference.TYPES.DAGRUN_CREATED, id="dagrun_created"),
            pytest.param(DeadlineReference.TYPES.DAGRUN_QUEUED, id="dagrun_queued"),
        ],
    )
    def test_register_custom_reference(self, timing, reference):
        if timing is None:
            result = DeadlineReference.register_custom_reference(reference)
            expected_timing = DeadlineReference.TYPES.DAGRUN_CREATED
        else:
            result = DeadlineReference.register_custom_reference(reference, timing)
            expected_timing = timing

        assert result is reference
        assert getattr(ReferenceModels, reference.__name__) is reference
        assert getattr(DeadlineReference, reference.__name__).__class__ is reference

        assert_correct_timing(reference, expected_timing)
        assert_builtin_types_unchanged(
            DeadlineReference.TYPES.DAGRUN_QUEUED, DeadlineReference.TYPES.DAGRUN_CREATED
        )

    def test_register_custom_reference_invalid_inheritance(self):
        with pytest.raises(ValueError, match="must inherit from BaseDeadlineReference"):
            DeadlineReference.register_custom_reference(self.MyInvalidCustomRef)

    def test_register_custom_reference_invalid_timing(self):
        invalid_timing = ("not", "a", "valid", "timing")

        with pytest.raises(
            ValueError,
            match=re.escape(
                f"Invalid deadline reference type {invalid_timing}; "
                f"must be a valid DeadlineReference.TYPES option."
            ),
        ):
            DeadlineReference.register_custom_reference(self.MyCustomRef, invalid_timing)

    def test_custom_reference_discoverable_by_get_reference_class(self):
        DeadlineReference.register_custom_reference(self.MyCustomRef)

        found_class = ReferenceModels.get_reference_class(self.MyCustomRef.__name__)

        assert found_class is self.MyCustomRef


class TestDeadlineReferenceDecorator:
    def setup_method(self):
        self.original_dagrun_created = DeadlineReference.TYPES.DAGRUN_CREATED
        self.original_dagrun_queued = DeadlineReference.TYPES.DAGRUN_QUEUED
        self.original_dagrun = DeadlineReference.TYPES.DAGRUN
        self.original_attrs = set(dir(ReferenceModels))

    def teardown_method(self):
        DeadlineReference.TYPES.DAGRUN_CREATED = self.original_dagrun_created
        DeadlineReference.TYPES.DAGRUN_QUEUED = self.original_dagrun_queued
        DeadlineReference.TYPES.DAGRUN = self.original_dagrun

        for attr in set(dir(ReferenceModels)):
            if attr not in self.original_attrs:
                delattr(ReferenceModels, attr)

    @staticmethod
    def create_decorated_custom_ref():
        @deadline_reference()
        class DecoratedCustomRef(ReferenceModels.BaseDeadlineReference):
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                return timezone.datetime(DEFAULT_DATE)

        return DecoratedCustomRef

    @staticmethod
    def create_decorated_custom_ref_with_kwargs():
        @deadline_reference()
        class DecoratedCustomRefWithKwargs(ReferenceModels.BaseDeadlineReference):
            required_kwargs = {"custom_id"}

            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                return timezone.datetime(DEFAULT_DATE)

        return DecoratedCustomRefWithKwargs

    @staticmethod
    def create_decorated_custom_ref_queued():
        @deadline_reference(DeadlineReference.TYPES.DAGRUN_QUEUED)
        class DecoratedCustomRefQueued(ReferenceModels.BaseDeadlineReference):
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                return timezone.datetime(DEFAULT_DATE)

        return DecoratedCustomRefQueued

    @pytest.mark.parametrize(
        ("reference_factory", "expected_timing"),
        [
            pytest.param(
                create_decorated_custom_ref,
                DeadlineReference.TYPES.DAGRUN_CREATED,
                id="basic_decorated_custom_ref",
            ),
            pytest.param(
                create_decorated_custom_ref_with_kwargs,
                DeadlineReference.TYPES.DAGRUN_CREATED,
                id="decorated_ref_with_kwargs",
            ),
            pytest.param(
                create_decorated_custom_ref_queued,
                DeadlineReference.TYPES.DAGRUN_QUEUED,
                id="decorated_ref_queued",
            ),
        ],
    )
    def test_deadline_reference_decorator(self, reference_factory, expected_timing):
        reference = reference_factory()

        assert getattr(ReferenceModels, reference.__name__) is reference
        assert getattr(DeadlineReference, reference.__name__).__class__ is reference

        assert_correct_timing(reference, expected_timing)
        assert_builtin_types_unchanged(
            DeadlineReference.TYPES.DAGRUN_QUEUED, DeadlineReference.TYPES.DAGRUN_CREATED
        )

    def test_deadline_reference_decorator_with_invalid_class(self):
        """Test that the decorator raises error for invalid classes."""
        with pytest.raises(ValueError, match="InvalidDecoratedRef must inherit from BaseDeadlineReference"):

            @deadline_reference()
            class InvalidDecoratedRef:
                pass

    def test_deadline_reference_decorator_with_invalid_timing(self):
        invalid_timing = ("not", "a", "valid", "timing")

        with pytest.raises(
            ValueError,
            match=re.escape(
                f"Invalid deadline reference type {invalid_timing}; "
                f"must be a valid DeadlineReference.TYPES option."
            ),
        ):

            @deadline_reference(invalid_timing)
            class DecoratedCustomRef(ReferenceModels.BaseDeadlineReference):
                def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                    return timezone.datetime(DEFAULT_DATE)

    @mock.patch.object(DeadlineReference, "register_custom_reference")
    def test_deadline_reference_decorator_calls_register_method(self, mock_register):
        timing = DeadlineReference.TYPES.DAGRUN_QUEUED

        @deadline_reference(timing)
        class DecoratedCustomRef(ReferenceModels.BaseDeadlineReference):
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                return timezone.datetime(DEFAULT_DATE)

        mock_register.assert_called_once_with(DecoratedCustomRef, timing)
