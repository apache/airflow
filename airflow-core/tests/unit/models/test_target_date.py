#
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
Tests for the target_date feature end-to-end:
  - DAG-level serialization (callable / date / None)
  - DagRun DB column roundtrip
  - Scheduler helper _evaluate_dag_target_date
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from airflow.sdk import DAG
from airflow.serialization.serialized_objects import DagSerialization


# ---------------------------------------------------------------------------
# Helper callable (module-level so qualname is importable)
# ---------------------------------------------------------------------------
def _last_day_of_prev_month(logical_date, data_interval_start, data_interval_end):
    """Return the last day of the month before logical_date."""
    first = logical_date.replace(day=1)
    from datetime import timedelta

    return (first - timedelta(days=1)).date()


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestTargetDateSerialization:
    def test_none_not_serialized(self):
        """When target_date is None it should not appear in serialized JSON."""
        dag = DAG("test_dag", schedule=None, target_date=None)
        serialized = DagSerialization.serialize_dag(dag)
        assert "target_date" not in serialized
        assert "target_date_callable" not in serialized

    def test_date_value_roundtrip(self):
        """A fixed date should be serialized as ISO string and deserialized back to date."""
        fixed = date(2025, 3, 31)
        dag = DAG("test_dag", schedule=None, target_date=fixed)
        serialized = DagSerialization.serialize_dag(dag)
        assert serialized["target_date"] == "2025-03-31"
        assert "target_date_callable" not in serialized

        deserialized = DagSerialization.deserialize_dag(serialized)
        assert deserialized.target_date == fixed

    def test_callable_roundtrip(self):
        """A callable should be stored as source code and reconstructed on deserialize."""
        dag = DAG("test_dag", schedule=None, target_date=_last_day_of_prev_month)
        serialized = DagSerialization.serialize_dag(dag)
        assert "target_date_callable" in serialized
        assert "target_date" not in serialized

        # Source code (not a qualname) should be stored
        assert "def " in serialized["target_date_callable"]

        deserialized = DagSerialization.deserialize_dag(serialized)
        assert callable(deserialized.target_date)
        # Check the callable gives the right answer
        ld = datetime(2025, 3, 15, tzinfo=timezone.utc)
        result = deserialized.target_date(ld, ld, ld)
        assert result == date(2025, 2, 28)


# ---------------------------------------------------------------------------
# DagRun model tests
# ---------------------------------------------------------------------------


@pytest.mark.db_test
class TestDagRunTargetDateColumn:
    def test_target_date_stored_and_retrieved(self, session):
        """target_date column on DagRun persists through a DB roundtrip."""
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        td = date(2025, 3, 31)
        run = DagRun(
            dag_id="test_dag",
            run_id="test_run_target_date",
            run_type=DagRunType.MANUAL,
            run_after=datetime(2025, 3, 31, tzinfo=timezone.utc),
            triggered_by=DagRunTriggeredByType.REST_API,
            state=DagRunState.QUEUED,
            target_date=td,
        )
        session.add(run)
        session.flush()
        session.expire(run)

        reloaded = session.get(DagRun, run.id)
        assert reloaded is not None
        assert reloaded.target_date == td

    def test_target_date_none_allowed(self, session):
        """target_date column allows NULL."""
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        run = DagRun(
            dag_id="test_dag",
            run_id="test_run_no_target",
            run_type=DagRunType.MANUAL,
            run_after=datetime(2025, 3, 31, tzinfo=timezone.utc),
            triggered_by=DagRunTriggeredByType.REST_API,
            state=DagRunState.QUEUED,
        )
        session.add(run)
        session.flush()
        session.expire(run)

        reloaded = session.get(DagRun, run.id)
        assert reloaded is not None
        assert reloaded.target_date is None


# ---------------------------------------------------------------------------
# Scheduler helper tests
# ---------------------------------------------------------------------------


class TestEvaluateDagTargetDate:
    def test_returns_none_when_no_target_date(self):
        from unittest.mock import MagicMock

        from airflow.jobs.scheduler_job_runner import _evaluate_dag_target_date

        serialized_dag = MagicMock(spec=["target_date"])
        serialized_dag.target_date = None
        assert _evaluate_dag_target_date(serialized_dag, None, None) is None

    def test_returns_fixed_date(self):
        from unittest.mock import MagicMock

        from airflow.jobs.scheduler_job_runner import _evaluate_dag_target_date

        fixed = date(2025, 1, 15)
        serialized_dag = MagicMock(spec=["target_date"])
        serialized_dag.target_date = fixed
        assert _evaluate_dag_target_date(serialized_dag, None, None) == fixed

    def test_evaluates_callable(self):
        from unittest.mock import MagicMock

        from airflow.jobs.scheduler_job_runner import _evaluate_dag_target_date

        serialized_dag = MagicMock(spec=["target_date"])
        serialized_dag.target_date = _last_day_of_prev_month

        ld = datetime(2025, 4, 10, tzinfo=timezone.utc)
        result = _evaluate_dag_target_date(serialized_dag, ld, None)
        assert result == date(2025, 3, 31)

    def test_callable_with_none_logical_date(self):
        """When logical_date is None (asset-triggered), callable receives None and should handle it."""
        from unittest.mock import MagicMock

        from airflow.jobs.scheduler_job_runner import _evaluate_dag_target_date

        def always_today(logical_date, data_interval_start, data_interval_end):
            return date(2025, 1, 1)

        serialized_dag = MagicMock(spec=["target_date"])
        serialized_dag.target_date = always_today
        result = _evaluate_dag_target_date(serialized_dag, None, None)
        assert result == date(2025, 1, 1)
