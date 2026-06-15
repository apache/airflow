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
"""QA tests for build_context_from_dag_run (PR #66608)."""

from __future__ import annotations

import pendulum

from airflow.sdk.api.datamodels._generated import DagRunState, DagRunType
from airflow.sdk.execution_time.comms import DagRunResult
from airflow.sdk.execution_time.context import build_context_from_dag_run


class MockDagRun:
    """Minimal DagRun stand-in for testing build_context_from_dag_run."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def make_dag_run_result(
    *,
    dag_id="test_dag",
    run_id="scheduled__2024-01-15",
    logical_date=None,
    data_interval_start=None,
    data_interval_end=None,
):
    """Helper to build a DagRunResult for tests."""
    return DagRunResult(
        dag_id=dag_id,
        run_id=run_id,
        logical_date=logical_date,
        data_interval_start=data_interval_start,
        data_interval_end=data_interval_end,
        run_after=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.RUNNING,
        consumed_asset_events=[],
    )


class TestBuildContextFromDagRun:
    """
    QA tests for PR #66608 - build_context_from_dag_run helper.

    Tests cover:
    - All expected context fields are present with correct values
    - logical_date=None case (asset-triggered runs)
    - DagRunResult (comms model) as input
    - MockDagRun (generic object) as input
    - Date formatting correctness
    """

    def test_all_context_fields_present_with_correct_values(self):
        """TEST 1: All expected context fields are populated correctly."""
        logical_date = pendulum.datetime(2024, 1, 15, 10, 30, 0)
        data_interval_start = pendulum.datetime(2024, 1, 14, 0, 0, 0)
        data_interval_end = pendulum.datetime(2024, 1, 15, 0, 0, 0)

        dag_run = make_dag_run_result(
            run_id="manual__2024-01-15T10:30:00+00:00",
            logical_date=logical_date,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
        )

        context = build_context_from_dag_run(dag_run)

        # Core run fields
        assert context["run_id"] == "manual__2024-01-15T10:30:00+00:00"
        assert context["dag_run"] is dag_run

        # Date fields
        assert context["ds"] == "2024-01-15", f"Expected ds=2024-01-15, got {context['ds']}"
        assert context["ds_nodash"] == "20240115"
        assert "ts" in context
        assert "ts_nodash" in context
        assert "ts_nodash_with_tz" in context

        # logical_date
        assert context["logical_date"] is not None

        # Interval fields
        assert context["data_interval_start"] is not None
        assert context["data_interval_end"] is not None

    def test_ds_format_is_yyyy_mm_dd(self):
        """TEST 10: Context field values are accurate - ds must be YYYY-MM-DD."""
        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2025, 3, 5, 14, 0, 0),
        )

        context = build_context_from_dag_run(dag_run)

        assert context["ds"] == "2025-03-05", "ds must be zero-padded YYYY-MM-DD"
        assert context["ds_nodash"] == "20250305"
        assert "ts" in context
        # ts should be ISO format
        ts = context["ts"]
        assert "2025-03-05" in ts or "2025" in ts

    def test_logical_date_none_returns_minimal_context(self):
        """TEST: When logical_date is None (asset-triggered), context has only run_id and dag_run."""
        dag_run = make_dag_run_result(
            run_id="asset__2024-01-15",
            logical_date=None,
            data_interval_start=None,
            data_interval_end=None,
        )

        context = build_context_from_dag_run(dag_run)

        # Must have these
        assert context["run_id"] == "asset__2024-01-15"
        assert context["dag_run"] is dag_run

        # Must NOT have date-derived fields
        assert "ds" not in context, "ds must not be present when logical_date is None"
        assert "ts" not in context, "ts must not be present when logical_date is None"
        assert "logical_date" not in context
        assert "data_interval_start" not in context
        assert "data_interval_end" not in context

    def test_context_with_mock_dag_run_object(self):
        """TEST: build_context_from_dag_run works with any duck-typed object."""
        dag_run = MockDagRun(
            run_id="manual__2024-06-01",
            logical_date=pendulum.datetime(2024, 6, 1, 0, 0, 0),
            data_interval_start=pendulum.datetime(2024, 5, 31, 0, 0, 0),
            data_interval_end=pendulum.datetime(2024, 6, 1, 0, 0, 0),
        )

        context = build_context_from_dag_run(dag_run)

        assert context["run_id"] == "manual__2024-06-01"
        assert context["ds"] == "2024-06-01"
        assert context["dag_run"] is dag_run

    def test_dag_run_is_original_object_reference(self):
        """TEST: dag_run in context is the SAME object, not a copy."""
        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        )

        context = build_context_from_dag_run(dag_run)

        # Should be the same reference, not a copy
        assert context["dag_run"] is dag_run

    def test_data_interval_start_and_end_in_context(self):
        """TEST: data_interval_start and data_interval_end are correctly populated."""
        data_start = pendulum.datetime(2024, 1, 14, 0, 0, 0)
        data_end = pendulum.datetime(2024, 1, 15, 0, 0, 0)

        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
            data_interval_start=data_start,
            data_interval_end=data_end,
        )

        context = build_context_from_dag_run(dag_run)

        assert context["data_interval_start"] is not None
        assert context["data_interval_end"] is not None
        # Verify they're timezone-aware pendulum DateTime objects
        assert hasattr(context["data_interval_start"], "tzinfo")
        assert hasattr(context["data_interval_end"], "tzinfo")

    def test_context_keys_match_expected_set(self):
        """TEST: Context contains exactly the expected keys (no extras, no missing)."""
        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
            data_interval_start=pendulum.datetime(2024, 1, 14, 0, 0, 0),
            data_interval_end=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        )

        context = build_context_from_dag_run(dag_run)

        expected_keys = {
            "dag_run",
            "run_id",
            "logical_date",
            "ds",
            "ds_nodash",
            "ts",
            "ts_nodash",
            "ts_nodash_with_tz",
            "data_interval_start",
            "data_interval_end",
        }
        assert set(context.keys()) == expected_keys, (
            f"Context keys mismatch.\n"
            f"Expected: {expected_keys}\n"
            f"Got: {set(context.keys())}\n"
            f"Missing: {expected_keys - set(context.keys())}\n"
            f"Extra: {set(context.keys()) - expected_keys}"
        )

    def test_deadline_id_and_deadline_time_are_not_in_context(self):
        """
        TEST 2: deadline_id and deadline_time are kwargs, NOT inside context.

        The new design puts deadline_id/deadline_time at the top level of callback
        kwargs (passed directly to the function), while context is built from DagRun.
        This test verifies the separation.
        """
        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        )

        context = build_context_from_dag_run(dag_run)

        # deadline_id and deadline_time must NOT be inside the context dict
        assert "deadline_id" not in context
        assert "deadline_time" not in context
        assert "deadline" not in context
