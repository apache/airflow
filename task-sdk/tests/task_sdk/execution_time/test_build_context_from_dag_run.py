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

    def test_deadline_dict_is_exposed_when_passed(self):
        """When a deadline dict is supplied it is exposed as ``context["deadline"]``.

        Both callback paths (executor + triggerer) pass the deadline through this single helper,
        so the resulting context is identical by construction.
        """
        dag_run = make_dag_run_result(
            run_id="test_run",
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        )
        deadline = {"id": "dl-1", "deadline_time": "2024-01-15T01:00:00+00:00"}

        context = build_context_from_dag_run(dag_run, deadline=deadline)

        assert context["deadline"] == deadline
