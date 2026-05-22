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
"""Tests for target_date template context in the task runner (telescope model)."""

from __future__ import annotations

from datetime import date

from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, TIRunContext

# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


def _make_dag_run(target_date=None, logical_date="2025-04-10T00:00:00Z"):
    return DagRun(
        dag_id="test_dag",
        run_id="test_run",
        logical_date=logical_date,
        data_interval_start=logical_date,
        data_interval_end=logical_date,
        clear_number=0,
        start_date=logical_date,
        run_type="manual",
        run_after=logical_date,
        state=DagRunState.RUNNING,
        consumed_asset_events=[],
        target_date=target_date,
    )


def _make_ti_context(target_date=None, logical_date="2025-04-10T00:00:00Z"):
    return TIRunContext(
        dag_run=_make_dag_run(target_date=target_date, logical_date=logical_date),
        task_reschedule_count=0,
        max_tries=0,
        should_retry=False,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTargetDateInTemplateContext:
    def test_fallback_to_logical_date(self, create_runtime_ti, mock_supervisor_comms):
        """When target_date is None the context should fall back to logical_date.date()."""
        ti_context = _make_ti_context(target_date=None, logical_date="2025-04-10T00:00:00Z")
        runtime_ti = create_runtime_ti(ti_context=ti_context)
        context = runtime_ti.get_template_context()
        assert context["target_date"] == date(2025, 4, 10)

    def test_dag_level_date_propagated(self, create_runtime_ti, mock_supervisor_comms):
        """When DagRun carries a target_date, it should appear in the template context."""
        td = date(2025, 3, 31)
        ti_context = _make_ti_context(target_date=td, logical_date="2025-04-10T00:00:00Z")
        runtime_ti = create_runtime_ti(ti_context=ti_context)
        context = runtime_ti.get_template_context()
        assert context["target_date"] == td

    def test_task_level_callable_overrides_dag_level(self, create_runtime_ti, mock_supervisor_comms):
        """A task-level target_date callable should receive the DAG-level value and override it."""
        dag_td = date(2025, 3, 31)

        def first_of_month(ctx):
            # derives from DAG-level value
            return ctx["target_date"].replace(day=1)

        ti_context = _make_ti_context(target_date=dag_td, logical_date="2025-04-10T00:00:00Z")
        runtime_ti = create_runtime_ti(ti_context=ti_context)
        # Attach the callable to the task
        runtime_ti.task.target_date = first_of_month
        context = runtime_ti.get_template_context()
        assert context["target_date"] == date(2025, 3, 1)

    def test_no_target_date_no_logical_date(self, create_runtime_ti, mock_supervisor_comms):
        """When both target_date and logical_date are None, context should have None."""
        ti_context = TIRunContext(
            dag_run=DagRun(
                dag_id="test_dag",
                run_id="test_run",
                logical_date=None,
                data_interval_start=None,
                data_interval_end=None,
                clear_number=0,
                start_date=None,
                run_type="manual",
                run_after="2025-04-10T00:00:00Z",
                state=DagRunState.RUNNING,
                consumed_asset_events=[],
                target_date=None,
            ),
            task_reschedule_count=0,
            max_tries=0,
            should_retry=False,
        )
        runtime_ti = create_runtime_ti(ti_context=ti_context)
        context = runtime_ti.get_template_context()
        assert context["target_date"] is None
