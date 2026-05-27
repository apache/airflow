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

from task_sdk import FAKE_BUNDLE
from uuid6 import uuid7

from airflow.sdk import DAG, BaseOperator, timezone
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.comms import StartupDetails


def get_inline_dag(dag_id: str, task: BaseOperator) -> DAG:
    dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
    task.dag = dag
    return dag


class TestTargetDateInTemplateContext:
    def test_fallback_to_logical_date(self, mocked_parse, make_ti_context, mock_supervisor_comms):
        """When target_date is None the context should fall back to logical_date.date()."""
        task = BaseOperator(task_id="op")
        dag_id = "test_target_date_fallback"
        get_inline_dag(dag_id=dag_id, task=task)

        ti_context = make_ti_context(dag_id=dag_id)

        what = StartupDetails(
            ti=TaskInstance(
                id=uuid7(),
                task_id="op",
                dag_id=dag_id,
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            bundle_info=FAKE_BUNDLE,
            dag_rel_path="",
            ti_context=ti_context,
            start_date=timezone.utcnow(),
            sentry_integration="",
        )
        ti = mocked_parse(what, dag_id, task)
        context = ti.get_template_context()
        # logical_date is "2024-12-01T01:00:00Z" so its .date() is 2024-12-01
        assert context["target_date"] == date(2024, 12, 1)

    def test_dag_level_date_propagated(self, mocked_parse, make_ti_context, mock_supervisor_comms):
        """When DagRun carries a target_date, it should appear in the template context."""
        task = BaseOperator(task_id="op")
        dag_id = "test_target_date_dag_level"
        get_inline_dag(dag_id=dag_id, task=task)

        ti_context = make_ti_context(dag_id=dag_id)
        ti_context.dag_run.target_date = date(2025, 3, 31)

        what = StartupDetails(
            ti=TaskInstance(
                id=uuid7(),
                task_id="op",
                dag_id=dag_id,
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            bundle_info=FAKE_BUNDLE,
            dag_rel_path="",
            ti_context=ti_context,
            start_date=timezone.utcnow(),
            sentry_integration="",
        )
        ti = mocked_parse(what, dag_id, task)
        context = ti.get_template_context()
        assert context["target_date"] == date(2025, 3, 31)

    def test_task_level_callable_overrides_dag_level(
        self, mocked_parse, make_ti_context, mock_supervisor_comms
    ):
        """A task-level target_date callable should receive the DAG-level value and override it."""

        def first_of_month(ctx):
            return ctx["target_date"].replace(day=1)

        task = BaseOperator(task_id="op_with_callable", target_date=first_of_month)
        dag_id = "test_target_date_task_callable"
        get_inline_dag(dag_id=dag_id, task=task)

        ti_context = make_ti_context(dag_id=dag_id)
        ti_context.dag_run.target_date = date(2025, 3, 31)

        what = StartupDetails(
            ti=TaskInstance(
                id=uuid7(),
                task_id="op_with_callable",
                dag_id=dag_id,
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            bundle_info=FAKE_BUNDLE,
            dag_rel_path="",
            ti_context=ti_context,
            start_date=timezone.utcnow(),
            sentry_integration="",
        )
        ti = mocked_parse(what, dag_id, task)
        context = ti.get_template_context()
        assert context["target_date"] == date(2025, 3, 1)

    def test_no_target_date_no_logical_date(self, mocked_parse, make_ti_context, mock_supervisor_comms):
        """When both target_date and logical_date are None, context should have None."""
        task = BaseOperator(task_id="op")
        dag_id = "test_target_date_none"
        get_inline_dag(dag_id=dag_id, task=task)

        ti_context = make_ti_context(dag_id=dag_id)
        ti_context.dag_run.target_date = None
        ti_context.dag_run.logical_date = None

        what = StartupDetails(
            ti=TaskInstance(
                id=uuid7(),
                task_id="op",
                dag_id=dag_id,
                run_id="test_run",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            bundle_info=FAKE_BUNDLE,
            dag_rel_path="",
            ti_context=ti_context,
            start_date=timezone.utcnow(),
            sentry_integration="",
        )
        ti = mocked_parse(what, dag_id, task)
        context = ti.get_template_context()
        assert context["target_date"] is None
