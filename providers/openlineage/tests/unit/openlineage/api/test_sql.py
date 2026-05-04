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

from datetime import datetime, timezone
from unittest import mock

import pytest
from openlineage.client.event_v2 import RunState

from airflow.providers.openlineage.api.sql import emit_query_lineage

_MODULE = "airflow.providers.openlineage.api.sql"
_CORE = "airflow.providers.openlineage.api.core"


def _make_task_instance(task_id: str = "task_id", dr_conf: dict | None = None):
    ti = mock.MagicMock(
        dag_id="dag_id",
        task_id=task_id,
        try_number=1,
        map_index=-1,
        logical_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    dag_run = mock.MagicMock(
        logical_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        clear_number=0,
        run_after=datetime(2024, 1, 1, tzinfo=timezone.utc),
        conf=dr_conf or {},
    )
    ti.dag_run = dag_run
    ti.get_template_context.return_value = {
        "dag_run": dag_run,
        "dag": mock.MagicMock(),
        "task": mock.MagicMock(),
        "task_instance": ti,
    }
    return ti


@pytest.fixture(autouse=True)
def fake_query_counter():
    """
    Replace `next_query_counter_from_context` (as bound in the api.sql module) with a per-test
    in-memory counter. Mirrors the real context-backed behavior (each call returns the next int)
    without needing a live Airflow execution context. Tests that simulate a fresh task context
    can reset the counter mid-test via ``fake_query_counter["counter"] = 0``.
    """
    state = {"counter": 0}

    def _next():
        state["counter"] += 1
        return str(state["counter"])

    with mock.patch(f"{_MODULE}.next_query_counter_from_context", side_effect=_next):
        yield state


@pytest.fixture
def patched_emit():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
    ):
        listener = mock.MagicMock()
        listener_factory.return_value = listener
        yield listener.adapter.emit


def test_emits_start_and_complete_pair(patched_emit):
    ti = _make_task_instance()
    emit_query_lineage(
        query_id="qid-1",
        query_source_namespace="snowflake://ACCT",
        task_instance=ti,
    )

    assert patched_emit.call_count == 2
    start_event = patched_emit.call_args_list[0].args[0]
    end_event = patched_emit.call_args_list[1].args[0]
    assert start_event.eventType == RunState.START
    assert end_event.eventType == RunState.COMPLETE
    assert start_event.run.runId == end_event.run.runId
    assert start_event.job.name == "dag_id.task_id.manual_query.1"
    assert start_event.run.facets["externalQuery"].externalQueryId == "qid-1"
    assert start_event.run.facets["externalQuery"].source == "snowflake://ACCT"
    # Parent is the task run; without explicit root info, root resolves to the DAG run.
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    expected_parent_run_id = OpenLineageAdapter.build_task_instance_run_id(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        try_number=ti.try_number,
        logical_date=ti.logical_date,
        map_index=ti.map_index,
    )
    expected_root_run_id = OpenLineageAdapter.build_dag_run_id(
        dag_id=ti.dag_id,
        logical_date=ti.dag_run.logical_date,
        clear_number=ti.dag_run.clear_number,
    )
    assert start_event.run.facets["parent"].run.runId == expected_parent_run_id
    assert start_event.run.facets["parent"].root.run.runId == expected_root_run_id
    assert start_event.job.facets["jobType"].integration == "AIRFLOW"
    assert start_event.job.facets["jobType"].jobType == "QUERY"


def test_propagates_root_from_dagrun_conf(patched_emit):
    root_uuid = "22222222-2222-2222-2222-222222222222"
    ti = _make_task_instance(
        dr_conf={
            "openlineage": {
                "rootParentRunId": root_uuid,
                "rootParentJobNamespace": "airflow://root",
                "rootParentJobName": "root_dag",
            }
        }
    )
    emit_query_lineage(
        query_id="qid",
        query_source_namespace="snowflake://ACCT",
        task_instance=ti,
    )
    start_event = patched_emit.call_args_list[0].args[0]
    parent_facet = start_event.run.facets["parent"]
    assert parent_facet.root.run.runId == root_uuid
    assert parent_facet.root.job.namespace == "airflow://root"
    assert parent_facet.root.job.name == "root_dag"


def test_parses_sql_text_when_provided(patched_emit):
    ti = _make_task_instance()
    emit_query_lineage(
        query_id="qid",
        query_source_namespace="snowflake://ACCT",
        query_text="SELECT * FROM analytics.public.users",
        task_instance=ti,
    )
    start_event = patched_emit.call_args_list[0].args[0]
    assert start_event.job.facets["sql"].query == "SELECT * FROM analytics.public.users"
    assert any(d.name.endswith("users") for d in start_event.inputs)


def test_increments_job_name_across_calls(patched_emit):
    ti = _make_task_instance()
    emit_query_lineage(query_id="a", query_source_namespace="snowflake://ACCT", task_instance=ti)
    emit_query_lineage(query_id="b", query_source_namespace="snowflake://ACCT", task_instance=ti)
    names = [call.args[0].job.name for call in patched_emit.call_args_list]
    assert "dag_id.task_id.manual_query.1" in names
    assert "dag_id.task_id.manual_query.2" in names


def test_counters_are_per_task(patched_emit, fake_query_counter):
    """Each task has its own context, so the counter resets to 1 in a new task."""
    ti1 = _make_task_instance("task_a")
    ti2 = _make_task_instance("task_b")
    emit_query_lineage(query_id="x", query_source_namespace="snowflake://ACCT", task_instance=ti1)
    # Simulate the second task starting with a fresh execution context.
    fake_query_counter["counter"] = 0
    emit_query_lineage(query_id="y", query_source_namespace="snowflake://ACCT", task_instance=ti2)
    names = {call.args[0].job.name for call in patched_emit.call_args_list}
    assert "dag_id.task_a.manual_query.1" in names
    assert "dag_id.task_b.manual_query.1" in names


def test_emits_fail_event_with_error_message(patched_emit):
    ti = _make_task_instance()
    emit_query_lineage(
        query_id="qid",
        query_source_namespace="snowflake://ACCT",
        is_successful=False,
        error_message="SOMETHING: broke",
        task_instance=ti,
    )
    end_event = patched_emit.call_args_list[1].args[0]
    assert end_event.eventType == RunState.FAIL
    assert end_event.run.facets["errorMessage"].message == "SOMETHING: broke"


def test_uses_explicit_start_and_end_times(patched_emit):
    ti = _make_task_instance()
    start = datetime(2024, 5, 1, 10, 0, tzinfo=timezone.utc)
    end = datetime(2024, 5, 1, 10, 5, tzinfo=timezone.utc)
    emit_query_lineage(
        query_id="qid",
        query_source_namespace="snowflake://ACCT",
        start_time=start,
        end_time=end,
        task_instance=ti,
    )
    start_event = patched_emit.call_args_list[0].args[0]
    end_event = patched_emit.call_args_list[1].args[0]
    assert start_event.eventTime == start.isoformat()
    assert end_event.eventTime == end.isoformat()


def test_merges_additional_facets(patched_emit):
    ti = _make_task_instance()
    extra_run_facet = mock.MagicMock()
    extra_job_facet = mock.MagicMock()
    emit_query_lineage(
        query_id="qid",
        query_source_namespace="snowflake://ACCT",
        additional_run_facets={"custom": extra_run_facet},
        additional_job_facets={"custom": extra_job_facet},
        task_instance=ti,
    )
    start_event = patched_emit.call_args_list[0].args[0]
    assert start_event.run.facets["custom"] is extra_run_facet
    assert start_event.job.facets["custom"] is extra_job_facet


def test_noop_when_openlineage_disabled():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=True),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
    ):
        emit_query_lineage(query_id="x", query_source_namespace="snowflake://ACCT")
        listener_factory.assert_not_called()


def test_resolves_task_instance_from_context(patched_emit):
    ti = _make_task_instance()
    with mock.patch(f"{_MODULE}.get_task_instance_from_context", return_value=ti) as get_ti:
        emit_query_lineage(query_id="qid", query_source_namespace="snowflake://ACCT")
        get_ti.assert_called_once()
    assert patched_emit.call_count == 2


def test_swallows_errors_by_default(patched_emit, caplog):
    """By default, exceptions raised while building the events are logged at WARNING and swallowed."""
    import logging

    ti = _make_task_instance()
    with (
        mock.patch(f"{_MODULE}._create_ol_event_pair", side_effect=RuntimeError("boom")),
        caplog.at_level(logging.WARNING),
    ):
        emit_query_lineage(
            query_id="qid",
            query_source_namespace="snowflake://ACCT",
            task_instance=ti,
        )

    patched_emit.assert_not_called()
    assert "emit_query_lineage raised an error" in caplog.text


def test_swallows_errors_from_context_resolution_by_default(patched_emit, caplog):
    """When task_instance can't be resolved, the failure is swallowed by default."""
    import logging

    with (
        mock.patch(
            f"{_MODULE}.get_task_instance_from_context",
            side_effect=RuntimeError("no context available"),
        ),
        caplog.at_level(logging.WARNING),
    ):
        emit_query_lineage(query_id="qid", query_source_namespace="snowflake://ACCT")

    patched_emit.assert_not_called()
    assert "emit_query_lineage raised an error" in caplog.text


def test_raises_on_error_when_flag_set(patched_emit):
    """With raise_on_error=True, exceptions raised during event construction propagate."""
    ti = _make_task_instance()
    with (
        mock.patch(f"{_MODULE}._create_ol_event_pair", side_effect=RuntimeError("boom")),
        pytest.raises(RuntimeError, match="boom"),
    ):
        emit_query_lineage(
            query_id="qid",
            query_source_namespace="snowflake://ACCT",
            task_instance=ti,
            raise_on_error=True,
        )

    patched_emit.assert_not_called()


def test_raises_on_context_resolution_failure_when_flag_set(patched_emit):
    """With raise_on_error=True, context-resolution failures propagate."""
    with (
        mock.patch(
            f"{_MODULE}.get_task_instance_from_context",
            side_effect=RuntimeError("no context available"),
        ),
        pytest.raises(RuntimeError, match="no context available"),
    ):
        emit_query_lineage(
            query_id="qid",
            query_source_namespace="snowflake://ACCT",
            raise_on_error=True,
        )

    patched_emit.assert_not_called()
