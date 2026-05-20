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
from openlineage.client.event_v2 import Dataset, RunEvent, RunState

from airflow.providers.openlineage.api.datasets import emit_dataset_lineage
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

_MODULE = "airflow.providers.openlineage.api.datasets"
_CORE = "airflow.providers.openlineage.api.core"
_UTILS = "airflow.providers.openlineage.utils.utils"


def _make_task_instance():
    ti = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=-1,
        logical_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    dag_run = mock.MagicMock(
        logical_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        clear_number=0,
        run_after=datetime(2024, 1, 1, tzinfo=timezone.utc),
        conf={},
        data_interval_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
        data_interval_end=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    task = mock.MagicMock(
        owner="alice,bob", doc=None, doc_md=None, doc_json=None, doc_yaml=None, doc_rst=None
    )
    dag = mock.MagicMock(dag_id="dag_id", owner="dag_owner", tags=["t1", "t2"], doc_md=None, description=None)
    task.dag = dag
    ti.get_template_context.return_value = {
        "dag_run": dag_run,
        "dag": dag,
        "task": task,
        "task_instance": ti,
    }
    ti.dag_run = dag_run
    ti.task = task
    return ti


@pytest.fixture
def patched_emit():
    # The facet-building helpers are composed inside build_task_event_run_facets in utils.utils,
    # so they must be patched at their source module — patching at the datasets module would
    # leave the composed helpers calling the real implementations.
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
        mock.patch(
            f"{_UTILS}.get_airflow_run_facet",
            return_value={"airflow": mock.MagicMock()},
        ),
        mock.patch(f"{_UTILS}.get_user_provided_run_facets", return_value={}),
        mock.patch(f"{_UTILS}.get_airflow_debug_facet", return_value={}),
        mock.patch(f"{_UTILS}.get_processing_engine_facet", return_value={}),
    ):
        listener = mock.MagicMock()
        listener_factory.return_value = listener
        yield listener.adapter.emit


def test_emits_running_event_with_datasets(patched_emit):
    ti = _make_task_instance()
    inputs = [Dataset(namespace="s3://bucket", name="in.csv")]
    outputs = [Dataset(namespace="snowflake://acct", name="db.schema.out")]

    emit_dataset_lineage(inputs=inputs, outputs=outputs, task_instance=ti)

    patched_emit.assert_called_once()
    (event,) = patched_emit.call_args.args
    assert isinstance(event, RunEvent)
    assert event.eventType == RunState.RUNNING
    assert event.inputs == inputs
    assert event.outputs == outputs
    assert event.job.name == "dag_id.task_id"
    assert event.run.runId == OpenLineageAdapter.build_task_instance_run_id(
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        try_number=ti.try_number,
        logical_date=ti.logical_date,
        map_index=ti.map_index,
    )
    assert event.run.facets["parent"].run.runId == OpenLineageAdapter.build_dag_run_id(
        dag_id=ti.dag_id,
        logical_date=ti.dag_run.logical_date,
        clear_number=ti.dag_run.clear_number,
    )
    # Without dr_conf root info, root falls back to parent.
    assert event.run.facets["parent"].root.run.runId == event.run.facets["parent"].run.runId
    assert event.job.facets["jobType"].jobType == "TASK"
    assert event.job.facets["jobType"].integration == "AIRFLOW"


def test_propagates_root_from_dagrun_conf(patched_emit):
    ti = _make_task_instance()
    root_uuid = "11111111-1111-1111-1111-111111111111"
    ti.dag_run.conf = {
        "openlineage": {
            "rootParentRunId": root_uuid,
            "rootParentJobNamespace": "airflow://root",
            "rootParentJobName": "root_dag",
        }
    }
    ti.get_template_context.return_value["dag_run"] = ti.dag_run

    emit_dataset_lineage(
        inputs=[Dataset(namespace="s3://bucket", name="a")],
        task_instance=ti,
    )

    (event,) = patched_emit.call_args.args
    parent_facet = event.run.facets["parent"]
    assert parent_facet.root.run.runId == root_uuid
    assert parent_facet.root.job.namespace == "airflow://root"
    assert parent_facet.root.job.name == "root_dag"


def test_attaches_listener_parity_facets(patched_emit):
    ti = _make_task_instance()
    emit_dataset_lineage(inputs=[Dataset(namespace="ns", name="a")], task_instance=ti)

    (event,) = patched_emit.call_args.args
    # nominalTime derived from dag_run.data_interval_start/end
    assert "nominalTime" in event.run.facets
    assert event.run.facets["nominalTime"].nominalStartTime.startswith("2024-01-01")
    # airflow run facet injected via patched helper
    assert "airflow" in event.run.facets
    # ownership sourced from task.owner (non-default)
    assert "ownership" in event.job.facets
    assert {o.name for o in event.job.facets["ownership"].owners} == {"alice", "bob"}
    # tags sourced from dag.tags
    assert "tags" in event.job.facets
    assert {t.key for t in event.job.facets["tags"].tags} == {"t1", "t2"}


def test_user_additional_facets_cannot_override_internal(patched_emit):
    ti = _make_task_instance()

    malicious_parent = mock.MagicMock(name="malicious_parent")
    malicious_jobtype = mock.MagicMock(name="malicious_jobtype")

    emit_dataset_lineage(
        inputs=[Dataset(namespace="ns", name="a")],
        task_instance=ti,
        additional_run_facets={
            "parent": malicious_parent,  # should be overridden
            "my_custom": mock.sentinel.custom_run,
        },
        additional_job_facets={
            "jobType": malicious_jobtype,  # should be overridden
            "my_custom": mock.sentinel.custom_job,
        },
    )
    (event,) = patched_emit.call_args.args
    # Internal facets win:
    assert event.run.facets["parent"] is not malicious_parent
    assert event.job.facets["jobType"] is not malicious_jobtype
    assert event.job.facets["jobType"].jobType == "TASK"
    # User-supplied non-colliding facets are preserved:
    assert event.run.facets["my_custom"] is mock.sentinel.custom_run
    assert event.job.facets["my_custom"] is mock.sentinel.custom_job


def test_noop_when_openlineage_disabled():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=True),
        mock.patch(f"{_CORE}.get_openlineage_listener") as listener_factory,
    ):
        emit_dataset_lineage(inputs=[Dataset(namespace="ns", name="a")], task_instance=_make_task_instance())
        listener_factory.assert_not_called()


def test_noop_when_listener_missing():
    with (
        mock.patch(f"{_CORE}.conf.is_disabled", return_value=False),
        mock.patch(f"{_CORE}.get_openlineage_listener", return_value=None),
    ):
        emit_dataset_lineage(inputs=[Dataset(namespace="ns", name="a")], task_instance=_make_task_instance())


def test_raises_when_inputs_and_outputs_empty(patched_emit):
    with pytest.raises(ValueError, match="inputs"):
        emit_dataset_lineage(task_instance=_make_task_instance(), raise_on_error=True)
    patched_emit.assert_not_called()


def test_raises_when_item_is_not_dataset(patched_emit):
    with pytest.raises(TypeError):
        emit_dataset_lineage(
            inputs=["not-a-dataset"],  # type: ignore[list-item]
            task_instance=_make_task_instance(),
            raise_on_error=True,
        )
    patched_emit.assert_not_called()


def test_swallows_errors_by_default(patched_emit, caplog):
    """By default, validation errors are logged at WARNING and swallowed (no emission)."""
    import logging

    with caplog.at_level(logging.WARNING):
        emit_dataset_lineage(task_instance=_make_task_instance())
    patched_emit.assert_not_called()
    assert "emit_dataset_lineage raised an error" in caplog.text


def test_resolves_task_instance_from_context(patched_emit):
    ti = _make_task_instance()
    with mock.patch(f"{_MODULE}.get_task_instance_from_context", return_value=ti) as get_ti:
        emit_dataset_lineage(inputs=[Dataset(namespace="ns", name="a")])
        get_ti.assert_called_once()
    patched_emit.assert_called_once()
