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

from unittest import mock

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.sdk import DAG
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.execution_time import task_runner

from tests_common.test_utils.config import conf_vars


def _make_dag_tagged_ti(create_runtime_ti, tags):
    with DAG("tagged_dag", tags=tags):
        task = BaseOperator(task_id="t")
    return create_runtime_ti(task=task)


def _run_main_and_get_worker_span(dag_tags: set[str]) -> ReadableSpan:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)

    startup_details = mock.Mock()
    startup_details.ti.task_id = "my_task"
    startup_details.ti.dag_id = "test_dag"
    startup_details.ti.run_id = "test_run"
    startup_details.ti.try_number = 1
    startup_details.ti.map_index = None
    startup_details.ti.context_carrier = {}

    ti = mock.Mock()
    ti.task.dag.tags = dag_tags
    ti.bundle_instance.name = "my-bundle"
    ti.bundle_instance.version = None
    ti._terminal_state_send_failed = False

    comms = mock.Mock()
    comms.socket = None

    with (
        mock.patch("airflow.sdk.execution_time.task_runner.CommsDecoder") as decoder_cls,
        mock.patch(
            "airflow.sdk.execution_time.task_runner.get_startup_details", return_value=startup_details
        ),
        mock.patch("airflow.sdk.execution_time.task_runner.startup", return_value=(ti, {}, mock.Mock())),
        mock.patch("airflow.sdk.execution_time.task_runner.BundleVersionLock"),
        mock.patch(
            "airflow.sdk.execution_time.task_runner.run", return_value=(mock.Mock(), mock.Mock(), None)
        ),
        mock.patch("airflow.sdk.execution_time.task_runner.finalize"),
        mock.patch("airflow.sdk.execution_time.task_runner.tracer", tracer),
    ):
        decoder_cls.__getitem__.return_value.return_value = comms
        task_runner.main()

    return {span.name: span for span in exporter.get_finished_spans()}["worker.my_task"]


def test_stats_tags_dag_tags_disabled_by_default(create_runtime_ti):
    ti = _make_dag_tagged_ti(create_runtime_ti, ["env:prod", "validation"])

    assert ti.stats_tags == {"dag_id": "tagged_dag", "task_id": "t", "run_type": "manual"}


@conf_vars({("metrics", "dag_tags_in_metrics"): "True"})
def test_stats_tags_default_to_expanded_dag_tags(create_runtime_ti):
    ti = _make_dag_tagged_ti(create_runtime_ti, ["production", "team:data"])

    assert ti.stats_tags == {
        "production": "",
        "team": "data",
        "dag_id": "tagged_dag",
        "task_id": "t",
        "run_type": "manual",
    }


@conf_vars({("traces", "dag_tags_in_spans"): "True"})
def test_main_adds_expanded_dag_tags_to_worker_span():
    worker = _run_main_and_get_worker_span({"production", "team:data"})

    assert worker.attributes is not None
    assert worker.attributes["team"] == "data"
    assert worker.attributes["production"] == ""


@conf_vars({("traces", "dag_tags_in_spans"): "True"})
def test_expanded_dag_tags_do_not_override_worker_identity():
    worker = _run_main_and_get_worker_span({"airflow.dag_id:wrong"})

    assert worker.attributes is not None
    assert worker.attributes["airflow.dag_id"] == "test_dag"


@conf_vars({("traces", "dag_tags_in_spans"): "False"})
def test_main_does_not_add_dag_tags_when_disabled():
    worker = _run_main_and_get_worker_span({"production", "finance", "team:data"})

    assert worker.attributes is not None
    assert "production" not in worker.attributes
    assert "team" not in worker.attributes
