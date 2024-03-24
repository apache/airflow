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

import os

# import os
from typing import TYPE_CHECKING

from airflow.listeners import hookimpl
from airflow.providers.opentelemetry.hooks.otel import OtelHook

if TYPE_CHECKING:
    from airflow.datasets import Dataset
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from airflow.providers.opentelemetry.hooks.otel import (
    OTEL_CONN_ID,
    is_listener_enabled,
)
from airflow.providers.opentelemetry.util import (
    datetime_to_nano,
    gen_dag_span_id,
    gen_span_id,
    gen_trace_id,
)
from airflow.utils.state import DagRunState, TaskInstanceState

_opentelemetry_listener: OpenTelemetryListener | None = None


class OpenTelemetryListener:
    """Produce span on dag run and task instance by listening to dag run events."""

    def __init__(self):
        self.listener_enabled = is_listener_enabled()
        self.conn_id = os.getenv(OTEL_CONN_ID, "otel_default")
        self.otel_hook = OtelHook(self.conn_id)

    @hookimpl
    def on_task_instance_running(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, session
    ):
        """Trigger task state changes to RUNNING."""
        pass

    @hookimpl
    def on_task_instance_success(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, session
    ):
        """Trigger task state changes to SUCCESS."""
        if self.listener_enabled is False:
            return
        self._handle_taskinstance(task_instance)

    @hookimpl
    def on_task_instance_failed(
        self, previous_state: TaskInstanceState, task_instance: TaskInstance, session
    ):
        """Trigger task state changes to FAILED."""
        if self.listener_enabled is False:
            return
        self._handle_taskinstance(task_instance)

    def _handle_taskinstance(self, task_instance: TaskInstance):
        trace_id = int(gen_trace_id(dag_run=task_instance.dag_run), 16)
        parent_id = int(gen_dag_span_id(dag_run=task_instance.dag_run), 16)
        span_id = int(gen_span_id(ti=task_instance), 16)
        span_ctx = SpanContext(
            trace_id=trace_id, span_id=parent_id, is_remote=True, trace_flags=TraceFlags(0x01)
        )
        ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
        with self.otel_hook.start_as_current_span(
            name=task_instance.task_id,
            library_name="taskinstance",
            trace_id=trace_id,
            span_id=span_id,
            context=ctx,
            start_time=datetime_to_nano(task_instance.queued_dttm),
        ) as span:
            span.set_attribute("category", "scheduler")
            span.set_attribute("task_id", task_instance.task_id)
            span.set_attribute("dag_id", task_instance.dag_id)
            span.set_attribute("state", task_instance.state)
            if task_instance.state == TaskInstanceState.FAILED:
                span.set_attribute("error", True)
            span.set_attribute("start_date", str(task_instance.start_date))
            span.set_attribute("end_date", str(task_instance.end_date))
            span.set_attribute("duration", task_instance.duration)
            span.set_attribute("executor_config", str(task_instance.executor_config))
            span.set_attribute("execution_date", str(task_instance.execution_date))
            span.set_attribute("hostname", task_instance.hostname)
            span.set_attribute("log_url", task_instance.log_url)
            span.set_attribute("operator", str(task_instance.operator))
            span.set_attribute("try_number", task_instance.try_number - 1)
            span.set_attribute("job_id", task_instance.job_id)
            span.set_attribute("pool", task_instance.pool)
            span.set_attribute("queue", task_instance.queue)
            span.set_attribute("priority_weight", task_instance.priority_weight)
            span.set_attribute("queued_dttm", str(task_instance.queued_dttm))
            span.set_attribute("ququed_by_job_id", task_instance.queued_by_job_id)
            span.set_attribute("pid", task_instance.pid)
            span.add_event(name="queued", timestamp=datetime_to_nano(task_instance.queued_dttm))
            span.add_event(name="started", timestamp=datetime_to_nano(task_instance.start_date))
            span.add_event(name="ended", timestamp=datetime_to_nano(task_instance.end_date))

    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        """Trigger when dag run is successful."""
        if self.listener_enabled is False:
            return
        self._handle_dagrun(dag_run)

    @hookimpl
    def on_dag_run_failed(self, dag_run: DagRun, msg: str):
        """Trigger when dag run is failed."""
        if self.listener_enabled is False:
            return
        self._handle_dagrun(dag_run)

    def _handle_dagrun(self, dag_run: DagRun):
        trace_id = int(gen_trace_id(dag_run=dag_run), 16)
        span_id = int(gen_dag_span_id(dag_run=dag_run), 16)
        with self.otel_hook.start_as_current_span(
            name=dag_run.dag_id,
            library_name="dagrun",
            trace_id=trace_id,
            span_id=span_id,
            start_time=datetime_to_nano(dag_run.queued_at),
        ) as span:
            if dag_run.state is DagRunState.FAILED:
                span.set_attribute("error", True)
            attributes = {
                "category": "DAG runs",
                "dag_id": str(dag_run.dag_id),
                "execution_date": str(dag_run.execution_date),
                "run_id": str(dag_run.run_id),
                "queued_at": str(dag_run.queued_at),
                "run_start_date": str(dag_run.start_date),
                "run_end_date": str(dag_run.end_date),
                "run_duration": str(
                    (dag_run.end_date - dag_run.start_date).total_seconds()
                    if dag_run.start_date and dag_run.end_date
                    else 0
                ),
                "state": str(dag_run.state),
                "external_trigger": str(dag_run.external_trigger),
                "run_type": str(dag_run.run_type),
                "data_interval_start": str(dag_run.data_interval_start),
                "data_interval_end": str(dag_run.data_interval_end),
                "dag_hash": str(dag_run.dag_hash),
                "conf": str(dag_run.conf),
            }
            span.add_event(name="queued", timestamp=datetime_to_nano(dag_run.queued_at))
            span.add_event(name="started", timestamp=datetime_to_nano(dag_run.start_date))
            span.add_event(name="ended", timestamp=datetime_to_nano(dag_run.end_date))
            span.set_attributes(attributes)

    @hookimpl
    def on_dag_run_running(self, dag_run: DagRun, msg: str):
        """Trigger when dag run state changes to RUNNING."""
        pass

    @hookimpl
    def on_dataset_created(self, dataset: Dataset):
        """Trigger when dataset is created."""
        pass

    @hookimpl
    def on_dataset_changed(self, dataset: Dataset):
        """Trigger when dataset is changed."""
        pass


def get_opentelemetry_listener() -> OpenTelemetryListener:
    """Get singleton listener manager."""
    global _opentelemetry_listener
    if not _opentelemetry_listener:
        _opentelemetry_listener = OpenTelemetryListener()
    return _opentelemetry_listener
