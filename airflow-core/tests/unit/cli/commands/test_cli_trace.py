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
"""Tests that CLI entry points emit OpenTelemetry spans honouring TRACEPARENT.

These tests mock the underlying work each CLI entry point performs and only
assert that the surrounding span is emitted with the right name, attributes,
and parent context.  The end-to-end behaviour of each command is covered by
its own dedicated test module; here we focus on the trace-context wiring.
"""

from __future__ import annotations

from datetime import datetime
from unittest import mock

import pendulum
import pytest
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow._shared.timezones import timezone
from airflow.cli import cli_parser
from airflow.cli.commands import backfill_command, dag_command, task_command

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1), timezone=timezone.utc)
EXAMPLE_TRACE_ID = "0af7651916cd43dd8448eb211c80319c"
EXAMPLE_SPAN_ID = "b7ad6b7169203331"
EXAMPLE_TRACEPARENT = f"00-{EXAMPLE_TRACE_ID}-{EXAMPLE_SPAN_ID}-01"

pytestmark = pytest.mark.db_test


@pytest.fixture
def captured_spans():
    """Install a real tracer provider with an in-memory exporter for assertion."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    # ``cli_span`` looks up the tracer via ``otel_trace.get_tracer`` at call time,
    # so we patch that lookup rather than swapping a module-level tracer attribute.
    with mock.patch.object(otel_trace, "get_tracer", return_value=tracer):
        yield exporter


@pytest.fixture
def parser():
    return cli_parser.get_parser()


def _cli_span(spans, name):
    matches = [s for s in spans if s.name == name]
    assert len(matches) == 1, f"expected exactly one {name!r} span, got {[s.name for s in spans]}"
    return matches[0]


class TestCliDagsTriggerSpan:
    @mock.patch("airflow.cli.commands.dag_command.get_current_api_client")
    def test_emits_span(self, mock_api_client, captured_spans, parser):
        mock_api_client.return_value.trigger_dag.return_value = None
        cli_args = parser.parse_args(["dags", "trigger", "example_bash_operator", "--run-id", "trace_run"])
        dag_command.dag_trigger(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.dags.trigger")
        assert span.attributes["airflow.dag_id"] == "example_bash_operator"
        assert span.attributes["airflow.dag_run.run_id"] == "trace_run"
        assert span.parent is None
        # The CLI did call into the API client inside the span.
        mock_api_client.assert_called_once()
        mock_api_client.return_value.trigger_dag.assert_called_once()

    @mock.patch("airflow.cli.commands.dag_command.get_current_api_client")
    def test_honours_traceparent(self, mock_api_client, captured_spans, parser, monkeypatch):
        mock_api_client.return_value.trigger_dag.return_value = None
        monkeypatch.setenv("TRACEPARENT", EXAMPLE_TRACEPARENT)
        cli_args = parser.parse_args(["dags", "trigger", "example_bash_operator", "--run-id", "trace_run_2"])
        dag_command.dag_trigger(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.dags.trigger")
        assert format(span.context.trace_id, "032x") == EXAMPLE_TRACE_ID
        assert format(span.parent.span_id, "016x") == EXAMPLE_SPAN_ID


class TestCliDagsTestSpan:
    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_emits_span(self, mock_get_dag, captured_spans, parser):
        cli_args = parser.parse_args(["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.dags.test")
        assert span.attributes["airflow.dag_id"] == "example_bash_operator"
        assert span.attributes["airflow.dag_run.logical_date"] == DEFAULT_DATE.isoformat()
        assert span.parent is None

    @mock.patch("airflow.cli.commands.dag_command.get_bagged_dag")
    def test_honours_traceparent(self, mock_get_dag, captured_spans, parser, monkeypatch):
        monkeypatch.setenv("TRACEPARENT", EXAMPLE_TRACEPARENT)
        cli_args = parser.parse_args(["dags", "test", "example_bash_operator", DEFAULT_DATE.isoformat()])
        dag_command.dag_test(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.dags.test")
        assert format(span.context.trace_id, "032x") == EXAMPLE_TRACE_ID
        assert format(span.parent.span_id, "016x") == EXAMPLE_SPAN_ID


class TestCliTasksTestSpan:
    @mock.patch("airflow.cli.commands.task_command._run_task")
    @mock.patch("airflow.cli.commands.task_command._get_ti")
    @mock.patch("airflow.cli.commands.task_command.get_db_dag")
    @mock.patch("airflow.cli.commands.task_command.get_bagged_dag")
    def test_emits_span(
        self,
        mock_get_bagged_dag,
        mock_get_db_dag,
        mock_get_ti,
        mock_run_task,
        captured_spans,
        parser,
    ):
        mock_get_ti.return_value = (mock.MagicMock(), False)
        cli_args = parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-01"]
        )
        task_command.task_test(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.tasks.test")
        assert span.attributes["airflow.dag_id"] == "example_python_operator"
        assert span.attributes["airflow.task_id"] == "print_the_context"
        assert span.attributes["airflow.logical_date_or_run_id"] == "2018-01-01"
        assert span.parent is None

    @mock.patch("airflow.cli.commands.task_command._run_task")
    @mock.patch("airflow.cli.commands.task_command._get_ti")
    @mock.patch("airflow.cli.commands.task_command.get_db_dag")
    @mock.patch("airflow.cli.commands.task_command.get_bagged_dag")
    def test_honours_traceparent(
        self,
        mock_get_bagged_dag,
        mock_get_db_dag,
        mock_get_ti,
        mock_run_task,
        captured_spans,
        parser,
        monkeypatch,
    ):
        mock_get_ti.return_value = (mock.MagicMock(), False)
        monkeypatch.setenv("TRACEPARENT", EXAMPLE_TRACEPARENT)
        cli_args = parser.parse_args(
            ["tasks", "test", "example_python_operator", "print_the_context", "2018-01-02"]
        )
        task_command.task_test(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.tasks.test")
        assert format(span.context.trace_id, "032x") == EXAMPLE_TRACE_ID
        assert format(span.parent.span_id, "016x") == EXAMPLE_SPAN_ID


class TestCliBackfillCreateSpan:
    @mock.patch("airflow.cli.commands.backfill_command._do_dry_run", return_value=[])
    def test_emits_span_on_dry_run(self, mock_dry_run, captured_spans, parser):
        start = pendulum.datetime(2021, 1, 1, tz="UTC")
        end = pendulum.datetime(2021, 1, 2, tz="UTC")
        cli_args = parser.parse_args(
            [
                "backfill",
                "create",
                "--dag-id",
                "example_bash_operator",
                "--from-date",
                start.to_iso8601_string(),
                "--to-date",
                end.to_iso8601_string(),
                "--dry-run",
            ]
        )
        backfill_command.create_backfill(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.backfill.create")
        assert span.attributes["airflow.dag_id"] == "example_bash_operator"
        assert span.attributes["airflow.backfill.dry_run"] is True
        assert span.parent is None
        mock_dry_run.assert_called_once()

    @mock.patch("airflow.cli.commands.backfill_command._do_dry_run", return_value=[])
    def test_honours_traceparent(self, mock_dry_run, captured_spans, parser, monkeypatch):
        monkeypatch.setenv("TRACEPARENT", EXAMPLE_TRACEPARENT)
        start = pendulum.datetime(2021, 1, 3, tz="UTC")
        end = pendulum.datetime(2021, 1, 4, tz="UTC")
        cli_args = parser.parse_args(
            [
                "backfill",
                "create",
                "--dag-id",
                "example_bash_operator",
                "--from-date",
                start.to_iso8601_string(),
                "--to-date",
                end.to_iso8601_string(),
                "--dry-run",
            ]
        )
        backfill_command.create_backfill(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.backfill.create")
        assert format(span.context.trace_id, "032x") == EXAMPLE_TRACE_ID
        assert format(span.parent.span_id, "016x") == EXAMPLE_SPAN_ID

    @mock.patch("airflow.cli.commands.backfill_command._create_backfill")
    def test_emits_span_on_non_dry_run(self, mock_create, captured_spans, parser):
        start = pendulum.datetime(2021, 1, 5, tz="UTC")
        end = pendulum.datetime(2021, 1, 6, tz="UTC")
        cli_args = parser.parse_args(
            [
                "backfill",
                "create",
                "--dag-id",
                "example_bash_operator",
                "--from-date",
                start.to_iso8601_string(),
                "--to-date",
                end.to_iso8601_string(),
            ]
        )
        backfill_command.create_backfill(cli_args)

        span = _cli_span(captured_spans.get_finished_spans(), "cli.backfill.create")
        assert span.attributes["airflow.dag_id"] == "example_bash_operator"
        assert span.attributes["airflow.backfill.dry_run"] is False
        mock_create.assert_called_once()
