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

from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import structlog.testing
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.sdk import ResumableJobMixin
from airflow.sdk.bases.operator import BaseOperator

if TYPE_CHECKING:
    from pydantic import JsonValue


class ConcreteResumableOperator(ResumableJobMixin, BaseOperator):
    """Minimal concrete implementation for testing the mixin."""

    external_id_key = "test_job_id"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.submitted_ids: list[str] = []
        self.polled_ids: list[str] = []
        self._next_id = "job-001"
        self._status_map: dict[str, str] = {}
        self._active_statuses = {"RUNNING", "PENDING"}
        self._succeeded_statuses = {"SUCCEEDED"}

    def submit_job(self, context) -> JsonValue:
        self.submitted_ids.append(self._next_id)
        return self._next_id

    def get_job_status(self, external_id: JsonValue, context) -> str:
        return self._status_map.get(str(external_id), "UNKNOWN")

    def is_job_active(self, status: str) -> bool:
        return status in self._active_statuses

    def is_job_succeeded(self, status: str) -> bool:
        return status in self._succeeded_statuses

    def poll_until_complete(self, external_id: JsonValue, context) -> None:
        self.polled_ids.append(str(external_id))

    def get_job_result(self, external_id: JsonValue, context) -> str:
        return f"result-of-{external_id}"


class FakeTaskState:
    def __init__(self, stored: dict[str, str] | None = None):
        self._store: dict[str, str] = stored or {}

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str) -> None:
        self._store[key] = value


def make_context(task_store: FakeTaskState | None = None) -> dict:
    ctx: dict = {}
    if task_store is not None:
        ctx["task_state_store"] = task_store
    return ctx


class FakeTI:
    """Minimal stand-in for RuntimeTaskInstance exposing stats_tags with an optional team_name."""

    def __init__(self, team_name: str | None = None):
        self._team_name = team_name

    @property
    def stats_tags(self) -> dict[str, str]:
        tags = {"dag_id": "d", "task_id": "t"}
        if self._team_name:
            tags["team_name"] = self._team_name
        return tags


class TestFirstSubmission:
    def test_submits_and_polls_when_no_prior_state(self):
        op = ConcreteResumableOperator(task_id="test_task")
        task_state = FakeTaskState()
        ctx = make_context(task_state)

        op.execute_resumable(ctx)

        assert op.submitted_ids == ["job-001"]
        assert op.polled_ids == ["job-001"]

    def test_persists_external_id_before_polling(self):
        """The ID must be in task_state before poll_until_complete is called."""
        op = ConcreteResumableOperator(task_id="test_task")
        task_state = FakeTaskState()
        persisted_at_poll: list[str | None] = []

        original_set = task_state.set

        def set_and_track(key, value):
            original_set(key, value)

        def poll_side_effect(external_id, context):
            persisted_at_poll.append(task_state.get("test_job_id"))

        task_state.set = set_and_track
        op.poll_until_complete = poll_side_effect

        op.execute_resumable(make_context(task_state))

        assert persisted_at_poll == ["job-001"], "ID must be persisted before polling starts"

    def test_returns_job_result(self):
        op = ConcreteResumableOperator(task_id="test_task")
        result = op.execute_resumable(make_context(FakeTaskState()))

        assert result == "result-of-job-001"


class TestRetryWithDifferentJobStatuses:
    def test_skips_submission_when_job_active(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "RUNNING"
        task_state = FakeTaskState({"test_job_id": "job-001"})
        ctx = make_context(task_state)

        op.execute_resumable(ctx)

        assert op.submitted_ids == [], "should not resubmit when job is active"
        assert op.polled_ids == ["job-001"]

    def test_pending_status_also_skips_submission(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "PENDING"
        task_state = FakeTaskState({"test_job_id": "job-001"})

        op.execute_resumable(make_context(task_state))

        assert op.submitted_ids == []
        assert op.polled_ids == ["job-001"]

    def test_returns_result_immediately_without_polling(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "SUCCEEDED"
        task_state = FakeTaskState({"test_job_id": "job-001"})

        result = op.execute_resumable(make_context(task_state))

        assert op.submitted_ids == [], "should not resubmit"
        assert op.polled_ids == [], "should not poll again"
        assert result == "result-of-job-001"

    @pytest.mark.parametrize("status", ["FAILED", "KILLED", "ERROR", "UNKNOWN"])
    def test_resubmits_when_prior_job_in_terminal_failure(self, status):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = status
        op._next_id = "job-002"
        task_state = FakeTaskState({"test_job_id": "job-001"})

        op.execute_resumable(make_context(task_state))

        assert op.submitted_ids == ["job-002"], "should resubmit fresh"
        assert op.polled_ids == ["job-002"]


class TestNoneExternalId:
    def test_none_external_id_is_not_stored(self):
        """submit_job() returning None must not call task_state.set()."""

        class NoneIdOp(ConcreteResumableOperator):
            def submit_job(self, context) -> JsonValue:
                return None

            def poll_until_complete(self, external_id, context) -> None:
                pass

            def get_job_result(self, external_id, context) -> str:
                return "done"

        op = NoneIdOp(task_id="test_task")
        task_state = FakeTaskState()

        op.execute_resumable(make_context(task_state))

        assert task_state._store == {}


class TestResumeOnRetryDisabled:
    def test_submits_and_polls_without_task_store_interaction(self):
        op = ConcreteResumableOperator(task_id="test_task", durable=False)
        task_store = FakeTaskState()
        op.execute_resumable(make_context(task_store))

        assert op.submitted_ids == ["job-001"]
        assert op.polled_ids == ["job-001"]
        assert task_store._store == {}, "task_store must not be written when durable=False"

    def test_does_not_reconnect_when_prior_id_exists(self):
        op = ConcreteResumableOperator(task_id="test_task", durable=False)
        op._status_map["job-001"] = "RUNNING"
        task_store = FakeTaskState({"test_job_id": "job-001"})

        op.execute_resumable(make_context(task_store))

        assert op.submitted_ids == ["job-001"], "should submit fresh even with a prior ID stored"

    def test_returns_result(self):
        op = ConcreteResumableOperator(task_id="test_task", durable=False)
        result = op.execute_resumable(make_context(FakeTaskState()))
        assert result == "result-of-job-001"

    def test_default_is_true(self):
        op = ConcreteResumableOperator(task_id="test_task")
        assert op.durable is True


class TestExternalIdKey:
    def test_custom_key_used_for_storage_and_retrieval(self):
        class CustomKeyOp(ConcreteResumableOperator):
            external_id_key = "my_custom_key"

        op = CustomKeyOp(task_id="test_task")
        task_state = FakeTaskState()

        op.execute_resumable(make_context(task_state))

        assert task_state.get("my_custom_key") == "job-001"


class TestMetrics:
    _PATCH = "airflow.sdk._shared.observability.metrics.stats.incr"
    _TAG = {"operator": "ConcreteResumableOperator"}

    def test_fresh_submit_fires_only_fresh_submit_counter(self):
        op = ConcreteResumableOperator(task_id="test_task")
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(make_context(FakeTaskState()))
        called_names = [call.args[0] for call in mock_incr.call_args_list]
        assert called_names == ["resumable_job.fresh_submit"]
        mock_incr.assert_called_once_with("resumable_job.fresh_submit", tags=self._TAG)

    def test_reconnect_fires_attempt_and_success(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "RUNNING"
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        called_names = [call.args[0] for call in mock_incr.call_args_list]
        assert "resumable_job.reconnect_attempt" in called_names
        assert "resumable_job.reconnect_success" in called_names
        assert "resumable_job.fresh_submit" not in called_names

    def test_already_succeeded_fires_when_job_succeeded(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "SUCCEEDED"
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        called_names = [call.args[0] for call in mock_incr.call_args_list]
        assert "resumable_job.reconnect_attempt" in called_names
        assert "resumable_job.already_succeeded" in called_names
        assert "resumable_job.reconnect_success" not in called_names
        assert "resumable_job.fresh_submit" not in called_names

    def test_terminal_resubmit_fires_when_job_failed(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "FAILED"
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        called_names = [call.args[0] for call in mock_incr.call_args_list]
        assert "resumable_job.reconnect_attempt" in called_names
        assert "resumable_job.terminal_resubmit" in called_names
        assert "resumable_job.reconnect_success" not in called_names
        assert "resumable_job.fresh_submit" not in called_names

    @pytest.mark.parametrize(
        ("team_name", "expected_tag"),
        [
            pytest.param(
                "team_alpha",
                {"operator": "ConcreteResumableOperator", "team_name": "team_alpha"},
                id="with_team",
            ),
            pytest.param(None, {"operator": "ConcreteResumableOperator"}, id="without_team"),
        ],
    )
    def test_team_name_added_to_metric_tags(self, team_name, expected_tag):
        op = ConcreteResumableOperator(task_id="test_task")
        ctx = make_context(FakeTaskState())
        ctx["ti"] = FakeTI(team_name)
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(ctx)
        mock_incr.assert_called_once_with("resumable_job.fresh_submit", tags=expected_tag)


class TestTracing:
    _MODULE_TRACER = "airflow.sdk.bases.resumablejobmixin.tracer"

    def _make_tracing_provider(self) -> tuple[InMemorySpanExporter, TracerProvider]:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        return exporter, provider

    def _get_decision_span(self, exporter: InMemorySpanExporter):
        spans = exporter.get_finished_spans()
        return next((s for s in spans if s.name == "resumable_job.resume_decision"), None)

    def _make_tracer(self):
        exporter, provider = self._make_tracing_provider()
        return exporter, provider.get_tracer("airflow.sdk.bases.resumablejobmixin")

    def test_fresh_submit_span_attributes(self):
        op = ConcreteResumableOperator(task_id="test_task")
        exporter, module_tracer = self._make_tracer()
        with mock.patch(self._MODULE_TRACER, module_tracer):
            op.execute_resumable(make_context(FakeTaskState()))
        span = self._get_decision_span(exporter)
        assert span is not None
        assert span.attributes["resumable.decision"] == "fresh_submit"
        assert span.attributes["operator"] == "ConcreteResumableOperator"
        assert span.attributes["resumable.external_id_key"] == "test_job_id"
        assert "resumable.external_id" not in span.attributes

    def test_reconnect_span_attributes(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "RUNNING"
        exporter, module_tracer = self._make_tracer()
        with mock.patch(self._MODULE_TRACER, module_tracer):
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        span = self._get_decision_span(exporter)
        assert span is not None
        assert span.attributes["resumable.decision"] == "reconnect"
        assert span.attributes["resumable.external_id"] == "job-001"
        assert span.attributes["resumable.prior_status"] == "RUNNING"

    @pytest.mark.parametrize(
        ("status", "expected_decision"),
        [("SUCCEEDED", "already_succeeded"), ("FAILED", "terminal_resubmit")],
    )
    def test_non_active_stored_job_span_attributes(self, status, expected_decision):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = status
        exporter, module_tracer = self._make_tracer()
        with mock.patch(self._MODULE_TRACER, module_tracer):
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        span = self._get_decision_span(exporter)
        assert span is not None
        assert span.attributes["resumable.decision"] == expected_decision
        assert span.attributes["resumable.external_id"] == "job-001"
        assert span.attributes["resumable.prior_status"] == status


class TestLogging:
    def test_warning_when_task_store_unavailable(self):
        op = ConcreteResumableOperator(task_id="test_task")
        with structlog.testing.capture_logs() as logs:
            op.execute_resumable(make_context(task_store=None))
        warnings = [entry for entry in logs if entry["log_level"] == "warning"]
        assert any("crash recovery is disabled" in entry["event"] for entry in warnings)

    @pytest.mark.parametrize(
        ("status", "event_fragment", "log_level", "extra_fields"),
        [
            ("RUNNING", "Reconnecting", "info", {"external_id_key": "test_job_id", "status": "RUNNING"}),
            ("SUCCEEDED", "already completed", "info", {"external_id_key": "test_job_id"}),
            ("FAILED", "terminal state", "warning", {"status": "FAILED"}),
        ],
    )
    def test_log_fields_for_stored_job(self, status, event_fragment, log_level, extra_fields):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = status
        with structlog.testing.capture_logs() as logs:
            op.execute_resumable(make_context(FakeTaskState({"test_job_id": "job-001"})))
        entry = next((e for e in logs if event_fragment in e["event"]), None)
        assert entry is not None
        assert entry["log_level"] == log_level
        assert entry["external_id"] == "job-001"
        for key, val in extra_fields.items():
            assert entry[key] == val


class _MissingSubmitJob(ResumableJobMixin, BaseOperator):
    def get_job_status(self, external_id, context) -> str:
        return "RUNNING"

    def is_job_active(self, status: str) -> bool:
        return True

    def is_job_succeeded(self, status: str) -> bool:
        return False

    def poll_until_complete(self, external_id, context) -> None:
        return None

    def get_job_result(self, external_id, context):
        return None


class _MissingGetJobStatus(ResumableJobMixin, BaseOperator):
    def submit_job(self, context):
        return "id"

    def is_job_active(self, status: str) -> bool:
        return True

    def is_job_succeeded(self, status: str) -> bool:
        return False

    def poll_until_complete(self, external_id, context) -> None:
        return None

    def get_job_result(self, external_id, context):
        return None


class _MissingIsJobActive(ResumableJobMixin, BaseOperator):
    def submit_job(self, context):
        return "id"

    def get_job_status(self, external_id, context) -> str:
        return "RUNNING"

    def is_job_succeeded(self, status: str) -> bool:
        return False

    def poll_until_complete(self, external_id, context) -> None:
        return None

    def get_job_result(self, external_id, context):
        return None


class _MissingIsJobSucceeded(ResumableJobMixin, BaseOperator):
    def submit_job(self, context):
        return "id"

    def get_job_status(self, external_id, context) -> str:
        return "RUNNING"

    def is_job_active(self, status: str) -> bool:
        return True

    def poll_until_complete(self, external_id, context) -> None:
        return None

    def get_job_result(self, external_id, context):
        return None


class _MissingPollUntilComplete(ResumableJobMixin, BaseOperator):
    def submit_job(self, context):
        return "id"

    def get_job_status(self, external_id, context) -> str:
        return "RUNNING"

    def is_job_active(self, status: str) -> bool:
        return True

    def is_job_succeeded(self, status: str) -> bool:
        return False

    def get_job_result(self, external_id, context):
        return None


class _MissingGetJobResult(ResumableJobMixin, BaseOperator):
    def submit_job(self, context):
        return "id"

    def get_job_status(self, external_id, context) -> str:
        return "RUNNING"

    def is_job_active(self, status: str) -> bool:
        return True

    def is_job_succeeded(self, status: str) -> bool:
        return False

    def poll_until_complete(self, external_id, context) -> None:
        return None


class _MissingEverything(ResumableJobMixin, BaseOperator):
    pass


class TestAbstractMethodEnforcement:
    def test_mixin_itself_cannot_be_instantiated(self):
        with pytest.raises(TypeError, match="Can't instantiate abstract class ResumableJobMixin"):
            ResumableJobMixin()

    def test_fully_implemented_subclass_has_no_abstract_methods(self):
        assert ConcreteResumableOperator.__abstractmethods__ == frozenset()

    def test_fully_implemented_subclass_instantiates_and_behaves_normally(self):
        op = ConcreteResumableOperator(task_id="test_task")
        assert isinstance(op, ResumableJobMixin)

        op.execute_resumable(make_context(FakeTaskState()))
        assert op.submitted_ids == ["job-001"]

    def test_missing_all_methods_reports_every_one(self):
        assert _MissingEverything.__abstractmethods__ == frozenset(
            {
                "submit_job",
                "get_job_status",
                "is_job_active",
                "is_job_succeeded",
                "poll_until_complete",
                "get_job_result",
            }
        )
        with pytest.raises(TypeError, match="Can't instantiate abstract class _MissingEverything"):
            _MissingEverything(task_id="test_task")

    @pytest.mark.parametrize(
        ("partial_cls", "missing_method"),
        [
            pytest.param(_MissingSubmitJob, "submit_job", id="submit_job"),
            pytest.param(_MissingGetJobStatus, "get_job_status", id="get_job_status"),
            pytest.param(_MissingIsJobActive, "is_job_active", id="is_job_active"),
            pytest.param(_MissingIsJobSucceeded, "is_job_succeeded", id="is_job_succeeded"),
            pytest.param(_MissingPollUntilComplete, "poll_until_complete", id="poll_until_complete"),
            pytest.param(_MissingGetJobResult, "get_job_result", id="get_job_result"),
        ],
    )
    def test_missing_single_method_fails_at_construction(self, partial_cls, missing_method):
        assert partial_cls.__abstractmethods__ == frozenset({missing_method})
        with pytest.raises(TypeError):
            partial_cls(task_id="test_task")
