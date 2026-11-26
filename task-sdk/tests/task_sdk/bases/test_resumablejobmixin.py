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
import time_machine
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.sdk import ResumableJobMixin
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.resumablejobmixin import DURABLE_RECORD_KEY, ResumeDecision

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
    """Minimal stand-in for RuntimeTaskInstance exposing stats_tags."""

    def __init__(self, team_name: str | None = None):
        self._team_name = team_name

    @property
    def stats_tags(self) -> dict[str, str]:
        tags = {"dag_id": "d", "task_id": "t", "run_type": "scheduled"}
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


class TestDurableRecord:
    def test_reconnect_records_the_full_job_time_once_the_job_finishes(self):
        task_state = FakeTaskState()
        ti = FakeTI()
        ti.try_number = 1
        with time_machine.travel("2026-11-10 10:00:00+00:00", tick=False):
            ConcreteResumableOperator(task_id="t").execute_resumable(
                {"task_state_store": task_state, "ti": ti}
            )

        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-001": "RUNNING"}
        ti.try_number = 2
        saved_while_running: list = []
        with time_machine.travel("2026-11-10 10:01:00+00:00", tick=False) as traveller:

            def poll(external_id, context):
                running = task_state.get(DURABLE_RECORD_KEY)["tries"][-1]
                saved_while_running.append((running["status"], running["saved_seconds"]))
                traveller.move_to("2026-11-10 10:20:00+00:00")
                op._status_map["job-001"] = "SUCCEEDED"

            op.poll_until_complete = poll
            op.execute_resumable({"task_state_store": task_state, "ti": ti})

        assert saved_while_running == [("running", None)]
        assert task_state.get(DURABLE_RECORD_KEY)["tries"] == [
            {
                "try_number": 1,
                "decision": "submitted",
                "external_id": "job-001",
                "status": None,
                "submitted_at": "2026-11-10T10:00:00+00:00",
                "saved_seconds": None,
            },
            {
                "try_number": 2,
                "decision": "reconnected",
                "external_id": "job-001",
                "status": "success",
                "submitted_at": "2026-11-10T10:00:00+00:00",
                "saved_seconds": 1200.0,
            },
        ]

    def test_already_succeeded_records_success_without_saved_time(self):
        task_state = FakeTaskState()
        ti = FakeTI()
        op = ConcreteResumableOperator(task_id="t")
        ctx = {"task_state_store": task_state, "ti": ti}

        ti.try_number = 1
        with time_machine.travel("2026-11-10 10:00:00+00:00", tick=False):
            op.execute_resumable(ctx)

        ti.try_number = 2
        op._status_map = {"job-001": "SUCCEEDED"}
        with time_machine.travel("2026-11-10 10:20:00+00:00", tick=False):
            op.execute_resumable(ctx)

        assert task_state.get(DURABLE_RECORD_KEY)["tries"][-1] == {
            "try_number": 2,
            "decision": "already_succeeded",
            "external_id": "job-001",
            "status": "success",
            "submitted_at": "2026-11-10T10:00:00+00:00",
            "saved_seconds": None,
        }

    def test_each_try_keeps_the_submit_time_of_its_own_job(self):
        task_state = FakeTaskState()
        ti = FakeTI()
        op = ConcreteResumableOperator(task_id="t")
        ctx = {"task_state_store": task_state, "ti": ti}

        ti.try_number = 1
        with time_machine.travel("2026-11-10 10:00:00+00:00", tick=False):
            op.execute_resumable(ctx)

        ti.try_number = 2
        op._status_map = {"job-001": "RUNNING"}

        def poll_job_fails(external_id, context):
            op._status_map["job-001"] = "FAILED"
            raise RuntimeError("job failed")

        op.poll_until_complete = poll_job_fails
        with pytest.raises(RuntimeError, match="job failed"):
            op.execute_resumable(ctx)

        ti.try_number = 3
        op._next_id = "job-002"
        op.poll_until_complete = lambda external_id, context: None
        with time_machine.travel("2026-11-10 11:00:00+00:00", tick=False):
            op.execute_resumable(ctx)

        tries = task_state.get(DURABLE_RECORD_KEY)["tries"]
        assert [(t["external_id"], t["submitted_at"]) for t in tries] == [
            ("job-001", "2026-11-10T10:00:00+00:00"),
            ("job-001", "2026-11-10T10:00:00+00:00"),
            ("job-002", "2026-11-10T11:00:00+00:00"),
        ]

    @pytest.mark.parametrize(
        ("status_after_poll", "expected_status"),
        [
            pytest.param("RUNNING", "running", id="poll-returned-early"),
            pytest.param("STOPPED", "success", id="poll-accepted-other-end-state"),
        ],
    )
    def test_reconnect_records_job_status_after_poll(self, status_after_poll, expected_status):
        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-001": "RUNNING"}
        task_state = FakeTaskState({"test_job_id": "job-001"})

        def poll(external_id, context):
            op._status_map["job-001"] = status_after_poll

        op.poll_until_complete = poll
        op.execute_resumable(make_context(task_state))

        last_try = task_state.get(DURABLE_RECORD_KEY)["tries"][-1]
        assert (last_try["status"], last_try["saved_seconds"]) == (expected_status, None)

    @pytest.mark.parametrize(
        ("status_after_raise", "expected_status"),
        [
            pytest.param("FAILED", "failed", id="job-failed"),
            pytest.param("RUNNING", "running", id="wait-timed-out"),
        ],
    )
    def test_reconnect_records_failed_only_when_the_job_ended(self, status_after_raise, expected_status):
        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-001": "RUNNING"}
        task_state = FakeTaskState({"test_job_id": "job-001"})

        def poll(external_id, context):
            op._status_map["job-001"] = status_after_raise
            raise RuntimeError("poll stopped")

        op.poll_until_complete = poll

        with pytest.raises(RuntimeError, match="poll stopped"):
            op.execute_resumable(make_context(task_state))

        assert task_state.get(DURABLE_RECORD_KEY)["tries"][-1]["status"] == expected_status

    def test_reconnect_status_read_failure_does_not_fail_the_task(self):
        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-001": "RUNNING"}
        task_state = FakeTaskState({"test_job_id": "job-001"})

        def get_job_status_after_poll(external_id, context):
            raise RuntimeError("api down")

        def poll(external_id, context):
            op.get_job_status = get_job_status_after_poll

        op.poll_until_complete = poll

        op.execute_resumable(make_context(task_state))

        assert task_state.get(DURABLE_RECORD_KEY)["tries"][-1]["status"] == "running"

    def test_resubmit_after_failed_job_records_the_failed_job(self):
        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-000": "FAILED"}
        task_state = FakeTaskState({"test_job_id": "job-000"})

        with time_machine.travel("2026-11-10 10:00:00+00:00", tick=False):
            op.execute_resumable(make_context(task_state))

        assert task_state.get(DURABLE_RECORD_KEY)["tries"][-1] == {
            "try_number": None,
            "decision": "resubmitted",
            "external_id": "job-001",
            "status": None,
            "submitted_at": "2026-11-10T10:00:00+00:00",
            "saved_seconds": None,
            "previous_external_id": "job-000",
        }

    def test_resubmit_reads_the_stored_job_id_once(self):
        op = ConcreteResumableOperator(task_id="t")
        op._status_map = {"job-000": "FAILED"}
        task_state = FakeTaskState({"test_job_id": "job-000"})
        read_keys: list[str] = []
        original_get = task_state.get

        def tracking_get(key):
            read_keys.append(key)
            return original_get(key)

        task_state.get = tracking_get

        op.execute_resumable(make_context(task_state))

        assert read_keys.count("test_job_id") == 1

    def test_record_failure_does_not_fail_the_task(self):
        task_state = FakeTaskState()
        original_set = task_state.set

        def set_failing_for_record(key, value):
            if key == DURABLE_RECORD_KEY:
                raise RuntimeError("store down")
            original_set(key, value)

        task_state.set = set_failing_for_record

        result = ConcreteResumableOperator(task_id="t").execute_resumable(make_context(task_state))

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

    def test_reconnects_to_stored_id_zero(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["0"] = "RUNNING"
        task_state = FakeTaskState({"test_job_id": 0})

        op.execute_resumable(make_context(task_state))

        assert op.submitted_ids == []
        assert op.polled_ids == ["0"]

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
    _TI_TAGS = {"dag_id": "d", "task_id": "t", "run_type": "scheduled"}
    _FULL_TAGS = {**_TI_TAGS, "team_name": "team_alpha", **_TAG}

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
        ("stored_status", "expected_names"),
        [
            pytest.param(None, ["resumable_job.fresh_submit"], id="fresh_submit"),
            pytest.param(
                "RUNNING",
                ["resumable_job.reconnect_attempt", "resumable_job.reconnect_success"],
                id="reconnect",
            ),
            pytest.param(
                "SUCCEEDED",
                ["resumable_job.reconnect_attempt", "resumable_job.already_succeeded"],
                id="already_succeeded",
            ),
            pytest.param(
                "FAILED",
                ["resumable_job.reconnect_attempt", "resumable_job.terminal_resubmit"],
                id="terminal_resubmit",
            ),
        ],
    )
    def test_every_counter_carries_task_instance_stats_tags(self, stored_status, expected_names):
        op = ConcreteResumableOperator(task_id="test_task")
        if stored_status is None:
            task_state = FakeTaskState()
        else:
            op._status_map["job-001"] = stored_status
            task_state = FakeTaskState({"test_job_id": "job-001"})
        ctx = make_context(task_state)
        ctx["ti"] = FakeTI("team_alpha")
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(ctx)
        assert [call.args[0] for call in mock_incr.call_args_list] == expected_names
        assert all(call.kwargs["tags"] == self._FULL_TAGS for call in mock_incr.call_args_list)

    def test_team_name_omitted_when_task_has_no_team(self):
        op = ConcreteResumableOperator(task_id="test_task")
        ctx = make_context(FakeTaskState())
        ctx["ti"] = FakeTI()
        mock_incr = MagicMock()
        with patch(self._PATCH, mock_incr):
            op.execute_resumable(ctx)
        mock_incr.assert_called_once_with("resumable_job.fresh_submit", tags={**self._TI_TAGS, **self._TAG})


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


class TestGetResumeDecision:
    @pytest.mark.parametrize(
        ("stored", "status", "expected"),
        [
            (None, None, ResumeDecision("submit")),
            ("job-001", "RUNNING", ResumeDecision("reconnect", "job-001")),
            ("job-001", "SUCCEEDED", ResumeDecision("already_succeeded", "job-001")),
            ("job-001", "FAILED", ResumeDecision("submit", "job-001")),
        ],
    )
    def test_decides_without_submitting_or_polling(self, stored, status, expected):
        op = ConcreteResumableOperator(task_id="test_task")
        if status:
            op._status_map[stored] = status
        task_state = FakeTaskState({"test_job_id": stored} if stored else None)

        assert op.get_resume_decision(make_context(task_state)) == expected
        assert op.submitted_ids == []
        assert op.polled_ids == []

    @pytest.mark.parametrize(
        ("status", "expected_entry"),
        [
            pytest.param("RUNNING", ("reconnected", "running"), id="reconnect"),
            pytest.param("SUCCEEDED", ("already_succeeded", "success"), id="already-succeeded"),
            pytest.param("FAILED", None, id="submit"),
        ],
    )
    def test_records_the_decision_for_the_banner(self, status, expected_entry):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = status
        task_state = FakeTaskState({"test_job_id": "job-001"})

        op.get_resume_decision(make_context(task_state))

        record = task_state.get(DURABLE_RECORD_KEY)
        if expected_entry is None:
            assert record is None
        else:
            assert (record["tries"][-1]["decision"], record["tries"][-1]["status"]) == expected_entry

    def test_submit_without_reading_store_when_not_durable(self):
        op = ConcreteResumableOperator(task_id="test_task", durable=False)
        task_state = MagicMock(spec=FakeTaskState)

        assert op.get_resume_decision(make_context(task_state)) == ResumeDecision("submit")
        task_state.get.assert_not_called()


class TestPersistJobId:
    def test_persisted_id_is_reconnected_on_retry(self):
        op = ConcreteResumableOperator(task_id="test_task")
        op._status_map["job-001"] = "RUNNING"
        task_state = FakeTaskState()

        op.persist_job_id(make_context(task_state), "job-001")

        assert op.get_resume_decision(make_context(task_state)) == ResumeDecision("reconnect", "job-001")

    @pytest.mark.parametrize(("durable", "external_id"), [(False, "job-001"), (True, None)])
    def test_skips_persist(self, durable, external_id):
        op = ConcreteResumableOperator(task_id="test_task", durable=durable)
        task_state = FakeTaskState()

        op.persist_job_id(make_context(task_state), external_id)

        assert task_state._store == {}
