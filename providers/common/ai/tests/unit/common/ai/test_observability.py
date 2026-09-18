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

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai import Agent
from pydantic_ai.models.instrumented import InstrumentationSettings
from pydantic_ai.models.test import TestModel

from airflow.providers.common.ai import observability
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook


def _conf(*, enabled: bool = False, capture: bool = False) -> MagicMock:
    """Return a conf double whose getboolean reflects the two flags."""
    values = {"otel_export_enabled": enabled, "capture_content": capture}
    m = MagicMock()
    m.getboolean.side_effect = lambda section, key, fallback=False: values.get(key, fallback)
    return m


class TestGenaiInstrumentationSettings:
    def test_returns_none_when_export_disabled(self):
        with patch.object(observability, "conf", _conf(enabled=False)):
            assert observability.genai_instrumentation_settings() is None

    def test_returns_none_when_no_live_provider(self):
        with (
            patch.object(observability, "conf", _conf(enabled=True)),
            patch.object(observability, "_live_tracer_provider", return_value=None),
        ):
            assert observability.genai_instrumentation_settings() is None

    def test_returns_settings_when_enabled_with_provider(self):
        with (
            patch.object(observability, "conf", _conf(enabled=True, capture=False)),
            patch.object(observability, "_live_tracer_provider", return_value=TracerProvider()),
        ):
            settings = observability.genai_instrumentation_settings()

        assert settings is not None
        assert settings.version == observability._SEMCONV_VERSION == 5
        # Content is never emitted unless explicitly opted in.
        assert settings.include_content is False
        assert settings.include_binary_content is False
        # The provided SDK provider was used to build the tracer.
        assert settings.tracer is not None

    def test_capture_content_opt_in_enables_content(self):
        with (
            patch.object(observability, "conf", _conf(enabled=True, capture=True)),
            patch.object(observability, "_live_tracer_provider", return_value=TracerProvider()),
        ):
            settings = observability.genai_instrumentation_settings()

        assert settings is not None
        assert settings.include_content is True
        # Binary content stays off even when text content is captured.
        assert settings.include_binary_content is False


class TestLiveTracerProvider:
    def test_none_when_provider_is_not_sdk(self):
        # The API's no-op proxy default (anything that is not an SDK
        # TracerProvider) must be rejected, so we never emit orphan spans.
        with patch("opentelemetry.trace.get_tracer_provider", return_value=object()):
            assert observability._live_tracer_provider() is None

    def test_returns_sdk_provider(self):
        provider = TracerProvider()
        with patch("opentelemetry.trace.get_tracer_provider", return_value=provider):
            assert observability._live_tracer_provider() is provider


class TestEndToEndSpanEmission:
    """Exercise the real instrumentation path: build an agent through the hook,
    run it, and assert on the genuine spans the OpenTelemetry SDK exports."""

    # A distinctive user-prompt string we can grep for in the exported spans.
    _PROMPT = "user-secret-needle-42"

    @staticmethod
    def _run(*, capture: bool):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        hook = PydanticAIHook(llm_conn_id="c", model_id="test")
        with (
            patch.object(observability, "conf", _conf(enabled=True, capture=capture)),
            patch.object(observability, "_live_tracer_provider", return_value=provider),
            patch.object(hook, "get_conn", return_value=TestModel()),
        ):
            agent = hook.create_agent(instructions="be helpful")
            # Stand in for the worker's task span: open a parent and run inside it.
            with provider.get_tracer("test").start_as_current_span("worker.task") as parent:
                parent_trace_id = parent.get_span_context().trace_id
                agent.run_sync(TestEndToEndSpanEmission._PROMPT)

        spans = exporter.get_finished_spans()
        genai = [s for s in spans if s.attributes and any(k.startswith("gen_ai.") for k in s.attributes)]
        attrs_blob = json.dumps([dict(s.attributes or {}) for s in genai])
        return genai, parent_trace_id, attrs_blob

    def test_spans_emitted_and_nested_without_content_by_default(self):
        genai, parent_trace_id, attrs_blob = self._run(capture=False)

        assert genai, "expected gen_ai spans to be emitted"
        # Token usage is captured even with content off. In pydantic-ai 2.x the
        # model-request span keeps ``gen_ai.usage.*`` while the agent-run span
        # reports ``gen_ai.aggregated_usage.*`` (avoids double-counting when a
        # backend sums parent and child spans); assert both so a change in that
        # split is caught here rather than silently shifting users' telemetry.
        assert any("gen_ai.usage.input_tokens" in (s.attributes or {}) for s in genai)
        assert any("gen_ai.aggregated_usage.input_tokens" in (s.attributes or {}) for s in genai)
        # Parenting is implicit: agent spans share the task span's trace_id.
        assert all(s.context.trace_id == parent_trace_id for s in genai)
        # The prompt text must not leak when content capture is off.
        assert self._PROMPT not in attrs_blob

    def test_capture_content_includes_prompt_text(self):
        genai, _, attrs_blob = self._run(capture=True)

        assert genai
        # With the opt-in, the prompt text is present on the spans.
        assert self._PROMPT in attrs_blob


class TestBuildRunIdentityAttributes:
    @pytest.mark.parametrize(("map_index", "expected_map_index"), [(3, 3), (None, -1)])
    def test_builds_expected_attributes(self, map_index, expected_map_index):
        ti = SimpleNamespace(
            id="ti-1", dag_id="d", task_id="t", run_id="r", try_number=2, map_index=map_index
        )
        assert observability.build_run_identity_attributes(ti) == {
            "airflow.dag_id": "d",
            "airflow.task_id": "t",
            "airflow.dag_run.run_id": "r",
            "airflow.task_instance.try_number": 2,
            "airflow.task_instance.map_index": expected_map_index,
            "airflow.task_instance.id": "ti-1",
        }


class TestStampIdentityOnAgentSpans:
    _ATTRS = {
        "airflow.dag_id": "d",
        "airflow.task_id": "t",
        "airflow.dag_run.run_id": "r",
        "airflow.task_instance.try_number": 2,
        "airflow.task_instance.map_index": -1,
        "airflow.task_instance.id": "ti-xyz",
    }

    def test_attributes_and_run_id_land_on_genai_spans(self):
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        hook = PydanticAIHook(llm_conn_id="c", model_id="test")
        with (
            patch.object(observability, "conf", _conf(enabled=True)),
            patch.object(observability, "_live_tracer_provider", return_value=provider),
            patch.object(hook, "get_conn", return_value=TestModel()),
        ):
            agent = hook.create_agent(instructions="be helpful")
            observability.stamp_identity_on_agent_spans(agent, self._ATTRS)
            agent.run_sync("hi", run_id="ti-xyz")

        genai = [
            s
            for s in exporter.get_finished_spans()
            if s.attributes and any(k.startswith("gen_ai.") for k in s.attributes)
        ]
        assert genai, "expected gen_ai spans to be emitted"
        # Every GenAI span carries the full Airflow identity, not just the agent-run span.
        for span in genai:
            for key, value in self._ATTRS.items():
                assert span.attributes.get(key) == value
        # run_id passed to the run surfaces as the OTel agent-call id.
        assert any(s.attributes.get("gen_ai.agent.call.id") == "ti-xyz" for s in genai)

    def test_noop_when_agent_not_instrumented(self):
        # A non-InstrumentationSettings ``instrument`` (here ``False``) is left
        # untouched: stamping must not wrap it or raise.
        agent = Agent(TestModel())
        agent.instrument = False
        observability.stamp_identity_on_agent_spans(agent, self._ATTRS)
        assert agent.instrument is False

    def test_wraps_a_copy_without_mutating_or_nesting_shared_settings(self):
        # A caller-owned settings object handed to several agents (or reused across
        # HITL re-runs) must stay untouched: each agent gets its own wrapped copy,
        # and every wrapper wraps the real tracer directly rather than nesting.
        provider = TracerProvider()
        shared = InstrumentationSettings(tracer_provider=provider)
        original_tracer = shared.tracer

        agent1, agent2 = Agent(TestModel()), Agent(TestModel())
        agent1.instrument = shared
        agent2.instrument = shared
        observability.stamp_identity_on_agent_spans(agent1, self._ATTRS)
        observability.stamp_identity_on_agent_spans(agent2, self._ATTRS)

        assert shared.tracer is original_tracer
        assert agent1.instrument is not shared
        assert agent1.instrument is not agent2.instrument
        assert isinstance(agent1.instrument.tracer, observability._IdentityTracer)
        assert agent1.instrument.tracer._tracer is original_tracer
        assert agent2.instrument.tracer._tracer is original_tracer
