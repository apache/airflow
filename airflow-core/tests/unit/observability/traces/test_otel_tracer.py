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
import logging
from datetime import datetime
from unittest.mock import patch

import pytest
from opentelemetry.sdk import util
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow._shared.observability.traces.base_tracer import EmptyTrace
from airflow._shared.observability.traces.otel_tracer import OtelTrace
from airflow._shared.observability.traces.utils import datetime_to_nano
from airflow.observability.trace import DebugTrace, Trace
from airflow.observability.traces import otel_tracer

from tests_common.test_utils.config import env_vars


@pytest.fixture
def name():
    return "test_traces_run"


class TestOtelTrace:
    @env_vars(
        {
            "AIRFLOW__TRACES__OTEL_ON": "True",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_TRACES_EXPORTER": "console",
        }
    )
    def test_get_otel_tracer_from_trace_metaclass(self):
        """Test that `Trace.some_method()`, uses an `OtelTrace` instance when otel is configured."""
        tracer = otel_tracer.get_otel_tracer(Trace)
        assert tracer.use_simple_processor is False

        assert isinstance(Trace.factory(), EmptyTrace)

        Trace.configure_factory()
        assert isinstance(Trace.factory(), OtelTrace)

        task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        assert task_tracer.use_simple_processor is True

        task_tracer.get_otel_tracer_provider()
        assert task_tracer.use_simple_processor is True

    @env_vars(
        {
            "AIRFLOW__TRACES__OTEL_ON": "True",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_TRACES_EXPORTER": "otlp",
        }
    )
    def test_debug_trace_metaclass(self):
        """Test that `DebugTrace.some_method()`, uses the correct instance when the debug_traces flag is configured."""
        assert DebugTrace.check_debug_traces_flag is True

        # Factory hasn't been configured, it defaults to EmptyTrace.
        assert not isinstance(DebugTrace.factory(), OtelTrace)
        assert isinstance(DebugTrace.factory(), EmptyTrace)

        DebugTrace.configure_factory()
        # Factory has been configured, it should still be EmptyTrace.
        assert not isinstance(DebugTrace.factory(), OtelTrace)
        assert isinstance(DebugTrace.factory(), EmptyTrace)

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow._shared.observability.otel_env_config.OtelEnvConfig")
    @env_vars(
        {
            "OTEL_SERVICE_NAME": "my_test_service",
            # necessary to speed up the span to be emitted
            "OTEL_BSP_SCHEDULE_DELAY": "1",
        }
    )
    def test_tracer(self, otel_env_conf, exporter):
        log = logging.getLogger("TestOtelTrace.test_tracer")
        log.setLevel(logging.DEBUG)

        # mocking console exporter with in mem exporter for better assertion
        in_mem_exporter = InMemorySpanExporter()
        exporter.return_value = in_mem_exporter

        tracer = otel_tracer.get_otel_tracer(Trace)
        assert otel_env_conf.called
        otel_env_conf.assert_called_once()
        with tracer.start_span(span_name="span1") as s1:
            with tracer.start_span(span_name="span2") as s2:
                s2.set_attribute("attr2", "val2")
                span2 = json.loads(s2.to_json())
            span1 = json.loads(s1.to_json())
        # assert the two span data
        assert span1["name"] == "span1"
        assert span2["name"] == "span2"
        trace_id = span1["context"]["trace_id"]
        s1_span_id = span1["context"]["span_id"]
        assert span2["context"]["trace_id"] == trace_id
        assert span2["parent_id"] == s1_span_id
        assert span2["attributes"]["attr2"] == "val2"
        assert span2["resource"]["attributes"]["service.name"] == "my_test_service"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @env_vars(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            # necessary to speed up the span to be emitted
            "OTEL_BSP_SCHEDULE_DELAY": "1",
        }
    )
    def test_dag_tracer(self, exporter):
        log = logging.getLogger("TestOtelTrace.test_dag_tracer")
        log.setLevel(logging.DEBUG)

        # mocking console exporter with in mem exporter for better assertion
        in_mem_exporter = InMemorySpanExporter()
        exporter.return_value = in_mem_exporter

        now = datetime.now()

        tracer = otel_tracer.get_otel_tracer(Trace)
        with tracer.start_root_span(span_name="span1", start_time=now) as s1:
            with tracer.start_span(span_name="span2") as s2:
                s2.set_attribute("attr2", "val2")
                span2 = json.loads(s2.to_json())
            span1 = json.loads(s1.to_json())

        # The otel sdk, accepts an int for the start_time, and converts it to an iso string,
        # using `util.ns_to_iso_str()`.
        nano_time = datetime_to_nano(now)
        assert span1["start_time"] == util.ns_to_iso_str(nano_time)
        # Same trace_id
        assert span1["context"]["trace_id"] == span2["context"]["trace_id"]
        assert span1["context"]["span_id"] == span2["parent_id"]

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @env_vars(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            # necessary to speed up the span to be emitted
            "OTEL_BSP_SCHEDULE_DELAY": "1",
        }
    )
    def test_context_propagation(self, exporter):
        log = logging.getLogger("TestOtelTrace.test_context_propagation")
        log.setLevel(logging.DEBUG)

        # mocking console exporter with in mem exporter for better assertion
        in_mem_exporter = InMemorySpanExporter()
        exporter.return_value = in_mem_exporter

        # Method that represents another service which is
        #  - getting the carrier
        #  - extracting the context
        #  - using the context to create a new span
        # The new span should be associated with the span from the injected context carrier.
        def _task_func(otel_tr, carrier):
            parent_context = otel_tr.extract(carrier)

            with otel_tr.start_child_span(span_name="sub_span", parent_context=parent_context) as span:
                span.set_attribute("attr2", "val2")
                json_span = json.loads(span.to_json())
            return json_span

        tracer = otel_tracer.get_otel_tracer(Trace)

        root_span = tracer.start_root_span(span_name="root_span", start_as_current=False)
        # The context is available, it can be injected into the carrier.
        context_carrier = tracer.inject()

        # Some function that uses the carrier to create a new span.
        json_span2 = _task_func(otel_tr=tracer, carrier=context_carrier)

        json_span1 = json.loads(root_span.to_json())
        # Manually end the span.
        root_span.end()

        # Verify that span1 is a root span.
        assert json_span1["parent_id"] is None
        # Check span2 parent_id to verify that it's a child of span1.
        assert json_span2["parent_id"] == json_span1["context"]["span_id"]
        # The trace_id and the span_id are randomly generated by the otel sdk.
        # Both spans should belong to the same trace.
        assert json_span1["context"]["trace_id"] == json_span2["context"]["trace_id"]

    @pytest.mark.parametrize(
        ("provided_env_vars", "expected_endpoint", "expected_exporter_module"),
        [
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
                    "AIRFLOW__TRACES__OTEL_HOST": "breeze-otel-collector",
                    "AIRFLOW__TRACES__OTEL_PORT": "4318",
                },
                "localhost:1234",
                "grpc",
                id="env_vars_with_grpc",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
                    "AIRFLOW__TRACES__OTEL_HOST": "breeze-otel-collector",
                    "AIRFLOW__TRACES__OTEL_PORT": "4318",
                },
                "http://breeze-otel-collector:4318/v1/traces",
                "http",
                id="protocol_is_ignored_if_no_env_endpoint",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                    "AIRFLOW__TRACES__OTEL_HOST": "breeze-otel-collector",
                    "AIRFLOW__TRACES__OTEL_PORT": "4318",
                },
                "http://localhost:1234/v1/traces",
                "http",
                id="for_http_with_env_vars_otel_builds_full_url",
            ),
            pytest.param(
                {
                    "AIRFLOW__TRACES__OTEL_HOST": "breeze-otel-collector",
                    "AIRFLOW__TRACES__OTEL_PORT": "4318",
                },
                "http://breeze-otel-collector:4318/v1/traces",
                "http",
                id="use_airflow_config",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                },
                "http://localhost:1234/v1/traces",
                "http",
                id="only_env_vars",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://localhost:2222",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "grpc",
                },
                "localhost:2222",
                "grpc",
                id="type_specific_vars_take_precedence",
            ),
        ],
    )
    def test_config_priorities(self, provided_env_vars, expected_endpoint, expected_exporter_module):
        with env_vars(provided_env_vars):
            tracer = otel_tracer.get_otel_tracer(Trace)

            assert tracer.span_exporter._endpoint == expected_endpoint

            assert (
                tracer.span_exporter.__class__.__module__
                == f"opentelemetry.exporter.otlp.proto.{expected_exporter_module}.trace_exporter"
            )
