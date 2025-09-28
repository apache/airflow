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

from airflow.configuration import conf
from airflow.observability.traces import otel_tracer
from airflow.observability.traces.base_tracer import DebugTrace, EmptyTrace, Trace
from airflow.observability.traces.otel_tracer import OtelTrace
from airflow.utils.dates import datetime_to_nano

from tests_common.test_utils.config import env_vars


@pytest.fixture
def name():
    return "test_traces_run"


class TestOtelTrace:
    def test_get_otel_tracer_from_trace_metaclass(self):
        """Test that `Trace.some_method()`, uses an `OtelTrace` instance when otel is configured."""
        conf.set("traces", "otel_on", "True")
        conf.set("traces", "otel_debugging_on", "True")

        tracer = otel_tracer.get_otel_tracer(Trace)
        assert tracer.use_simple_processor is False

        assert isinstance(Trace.factory(), EmptyTrace)

        Trace.configure_factory()
        assert isinstance(Trace.factory(), OtelTrace)

        task_tracer = otel_tracer.get_otel_tracer_for_task(Trace)
        assert task_tracer.use_simple_processor is True

        task_tracer.get_otel_tracer_provider()
        assert task_tracer.use_simple_processor is True

    def test_debug_trace_metaclass(self):
        """Test that `DebugTrace.some_method()`, uses the correct instance when the debug_traces flag is configured."""
        conf.set("traces", "otel_on", "True")
        conf.set("traces", "otel_debug_traces_on", "False")

        assert DebugTrace.check_debug_traces_flag is True

        # Factory hasn't been configured, it defaults to EmptyTrace.
        assert not isinstance(DebugTrace.factory(), OtelTrace)
        assert isinstance(DebugTrace.factory(), EmptyTrace)

        DebugTrace.configure_factory()
        # Factory has been configured, it should still be EmptyTrace.
        assert not isinstance(DebugTrace.factory(), OtelTrace)
        assert isinstance(DebugTrace.factory(), EmptyTrace)

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.observability.traces.otel_tracer.conf")
    def test_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_tracer")
            log.setLevel(logging.DEBUG)
            # hijacking airflow conf with pre-defined
            # values
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

            # mocking console exporter with in mem exporter for better assertion
            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            tracer = otel_tracer.get_otel_tracer(Trace)
            assert conf_a.get.called
            assert conf_a.getint.called
            assert conf_a.getboolean.called
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
            assert span2["resource"]["attributes"]["service.name"] == "abc"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.observability.traces.otel_tracer.conf")
    def test_dag_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_dag_tracer")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

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
    @patch("airflow.observability.traces.otel_tracer.conf")
    def test_context_propagation(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_context_propagation")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

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
