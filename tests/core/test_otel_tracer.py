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
from unittest.mock import MagicMock, patch

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from airflow.traces import TRACEPARENT, TRACESTATE, otel_tracer, utils
from airflow.traces.tracer import Trace
from tests_common.test_utils.config import env_vars


@pytest.fixture
def name():
    return "test_traces_run"


class TestOtelTrace:
    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
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
    @patch("airflow.traces.otel_tracer.conf")
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
            dag_run = MagicMock()

            parent_trace_id = "0af7651916cd43dd8448eb211c80319c"
            parent_span_id = "b7ad6b7169203331"

            dag_run.conf = {
                TRACEPARENT: f"00-{parent_trace_id}-{parent_span_id}-01",
                TRACESTATE: "key1=val1,key2=val2",
            }
            dag_run.dag_id = "dag_id"
            dag_run.run_id = "run_id"
            dag_run.dag_hash = "hashcode"
            dag_run.run_type = "manual"
            dag_run.queued_at = now
            dag_run.start_date = now

            tracer = otel_tracer.get_otel_tracer(Trace)
            with tracer.start_span_from_dagrun(dagrun=dag_run) as s1:
                with tracer.start_span(span_name="span2") as s2:
                    s2.set_attribute("attr2", "val2")
                span1 = json.loads(s1.to_json())
            assert span1["context"]["trace_id"] != f"0x{parent_trace_id}"
            assert span1["links"][1]["context"]["trace_id"] == f"0x{parent_trace_id}"
            assert span1["links"][1]["context"]["span_id"] == f"0x{parent_span_id}"

    @patch("opentelemetry.sdk.trace.export.ConsoleSpanExporter")
    @patch("airflow.traces.otel_tracer.conf")
    def test_traskinstance_tracer(self, conf_a, exporter):
        # necessary to speed up the span to be emitted
        with env_vars({"OTEL_BSP_SCHEDULE_DELAY": "1"}):
            log = logging.getLogger("TestOtelTrace.test_taskinstance_tracer")
            log.setLevel(logging.DEBUG)
            conf_a.get.return_value = "abc"
            conf_a.getint.return_value = 123
            # this will enable debug to set - which outputs the result to console
            conf_a.getboolean.return_value = True

            # mocking console exporter with in mem exporter for better assertion
            in_mem_exporter = InMemorySpanExporter()
            exporter.return_value = in_mem_exporter

            now = datetime.now()
            # magic mock
            ti = MagicMock()
            ti.dag_run.conf = {}
            ti.task_id = "task_id"
            ti.start_date = now
            ti.dag_run.dag_id = "dag_id"
            ti.dag_run.run_id = "run_id"
            ti.dag_run.dag_hash = "hashcode"
            ti.dag_run.run_type = "manual"
            ti.dag_run.queued_at = now
            ti.dag_run.start_date = now

            tracer = otel_tracer.get_otel_tracer(Trace)
            with tracer.start_span_from_taskinstance(ti=ti, span_name="mydag") as s1:
                with tracer.start_span(span_name="span2") as s2:
                    s2.set_attribute("attr2", "val2")
                    span2 = json.loads(s2.to_json())
                span1 = json.loads(s1.to_json())

            log.info(span1)
            log.info(span2)
            assert span1["context"]["trace_id"] == f"0x{utils.gen_trace_id(ti.dag_run)}"
            assert span1["context"]["span_id"] == f"0x{utils.gen_span_id(ti)}"
