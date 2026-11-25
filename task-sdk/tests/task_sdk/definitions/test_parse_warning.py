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

import pytest

from airflow.sdk.definitions._internal.contextmanager import DagContext
from airflow.sdk.definitions.parse_warning import drain_parse_warnings, emit_dag_warning
from airflow.sdk.definitions.parse_warning_record import DagParseWarningRecord


@pytest.fixture(autouse=True)
def _isolate_dag_context_parse_state():
    """Keep emit/drain tests from leaking parse state across cases."""
    previous_module = DagContext.current_autoregister_module_name
    previous_warnings = DagContext.parse_warnings
    DagContext.current_autoregister_module_name = "test_parse_warning_module"
    DagContext.parse_warnings = []
    try:
        yield
    finally:
        DagContext.current_autoregister_module_name = previous_module
        DagContext.parse_warnings = previous_warnings


class TestEmitDagWarning:
    def test_records_default_warning_type(self):
        emit_dag_warning("failed_dag", "factory blew up")

        assert DagContext.parse_warnings == [
            DagParseWarningRecord(
                dag_id="failed_dag",
                message="factory blew up",
                warning_type="user:dynamic_dag_failure",
            )
        ]

    def test_appends_stacktrace_to_message(self):
        emit_dag_warning("failed_dag", "boom", stacktrace="Traceback (most recent call last):\n  ...")

        assert DagContext.parse_warnings == [
            DagParseWarningRecord(
                dag_id="failed_dag",
                message="boom\n\nTraceback (most recent call last):\n  ...",
                warning_type="user:dynamic_dag_failure",
            )
        ]

    def test_empty_dag_id_raises(self):
        with pytest.raises(ValueError, match="dag_id must be non-empty"):
            emit_dag_warning("", "message")

    def test_outside_dag_context_raises(self):
        DagContext.current_autoregister_module_name = None

        with pytest.raises(RuntimeError, match="can only be called while a Dag file is being parsed"):
            emit_dag_warning("failed_dag", "message")


class TestDrainParseWarnings:
    def test_returns_and_clears_recorded_warnings(self):
        emit_dag_warning("dag_a", "one")
        emit_dag_warning("dag_b", "two")

        drained = drain_parse_warnings()

        assert drained == [
            DagParseWarningRecord(dag_id="dag_a", message="one", warning_type="user:dynamic_dag_failure"),
            DagParseWarningRecord(dag_id="dag_b", message="two", warning_type="user:dynamic_dag_failure"),
        ]
        assert DagContext.parse_warnings == []
        assert drain_parse_warnings() == []
