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
from __future__ import annotations

from types import SimpleNamespace

import pytest

from airflow.sdk import Asset
from airflow.sdk.definitions._internal.contextmanager import _CURRENT_CONTEXT
from airflow.sdk.definitions.context import Context, clone_context, get_current_context
from airflow.sdk.execution_time.context import InletEventsAccessors


class TestCurrentContext:
    def test_current_context_no_context_raise(self):
        with pytest.raises(RuntimeError):
            get_current_context()

    def test_get_current_context_with_context(self):
        mock_context = {"ti": "task_instance", "key": "value"}
        token = _CURRENT_CONTEXT.set([mock_context])
        try:
            result = get_current_context()
            assert result == mock_context
        finally:
            _CURRENT_CONTEXT.reset(token)

    def test_get_current_context_without_context(self):
        token = _CURRENT_CONTEXT.set([])
        try:
            with pytest.raises(RuntimeError, match="Current context was requested but no context was found!"):
                get_current_context()
        finally:
            _CURRENT_CONTEXT.reset(token)

    def test_clone_context_deep_and_shallow_copy_semantics(self):
        outlet_events = [Asset(name="dummy")]
        inlet_events = InletEventsAccessors(inlets=[])

        dag_run = SimpleNamespace(
            dag_id="dag",
            run_id="r1",
            logical_date=None,
            data_interval_start=None,
            data_interval_end=None,
            run_after=None,
            start_date=None,
            end_date=None,
            clear_number=None,
            run_type=None,
            state=None,
            conf=None,
            triggering_user_name=None,
            consumed_asset_events=[],
            partition_key=None,
            note=None,
        )

        actual = Context()
        actual.update(
            {
                "params": {"p": {"n": 1}},
                "templates_dict": {"tpl": ["a", {"x": 1}]},
                "inlets": [object()],
                "outlets": [object()],
                "outlet_events": outlet_events,
                "inlet_events": inlet_events,
                "dag_run": dag_run,
            }
        )
        cloned = clone_context(actual)

        assert cloned is not actual

        actual["params"]["p"]["n"] = 999
        assert cloned["params"]["p"]["n"] == 1

        actual["templates_dict"]["tpl"][1]["x"] = 42
        assert cloned["templates_dict"]["tpl"][1]["x"] == 1

        actual["outlet_events"].append(Asset(name="another"))
        assert cloned["outlet_events"] is actual["outlet_events"]
        assert len(cloned["outlet_events"]) == 2
        assert Asset(name="another") in cloned["outlet_events"]
        assert cloned["inlet_events"] is not actual["inlet_events"]

        actual["dag_run"].dag_id = "changed"
        assert cloned["dag_run"].dag_id == "dag"
