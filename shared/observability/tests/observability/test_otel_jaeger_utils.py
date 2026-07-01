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

from tests_common.test_utils.otel_jaeger_utils import (
    get_descendant_span_names,
    get_parent_span_id,
    get_span_tags,
    provided_child_spans_found_under_span,
)

# A Jaeger trace with the following span hierarchy:
#
# scheduler.scheduler_loop
# ├── scheduler._do_scheduling
# │   └── scheduler.critical_section
# └── scheduler._process_executor_events
LOOP_SPAN = {
    "spanID": "loop",
    "operationName": "scheduler.scheduler_loop",
    "references": [],
    "tags": [
        {"key": "airflow.scheduler.loop_iteration.idle", "type": "bool", "value": False},
        {"key": "airflow.category", "type": "string", "value": "scheduler"},
    ],
}
DO_SCHEDULING_SPAN = {
    "spanID": "do_scheduling",
    "operationName": "scheduler._do_scheduling",
    "references": [{"refType": "CHILD_OF", "traceID": "trace-1", "spanID": "loop"}],
    "tags": [],
}
CRITICAL_SECTION_SPAN = {
    "spanID": "critical_section",
    "operationName": "scheduler.critical_section",
    "references": [{"refType": "CHILD_OF", "traceID": "trace-1", "spanID": "do_scheduling"}],
    "tags": [],
}
PROCESS_EVENTS_SPAN = {
    "spanID": "process_events",
    "operationName": "scheduler._process_executor_events",
    "references": [{"refType": "CHILD_OF", "traceID": "trace-1", "spanID": "loop"}],
    "tags": [],
}

TRACE = {
    "traceID": "trace-1",
    "spans": [LOOP_SPAN, DO_SCHEDULING_SPAN, CRITICAL_SECTION_SPAN, PROCESS_EVENTS_SPAN],
}

PARENT_CHILDREN_MAP = {
    "loop": [DO_SCHEDULING_SPAN, PROCESS_EVENTS_SPAN],
    "do_scheduling": [CRITICAL_SECTION_SPAN],
}


class TestJaegerUtilsUnit:
    def test_get_span_tags(self):
        assert get_span_tags(LOOP_SPAN) == {
            "airflow.scheduler.loop_iteration.idle": False,
            "airflow.category": "scheduler",
        }

    @pytest.mark.parametrize(
        "span",
        [
            pytest.param({"tags": []}, id="empty-tags"),
            pytest.param({}, id="missing-tags-key"),
        ],
    )
    def test_get_span_tags_without_tags(self, span: dict):
        assert get_span_tags(span) == {}

    @pytest.mark.parametrize(
        ("span", "expected_parent_id"),
        [
            pytest.param(DO_SCHEDULING_SPAN, "loop", id="direct-child"),
            pytest.param(CRITICAL_SECTION_SPAN, "do_scheduling", id="child-of-intermediate-span"),
            pytest.param(LOOP_SPAN, None, id="root-span-empty-references"),
            pytest.param({"operationName": "test"}, None, id="missing-references-key"),
            pytest.param(
                {"references": [{"refType": "FOLLOWS_FROM", "spanID": "other"}]},
                None,
                id="non-child-of-ref-type",
            ),
        ],
    )
    def test_get_parent_span_id(self, span: dict, expected_parent_id: str):
        assert get_parent_span_id(span) == expected_parent_id

    @pytest.mark.parametrize(
        ("span_id", "expected_names"),
        [
            pytest.param(
                "loop",
                {
                    "scheduler._do_scheduling",
                    "scheduler.critical_section",
                    "scheduler._process_executor_events",
                },
                id="root-collects-all-even-nested",
            ),
            pytest.param("do_scheduling", {"scheduler.critical_section"}, id="intermediate-node"),
            pytest.param("critical_section", set(), id="leaf-has-no-descendants"),
            pytest.param("unknown", set(), id="unknown-span-id"),
        ],
    )
    def test_get_descendant_span_names(self, span_id: str, expected_names: set):
        assert get_descendant_span_names(PARENT_CHILDREN_MAP, span_id) == expected_names

    @pytest.mark.parametrize(
        ("span_name", "child_span_names", "expected_bool_result"),
        [
            pytest.param(
                "scheduler.scheduler_loop",
                ["scheduler._do_scheduling", "scheduler._process_executor_events"],
                True,
                id="all-direct-children-present",
            ),
            pytest.param(
                "scheduler.scheduler_loop",
                ["scheduler.critical_section"],
                True,
                id="nested-descendant-present",
            ),
            pytest.param(
                "scheduler.scheduler_loop",
                ["scheduler._do_scheduling", "scheduler.unknown_span"],
                False,
                id="unknown-child-span",
            ),
            pytest.param(
                "scheduler.unknown_span",
                ["scheduler._do_scheduling"],
                False,
                id="unknown-parent-span",
            ),
            pytest.param(
                "scheduler._do_scheduling",
                ["scheduler.critical_section"],
                True,
                id="descendant-under-intermediate-span",
            ),
            pytest.param(
                "scheduler._do_scheduling",
                ["scheduler._process_executor_events"],
                False,
                id="sibling-is-not-a-descendant",
            ),
        ],
    )
    def test_provided_child_spans_found_under_span(
        self, span_name: str, child_span_names: list, expected_bool_result
    ):
        assert (
            provided_child_spans_found_under_span(TRACE, span_name, child_span_names) is expected_bool_result
        )
