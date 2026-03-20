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

import ast
import textwrap
from pathlib import Path

import pytest
from ci.prek.check_metrics_synced_with_the_registry import (
    _PREFIX_MATCHED,
    extract_metric_name_from_ast_node,
    find_registry_match,
    get_stats_obj_name,
    normalize_metric_name,
    scan_file_for_metrics,
)

METRICS_REGISTRY = {
    "scheduler.heartbeat": {
        "name": "scheduler.heartbeat",
        "type": "counter",
        "legacy_name": "-",
    },
    "pool.open_slots": {
        "name": "pool.open_slots",
        "type": "gauge",
        "legacy_name": "pool.open_slots.{pool_name}",
    },
    "ti.scheduled": {
        "name": "ti.scheduled",
        "type": "counter",
        "legacy_name": "ti.scheduled.{queue}.{dag_id}.{task_id}",
    },
    "ti.queued": {
        "name": "ti.queued",
        "type": "counter",
        "legacy_name": "ti.queued.{queue}.{dag_id}.{task_id}",
    },
    "dagrun.duration.success": {
        "name": "dagrun.duration.success",
        "type": "timer",
        "legacy_name": "dagrun.duration.success.{dag_id}",
    },
    "task.duration": {
        "name": "task.duration",
        "type": "timer",
        "legacy_name": "dag.{dag_id}.{task_id}.duration",
    },
    "executor.open_slots": {
        "name": "executor.open_slots",
        "type": "gauge",
        "legacy_name": "executor.open_slots.{executor_class_name}",
    },
    "ti.start.{dag_id}.{task_id}": {
        "name": "ti.start.{dag_id}.{task_id}",
        "type": "timer",
        "legacy_name": "-",
    },
}


@pytest.mark.parametrize(
    "metric_name, expected_result",
    [
        pytest.param("pool.open_slots", "pool.open_slots", id="static_name_unchanged"),
        pytest.param("ti.{state}", "ti.*", id="single_placeholder_replaced"),
        pytest.param("ti.start.{dag_id}.{task_id}", "ti.start.*.*", id="multiple_placeholders_replaced"),
        pytest.param("{job_type}.heartbeat", "*.heartbeat", id="placeholder_at the start"),
        pytest.param("pool.{pool_name}", "pool.*", id="placeholder_at_end"),
        pytest.param("{job_name}_start", "*_start", id="name_with_placeholder_and_underscores"),
    ],
)
def test_normalize_metric_name(metric_name, expected_result):
    assert normalize_metric_name(metric_name) == expected_result


@pytest.mark.parametrize(
    "metric_name, expected_result",
    [
        pytest.param("scheduler.heartbeat", "scheduler.heartbeat", id="exact_match"),
        pytest.param("unknown.metric", None, id="no_match_returns_none"),
        pytest.param(
            "ti.start.{a}.{b}", "ti.start.{dag_id}.{task_id}", id="format_structure_match_different_variables"
        ),
        pytest.param(
            "executor.open_slots.{my_class}",
            "executor.open_slots",
            id="legacy_name_match_after_normalization",
        ),
        pytest.param(
            "pool.open_slots.{my_pool}", "pool.open_slots", id="legacy_name_match_same_prefix_structure"
        ),
        # In this case, the legacy name of 'task.duration', is 'dag.{dag_id}.{task_id}.duration'.
        # Once normalized, both will be 'dag.*.*.duration' and there should be a match.
        pytest.param("dag.{x}.{y}.duration", "task.duration", id="legacy_name_match_different_structure"),
        pytest.param("ti.{state}", _PREFIX_MATCHED, id="prefix_match_returns_sentinel"),
        pytest.param("dagrun.duration.{state}", _PREFIX_MATCHED, id="prefix_match_dotted_base"),
        pytest.param("non.existent.{var}", None, id="dynamic_metric_no_prefix_match_returns_none"),
        pytest.param("non.existent", None, id="static_metric_not_in_registry_returns_none"),
    ],
)
def test_find_registry_match(metric_name, expected_result):
    assert find_registry_match(metric_name, METRICS_REGISTRY) == expected_result


@pytest.mark.parametrize(
    "code, expected_result",
    [
        pytest.param("Stats", "Stats", id="name_node_returns_id"),
        pytest.param("self.stats", "stats", id="attribute_node_returns_attr"),
        pytest.param("self.some.module.stats", "stats", id="nested_attribute_returns_last_attr"),
        pytest.param("42", None, id="other_node_returns_none"),
    ],
)
def test_get_stats_obj_name(code: str, expected_result):
    node = ast.parse(code, mode="eval").body
    assert get_stats_obj_name(node) == expected_result


@pytest.mark.parametrize(
    "code, expected_result",
    [
        pytest.param('"scheduler_heartbeat"', "scheduler_heartbeat", id="static_string"),
        pytest.param(
            'f"dag_processing.last_run.seconds_ago.{dag_file}"',
            "dag_processing.last_run.seconds_ago.{dag_file}",
            id="fstring_with_name_variable",
        ),
        pytest.param(
            'f"dag_processing.last_num_of_db_queries.{self.dag_file}"',
            "dag_processing.last_num_of_db_queries.{dag_file}",
            id="fstring_with_attribute_variable",
        ),
        pytest.param(
            'f"dag_processing.last_run.seconds_ago.{get_dag_file()}"',
            "dag_processing.last_run.seconds_ago.{variable}",
            id="fstring_with_complex_inner_expression",
        ),
        pytest.param('"pool." + "open_slots"', "pool.open_slots", id="string_concatenation_both_static"),
        pytest.param(
            '"dag_processing.last_run.seconds_ago." + dag_file',
            "dag_processing.last_run.seconds_ago.{variable}",
            id="string_concatenation_left_static_right_dynamic",
        ),
        pytest.param(
            'job_name + "_start"', "{variable}_start", id="string_concatenation_left_dynamic_right_static"
        ),
        # Currently, there are no YAML entries with a variable in the middle.
        pytest.param(
            'f"ti.{state}.queued"',
            "ti.{state}.queued",
            id="fstring_variable_in_the_middle",
        ),
        pytest.param(
            '"ti." + state + ".queued"',
            "ti.{variable}.queued",
            id="string_concatenation_variable_in_the_middle",
        ),
        pytest.param("some_variable", None, id="unresolvable_name_returns_none"),
        pytest.param("get_metric_name()", None, id="unresolvable_call_returns_none"),
    ],
)
def test_extract_metric_name_from_ast_node(code: str, expected_result):
    node = ast.parse(code, mode="eval").body
    assert extract_metric_name_from_ast_node(node) == expected_result


@pytest.fixture
def code_to_py_file(tmp_path):
    """Write python source code to a tmp file and return its path."""

    def _write(code: str) -> Path:
        path = tmp_path / "tmp_test_file.py"
        path.write_text(textwrap.dedent(code))
        return path

    return _write


@pytest.mark.parametrize(
    "code, expected_calls",
    [
        pytest.param(
            'Stats.incr("triggerer_heartbeat", 1, 1)',
            [
                {
                    "metric_name": "triggerer_heartbeat",
                    "method": "incr",
                    "stats_obj": "Stats",
                    "is_dynamic": False,
                }
            ],
            id="static_incr_call",
        ),
        pytest.param(
            'Stats.gauge("scheduler.tasks.starving", num_starving_tasks_total)',
            [{"metric_name": "scheduler.tasks.starving", "method": "gauge"}],
            id="gauge_call",
        ),
        pytest.param(
            'Stats.gauge(f"dag_processing.last_run.seconds_ago.{file_name}", seconds_ago)',
            [{"metric_name": "dag_processing.last_run.seconds_ago.{file_name}", "is_dynamic": True}],
            id="fstring_dynamic_call",
        ),
        pytest.param(
            'Stats.incr(job_name + "_start", 1, 1)',
            [{"metric_name": "{variable}_start", "is_dynamic": True}],
            id="string_concatenation_call",
        ),
        pytest.param(
            'Stats.incr(stat="triggerer_heartbeat", count=1)',
            [{"metric_name": "triggerer_heartbeat"}],
            id="keyword_stat_argument",
        ),
        pytest.param(
            'stats.incr("triggerer_heartbeat")',
            [{"stats_obj": "stats"}],
            id="lowercase_stats_object",
        ),
        pytest.param(
            'self.stats.incr("triggerer_heartbeat")',
            [{"stats_obj": "stats"}],
            id="self_stats_attribute",
        ),
        pytest.param('metrics.incr("triggerer_heartbeat")', [], id="unknown_stats_object_ignored"),
        pytest.param("Stats.incr(get_metric_name())", [], id="unresolvable_metric_name_skipped"),
        pytest.param("def foo(:\n    pass\n", [], id="syntax_error_returns_empty"),
        pytest.param(
            "x = 1\ny = 2\nStats.incr('triggerer_heartbeat', 1, 1)",
            [{"line_num": 3}],
            id="line_number_recorded",
        ),
    ],
)
def test_scan_file_for_metrics(code_to_py_file, code, expected_calls):
    calls_from_scan = scan_file_for_metrics(code_to_py_file(code))
    assert len(calls_from_scan) == len(expected_calls)
    for call, expected in zip(calls_from_scan, expected_calls):
        for field, value in expected.items():
            assert getattr(call, field) == value


def test_scan_file_records_file_path(code_to_py_file):
    path = code_to_py_file('Stats.incr("triggerer_heartbeat", 1, 1)')
    assert scan_file_for_metrics(path)[0].file_path == str(path)


def test_scan_file_with_multiple_calls(code_to_py_file):
    path = code_to_py_file(
        'Stats.incr("triggerer_heartbeat", 1, 1)\nStats.gauge("scheduler.tasks.starving", n)\nStats.timing("dagrun.duration.success", 1.0)'
    )
    calls = scan_file_for_metrics(path)
    assert len(calls) == 3
    assert {c.metric_name for c in calls} == {
        "triggerer_heartbeat",
        "scheduler.tasks.starving",
        "dagrun.duration.success",
    }


def test_scan_file_nonexistent_file_returns_empty(tmp_path):
    assert scan_file_for_metrics(tmp_path / "non_existent.py") == []
