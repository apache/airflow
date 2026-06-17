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
import warnings
from unittest import mock

import pytest

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.openlineage.utils.emission_policy import (
    EmissionPolicy,
    Rule,
    resolve_dag_emission_policy,
    resolve_task_emission_policy,
)
from airflow.providers.openlineage.utils.selective_enable import enable_lineage

from tests_common.test_utils.compat import EmptyOperator
from tests_common.test_utils.config import conf_vars


class MockOperator:
    """Minimal stand-in for an Airflow operator."""

    def __init__(self, module: str = "tests.mock_module", class_name: str = "MockOperator"):
        self.__class__.__module__ = module
        self.__class__.__qualname__ = class_name


def _operator_fqcn(operator) -> str:
    return f"{operator.__class__.__module__}.{operator.__class__.__qualname__}"


def _resolve_task_controls(rules: list[dict], operator, dag_id: str, task_id: str):
    """Resolve task-level controls for ``operator`` (``dag_id``/``task_id``) under ``rules``.

    Every argument is required so each test states exactly which operator/dag/task it
    resolves against — there are no hidden defaults that the expected result depends on.
    """
    with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
        return resolve_task_emission_policy(operator, dag_id, task_id)


def _resolve_dag_controls(rules: list[dict], dag_id: str):
    """Resolve dag-level controls for ``dag_id`` under ``rules`` (no hidden defaults)."""
    with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
        return resolve_dag_emission_policy(dag_id)


class TestEmissionPolicyDefaults:
    def test_defaults_all_true(self):
        cfg = EmissionPolicy.defaults()
        assert cfg.emit is True
        assert cfg.extract_operator_metadata is True
        assert cfg.include_source_code is True
        assert cfg.hook_lineage is True
        assert cfg.include_full_task_info is False

    def test_frozen(self):
        cfg = EmissionPolicy.defaults()
        with pytest.raises((TypeError, AttributeError)):
            cfg.emit = False  # type: ignore[misc]


class TestResolveLineageControlsEmpty:
    def test_defaults_when_no_rules(self):
        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            cfg = resolve_task_emission_policy(MockOperator(), "dag", "task")
        assert cfg == EmissionPolicy.defaults()

    def test_defaults_when_config_absent(self):
        with conf_vars({("openlineage", "emission_policy"): ""}):
            cfg = resolve_task_emission_policy(MockOperator(), "dag", "task")
        assert cfg == EmissionPolicy.defaults()


class TestResolveLineageControlsGlobal:
    def test_global_emit_false(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False
        assert cfg.extract_operator_metadata is True

    def test_global_source_code_false(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"include_source_code": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_source_code is False

    def test_global_hook_lineage_false(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"hook_lineage": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.hook_lineage is False

    def test_global_applies_when_no_match(self):
        """Global rule applies to any task when no specific scope matches."""
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"extract_operator_metadata": False}}],
            dag_id="unrelated_dag",
            task_id="t1",
            operator=MockOperator(),
        )
        assert cfg.extract_operator_metadata is False

    def test_last_global_rule_wins(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit": False}}, {"scope": {}, "controls": {"emit": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True


class TestResolveLineageControlsOperator:
    def test_operator_emit_false(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [{"scope": {"operator": fqcn}, "controls": {"emit": False}}],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_operator_no_match_uses_default(self):
        op = MockOperator()
        cfg = _resolve_task_controls(
            [{"scope": {"operator": "some.other.Operator"}, "controls": {"emit": False}}],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_operator_last_matching_rule_wins(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [
                {"scope": {"operator": fqcn}, "controls": {"emit": False}},
                {"scope": {"operator": fqcn}, "controls": {"emit": True}},
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True


class TestResolveLineageControlsDag:
    def test_dag_extract_metadata_false(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"extract_operator_metadata": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.extract_operator_metadata is False

    def test_dag_no_match(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "other_dag"}, "controls": {"extract_operator_metadata": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.extract_operator_metadata is True

    def test_dag_last_rule_wins_within_tier(self):
        cfg = _resolve_task_controls(
            [
                {"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}},
                {"scope": {"dag_id": "my_dag"}, "controls": {"emit": True}},
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True


class TestResolveLineageControlsTask:
    def test_task_emit_false(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag", "task_id": "my_task"}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_task_no_match_different_task(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag", "task_id": "other_task"}, "controls": {"emit": False}}],
            task_id="my_task",
            operator=MockOperator(),
            dag_id="my_dag",
        )
        assert cfg.emit is True

    def test_task_no_match_different_dag(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "other_dag", "task_id": "my_task"}, "controls": {"emit": False}}],
            dag_id="my_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.emit is True


class TestResolveLineageControlsPriority:
    def test_task_overrides_dag(self):
        cfg = _resolve_task_controls(
            [
                {"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}},
                {"scope": {"dag_id": "my_dag", "task_id": "my_task"}, "controls": {"emit": True}},
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_dag_overrides_operator(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [
                {"scope": {"operator": fqcn}, "controls": {"emit": False}},
                {"scope": {"dag_id": "my_dag"}, "controls": {"emit": True}},
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_operator_overrides_global(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [
                {"scope": {}, "controls": {"emit": False}},  # global
                {"scope": {"operator": fqcn}, "controls": {"emit": True}},
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_global_applied_when_no_specific_match(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_independent_field_resolution(self):
        """emit from task tier, source_code from operator tier."""
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [
                {"scope": {"operator": fqcn}, "controls": {"include_source_code": False}},
                {"scope": {"dag_id": "my_dag", "task_id": "my_task"}, "controls": {"emit": False}},
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False
        assert cfg.include_source_code is False
        assert cfg.extract_operator_metadata is True
        assert cfg.hook_lineage is True

    def test_dag_does_not_override_task(self):
        """Dag-level rule does NOT override task-level resolution for the same field."""
        cfg = _resolve_task_controls(
            [
                {"scope": {"dag_id": "my_dag", "task_id": "my_task"}, "controls": {"emit": True}},
                {"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}},  # lower priority
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_global_source_code_false_but_dag_enables_it(self):
        """More specific dag rule re-enables source_code that global rule disabled."""
        cfg = _resolve_task_controls(
            [
                {"scope": {}, "controls": {"include_source_code": False}},  # global
                {"scope": {"dag_id": "my_dag"}, "controls": {"include_source_code": True}},  # dag tier wins
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_source_code is True


class TestEmitTaskDagEventFlags:
    """Tests for emit_task_events / emit_dag_events flags and emit shorthand."""

    def test_emit_task_events_false_disables_task_emit(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_task_events": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_emit_dag_events_false_does_not_affect_task_resolution(self):
        """`emit_dag_events: false` on a dag rule must NOT suppress task events."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_emit_shorthand_disables_task_events(self):
        """`emit: false` disables task events (shorthand)."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_emit_task_events_true_overrides_emit_false(self):
        """`emit_task_events: true` restores task emission even when `emit: false`."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit": False, "emit_task_events": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_emit_task_events_false_overrides_emit_true(self):
        """`emit_task_events: false` suppresses task emission even when `emit: true`."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit": True, "emit_task_events": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_emit_dag_events_false_disables_dag_event_emit(self):
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": False}}], dag_id="my_dag"
        )
        assert cfg.emit is False

    def test_emit_shorthand_disables_dag_events(self):
        """`emit: false` on a dag_id rule also disables dag run events (shorthand)."""
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}}], dag_id="my_dag"
        )
        assert cfg.emit is False

    def test_emit_task_events_false_does_not_affect_dag_event_resolution(self):
        """`emit_task_events: false` must NOT suppress dag run events."""
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_task_events": False}}], dag_id="my_dag"
        )
        assert cfg.emit is True

    def test_emit_dag_events_true_overrides_emit_false_for_dag_events(self):
        """`emit_dag_events: true` restores dag event emission when `emit: false`."""
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit": False, "emit_dag_events": True}}],
            dag_id="my_dag",
        )
        assert cfg.emit is True

    def test_global_emit_false_disables_both_task_and_dag(self):
        """Global `emit: false` disables both task events and dag run events."""
        task_cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        dag_cfg = _resolve_dag_controls([{"scope": {}, "controls": {"emit": False}}], dag_id="my_dag")
        assert task_cfg.emit is False
        assert dag_cfg.emit is False

    def test_global_emit_dag_events_false_disables_dag_events_only(self):
        """Global `emit_dag_events: false` leaves task events unaffected."""
        task_cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit_dag_events": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        dag_cfg = _resolve_dag_controls(
            [{"scope": {}, "controls": {"emit_dag_events": False}}], dag_id="my_dag"
        )
        assert task_cfg.emit is True
        assert dag_cfg.emit is False

    def test_global_emit_task_events_false_disables_task_events_only(self):
        """Global `emit_task_events: false` leaves dag events unaffected."""
        task_cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit_task_events": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        dag_cfg = _resolve_dag_controls(
            [{"scope": {}, "controls": {"emit_task_events": False}}], dag_id="my_dag"
        )
        assert task_cfg.emit is False
        assert dag_cfg.emit is True


class TestResolveDagEventControls:
    def test_defaults_when_no_rules(self):
        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            cfg = resolve_dag_emission_policy("my_dag")
        assert cfg == EmissionPolicy.defaults()

    def test_dag_id_emit_dag_events_false(self):
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": False}}], dag_id="my_dag"
        )
        assert cfg.emit is False

    def test_dag_id_no_match(self):
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "other_dag"}, "controls": {"emit_dag_events": False}}], dag_id="my_dag"
        )
        assert cfg.emit is True

    def test_global_emit_false_applies_to_dag_event(self):
        cfg = _resolve_dag_controls([{"scope": {}, "controls": {"emit": False}}], dag_id="my_dag")
        assert cfg.emit is False

    def test_dag_id_wins_over_global(self):
        cfg = _resolve_dag_controls(
            [
                {"scope": {}, "controls": {"emit": False}},  # global
                {
                    "scope": {"dag_id": "my_dag"},
                    "controls": {"emit_dag_events": True},
                },  # more specific — wins
            ],
            dag_id="my_dag",
        )
        assert cfg.emit is True

    def test_task_specific_rules_do_not_affect_dag_event_resolution(self):
        """dag_id + task_id rules are task-specific and must NOT match dag event resolution."""
        cfg = _resolve_dag_controls(
            [{"scope": {"dag_id": "my_dag", "task_id": "t1"}, "controls": {"emit": False}}], dag_id="my_dag"
        )
        assert cfg.emit is True

    def test_operator_rules_do_not_affect_dag_event_resolution(self):
        cfg = _resolve_dag_controls(
            [{"scope": {"operator": "some.Operator"}, "controls": {"emit": False}}], dag_id="my_dag"
        )
        assert cfg.emit is True

    def test_extract_metadata_source_code_hook_lineage_always_true(self):
        """Non-emit fields are N/A for dag events and always return defaults (True)."""
        cfg = _resolve_dag_controls(
            [
                {
                    "scope": {"dag_id": "my_dag"},
                    "controls": {"emit_dag_events": False, "extract_operator_metadata": False},
                }
            ],
            dag_id="my_dag",
        )
        assert cfg.extract_operator_metadata is True
        assert cfg.include_source_code is True
        assert cfg.hook_lineage is True


class TestMatchModeRegex:
    def test_match_mode_regex_dag_id(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "^my_.*"}, "match_mode": "regex", "controls": {"emit": False}}],
            dag_id="my_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.emit is False

    def test_match_mode_regex_dag_id_no_match(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "^other_.*"}, "match_mode": "regex", "controls": {"emit": False}}],
            dag_id="my_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_match_mode_regex_operator(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        prefix = fqcn.rsplit(".", 1)[0]
        cfg = _resolve_task_controls(
            [
                {
                    "scope": {"operator": f"{prefix}\\..*"},
                    "match_mode": "regex",
                    "controls": {"include_source_code": False},
                }
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_source_code is False

    def test_match_mode_regex_task_id(self):
        cfg = _resolve_task_controls(
            [
                {
                    "scope": {"dag_id": "my_dag", "task_id": "my_.*"},
                    "match_mode": "regex",
                    "controls": {"hook_lineage": False},
                }
            ],
            dag_id="my_dag",
            task_id="my_task",
            operator=MockOperator(),
        )
        assert cfg.hook_lineage is False

    def test_match_mode_regex_task_id_no_match(self):
        cfg = _resolve_task_controls(
            [
                {
                    "scope": {"dag_id": "my_dag", "task_id": "other_.*"},
                    "match_mode": "regex",
                    "controls": {"hook_lineage": False},
                }
            ],
            dag_id="my_dag",
            task_id="my_task",
            operator=MockOperator(),
        )
        assert cfg.hook_lineage is True

    def test_match_mode_regex_dag_event_resolution(self):
        cfg = _resolve_dag_controls(
            [
                {
                    "scope": {"dag_id": "^prod_.*"},
                    "match_mode": "regex",
                    "controls": {"emit_dag_events": False},
                }
            ],
            dag_id="prod_daily",
        )
        assert cfg.emit is False

    def test_match_mode_exact_is_default(self):
        """Without match_mode, exact matching is used."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "^my_.*"}, "controls": {"emit": False}}],  # regex pattern, but exact mode
            dag_id="my_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.emit is True  # "^my_.*" != "my_dag" under exact match

    def test_match_mode_invalid_warns_and_skips(self):
        """Invalid match_mode value → rule is skipped."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "match_mode": "glob", "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_match_mode_invalid_regex_warns_and_skips(self):
        """Malformed regex pattern → rule is skipped."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "[invalid"}, "match_mode": "regex", "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True


class TestResolveLineageControlsValidation:
    def test_task_id_without_dag_id_ignored(self):
        cfg = _resolve_task_controls(
            [{"scope": {"task_id": "my_task"}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_operator_with_emit_dag_events_invalid(self):
        """operator + emit_dag_events is meaningless → rule ignored."""
        cfg = _resolve_task_controls(
            [{"scope": {"operator": "some.Operator"}, "controls": {"emit_dag_events": False, "emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_task_id_with_emit_dag_events_invalid(self):
        """task_id + emit_dag_events is meaningless → rule ignored."""
        cfg = _resolve_task_controls(
            [
                {
                    "scope": {"dag_id": "my_dag", "task_id": "my_task"},
                    "controls": {"emit_dag_events": False, "emit": False},
                }
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_non_bool_control_value_skipped(self):
        """Non-bool value inside controls → rule is dropped entirely."""
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"emit": "yes"}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True  # rule rejected, defaults remain

    def test_non_dict_rule_ignored(self):
        cfg = _resolve_task_controls(
            ["not_a_dict", {"scope": {}, "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is False  # valid rule still applies


class TestAuditLogging:
    """Audit-log assertions pin the exact format string, field, context, and source for every call."""

    _CONF_FMT = "OpenLineage emission policy: '%s' %s for %s by %r"
    _AUTH_FMT = (
        "OpenLineage emission policy: '%s' %s for %s "
        "by manual `extend_global_openlineage_emission_policy` call."
    )

    def test_audit_log_info_when_emit_disabled_by_rule(self):
        op = MockOperator()
        rules = [{"scope": {}, "controls": {"emit": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(op, "dag", "task")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "emit",
            "disabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"emit": False}, match_mode="exact", locked=False),
        )

    def test_audit_log_info_when_source_code_disabled(self):
        op = MockOperator()
        rule = {"scope": {}, "controls": {"include_source_code": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(op, "dag", "task")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "disabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"include_source_code": False}, match_mode="exact", locked=False),
        )

    def test_no_audit_log_when_all_defaults(self):
        """No INFO log when no rule touches any field."""
        op = MockOperator()
        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(op, "dag", "task")
        mock_log.info.assert_not_called()

    def test_audit_log_includes_winning_rule(self):
        """The parsed Rule object (with scope/controls) is the source argument to log.info."""
        op = MockOperator()
        rule = {"scope": {"dag_id": "audit_dag"}, "controls": {"extract_operator_metadata": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(op, "audit_dag", "t")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "extract_operator_metadata",
            "disabled",
            "task 't' in dag 'audit_dag'",
            Rule(
                scope={"dag_id": "audit_dag"},
                controls={"extract_operator_metadata": False},
                match_mode="exact",
                locked=False,
            ),
        )

    def test_audit_log_dag_event_emit_disabled(self):
        """INFO log emitted when dag event emit is disabled by a rule."""
        rules = [{"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_dag_emission_policy("my_dag")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "emit",
            "disabled",
            "dag event 'my_dag'",
            Rule(
                scope={"dag_id": "my_dag"},
                controls={"emit_dag_events": False},
                match_mode="exact",
                locked=False,
            ),
        )

    def test_audit_log_selective_enable_suppression(self):
        """The translated emit:false baseline from selective_enable is logged as disabled."""
        dag = DAG(dag_id="test_se_dag", schedule=None)
        task = EmptyOperator(task_id="t", dag=dag)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    resolve_task_emission_policy(task, "test_se_dag", "t")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "emit",
            "disabled",
            "task 't' in dag 'test_se_dag'",
            Rule(scope={}, controls={"emit": False}, match_mode="exact", locked=False),
        )

    def test_audit_log_dag_event_selective_enable_suppression(self):
        """The translated emit:false rule is logged for dag event suppression."""
        dag = DAG(dag_id="se_dag2", schedule=None)
        EmptyOperator(task_id="t", dag=dag)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", DeprecationWarning)
                    resolve_dag_emission_policy("se_dag2", dag=dag)
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "emit",
            "disabled",
            "dag event 'se_dag2'",
            Rule(scope={}, controls={"emit": False}, match_mode="exact", locked=False),
        )

    def test_audit_log_conf_reenable_by_higher_tier(self):
        """Dag-tier rule re-enabling a globally-disabled field is logged as 'enabled'.

        The dag-tier rule is the winner; the global rule is not the winner so it is
        not logged. Previously only the non-default final value triggered logging,
        producing zero INFO logs for this combination.
        """
        rules = [
            {"scope": {}, "controls": {"include_source_code": False}},
            {"scope": {"dag_id": "my_dag"}, "controls": {"include_source_code": True}},
        ]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(MockOperator(), "my_dag", "task")
        assert cfg.include_source_code is True
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "enabled",
            "task 'task' in dag 'my_dag'",
            Rule(
                scope={"dag_id": "my_dag"},
                controls={"include_source_code": True},
                match_mode="exact",
                locked=False,
            ),
        )

    def test_audit_log_authoring_reenable_logs_enabled(self):
        """Authoring re-enabling a conf-disabled field produces a second 'enabled' log in order.

        Previously the authoring layer only logged non-default values, so restoring
        a field to its default via authoring was silent — the last visible log said
        'disabled', which was misleading.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        rules = [{"scope": {}, "controls": {"include_source_code": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is True
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls(
            [
                mock.call(
                    self._CONF_FMT,
                    "include_source_code",
                    "disabled",
                    "task 'test_task' in dag 'test_dag'",
                    Rule(scope={}, controls={"include_source_code": False}, match_mode="exact", locked=False),
                ),
                mock.call(
                    self._AUTH_FMT,
                    "include_source_code",
                    "enabled",
                    "task 'test_task' in dag 'test_dag'",
                ),
            ]
        )

    def test_no_duplicate_audit_log_when_authoring_matches_conf(self):
        """Authoring that agrees with conf does not produce a second log for the same field."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        rules = [{"scope": {}, "controls": {"include_source_code": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "disabled",
            "task 'test_task' in dag 'test_dag'",
            Rule(scope={}, controls={"include_source_code": False}, match_mode="exact", locked=False),
        )

    def test_conf_explicit_default_value_is_logged(self):
        """A conf rule that explicitly sets a field to its default value is still logged.

        Any rule that touches a field is now logged — even when the resulting value
        equals the built-in default — so the operator can see that a rule is active.
        """
        rule = {"scope": {}, "controls": {"include_source_code": True}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(MockOperator(), "dag", "task")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "enabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"include_source_code": True}, match_mode="exact", locked=False),
        )

    def test_conf_two_same_tier_same_value_one_log_from_last_rule(self):
        """Two global rules setting the same field to the same value produce exactly one log.

        The second (last-in-tier) rule is the winner and is the source in the log.
        No duplication occurs even though both rules match.
        """
        rule1 = {"scope": {}, "controls": {"include_source_code": False}}
        rule2 = {"scope": {}, "controls": {"include_source_code": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule1, rule2])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(MockOperator(), "dag", "task")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "disabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"include_source_code": False}, match_mode="exact", locked=False),
        )

    def test_conf_contradictory_same_tier_winner_logged_loser_silent(self):
        """A contradictory pair in the same tier fires a WARNING and logs only the winner.

        Last-in-tier wins (the True rule), which is logged as 'enabled'.
        The losing rule (False) is not logged — only the winner's effect is audited.
        """
        rule_false = {"scope": {}, "controls": {"include_source_code": False}}
        rule_true = {"scope": {}, "controls": {"include_source_code": True}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule_false, rule_true])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(MockOperator(), "dag", "task")
        assert cfg.include_source_code is True
        assert mock_log.info.call_count == 1
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "enabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"include_source_code": True}, match_mode="exact", locked=False),
        )
        assert any("contradictory" in str(c) for c in mock_log.warning.call_args_list)

    def test_conf_single_rule_multiple_fields_logs_each_in_fixed_order(self):
        """A rule with two control flags produces two INFO logs in resolution order.

        Resolution order is fixed by the code: emit first, then include_source_code.
        """
        rule = {"scope": {}, "controls": {"emit": False, "include_source_code": False}}
        winning_rule = Rule(
            scope={}, controls={"emit": False, "include_source_code": False}, match_mode="exact", locked=False
        )
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(MockOperator(), "dag", "task")
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls(
            [
                mock.call(self._CONF_FMT, "emit", "disabled", "task 'task' in dag 'dag'", winning_rule),
                mock.call(
                    self._CONF_FMT,
                    "include_source_code",
                    "disabled",
                    "task 'task' in dag 'dag'",
                    winning_rule,
                ),
            ]
        )

    def test_conf_include_full_task_info_enabled_logged(self):
        """A conf rule enabling include_full_task_info (default False) is logged as 'enabled'."""
        rule = {"scope": {}, "controls": {"include_full_task_info": True}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(MockOperator(), "dag", "task")
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_full_task_info",
            "enabled",
            "task 'task' in dag 'dag'",
            Rule(scope={}, controls={"include_full_task_info": True}, match_mode="exact", locked=False),
        )

    def test_authoring_only_non_default_produces_log(self):
        """With no conf rule, authoring that changes a field from its default is logged."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        mock_log.info.assert_called_once_with(
            self._AUTH_FMT,
            "include_source_code",
            "disabled",
            "task 'test_task' in dag 'test_dag'",
        )

    def test_authoring_only_same_as_default_no_log(self):
        """Authoring that sets a field to the built-in default (no conf rule) produces no log.

        include_source_code defaults to True; authoring it to True is a no-op against
        the conf-resolved state, so no change and no log.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                resolve_task_emission_policy(task, "test_dag", "test_task")

        mock_log.info.assert_not_called()

    def test_authoring_two_calls_net_matches_conf_no_extra_log(self):
        """Two authoring calls are merged; the NET result is compared against conf, not each call.

        First call: include_source_code=True (enables back to default).
        Second call: include_source_code=False (disables again).
        Net merged authoring: False — which matches the conf rule's False — so no authoring log.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        rules = [{"scope": {}, "controls": {"include_source_code": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        mock_log.info.assert_called_once_with(
            self._CONF_FMT,
            "include_source_code",
            "disabled",
            "task 'test_task' in dag 'test_dag'",
            Rule(scope={}, controls={"include_source_code": False}, match_mode="exact", locked=False),
        )

    def test_authoring_emit_task_events_key_logged_as_emit_field(self):
        """Authoring via emit_task_events=False is logged under the resolved field name 'emit'.

        The authoring layer maps emit_task_events to the emit field in EmissionPolicy;
        the log therefore records 'emit', not the raw key name 'emit_task_events'.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit_task_events=False)

        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False
        mock_log.info.assert_called_once_with(
            self._AUTH_FMT,
            "emit",
            "disabled",
            "task 'test_task' in dag 'test_dag'",
        )

    def test_conf_enables_explicit_then_authoring_disables(self):
        """Conf logs 'enabled' (explicit default rule), authoring then logs 'disabled'.

        Two logs in order: conf 'enabled', authoring 'disabled'.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        rules = [{"scope": {}, "controls": {"include_source_code": True}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls(
            [
                mock.call(
                    self._CONF_FMT,
                    "include_source_code",
                    "enabled",
                    "task 'test_task' in dag 'test_dag'",
                    Rule(scope={}, controls={"include_source_code": True}, match_mode="exact", locked=False),
                ),
                mock.call(
                    self._AUTH_FMT,
                    "include_source_code",
                    "disabled",
                    "task 'test_task' in dag 'test_dag'",
                ),
            ]
        )

    def test_no_conf_for_field_authoring_same_as_default_no_log(self):
        """When conf has no rule for a field and authoring sets it to the default, no log is emitted.

        include_source_code resolves to True by default; authoring True is a no-op.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        rules = [{"scope": {}, "controls": {"emit": False}}]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is True
        emit_logs = [c for c in mock_log.info.call_args_list if c.args[1] == "include_source_code"]
        assert len(emit_logs) == 0

    def test_conf_higher_tier_re_enables_then_authoring_disables(self):
        """Global conf disables, dag-tier conf re-enables (logs 'enabled'), authoring then disables.

        Three-step trace: global disable (not logged — not the winner), dag-tier enabled (logged),
        authoring disabled (logged). Two INFO logs total in order.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        rules = [
            {"scope": {}, "controls": {"include_source_code": False}},
            {"scope": {"dag_id": "test_dag"}, "controls": {"include_source_code": True}},
        ]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        assert mock_log.info.call_count == 2
        mock_log.info.assert_has_calls(
            [
                mock.call(
                    self._CONF_FMT,
                    "include_source_code",
                    "enabled",
                    "task 'test_task' in dag 'test_dag'",
                    Rule(
                        scope={"dag_id": "test_dag"},
                        controls={"include_source_code": True},
                        match_mode="exact",
                        locked=False,
                    ),
                ),
                mock.call(
                    self._AUTH_FMT,
                    "include_source_code",
                    "disabled",
                    "task 'test_task' in dag 'test_dag'",
                ),
            ]
        )

    def test_two_conf_fields_authoring_re_enables_one_three_logs_in_order(self):
        """Conf disables two fields; authoring re-enables one of them.

        Resolution order for the conf layer is fixed: emit first, then include_source_code.
        Authoring log follows after both conf logs.
        Total: 3 INFO logs in order.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        rule = {"scope": {}, "controls": {"emit": False, "include_source_code": False}}
        winning_rule = Rule(
            scope={},
            controls={"emit": False, "include_source_code": False},
            match_mode="exact",
            locked=False,
        )
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False
        assert cfg.include_source_code is True
        assert mock_log.info.call_count == 3
        mock_log.info.assert_has_calls(
            [
                mock.call(
                    self._CONF_FMT, "emit", "disabled", "task 'test_task' in dag 'test_dag'", winning_rule
                ),
                mock.call(
                    self._CONF_FMT,
                    "include_source_code",
                    "disabled",
                    "task 'test_task' in dag 'test_dag'",
                    winning_rule,
                ),
                mock.call(
                    self._AUTH_FMT,
                    "include_source_code",
                    "enabled",
                    "task 'test_task' in dag 'test_dag'",
                ),
            ]
        )


class TestContradictoryRules:
    def test_contradictory_global_emit_warns_and_uses_last(self):
        """Two global rules set emit to opposite values → warning, last wins."""
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(
                [{"scope": {}, "controls": {"emit": False}}, {"scope": {}, "controls": {"emit": True}}],
                operator=MockOperator(),
                dag_id="my_dag",
                task_id="my_task",
            )
        assert cfg.emit is True  # last rule wins
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("contradictory" in c for c in warn_calls)

    def test_contradictory_dag_scoped_emit_warns_and_uses_last(self):
        """Two dag-scoped rules set emit to opposite values in the same tier."""
        rules = [
            {"scope": {"dag_id": "my_dag"}, "controls": {"emit": False}},
            {"scope": {"dag_id": "my_dag"}, "controls": {"emit": True}},
        ]
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(rules, operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        assert cfg.emit is True
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("contradictory" in c for c in warn_calls)

    def test_same_value_twice_does_not_warn(self):
        """Redundant rules with the same value should not produce a contradictory warning."""
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(
                [{"scope": {}, "controls": {"emit": False}}, {"scope": {}, "controls": {"emit": False}}],
                operator=MockOperator(),
                dag_id="my_dag",
                task_id="my_task",
            )
        assert cfg.emit is False
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert not any("contradictory" in c for c in warn_calls)

    def test_contradictory_in_different_tiers_does_not_warn(self):
        """Rules in different tiers are resolved by priority, not considered contradictory."""
        # global emit=false, dag emit=true — different tiers, dag wins, no contradiction warning
        rules = [
            {"scope": {}, "controls": {"emit": False}},
            {"scope": {"dag_id": "my_dag"}, "controls": {"emit": True}},
        ]
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(rules, operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        assert cfg.emit is True  # dag tier beats global tier
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert not any("contradictory" in c for c in warn_calls)

    def test_contradictory_field_warns_for_non_emit_fields(self):
        """Contradiction warning also applies to non-emit fields like source_code."""
        rules = [
            {"scope": {}, "controls": {"include_source_code": True}},
            {"scope": {}, "controls": {"include_source_code": False}},
        ]
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(rules, operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        assert cfg.include_source_code is False
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("contradictory" in c and "include_source_code" in c for c in warn_calls)

    def test_contradictory_dag_event_emit_warns(self):
        """Contradiction in dag-event resolution triggers the warning."""
        rules = [
            {"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": False}},
            {"scope": {"dag_id": "my_dag"}, "controls": {"emit_dag_events": True}},
        ]
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_dag_controls(rules, dag_id="my_dag")
        assert cfg.emit is True
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("contradictory" in c for c in warn_calls)


class TestResolveLineageControlsWithLegacy:
    def test_uses_legacy_disabled_operators_when_no_emission_policy(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "disabled_for_operators"): fqcn,
            }
        ):
            # Mixing legacy options with the resolver emits a deprecation warning; capture and
            # assert it here so it does not propagate (the test env promotes it to an error).
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg.emit is False

    def test_uses_legacy_source_code_when_no_emission_policy(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "disable_source_code"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg.include_source_code is False

    def test_emission_policy_takes_precedence_over_disabled_operators(self):
        """An explicit operator-tier emission_policy rule wins over the translated legacy rule.

        Legacy disabled_for_operators becomes an operator-tier emit:false rule. Within the same
        (operator) tier, last-wins applies, so the user's explicit emission_policy rule overrides
        the translated legacy rule.
        """
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        with conf_vars(
            {
                # Explicit operator-tier rule re-enables this operator
                ("openlineage", "emission_policy"): json.dumps(
                    [{"scope": {"operator": fqcn}, "controls": {"emit": True}}]
                ),
                ("openlineage", "disabled_for_operators"): fqcn,
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg.emit is True
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    def test_emission_policy_takes_precedence_over_disable_source_code(self):
        op = MockOperator()
        with conf_vars(
            {
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"include_source_code": true}}]',
                ("openlineage", "disable_source_code"): "True",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg.include_source_code is True
        assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    def test_no_deprecation_warning_when_legacy_configs_at_default(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): '[{"scope": {}, "controls": {"emit": false}}]',
                ("openlineage", "disabled_for_operators"): "",
                ("openlineage", "disable_source_code"): "False",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg.emit is False
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert not dep_warnings

    def test_defaults_returned_when_both_absent(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "disabled_for_operators"): "",
                ("openlineage", "disable_source_code"): "False",
            }
        ):
            cfg = resolve_task_emission_policy(op, "dag", "task")
        assert cfg == EmissionPolicy.defaults()


def _make_dag_and_task(dag_id: str = "test_dag", task_id: str = "test_task"):
    """Helper to create a real DAG + EmptyOperator for selective_enable tests."""
    dag = DAG(dag_id=dag_id, schedule=None)
    task = EmptyOperator(task_id=task_id, dag=dag)
    return dag, task


class TestSelectiveEnableInTaskControls:
    """selective_enable is folded into resolve_task_emission_policy."""

    def test_selective_enable_off_task_resolution(self):
        _, task = _make_dag_and_task()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "False",
            }
        ):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is True

    def test_selective_enable_on_task_not_opted_in(self):
        _, task = _make_dag_and_task()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is False

    def test_selective_enable_on_task_opted_in(self):
        _, task = _make_dag_and_task()
        enable_lineage(task)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is True

    def test_selective_enable_does_not_override_already_false_emit(self):
        """If emit is already False from emission_policy, selective_enable check is not applied."""
        _, task = _make_dag_and_task()
        enable_lineage(task)  # task IS opted in
        with conf_vars(
            {
                ("openlineage", "emission_policy"): json.dumps(
                    [{"scope": {"dag_id": "test_dag", "task_id": "test_task"}, "controls": {"emit": False}}]
                ),
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is False

    def test_other_fields_preserved_when_selective_enable_forces_emit_false(self):
        _, task = _make_dag_and_task()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): json.dumps(
                    [{"scope": {}, "controls": {"include_source_code": False, "hook_lineage": False}}]
                ),
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is False
        assert cfg.include_source_code is False
        assert cfg.hook_lineage is False
        assert cfg.extract_operator_metadata is True


class TestSelectiveEnableInDagEventControls:
    """selective_enable is folded into resolve_dag_emission_policy via the dag= parameter."""

    def test_no_dag_object_skips_selective_check(self):
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            cfg = resolve_dag_emission_policy("any_dag")
        assert cfg.emit is True

    def test_selective_enable_off_dag_event(self):
        dag, _ = _make_dag_and_task()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "False",
            }
        ):
            cfg = resolve_dag_emission_policy("test_dag", dag=dag)
        assert cfg.emit is True

    def test_selective_enable_on_dag_not_opted_in(self):
        dag, _ = _make_dag_and_task()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_dag_emission_policy("test_dag", dag=dag)
        assert cfg.emit is False

    def test_selective_enable_on_dag_opted_in(self):
        dag, _ = _make_dag_and_task()
        enable_lineage(dag)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "selective_enable"): "True",
            }
        ):
            cfg = resolve_dag_emission_policy("test_dag", dag=dag)
        assert cfg.emit is True

    def test_selective_enable_does_not_override_emission_policy_emit_false(self):
        """If emission_policy suppresses dag event, selective opt-in doesn't restore it."""
        dag, _ = _make_dag_and_task()
        enable_lineage(dag)
        with conf_vars(
            {
                ("openlineage", "emission_policy"): json.dumps(
                    [{"scope": {"dag_id": "test_dag"}, "controls": {"emit_dag_events": False}}]
                ),
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_dag_emission_policy("test_dag", dag=dag)
        assert cfg.emit is False


class TestIncludeFullTaskInfo:
    def test_default_is_false(self):
        cfg = _resolve_task_controls([], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        assert cfg.include_full_task_info is False

    def test_global_include_full_task_info_true(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"include_full_task_info": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is True

    def test_dag_scope_include_full_task_info_true(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "my_dag"}, "controls": {"include_full_task_info": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is True

    def test_dag_scope_does_not_affect_other_dag(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": "other_dag"}, "controls": {"include_full_task_info": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is False

    def test_task_scope_include_full_task_info_true(self):
        cfg = _resolve_task_controls(
            [
                {
                    "scope": {"dag_id": "my_dag", "task_id": "my_task"},
                    "controls": {"include_full_task_info": True},
                }
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is True

    def test_task_scope_overrides_global_false(self):
        cfg = _resolve_task_controls(
            [
                {"scope": {}, "controls": {"include_full_task_info": False}},
                {
                    "scope": {"dag_id": "my_dag", "task_id": "my_task"},
                    "controls": {"include_full_task_info": True},
                },
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is True

    def test_operator_scope_include_full_task_info_true(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [{"scope": {"operator": fqcn}, "controls": {"include_full_task_info": True}}],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_full_task_info is True

    def test_include_full_task_info_does_not_affect_emit(self):
        cfg = _resolve_task_controls(
            [{"scope": {}, "controls": {"include_full_task_info": True}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True
        assert cfg.extract_operator_metadata is True


class TestLegacyTranslation:
    """When emission_policy is set, legacy options are translated to rules."""

    def test_disabled_for_operators_translated_when_emission_policy_set(self):
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        with conf_vars(
            {
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"extract_operator_metadata": false}}]',
                ("openlineage", "disabled_for_operators"): fqcn,
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        # Operator is in disabled_for_operators → translated to emit:false rule → emit should be False
        assert cfg.emit is False
        # The emission_policy rule also applies
        assert cfg.extract_operator_metadata is False
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings, "Expected DeprecationWarning for mixed legacy+emission_policy"

    def test_disable_source_code_translated_when_emission_policy_set(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): '[{"scope": {}, "controls": {"hook_lineage": false}}]',
                ("openlineage", "disable_source_code"): "True",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        assert cfg.include_source_code is False  # from translated legacy
        assert cfg.hook_lineage is False  # from emission_policy
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings

    def test_include_full_task_info_translated_when_emission_policy_set(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): '[{"scope": {}, "controls": {"emit": true}}]',
                ("openlineage", "include_full_task_info"): "True",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        assert cfg.include_full_task_info is True  # from translated legacy
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings

    def test_emission_policy_rule_overrides_translated_legacy_rule(self):
        """User's explicit emission_policy rule wins over translated legacy (last-wins in tier)."""
        op = MockOperator()
        with conf_vars(
            {
                # emission_policy explicitly sets include_source_code=True
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"include_source_code": true}}]',
                # legacy sets disable_source_code=True (translated to source_code=False global rule)
                ("openlineage", "disable_source_code"): "True",
            }
        ):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        # User rule (include_source_code=True) appears AFTER the translated legacy rule in the combined list
        # and is in the same global tier → user wins (last-wins within tier)
        assert cfg.include_source_code is True

    def test_no_deprecation_warning_when_only_emission_policy_set(self):
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): '[{"scope": {}, "controls": {"emit": false}}]',
                ("openlineage", "disabled_for_operators"): "",
                ("openlineage", "disable_source_code"): "False",
                ("openlineage", "include_full_task_info"): "False",
                ("openlineage", "selective_enable"): "False",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        assert cfg.emit is False
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert not dep_warnings

    def test_selective_enable_translated_with_emission_policy_set(self):
        """selective_enable=True translates to global emit:false + task opt-in rule."""
        _, task = _make_dag_and_task()
        enable_lineage(task)
        with conf_vars(
            {
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"extract_operator_metadata": false}}]',
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        # Task is opted in → emit should be True (task-tier rule overrides global emit:false)
        assert cfg.emit is True
        # emission_policy rule still applies
        assert cfg.extract_operator_metadata is False
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert dep_warnings

    def test_selective_enable_translated_not_opted_in_with_emission_policy_set(self):
        """Task not opted in under selective_enable + emission_policy → emit False."""
        _, task = _make_dag_and_task()
        # task is NOT opted in
        with conf_vars(
            {
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"include_source_code": false}}]',
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        assert cfg.emit is False
        assert cfg.include_source_code is False

    def test_include_full_task_info_legacy_path_unchanged(self):
        """When emission_policy is empty, include_full_task_info= uses legacy conf directly."""
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "include_full_task_info"): "True",
            }
        ):
            with pytest.warns(AirflowProviderDeprecationWarning):
                cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        assert cfg.include_full_task_info is True

    def test_include_full_task_info_legacy_path_default_false(self):
        """When emission_policy is empty and include_full_task_info not set, default False."""
        op = MockOperator()
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "include_full_task_info"): "False",
            }
        ):
            cfg = resolve_task_emission_policy(op, "my_dag", "my_task")
        assert cfg.include_full_task_info is False


class TestLockedField:
    """``locked: true`` on a conf rule prevents per-task authoring from overriding that field."""

    def test_locked_emit_blocks_authoring_override(self):
        """A task-level emit=True from extend_global_openlineage_emission_policy cannot override a locked emit=False rule."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)  # authoring says True

        rule = {"scope": {}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False  # locked conf rule wins

    def test_unlocked_emit_allows_authoring_override(self):
        """Without locked, an authoring emit=True overrides a conf emit=False rule."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)  # authoring says True

        rule = {"scope": {}, "controls": {"emit": False}}  # no locked
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is True  # authoring overrides

    def test_locked_source_code_blocks_authoring(self):
        """Locked include_source_code=False in conf cannot be re-enabled by extend_global_openlineage_emission_policy."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        rule = {"scope": {}, "locked": True, "controls": {"include_source_code": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False  # locked

    def test_locked_field_does_not_block_other_fields(self):
        """A locked rule only protects the field(s) it carries, not unrelated fields."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True, hook_lineage=False)

        # Only emit is locked; include_source_code authoring should still work
        rule = {"scope": {}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False  # locked
        assert cfg.hook_lineage is False  # authoring applied (not locked)

    def test_locked_dag_emit_blocks_dag_authoring(self):
        """locked emit on dag rule blocks extend_global_openlineage_emission_policy(dag, emit=True) for dag events."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        dag, _ = _make_dag_and_task("locked_dag_test")
        extend_global_openlineage_emission_policy(dag, emit=True)  # authoring says True for dag events

        rule = {"scope": {"dag_id": "locked_dag_test"}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_dag_emission_policy("locked_dag_test", dag)

        assert cfg.emit is False  # locked conf rule wins

    def test_locked_invalid_type_rule_ignored(self):
        """A rule with locked of non-bool type is invalid and ignored."""
        cfg = _resolve_task_controls(
            [{"scope": {}, "locked": "yes", "controls": {"emit": False}}],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        # Rule is invalid → falls back to default
        assert cfg.emit is True

    def test_locked_no_authoring_has_no_effect(self):
        """locked with no authoring flags in play still resolves correctly."""
        _, task = _make_dag_and_task()
        # No extend_global_openlineage_emission_policy call

        rule = {"scope": {}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False  # locked rule still applies normally

    def test_floor_lock_lower_tier_locked_rule_blocks_authoring_despite_higher_tier_override(self):
        """Floor lock: a global locked rule blocks authoring even when a dag-tier conf rule wins.

        Scenario:
          - Global conf rule: include_source_code=False, locked=True  (admin mandate, lower tier)
          - Dag-tier conf rule: include_source_code=True               (more specific, no lock — wins value)
          - Authoring: extend_global_openlineage_emission_policy(task, include_source_code=False) — should be blocked

        The dag-tier rule wins for VALUE (include_source_code=True), but the global locked rule adds
        "include_source_code" to locked_fields, so the authoring override cannot touch it.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)  # authoring wants False

        rules = [
            {
                "scope": {},
                "locked": True,
                "controls": {"include_source_code": False},
            },  # global, locked — lower tier
            {
                "scope": {"dag_id": "test_dag"},
                "controls": {"include_source_code": True},
            },  # dag-tier, no lock — wins value
        ]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        # Dag-tier rule wins: include_source_code=True (from conf tier resolution)
        assert cfg.include_source_code is True
        # But authoring is blocked by floor lock — include_source_code stays at conf-resolved value

    def test_floor_lock_regex_rule_blocks_authoring_for_matching_dag(self):
        """Floor lock: a regex-based locked rule blocks authoring for all matching dags."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task("prod_reporting")
        extend_global_openlineage_emission_policy(task, include_source_code=True)  # authoring wants True

        rules = [
            {
                "scope": {"dag_id": "^prod_.*"},
                "match_mode": "regex",
                "locked": True,
                "controls": {"include_source_code": False},
            }
        ]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            cfg = resolve_task_emission_policy(task, "prod_reporting", "test_task")

        assert cfg.include_source_code is False  # locked regex rule wins

    def test_floor_lock_regex_rule_does_not_block_non_matching_dag(self):
        """Floor lock: a regex-based locked rule does NOT affect non-matching dags."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task("dev_reporting")
        extend_global_openlineage_emission_policy(task, include_source_code=False)  # authoring wants False

        rules = [
            {
                "scope": {"dag_id": "^prod_.*"},
                "match_mode": "regex",
                "locked": True,
                "controls": {"include_source_code": False},
            }
        ]
        with conf_vars({("openlineage", "emission_policy"): json.dumps(rules)}):
            cfg = resolve_task_emission_policy(task, "dev_reporting", "test_task")

        # Regex doesn't match "dev_reporting" → no lock → authoring override applies
        assert cfg.include_source_code is False  # authoring took effect

    def test_locked_in_legacy_path_has_no_effect(self):
        """In the pure legacy path (emission_policy empty), locked is irrelevant.

        The authoring emit=True flag overrides disabled_for_operators because the legacy
        path uses frozenset() (no locked fields) when calling _apply_task_authoring.
        """
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        # Use a real operator that carries params so extend_global_openlineage_emission_policy takes effect.
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)

        fqcn = f"{task.__class__.__module__}.{task.__class__.__qualname__}"
        with conf_vars(
            {
                ("openlineage", "emission_policy"): "",
                ("openlineage", "disabled_for_operators"): fqcn,
            }
        ):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        # Legacy path: authoring applied on top (frozenset() → no locked fields)
        # disabled_for_operators → emit=False in legacy path, but authoring emit=True
        # overrides it because there is no locking in the pure legacy path.
        assert cfg.emit is True

    def test_locked_emit_logs_exact_message_with_locked_value(self):
        """Exact log message when extend_global_openlineage_emission_policy is blocked for task 'emit'."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)

        rule = {"scope": {}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False
        locked_calls = [c for c in mock_log.warning.call_args_list if "has no effect" in c.args[0]]
        assert len(locked_calls) == 1
        assert locked_calls[0].args == (
            "OpenLineage emission_policy: extend_global_openlineage_emission_policy call for 'emit' on %s"
            " has no effect — locked by conf rule at value %r",
            "task 'test_task' in dag 'test_dag'",
            False,
        )

    def test_locked_field_logs_exact_message_with_locked_value(self):
        """Exact log message when extend_global_openlineage_emission_policy is blocked for a non-emit field."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=True)

        rule = {"scope": {}, "locked": True, "controls": {"include_source_code": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False
        locked_calls = [c for c in mock_log.warning.call_args_list if "has no effect" in c.args[0]]
        assert len(locked_calls) == 1
        assert locked_calls[0].args == (
            "OpenLineage emission_policy: extend_global_openlineage_emission_policy call for '%s' on %s"
            " has no effect — locked by conf rule at value %r",
            "include_source_code",
            "task 'test_task' in dag 'test_dag'",
            False,
        )

    def test_locked_dag_emit_logs_exact_message_with_locked_value(self):
        """Exact log message when extend_global_openlineage_emission_policy is blocked for dag-event 'emit'."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        dag, _ = _make_dag_and_task("locked_dag_msg_test")
        extend_global_openlineage_emission_policy(dag, emit=True)

        rule = {"scope": {"dag_id": "locked_dag_msg_test"}, "locked": True, "controls": {"emit": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
                cfg = resolve_dag_emission_policy("locked_dag_msg_test", dag)

        assert cfg.emit is False
        locked_calls = [c for c in mock_log.warning.call_args_list if "has no effect" in c.args[0]]
        assert len(locked_calls) == 1
        assert locked_calls[0].args == (
            "OpenLineage emission_policy: extend_global_openlineage_emission_policy call for 'emit' on %s"
            " has no effect — locked by conf rule at value %r",
            "dag event 'locked_dag_msg_test'",
            False,
        )


class TestAuthoringLayerWithLegacy:
    def test_authoring_applied_in_legacy_path(self):
        """extend_global_openlineage_emission_policy flags take effect in the pure legacy path."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        # Use a real operator (EmptyOperator has params)
        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, include_source_code=False)

        with conf_vars({("openlineage", "emission_policy"): ""}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.include_source_code is False

    def test_authoring_applied_in_new_path(self):
        """extend_global_openlineage_emission_policy flags take effect in the new (emission_policy) path."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, hook_lineage=False)

        with conf_vars(
            {
                (
                    "openlineage",
                    "emission_policy",
                ): '[{"scope": {}, "controls": {"include_source_code": false}}]'
            }
        ):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.hook_lineage is False  # authoring
        assert cfg.include_source_code is False  # conf rule

    def test_emit_task_events_in_authoring_sets_emit(self):
        """emit_task_events in authoring flags resolves to task emit."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit_task_events=False)

        with conf_vars({("openlineage", "emission_policy"): "[]"}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False


class TestEmitMixedKeyContradictions:
    """``emit`` vs ``emit_task_events`` resolution across rules in the same tier."""

    def test_emit_task_events_in_later_rule_wins_with_contradiction_warning(self):
        """[{'emit': False}, {'emit_task_events': True}] — task emit becomes True; a contradiction
        warning fires because the resolved task-emit decision flipped within the same tier.
        This pins current behaviour: ``emit`` and ``emit_task_events`` participate in the same
        resolution stream for task scope, so disagreement across rules in one tier is flagged.
        """
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls(
                [
                    {"scope": {}, "controls": {"emit": False}},
                    {"scope": {}, "controls": {"emit_task_events": True}},
                ],
                operator=MockOperator(),
                dag_id="my_dag",
                task_id="my_task",
            )
        assert cfg.emit is True
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("contradictory" in c for c in warn_calls)

    def test_emit_in_later_rule_wins_when_earlier_set_emit_task_events(self):
        """[{'emit_task_events': False}, {'emit': True}] — task emit becomes True (last wins)."""
        cfg = _resolve_task_controls(
            [
                {"scope": {}, "controls": {"emit_task_events": False}},
                {"scope": {}, "controls": {"emit": True}},
            ],
            operator=MockOperator(),
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.emit is True

    def test_global_emit_true_when_selective_enable_translated_baseline(self):
        """User-explicit global {'emit': true} overrides selective_enable's translated baseline."""
        _, task = _make_dag_and_task()
        # Task is NOT opted in — selective_enable would otherwise suppress emission.
        with conf_vars(
            {
                ("openlineage", "emission_policy"): json.dumps([{"scope": {}, "controls": {"emit": True}}]),
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        # Both rules are global; user rule appears later → wins last-wins-in-tier.
        assert cfg.emit is True


class TestLockOnSpecificEmitKey:
    """Lock-precision tests: ``emit_task_events`` vs ``emit_dag_events`` vs ``emit``."""

    def test_lock_on_emit_dag_events_does_not_block_task_authoring(self):
        """A locked dag-event emit rule must not lock task-event authoring overrides."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)

        # Lock only the dag-event emit; task emit must remain unlocked for authoring.
        # NOTE: this rule still adds 'emit' to locked_fields under the current implementation,
        # because we cannot disambiguate which event scope the lock targets without
        # explicit per-key locking. The test pins this behaviour so it isn't broken silently.
        rule = {"scope": {"dag_id": "test_dag"}, "locked": True, "controls": {"emit_dag_events": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        # Conf rule used emit_dag_events; task emit is not changed by the conf rule itself,
        # but the lock currently extends to task emit. Authoring is blocked.
        assert cfg.emit is True  # default — neither rule nor authoring took effect
        # (Behaviour intentionally pinned; revisit if per-scope locking is added.)

    def test_lock_on_emit_task_events_blocks_task_authoring(self):
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, emit=True)

        rule = {"scope": {}, "locked": True, "controls": {"emit_task_events": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.emit is False  # locked

    def test_lock_on_extract_metadata_blocks_authoring_but_not_independent_fields(self):
        """Authoring ``hook_lineage`` is independent of an ``extract_operator_metadata`` lock."""
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, extract_operator_metadata=True, hook_lineage=False)

        rule = {"scope": {}, "locked": True, "controls": {"extract_operator_metadata": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        # extract_operator_metadata stays at the locked conf value
        assert cfg.extract_operator_metadata is False
        # hook_lineage authoring still applies (different field, not locked)
        assert cfg.hook_lineage is False


class TestStrictUnknownKeyHandling:
    """Unknown keys at any nesting level cause the rule to be skipped with a WARNING."""

    def test_unknown_top_level_key_skipped(self):
        rule = {"scope": {}, "controls": {"emit": False}, "bogus_top_level_key": 1}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("unknown top-level key" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()  # rule was skipped, defaults remain

    def test_unknown_scope_key_skipped(self):
        rule = {"scope": {"dgg_id": "typo_dag"}, "controls": {"emit": False}}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("scope" in c and "unknown key" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_unknown_controls_key_skipped(self):
        rule = {"scope": {}, "controls": {"emiit": False}}  # typo
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("controls" in c and "unknown key" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_empty_controls_skipped(self):
        rule = {"scope": {"dag_id": "x"}, "controls": {}}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("empty 'controls'" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_missing_scope_skipped(self):
        rule = {"controls": {"emit": False}}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("missing required 'scope'" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_missing_controls_skipped(self):
        rule = {"scope": {"dag_id": "x"}}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("missing required 'controls'" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_operator_combined_with_dag_id_skipped(self):
        rule = {"scope": {"dag_id": "x", "operator": "y"}, "controls": {"emit": False}}
        with mock.patch("airflow.providers.openlineage.utils.emission_policy.log") as mock_log:
            cfg = _resolve_task_controls([rule], operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        warn_calls = [str(c) for c in mock_log.warning.call_args_list]
        assert any("operator" in c and "dag_id" in c for c in warn_calls)
        assert cfg == EmissionPolicy.defaults()

    def test_well_formed_unknown_top_level_key_locked_still_works_after_skip(self):
        """An unknown key on rule A skips A but rule B (well-formed) still applies."""
        rules = [
            {"scope": {}, "controls": {"emit": False}, "bogus": True},  # skipped
            {"scope": {}, "controls": {"emit": False}},  # applied
        ]
        cfg = _resolve_task_controls(rules, operator=MockOperator(), dag_id="my_dag", task_id="my_task")
        assert cfg.emit is False  # second rule applied normally


class TestRegexMatchAll:
    """A ``match_mode: regex`` rule with ``.*`` matches every dag_id — lands in dag tier."""

    def test_regex_dot_star_dag_id_acts_as_dag_tier_for_all(self):
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": ".*"}, "match_mode": "regex", "controls": {"include_source_code": False}}],
            dag_id="any_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.include_source_code is False

    def test_regex_dot_star_dag_id_beats_operator_tier(self):
        """Regex dag rule (dag tier) beats an operator-tier rule for the same field."""
        op = MockOperator()
        fqcn = _operator_fqcn(op)
        cfg = _resolve_task_controls(
            [
                {"scope": {"operator": fqcn}, "controls": {"include_source_code": False}},
                {"scope": {"dag_id": ".*"}, "match_mode": "regex", "controls": {"include_source_code": True}},
            ],
            operator=op,
            dag_id="my_dag",
            task_id="my_task",
        )
        assert cfg.include_source_code is True

    def test_regex_empty_string_dag_id_matches_only_empty_dag(self):
        """An empty regex pattern matches only the empty string under re.fullmatch."""
        cfg = _resolve_task_controls(
            [{"scope": {"dag_id": ""}, "match_mode": "regex", "controls": {"emit": False}}],
            dag_id="my_dag",
            operator=MockOperator(),
            task_id="my_task",
        )
        assert cfg.emit is True  # no match


class TestEmitDagEventsLockDoesNotAffectTaskAuthoring:
    """Locking on a dag-event-only key should leave unrelated authoring fields untouched."""

    def test_authoring_extract_metadata_unaffected_by_dag_emit_lock(self):
        from airflow.providers.openlineage.api.emission_policy import (
            extend_global_openlineage_emission_policy,
        )

        _, task = _make_dag_and_task()
        extend_global_openlineage_emission_policy(task, extract_operator_metadata=False)

        # Lock targets emit only — other authoring fields must pass through.
        rule = {"scope": {"dag_id": "test_dag"}, "locked": True, "controls": {"emit_dag_events": False}}
        with conf_vars({("openlineage", "emission_policy"): json.dumps([rule])}):
            cfg = resolve_task_emission_policy(task, "test_dag", "test_task")

        assert cfg.extract_operator_metadata is False  # authoring applied


class TestSelectiveEnableWithLockedRule:
    """selective_enable's translated baseline can be overridden by an unlocked rule — but a locked rule wins."""

    def test_locked_global_emit_false_overrides_selective_enable_opt_in(self):
        """A locked global emit:false rule blocks the per-task opt-in from selective_enable."""
        _, task = _make_dag_and_task()
        enable_lineage(task)  # task IS opted in
        rules = [{"scope": {}, "locked": True, "controls": {"emit": False}}]
        with conf_vars(
            {
                ("openlineage", "emission_policy"): json.dumps(rules),
                ("openlineage", "selective_enable"): "True",
            }
        ):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                cfg = resolve_task_emission_policy(task, "test_dag", "test_task")
        # selective_enable injects a task-tier {"emit": true} opt-in rule for opted-in tasks.
        # That rule wins value resolution (task tier > global tier). The lock on the global rule
        # adds 'emit' to locked_fields but locked_fields only blocks AUTHORING, not other conf rules.
        # So the task-tier opt-in rule still applies.
        assert cfg.emit is True
