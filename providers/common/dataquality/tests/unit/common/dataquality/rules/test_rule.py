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
from pathlib import Path

import pytest
from pydantic import ValidationError

import airflow.providers.common.dataquality
from airflow.providers.common.dataquality.rules import Condition, DQRule, RuleSet, Severity, describe_rule


def test_skill_reference_schema_matches_live_model():
    """
    The dataquality-rule-authoring skill ships a generated RuleSet.model_json_schema() snapshot for
    agents to consult. Guard against it silently drifting out of sync with the real model --
    regenerate with ``json.dumps(RuleSet.model_json_schema(), indent=2)`` (plus a trailing
    newline) if this fails after an intentional schema change.
    """
    schema_path = (
        Path(airflow.providers.common.dataquality.__file__).parent
        / "skills"
        / "dataquality-rule-authoring"
        / "references"
        / "ruleset.schema.json"
    )
    checked_in = json.loads(schema_path.read_text())
    assert checked_in == RuleSet.model_json_schema()


class TestCondition:
    @pytest.mark.parametrize(
        ("condition", "observed", "expected"),
        [
            ({"equal_to": 0}, None, False),
            ({"equal_to": 0}, 0, True),
            ({"equal_to": 0}, 1, False),
            ({"equal_to": 100, "tolerance": 0.1}, 105, True),
            ({"equal_to": 100, "tolerance": 0.1}, 111, False),
            ({"equal_to": -100, "tolerance": 0.1}, -95, True),
            ({"equal_to": -100, "tolerance": 0.1}, -105, True),
            ({"equal_to": -100, "tolerance": 0.1}, -89, False),
            ({"equal_to": -100, "tolerance": 0.1}, -111, False),
            ({"geq_to": 10, "tolerance": 0.1}, 9, True),
            ({"geq_to": 10, "tolerance": 0.1}, 8.9, False),
            ({"geq_to": -1000, "tolerance": 0.1}, -1000, True),
            ({"geq_to": -1000, "tolerance": 0.1}, -1100, True),
            ({"geq_to": -1000, "tolerance": 0.1}, -1101, False),
            ({"greater_than": 5}, 6, True),
            ({"greater_than": 5}, 5, False),
            ({"greater_than": 10, "tolerance": 0.1}, 9.1, True),
            ({"greater_than": 10, "tolerance": 0.1}, 9, False),
            ({"greater_than": -100, "tolerance": 0.1}, -109.9, True),
            ({"greater_than": -100, "tolerance": 0.1}, -110, False),
            ({"leq_to": 5}, 6, False),
            ({"leq_to": 10, "tolerance": 0.1}, 11, True),
            ({"leq_to": 10, "tolerance": 0.1}, 11.1, False),
            ({"leq_to": -10, "tolerance": 0.1}, -10, True),
            ({"leq_to": -10, "tolerance": 0.1}, -9, True),
            ({"leq_to": -10, "tolerance": 0.1}, -8.9, False),
            ({"less_than": 5}, 4, True),
            ({"less_than": 10, "tolerance": 0.1}, 10.9, True),
            ({"less_than": 10, "tolerance": 0.1}, 11, False),
            ({"less_than": -100, "tolerance": 0.1}, -90.1, True),
            ({"less_than": -100, "tolerance": 0.1}, -90, False),
            ({"geq_to": 0, "leq_to": 10}, 5, True),
            ({"geq_to": 0, "leq_to": 10}, 11, False),
            ({"geq_to": 10, "leq_to": 20, "tolerance": 0.1}, 9, True),
            ({"geq_to": 10, "leq_to": 20, "tolerance": 0.1}, 22, True),
            ({"geq_to": 10, "leq_to": 20, "tolerance": 0.1}, 8.9, False),
            ({"geq_to": 10, "leq_to": 20, "tolerance": 0.1}, 22.1, False),
            ({"geq_to": -20, "leq_to": -10, "tolerance": 0.1}, -22, True),
            ({"geq_to": -20, "leq_to": -10, "tolerance": 0.1}, -9, True),
            ({"geq_to": -20, "leq_to": -10, "tolerance": 0.1}, -22.1, False),
            ({"geq_to": -20, "leq_to": -10, "tolerance": 0.1}, -8.9, False),
        ],
    )
    def test_evaluate(self, condition, observed, expected):
        assert Condition.from_dict(condition).evaluate(observed) is expected

    @pytest.mark.parametrize(
        "condition",
        [
            {},
            {"tolerance": 0.1},
            {"equal_to": 1, "greater_than": 0},
            {"nonsense": 1},
        ],
    )
    def test_invalid_conditions_rejected(self, condition):
        with pytest.raises(ValidationError):
            Condition.from_dict(condition)

    def test_to_dict_round_trip(self):
        condition = Condition.from_dict({"geq_to": 1, "leq_to": 2})
        assert Condition.from_dict(condition.to_dict()) == condition


class TestDQRule:
    def test_condition_dict_is_coerced(self):
        rule = DQRule(name="r", check="null_count", column="c", condition={"equal_to": 0})  # type: ignore[arg-type]
        assert isinstance(rule.condition, Condition)

    @pytest.mark.parametrize(
        "condition",
        [
            {"nonsense": 1},
            {"tolerance": 0.1},
            {"equal_to": 1, "greater_than": 0},
        ],
    )
    def test_malformed_condition_dict_rejected_at_construction(self, condition):
        with pytest.raises(ValidationError):
            DQRule(name="r", check="null_count", column="c", condition=condition)

    def test_rule_uid_is_stable_across_severity(self):
        base = DQRule(name="r", check="null_count", column="c", condition=Condition(equal_to=0))
        tweaked = DQRule(
            name="r",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            severity=Severity.WARN,
        )
        assert base.rule_uid == tweaked.rule_uid

    def test_rule_uid_is_stable_across_description(self):
        base = DQRule(name="r", check="null_count", column="c", condition=Condition(equal_to=0))
        described = DQRule(
            name="r",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            description="Column c must be populated.",
        )
        assert base.rule_uid == described.rule_uid

    def test_rule_uid_changes_with_condition(self):
        one = DQRule(name="r", check="null_count", column="c", condition=Condition(equal_to=0))
        two = DQRule(name="r", check="null_count", column="c", condition=Condition(equal_to=1))
        assert one.rule_uid != two.rule_uid

    def test_previous_name_keeps_uid(self):
        old = DQRule(name="old_name", check="null_count", column="c", condition=Condition(equal_to=0))
        renamed = DQRule(
            name="new_name",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            previous_name="old_name",
        )
        assert renamed.rule_uid == old.rule_uid

    def test_previous_name_can_collide_with_another_rules_identity(self):
        """
        Documents the known collision this ``rule_uid`` scheme can produce.

        A rule renamed away from ``old_name`` derives the same uid as another, unrelated rule
        that is still actually named ``old_name`` with the same check/column/condition. Set an
        explicit ``id`` on one of them to avoid this (see ``test_explicit_id_avoids_collision``).
        """
        renamed = DQRule(
            name="new_name",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            previous_name="old_name",
        )
        unrelated = DQRule(name="old_name", check="null_count", column="c", condition=Condition(equal_to=0))
        assert renamed.rule_uid == unrelated.rule_uid

    def test_explicit_id_is_used_directly_as_rule_uid(self):
        rule = DQRule(
            name="r", check="null_count", column="c", condition=Condition(equal_to=0), id="my-stable-id"
        )
        assert rule.rule_uid == "my-stable-id"

    def test_explicit_id_avoids_collision(self):
        renamed = DQRule(
            name="new_name",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            previous_name="old_name",
            id="renamed-rule",
        )
        unrelated = DQRule(name="old_name", check="null_count", column="c", condition=Condition(equal_to=0))
        assert renamed.rule_uid != unrelated.rule_uid

    def test_explicit_id_survives_condition_and_previous_name_changes(self):
        first = DQRule(name="r", check="null_count", column="c", condition=Condition(equal_to=0), id="stable")
        tightened = DQRule(
            name="r", check="null_count", column="c", condition=Condition(equal_to=5), id="stable"
        )
        assert first.rule_uid == tightened.rule_uid

    def test_id_is_serialized_and_round_trips(self):
        rule = DQRule(
            name="r", check="null_count", column="c", condition=Condition(equal_to=0), id="my-stable-id"
        )
        assert rule.to_dict()["id"] == "my-stable-id"
        assert DQRule.from_dict(rule.to_dict()) == rule

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"name": "", "check": "row_count", "condition": {"equal_to": 0}},
            {"name": "r", "check": "no_such_check", "condition": {"equal_to": 0}},
            {"name": "r", "check": "null_count", "condition": {"equal_to": 0}},  # missing column
            {"name": "r", "check": "custom_sql", "condition": {"equal_to": 0}},  # missing sql
            {"name": "r", "check": "row_count", "condition": {"equal_to": 0}, "sql": "SELECT 1"},
            {"name": "r", "check": "row_count", "condition": {"equal_to": 0}, "severity": "fatal"},
        ],
    )
    def test_invalid_rules_rejected(self, kwargs):
        with pytest.raises(ValidationError):
            DQRule(**kwargs)

    def test_row_count_needs_no_column(self):
        rule = DQRule(name="volume", check="row_count", condition=Condition(greater_than=0))
        assert rule.column is None

    @pytest.mark.parametrize(
        ("check", "column", "expected_dimension"),
        [
            ("null_count", "c", "completeness"),
            ("null_ratio", "c", "completeness"),
            ("distinct_count", "c", "uniqueness"),
            ("unique_violations", "c", "uniqueness"),
            ("min", "c", "validity"),
            ("max", "c", "validity"),
            ("mean", "c", "validity"),
            ("row_count", None, "volume"),
        ],
    )
    def test_default_dimension_comes_from_check_catalog(self, check, column, expected_dimension):
        rule = DQRule(name="r", check=check, column=column, condition=Condition(equal_to=0))
        assert rule.dimension.value == expected_dimension

    def test_custom_sql_dimension_can_be_overridden(self):
        rule = DQRule(
            name="freshness_check",
            check="custom_sql",
            sql="SELECT COUNT(*) FROM {table} WHERE updated_at < NOW() - INTERVAL '1 day'",
            condition=Condition(equal_to=0),
            dimension="freshness",
        )
        assert rule.dimension.value == "freshness"

    def test_invalid_dimension_rejected(self):
        with pytest.raises(ValidationError):
            DQRule(
                name="r", check="row_count", condition=Condition(greater_than=0), dimension="not_a_dimension"
            )

    def test_explicit_dimension_matching_default_is_omitted_from_to_dict(self):
        rule = DQRule(
            name="r",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            dimension="completeness",
        )
        assert "dimension" not in rule.to_dict()

    def test_dimension_override_round_trips(self):
        rule = DQRule(
            name="freshness_check",
            check="custom_sql",
            sql="SELECT COUNT(*) FROM {table} WHERE updated_at < NOW() - INTERVAL '1 day'",
            condition=Condition(equal_to=0),
            dimension="freshness",
        )
        assert rule.to_dict()["dimension"] == "freshness"
        assert DQRule.from_dict(rule.to_dict()) == rule

    def test_missing_condition_without_catalog_default_raises(self):
        with pytest.raises(ValidationError, match="condition is required"):
            DQRule(name="r", check="null_count", column="c")

    def test_to_dict_round_trip(self):
        rule = DQRule(
            name="r",
            check="custom_sql",
            sql="SELECT COUNT(*) FROM {table} WHERE x < 0",
            condition=Condition(equal_to=0),
            severity=Severity.WARN,
            partition_clause="ds = '2026-07-04'",
            description="No rows should have negative x.",
        )
        assert DQRule.from_dict(rule.to_dict()) == rule

    def test_description_is_serialized(self):
        rule = DQRule(
            name="r",
            check="row_count",
            condition=Condition(greater_than=0),
            description="Orders table should not be empty.",
        )
        assert rule.to_dict()["description"] == "Orders table should not be empty."

    def test_default_description_uses_column_when_available(self):
        rule = DQRule(name="amount_min", check="min", column="amount", condition=Condition(geq_to=0))
        assert describe_rule(rule) == "amount should be greater than or equal to 0.0"


class TestRuleSet:
    def test_duplicate_rule_names_rejected(self):
        rule = DQRule(name="r", check="row_count", condition=Condition(greater_than=0))
        with pytest.raises(ValidationError, match="Duplicate rule names"):
            RuleSet(name="s", rules=(rule, rule))

    def test_colliding_rule_uids_rejected(self):
        renamed = DQRule(
            name="new_name",
            check="null_count",
            column="c",
            condition=Condition(equal_to=0),
            previous_name="old_name",
        )
        unrelated = DQRule(name="old_name", check="null_count", column="c", condition=Condition(equal_to=0))
        with pytest.raises(ValidationError, match="collide on rule_uid"):
            RuleSet(name="s", rules=(renamed, unrelated))

    def test_from_dict_round_trip(self):
        ruleset = RuleSet(
            name="orders",
            rules=(
                DQRule(name="volume", check="row_count", condition=Condition(greater_than=0)),
                DQRule(name="ids", check="null_count", column="id", condition=Condition(equal_to=0)),
            ),
        )
        assert RuleSet.from_dict(ruleset.to_dict()) == ruleset

    def test_from_file(self, tmp_path):
        ruleset_file = tmp_path / "orders.yaml"
        ruleset_file.write_text(
            """
            name: orders
            rules:
              - name: ids_not_null
                check: null_count
                column: id
                condition:
                  equal_to: 0
              - name: volume
                check: row_count
                condition:
                  greater_than: 100
                severity: warn
            """
        )
        ruleset = RuleSet.from_file(str(ruleset_file))
        assert ruleset.name == "orders"
        assert [rule.name for rule in ruleset.rules] == ["ids_not_null", "volume"]
        assert ruleset.rules[1].severity.value == "warn"
