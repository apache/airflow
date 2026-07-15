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

from unittest import mock

import pytest

from airflow.providers.common.dataquality.engines.sql import SQLDQEngine
from airflow.providers.common.dataquality.rules import Condition, DQRule, RuleSet
from airflow.providers.common.sql.hooks.sql import DbApiHook


@pytest.fixture
def hook():
    return mock.create_autospec(DbApiHook, instance=True)


NULLS = DQRule(name="nulls", check="null_count", column="id", condition=Condition(equal_to=0))
VOLUME = DQRule(name="volume", check="row_count", condition=Condition(greater_than=0))
CUSTOM = DQRule(
    name="negatives",
    check="custom_sql",
    sql="SELECT COUNT(*) FROM {table} WHERE amount < 0",
    condition=Condition(equal_to=0),
)


class TestBuildBatchSql:
    def test_batch_sql_unions_one_select_per_rule(self, hook):
        engine = SQLDQEngine(hook)
        sql = engine.build_batch_sql([NULLS, VOLUME], "orders", None)
        assert sql.count("UNION ALL") == 1
        assert f"'{NULLS.rule_uid}' AS rule_uid" in sql
        assert "SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END)" in sql
        assert "COUNT(*)" in sql
        assert "WHERE" not in sql

    def test_partition_clauses_are_anded(self, hook):
        rule = DQRule(
            name="nulls",
            check="null_count",
            column="id",
            condition=Condition(equal_to=0),
            partition_clause="region = 'EU'",
        )
        sql = SQLDQEngine(hook).build_batch_sql([rule], "orders", "ds = '2026-07-04'")
        assert "WHERE ds = '2026-07-04' AND region = 'EU'" in sql


class TestMeasure:
    def test_builtin_rules_measured_from_single_query(self, hook):
        hook.get_records.return_value = [(NULLS.rule_uid, 0), (VOLUME.rule_uid, 42)]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        hook.get_records.assert_called_once()
        by_name = {obs.rule.name: obs for obs in observations}
        assert by_name["nulls"].observed_value == 0
        assert by_name["volume"].observed_value == 42
        assert by_name["nulls"].sql == SQLDQEngine(hook).build_rule_sql(NULLS, "orders")
        assert by_name["volume"].sql == SQLDQEngine(hook).build_rule_sql(VOLUME, "orders")
        assert all(obs.error_message is None for obs in observations)

    def test_builds_each_rules_sql_exactly_once_on_success(self, hook):
        hook.get_records.return_value = [(NULLS.rule_uid, 0), (VOLUME.rule_uid, 42)]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))
        engine = SQLDQEngine(hook)

        with mock.patch.object(engine, "build_rule_sql", wraps=engine.build_rule_sql) as build_rule_sql:
            engine.measure(ruleset, "orders")

        assert build_rule_sql.call_count == 2

    def test_builds_each_rules_sql_exactly_once_on_batch_failure(self, hook):
        hook.get_records.side_effect = RuntimeError("connection refused")
        hook.get_first.side_effect = [(NULLS.rule_uid, 0), (VOLUME.rule_uid, 0)]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))
        engine = SQLDQEngine(hook)

        with mock.patch.object(engine, "build_rule_sql", wraps=engine.build_rule_sql) as build_rule_sql:
            engine.measure(ruleset, "orders")

        assert build_rule_sql.call_count == 2

    def test_batch_failure_falls_back_to_per_rule_queries(self, hook):
        hook.get_records.side_effect = RuntimeError("connection refused")
        hook.get_first.side_effect = [(NULLS.rule_uid, 0), (VOLUME.rule_uid, 0)]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        assert hook.get_first.call_count == 2
        assert len(observations) == 2
        assert all(obs.error_message is None for obs in observations)
        assert all(obs.observed_value == 0 for obs in observations)
        assert all(obs.sql for obs in observations)

    def test_batch_failure_fallback_isolates_a_single_bad_rule(self, hook):
        """One rule's query failing in the per-rule fallback must not fail the other rules."""
        hook.get_records.side_effect = RuntimeError("connection refused")
        hook.get_first.side_effect = [
            (NULLS.rule_uid, 0),
            RuntimeError("no such column: no_such_column"),
        ]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        by_name = {obs.rule.name: obs for obs in observations}
        assert by_name["nulls"].observed_value == 0
        assert by_name["nulls"].error_message is None
        assert by_name["volume"].observed_value is None
        assert by_name["volume"].error_message == "no such column: no_such_column"

    def test_per_rule_fallback_query_with_no_rows_is_an_error(self, hook):
        hook.get_records.side_effect = RuntimeError("connection refused")
        hook.get_first.return_value = None
        ruleset = RuleSet(name="s", rules=(NULLS,))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        assert observations[0].error_message == "No result returned for rule"
        assert observations[0].observed_value is None

    def test_missing_rule_in_result_is_an_error(self, hook):
        hook.get_records.return_value = [(NULLS.rule_uid, 0)]
        ruleset = RuleSet(name="s", rules=(NULLS, VOLUME))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        by_name = {obs.rule.name: obs for obs in observations}
        assert by_name["nulls"].error_message is None
        assert by_name["volume"].error_message == "No result returned for rule"

    def test_custom_sql_rule_runs_individually_with_table_substituted(self, hook):
        hook.get_first.return_value = (3,)
        ruleset = RuleSet(name="s", rules=(CUSTOM,))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        hook.get_first.assert_called_once_with("SELECT COUNT(*) FROM orders WHERE amount < 0")
        hook.get_records.assert_not_called()
        assert observations[0].observed_value == 3
        assert observations[0].sql == "SELECT COUNT(*) FROM orders WHERE amount < 0"

    def test_custom_sql_no_rows_is_an_error(self, hook):
        hook.get_first.return_value = None
        ruleset = RuleSet(name="s", rules=(CUSTOM,))

        observations = SQLDQEngine(hook).measure(ruleset, "orders")

        assert observations[0].error_message == "Query returned no rows"
