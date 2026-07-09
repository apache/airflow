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
"""SQL execution engine: compiles a ruleset into check queries run through a DB-API hook."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.common.dataquality.rules import CUSTOM_SQL_CHECK
from airflow.providers.common.dataquality.rules.checks import CHECK_SPECS

if TYPE_CHECKING:
    from airflow.providers.common.dataquality.rules import DQRule, RuleSet
    from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Observation:
    """Raw value a rule observed, before its condition is evaluated."""

    rule: DQRule
    observed_value: Any = None
    duration_ms: float | None = None
    error_message: str | None = None
    sql: str | None = None


class SQLDQEngine:
    """
    Runs built-in checks as a single UNION ALL query and custom SQL rules individually.

    Table, column, and partition-clause values come from the Dag author and are interpolated
    into SQL the same way the ``common.sql`` check operators do — they are trusted input.
    """

    check_sql_template = "SELECT '{rule_uid}' AS rule_uid, {expression} AS observed FROM {table}{where}"

    def __init__(self, hook: DbApiHook) -> None:
        self.hook = hook

    def measure(self, ruleset: RuleSet, table: str, partition_clause: str | None = None) -> list[Observation]:
        builtin_rules = [rule for rule in ruleset.rules if rule.check != CUSTOM_SQL_CHECK]
        custom_rules = [rule for rule in ruleset.rules if rule.check == CUSTOM_SQL_CHECK]

        observations = []
        if builtin_rules:
            observations.extend(self._measure_builtin(builtin_rules, table, partition_clause))
        for rule in custom_rules:
            observations.append(self._measure_custom(rule, table))
        return observations

    def build_batch_sql(self, rules: list[DQRule], table: str, partition_clause: str | None) -> str:
        return " UNION ALL ".join(self.build_rule_sql(rule, table, partition_clause) for rule in rules)

    def build_rule_sql(self, rule: DQRule, table: str, partition_clause: str | None = None) -> str:
        """Build the SQL used to measure one built-in rule."""
        expression = CHECK_SPECS[rule.check].expression.format(column=rule.column)
        predicates = [p for p in (partition_clause, rule.partition_clause) if p]
        where = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        return self.check_sql_template.format(
            rule_uid=rule.rule_uid, expression=expression, table=table, where=where
        )

    def _measure_builtin(
        self, rules: list[DQRule], table: str, partition_clause: str | None
    ) -> list[Observation]:
        sql = self.build_batch_sql(rules, table, partition_clause)
        log.info("Running %d built-in checks against %s", len(rules), table)
        started = time.monotonic()
        try:
            records = self.hook.get_records(sql)
        except Exception as e:
            elapsed_ms = (time.monotonic() - started) * 1000
            log.exception("Check query failed for table %s", table)
            return [
                Observation(
                    rule=rule,
                    duration_ms=elapsed_ms,
                    error_message=str(e),
                    sql=self.build_rule_sql(rule, table, partition_clause),
                )
                for rule in rules
            ]
        elapsed_ms = (time.monotonic() - started) * 1000
        observed_by_uid = {str(row[0]): row[1] for row in records or []}
        return [
            Observation(
                rule=rule,
                observed_value=observed_by_uid.get(rule.rule_uid),
                duration_ms=elapsed_ms,
                error_message=None if rule.rule_uid in observed_by_uid else "No result returned for rule",
                sql=self.build_rule_sql(rule, table, partition_clause),
            )
            for rule in rules
        ]

    def _measure_custom(self, rule: DQRule, table: str) -> Observation:
        if rule.sql is None:
            raise ValueError(f"Rule {rule.name!r} has no SQL to execute")
        sql = rule.sql.replace("{table}", table)
        log.info("Running custom SQL check %s", rule.name)
        started = time.monotonic()
        try:
            row = self.hook.get_first(sql)
        except Exception as e:
            log.exception("Custom SQL check %s failed", rule.name)
            return Observation(
                rule=rule,
                duration_ms=(time.monotonic() - started) * 1000,
                error_message=str(e),
                sql=sql,
            )
        elapsed_ms = (time.monotonic() - started) * 1000
        if row is None:
            return Observation(
                rule=rule, duration_ms=elapsed_ms, error_message="Query returned no rows", sql=sql
            )
        return Observation(rule=rule, observed_value=row[0], duration_ms=elapsed_ms, sql=sql)
