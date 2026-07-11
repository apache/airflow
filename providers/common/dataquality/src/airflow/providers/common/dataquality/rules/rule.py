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
"""Declarative data quality rule model: rules are data, not code."""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Any, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from airflow.providers.common.dataquality.exceptions import DQRuleValidationError
from airflow.providers.common.dataquality.rules.checks import CHECK_SPECS, Dimension

CUSTOM_SQL_CHECK = "custom_sql"


class Severity(str, Enum):
    """Severity used to decide whether a failing rule fails the task."""

    WARN = "warn"
    ERROR = "error"


class Condition(BaseModel):
    """
    Pass/fail condition evaluated against a rule's observed value.

    Uses the same grammar as the ``common.sql`` check operators: ``equal_to``,
    ``greater_than``, ``less_than``, ``geq_to``, ``leq_to``, plus a percentage
    ``tolerance`` that widens comparisons.

    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    equal_to: float | None = None
    greater_than: float | None = None
    less_than: float | None = None
    geq_to: float | None = None
    leq_to: float | None = None
    tolerance: float | None = None

    @model_validator(mode="after")
    def _validate_comparisons(self) -> Condition:
        comparisons = {
            "equal_to": self.equal_to,
            "greater_than": self.greater_than,
            "less_than": self.less_than,
            "geq_to": self.geq_to,
            "leq_to": self.leq_to,
        }
        set_comparisons = {name for name, value in comparisons.items() if value is not None}
        if not set_comparisons:
            raise ValueError(f"Condition needs at least one comparison out of: {', '.join(comparisons)}")
        if self.equal_to is not None and len(set_comparisons) > 1:
            raise ValueError("equal_to cannot be combined with other comparisons")
        return self

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Condition:
        return cls(**data)

    def to_dict(self) -> dict[str, float]:
        data = {
            "equal_to": self.equal_to,
            "greater_than": self.greater_than,
            "less_than": self.less_than,
            "geq_to": self.geq_to,
            "leq_to": self.leq_to,
            "tolerance": self.tolerance,
        }
        return {k: v for k, v in data.items() if v is not None}

    def evaluate(self, observed: Any) -> bool:
        if observed is None:
            return False
        value = float(observed)
        if self.equal_to is not None:
            if self.tolerance is not None:
                delta = abs(self.equal_to) * self.tolerance
                return self.equal_to - delta <= value <= self.equal_to + delta
            return value == self.equal_to
        geq_to = self.geq_to
        greater_than = self.greater_than
        leq_to = self.leq_to
        less_than = self.less_than
        if self.tolerance is not None:
            if geq_to is not None:
                geq_to -= abs(geq_to) * self.tolerance
            if greater_than is not None:
                greater_than -= abs(greater_than) * self.tolerance
            if leq_to is not None:
                leq_to += abs(leq_to) * self.tolerance
            if less_than is not None:
                less_than += abs(less_than) * self.tolerance
        return (
            (greater_than is None or value > greater_than)
            and (less_than is None or value < less_than)
            and (geq_to is None or value >= geq_to)
            and (leq_to is None or value <= leq_to)
        )


class DQRule(BaseModel):
    """
    A single, named data quality rule.

    :param name: Rule name, unique within its ruleset.
    :param check: One of the built-in checks (see ``CHECK_SPECS``) or ``custom_sql``. Built-in
        checks are plain ANSI SQL and are not guaranteed to work against every ``DbApiHook``;
        see "Supported checks and databases" in the provider docs. Use ``custom_sql`` if a
        built-in check doesn't fit your database's dialect.
    :param condition: Pass condition for the observed value (``Condition`` or its dict form).
        Optional only for checks whose ``CheckSpec.default_condition`` is set; every current
        built-in check requires one explicitly.
    :param column: Target column; required for column-level built-in checks.
    :param sql: SQL statement returning a single scalar; required for ``custom_sql``.
        May reference the target table as ``{table}``.
    :param severity: ``error`` (fails the task by default) or ``warn`` (recorded only).
    :param partition_clause: Extra predicate ANDed into the check's WHERE clause.
    :param previous_name: Set when renaming a rule, to keep its history continuous.
    :param description: Human-readable description shown in results and the UI. When omitted,
        the provider generates a short default description from the rule and condition.

    Invalid input raises pydantic's own :class:`~pydantic.ValidationError`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    check: str = Field(description="One of the built-in checks, or custom_sql.")
    condition: Condition | dict[str, Any] | None = None
    column: str | None = None
    sql: str | None = None
    severity: Severity = Severity.ERROR
    partition_clause: str | None = None
    previous_name: str | None = None
    description: str | None = Field(
        default=None,
        description=(
            "Human-readable description shown in results and the UI. When omitted, the provider "
            "generates a short default description from the rule and condition."
        ),
    )
    dimension: Dimension | None = Field(
        default=None,
        description=(
            "Data quality dimension recorded for this rule's results. Defaults to the check's "
            "catalog dimension (validity for custom_sql) when not set explicitly."
        ),
    )

    @model_validator(mode="after")
    def _validate(self) -> DQRule:
        if not self.name:
            raise ValueError("Rule name cannot be empty")
        catalog_spec = None
        if self.check == CUSTOM_SQL_CHECK:
            if not self.sql:
                raise ValueError(f"Rule {self.name!r}: custom_sql check requires 'sql'")
        else:
            catalog_spec = CHECK_SPECS.get(self.check)
            if catalog_spec is None:
                supported = sorted([*CHECK_SPECS, CUSTOM_SQL_CHECK])
                raise ValueError(f"Rule {self.name!r}: unknown check {self.check!r}; supported: {supported}")
            if self.sql:
                raise ValueError(f"Rule {self.name!r}: 'sql' is only valid with custom_sql")
            if catalog_spec.requires_column and not self.column:
                raise ValueError(f"Rule {self.name!r}: check {self.check!r} requires 'column'")
            if self.condition is None and catalog_spec.default_condition is not None:
                object.__setattr__(self, "condition", Condition.from_dict(catalog_spec.default_condition))
        if self.condition is None:
            raise ValueError(f"Rule {self.name!r}: condition is required for check {self.check!r}")
        if self.dimension is None:
            object.__setattr__(
                self, "dimension", catalog_spec.dimension if catalog_spec else Dimension.VALIDITY
            )
        return self

    @property
    def rule_uid(self) -> str:
        """Stable identity across runs: survives severity/dimension tweaks and Dag refactors."""
        condition = cast("Condition", self.condition)
        identity = {
            "name": self.previous_name or self.name,
            "check": self.check,
            "column": self.column,
            "sql": self.sql,
            "condition": condition.to_dict(),
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        return digest[:16]

    def to_dict(self) -> dict[str, Any]:
        condition = cast("Condition", self.condition)
        data: dict[str, Any] = {
            "name": self.name,
            "check": self.check,
            "condition": condition.to_dict(),
            "severity": self.severity.value,
        }
        for optional in ("column", "sql", "partition_clause", "previous_name", "description"):
            value = getattr(self, optional)
            if value is not None:
                data[optional] = value
        # Only emit dimension when it overrides the check's catalog default, so a rule that
        # never set one explicitly round-trips through to_dict()/from_dict() unchanged.
        catalog_spec = CHECK_SPECS.get(self.check)
        default_dimension = catalog_spec.dimension if catalog_spec else Dimension.VALIDITY
        if self.dimension != default_dimension:
            data["dimension"] = cast("Dimension", self.dimension).value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DQRule:
        return cls(**data)


def describe_rule(rule: DQRule) -> str:
    """Return a default human-readable description for a rule."""
    condition = cast("Condition", rule.condition).to_dict()
    subject = rule.column or rule.name.replace("_", " ")
    if "equal_to" in condition:
        return f"{subject} should equal {condition['equal_to']}"
    if "geq_to" in condition:
        return f"{subject} should be greater than or equal to {condition['geq_to']}"
    if "greater_than" in condition:
        return f"{subject} should be greater than {condition['greater_than']}"
    if "leq_to" in condition:
        return f"{subject} should be less than or equal to {condition['leq_to']}"
    if "less_than" in condition:
        return f"{subject} should be less than {condition['less_than']}"
    return subject


class RuleSet(BaseModel):
    """A named collection of rules, typically attached to one table or asset."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    rules: tuple[DQRule, ...] = ()

    @model_validator(mode="after")
    def _validate(self) -> RuleSet:
        if not self.name:
            raise ValueError("RuleSet name cannot be empty")
        names = [rule.name for rule in self.rules]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(f"Duplicate rule names in ruleset {self.name!r}: {sorted(duplicates)}")
        return self

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "rules": [rule.to_dict() for rule in self.rules]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RuleSet:
        rules = [
            rule if isinstance(rule, DQRule) else DQRule.from_dict(rule) for rule in data.get("rules", [])
        ]
        return cls(name=data.get("name", ""), rules=tuple(rules))

    @classmethod
    def from_file(cls, path: str) -> RuleSet:
        """Load a ruleset from a YAML (or JSON, being a YAML subset) file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise DQRuleValidationError(f"Ruleset file {path!r} must contain a mapping at top level")
        return cls.from_dict(data)
