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
"""
Pydantic models for the LLM data quality plan and result reporting.

``DQPlan`` is the structured output type requested from the LLM.  The remaining
dataclasses hold execution results and are never serialised back to the model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field, computed_field


class DQCheck(BaseModel):
    """
    A single data-quality check produced by the LLM.

    :param check_name: Matches the key supplied by the user in ``prompts``.
    :param metric_key: Column alias used in the generated SQL (e.g. ``null_email_count``).
        The operator reads ``row[metric_key]`` from the query result.
    :param group_id: Logical bucket for grouping checks into a single SQL query
        (e.g. ``numeric_aggregate``, ``null_check``, ``uniqueness``, ``text``).
    """

    check_name: str = Field(description="Matches the key supplied by the user in prompts.")
    metric_key: str = Field(
        description="Column alias used in the generated SQL (e.g. null_email_count). The operator reads row[metric_key] from the query result."
    )
    group_id: str = Field(
        description="Logical bucket for grouping checks into a single SQL query (e.g. numeric_aggregate, null_check, uniqueness, text)."
    )


class DQCheckGroup(BaseModel):
    """
    A group of related :class:`DQCheck` items that share one SQL query.

    :param group_id: Matches :attr:`DQCheck.group_id` for each member check.
    :param query: A single SELECT statement whose result columns correspond to
        the ``metric_key`` values of all member checks.
    :param checks: The checks whose metrics can be extracted from ``query``'s result.
    """

    group_id: str = Field(description="Matches DQCheck.group_id for each member check.")
    query: str = Field(
        description="A single SELECT statement whose result columns correspond to the metric_key values of all member checks."
    )
    checks: list[DQCheck] = Field(
        description="The checks whose metrics can be extracted from query's result."
    )


class DQPlan(BaseModel):
    """
    Complete data-quality execution plan returned by the LLM.

    :param groups: All SQL check groups.  Together, the checks inside every group
        must cover every key in the user's ``prompts`` dict exactly once.
    :param plan_hash: SHA-256 fingerprint of the prompt dict (set by the operator
        after generation — not populated by the LLM).
    """

    groups: list[DQCheckGroup] = Field(
        description="All SQL check groups. Together, the checks inside every group must cover every key in the user's prompts dict exactly once."
    )
    plan_hash: str = Field(
        default="",
        description="SHA-256 fingerprint of the prompt dict (set by the operator after generation — not populated by the LLM).",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def check_names(self) -> list[str]:
        """Flat list of all ``check_name`` values across every group."""
        return [check.check_name for group in self.groups for check in group.checks]


@dataclass
class DQCheckResult:
    """
    Result of executing and validating a single :class:`DQCheck`.

    :param check_name: Corresponds to the user prompt key.
    :param metric_key: SQL column alias whose value was extracted.
    :param value: Raw value returned by the database for this metric.
    :param passed: ``True`` if every applicable validator approved the value.
    :param failure_reason: Human-readable description of why validation failed,
        or ``None`` when ``passed`` is ``True``.
    """

    check_name: str
    metric_key: str
    value: Any
    passed: bool
    failure_reason: str | None = None


@dataclass
class DQReport:
    """
    Aggregated outcome of all data-quality checks in a single operator run.

    :param results: One :class:`DQCheckResult` per prompt key.
    :param passed: ``True`` only when every result passed.
    :param failure_summary: Multi-line string listing every failed check;
        empty when all checks pass.
    """

    results: list[DQCheckResult] = field(default_factory=list)
    passed: bool = True
    failure_summary: str = ""

    @classmethod
    def build(cls, results: list[DQCheckResult]) -> DQReport:
        """Construct a :class:`DQReport` from a list of individual results."""
        failed = [r for r in results if not r.passed]
        if not failed:
            return cls(results=results, passed=True, failure_summary="")

        lines = [f"Data quality checks failed ({len(failed)}/{len(results)}):"]
        for r in failed:
            reason = r.failure_reason or "validator returned False"
            lines.append(f"  - {r.check_name} (metric: {r.metric_key}, value: {r.value!r}): {reason}")
        return cls(results=results, passed=False, failure_summary="\n".join(lines))
