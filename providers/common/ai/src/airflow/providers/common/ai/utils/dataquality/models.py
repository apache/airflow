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
"""Pydantic models for the LLM data quality operator and toolsets."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from airflow.providers.common.compat.sdk import AirflowException


class DQCheckInput:
    """
    A single user-supplied data-quality check definition.

    The LLM selects the appropriate validator automatically from the registry;
    supply *validator* only when you want to force a specific callable and bypass
    the LLM suggestion entirely.

    :param name: Unique check identifier.  Used as ``check_name`` throughout the
        results.
    :param description: Natural-language description of the expectation.  This is
        what the LLM reads to produce the SQL and select a validator.
    :param validator: Optional fixed validator callable.  When provided the LLM is
        **not** asked to suggest a validator for this check; the supplied callable is
        used directly.  Accepts plain lambdas or any factory-produced callable from
        :mod:`~airflow.providers.common.ai.utils.dataquality.validation`.
    """

    template_fields: tuple[str, ...] = ("name", "description")
    __slots__ = ("description", "name", "validator")

    def __init__(
        self,
        *,
        name: str,
        description: str,
        validator: Callable[[Any], bool] | None = None,
    ) -> None:
        if not name or not name.strip():
            raise ValueError("DQCheckInput.name must not be empty.")
        if not description or not description.strip():
            raise ValueError("DQCheckInput.description must not be empty.")
        self.name = name
        self.description = description
        self.validator = validator

    @classmethod
    def coerce(cls, value: Any) -> DQCheckInput:
        """
        Accept a :class:`DQCheckInput` instance or a plain dict and return a :class:`DQCheckInput`.

        Plain dicts must contain ``name`` and ``description`` keys; ``validator`` is optional.

        :raises TypeError: If *value* is neither a :class:`DQCheckInput` nor a ``dict``.
        :raises KeyError: If *value* is a dict missing required keys.
        """
        if isinstance(value, cls):
            return value
        if isinstance(value, dict):
            return cls(
                name=value["name"],
                description=value["description"],
                validator=value.get("validator"),
            )
        raise TypeError(
            f"Expected DQCheckInput or dict, got {type(value).__name__!r}. "
            "Each check must have 'name' and 'description' fields."
        )

    def __repr__(self) -> str:
        has_validator = self.validator is not None
        return (
            f"DQCheckInput(name={self.name!r}, description={self.description!r}, validator={has_validator})"
        )


class RowLevelResult(BaseModel):
    """Summary of row-level validator outcomes for a single check."""

    total: int
    invalid: int
    invalid_pct: float
    sample_violations: list[str] = Field(default_factory=list)
    sample_size: int = 0

    @classmethod
    def build(cls, invalid_values: list[Any], total: int, *, sample_limit: int = 20) -> RowLevelResult:
        """Build a :class:`RowLevelResult` from invalid values and total row count."""
        invalid = len(invalid_values)
        invalid_pct = (invalid / total) if total else 0.0
        sample_violations = [repr(v) for v in invalid_values[:sample_limit]]
        return cls(
            total=total,
            invalid=invalid,
            invalid_pct=invalid_pct,
            sample_violations=sample_violations,
            sample_size=len(sample_violations),
        )


class DQCheckResult(BaseModel):
    """
    Result of executing and validating a single data-quality check.

    This is a Pydantic model so it can be part of :class:`DQReport` when used
    as ``output_type`` on a pydantic-ai Agent.

    :param check_name: Corresponds to the user-supplied check name.
    :param passed: ``True`` if the validator approved the metric value.
    :param value: Metric value returned for this check, or ``None`` when not
        applicable (e.g. for config-generation backends).
    :param failure_reason: Human-readable description of why the check failed,
        or ``None`` when ``passed`` is ``True``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    check_name: str
    passed: bool
    value: Any = None
    failure_reason: str | None = None
    metric_key: str | None = None
    sql_query: str | None = None
    validator_info: dict[str, Any] | None = None


class DQReport(BaseModel):
    """
    Aggregated outcome of all data-quality checks in a single operator run.

    This is a Pydantic model so it can be used as ``output_type`` on a
    pydantic-ai Agent — the agent produces it as its final structured output.

    :param results: One :class:`DQCheckResult` per check.
    :param passed: ``True`` only when every result passed.
    :param failure_summary: Multi-line string listing every failed check;
        empty when all checks pass.
    """

    results: list[DQCheckResult] = Field(default_factory=list)
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
            metric_info = f", metric: {r.metric_key}" if r.metric_key else ""
            validator_part = ""
            if r.validator_info:
                vname = r.validator_info.get("name", "")
                vargs = r.validator_info.get("args", {})
                validator_part = f", validator: {vname}({vargs})" if vname else ""
            lines.append(f"  - {r.check_name} (value: {r.value!r}{metric_info}{validator_part}): {reason}")
        return cls(results=results, passed=False, failure_summary="\n".join(lines))


class DQCheckPlan(BaseModel):
    """
    LLM-generated execution plan for a single data-quality check.

    Produced during Phase 1 of the two-phase approval flow: the LLM uses schema
    discovery to write the SQL query and selects the appropriate validator, but
    does **not** execute the SQL or call ``apply_validator``.

    Phase 2 (after human approval) executes the SQL, collects the metric value,
    and runs the validator in pure Python.

    :param check_name: Exact name of the check (matches :attr:`DQCheckInput.name`).
    :param sql_query: The SQL statement the LLM generated (including the leading
        ``-- check: <name>`` comment).  Not yet executed at this stage.
    :param metric_key: SQL column alias / key to read from the query result as the
        primary metric value.
    :param row_level: ``True`` when the SQL returns one value per row (no
        aggregation) and the validator expects a list.  ``False`` for scalar
        aggregate queries.
    :param validator_name: Validator to apply — ``"fixed"`` for pre-assigned
        validators, a registered validator name, or ``"none"`` to skip validation.
    :param validator_args: Keyword arguments for the validator factory.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    check_name: str
    sql_query: str
    metric_key: str
    row_level: bool = False
    validator_name: str
    validator_args: dict[str, Any] = Field(default_factory=dict)


class DQPlan(BaseModel):
    """
    LLM-generated execution plan covering all data-quality checks.

    Produced by the LLM agent during Phase 1 of the two-phase approval flow.
    Contains one :class:`DQCheckPlan` per check.  After human approval, the
    operator executes Phase 2: applying each validator in pure Python.
    """

    checks: list[DQCheckPlan] = Field(default_factory=list)


class DQCheckFailedError(AirflowException):
    """Raised when one or more data-quality checks fail threshold validation."""
