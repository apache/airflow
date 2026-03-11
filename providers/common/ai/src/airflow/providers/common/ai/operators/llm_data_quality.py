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
"""Operator for generating and executing data-quality checks from natural language using LLMs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.utils.db_schema import get_db_hook
from airflow.providers.common.ai.utils.dq_models import DQCheckResult, DQPlan, DQReport
from airflow.providers.common.ai.utils.dq_planner import SQLDQPlanner
from airflow.providers.common.compat.sdk import AirflowException, Variable

if TYPE_CHECKING:
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import Context

_PLAN_VARIABLE_PREFIX = "dq_plan_"
_PLAN_VARIABLE_KEY_MAX_LEN = 200  # stay well under Airflow Variable key length limit


class LLMDataQualityOperator(LLMOperator):
    """
    Generate and execute data-quality checks from natural language descriptions.

    Each entry in ``prompts`` describes **one** data-quality expectation.
    The LLM groups related checks into optimised SQL queries, executes them
    against the target database, and validates each metric against the
    corresponding entry in ``validators``.  The task fails if any check
    does not pass, gating downstream tasks on data quality.

    Generated SQL plans are cached in Airflow
    :class:`~airflow.models.variable.Variable` to avoid repeat LLM calls.
    Set ``dry_run=True`` to preview the plan without executing it — useful
    for pairing with :class:`~airflow.providers.standard.operators.hitl.ApprovalOperator`.

    :param prompts: Mapping of ``{check_name: natural_language_description}``.
        Each key must be unique.  Use one check per key; the operator enforces
        a strict one-key → one-check mapping.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-4o"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: Additional instructions appended to the planning prompt.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).
    :param db_conn_id: Connection ID for the database to run checks against.
        Must resolve to a :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`.
    :param table_names: Tables to include in the LLM's schema context.
    :param schema_context: Manual schema description; bypasses DB introspection.
    :param validators: Mapping of ``{check_name: callable}`` where each callable
        receives the raw metric value and returns ``True`` (pass) or ``False`` (fail).
        Keys must be a subset of ``prompts.keys()``.
        Use built-in factories from
        :mod:`~airflow.providers.common.ai.utils.dq_validation` or plain lambdas::

            from airflow.providers.common.ai.utils.dq_validation import null_pct_check

            validators = {
                "email_nulls": null_pct_check(max_pct=0.05),
                "row_check": lambda v: v >= 1000,
            }

    :param dialect: SQL dialect override (``postgres``, ``mysql``, etc.).
        Auto-detected from *db_conn_id* when not set.
    :param datasource_config: DataFusion datasource for object-storage schema.
    :param dry_run: When ``True``, generate and cache the plan but skip execution.
        Returns the serialised plan dict instead of a :class:`~airflow.providers.common.ai.utils.dq_models.DQReport`.
    :param prompt_version: Optional version tag included in the plan cache key.
        Bump this to invalidate cached plans when prompts change semantically
        without changing their text.
    """

    template_fields: Sequence[str] = (
        *LLMOperator.template_fields,
        "prompts",
        "db_conn_id",
        "table_names",
        "schema_context",
        "prompt_version",
    )

    def __init__(
        self,
        *,
        prompts: dict[str, str],
        db_conn_id: str | None = None,
        table_names: list[str] | None = None,
        schema_context: str | None = None,
        validators: dict[str, Callable[[Any], bool]] | None = None,
        dialect: str | None = None,
        datasource_config: DataSourceConfig | None = None,
        prompt_version: str | None = None,
        dry_run: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        kwargs.setdefault("prompt", "LLMDataQualityOperator")
        super().__init__(**kwargs)

        self.prompts = prompts
        self.db_conn_id = db_conn_id
        self.table_names = table_names
        self.schema_context = schema_context
        self.validators = validators or {}
        self.dialect = dialect
        self.datasource_config = datasource_config
        self.prompt_version = prompt_version
        self.dq_dry_run = dry_run
        self._plan_hash: str | None = None
        if isinstance(self.prompts, dict):
            self._plan_hash = _compute_plan_hash(self.prompts, self.prompt_version)

        self._validate_validator_keys()

    def execute(self, context: Context) -> str | dict[str, Any]:
        """
        Generate the DQ plan (or load from cache), optionally execute it, and validate results.

        :returns: Serialised :class:`~airflow.providers.common.ai.utils.dq_models.DQReport`
            dict on success, or a markdown preview string when ``dry_run=True``.
        :raises AirflowException: If any data-quality check fails threshold validation.
        """
        db_hook = self._resolve_db_hook()
        planner = SQLDQPlanner(
            llm_hook=self.llm_hook,
            db_hook=db_hook,
            dialect=self.dialect,
            datasource_config=self.datasource_config,
            system_prompt=self.system_prompt,
            agent_params=self.agent_params,
        )

        schema_ctx = planner.build_schema_context(
            table_names=self.table_names, schema_context=self.schema_context
        )

        self.log.info("Using schema context:\n%s", schema_ctx)

        plan = self._load_or_generate_plan(planner, schema_ctx)

        if self.dq_dry_run:
            self.log.info(
                "dry_run=True — skipping execution. Plan contains %d group(s), %d check(s).",
                len(plan.groups),
                len(plan.check_names),
            )
            for group in plan.groups:
                self.log.info(
                    "Group: %s\nChecks: %s\nSQL Query:\n%s\n",
                    group.group_id,
                    ", ".join(c.check_name for c in group.checks),
                    group.query,
                )
            return self._build_dry_run_markdown(plan)

        results_map = planner.execute_plan(plan)
        check_results = self._validate_results(results_map, plan)
        report = DQReport.build(check_results)

        if not report.passed:
            raise AirflowException(report.failure_summary)

        self.log.info("All %d data-quality check(s) passed.", len(report.results))
        return {
            "passed": report.passed,
            "results": [
                {
                    "check_name": r.check_name,
                    "metric_key": r.metric_key,
                    "value": r.value,
                    "passed": r.passed,
                    "failure_reason": r.failure_reason,
                }
                for r in report.results
            ],
        }

    def _build_dry_run_markdown(self, plan: DQPlan) -> str:
        """Build a markdown summary of grouped SQL checks for dry-run XCom output."""
        lines = [
            "# LLM Data Quality Dry Run",
            "",
            f"- Plan hash: {plan.plan_hash or 'N/A'}",
            f"- Groups: {len(plan.groups)}",
            f"- Checks: {len(plan.check_names)}",
            "",
        ]

        for group_index, group in enumerate(plan.groups, start=1):
            lines.extend(
                [
                    f"## Group {group_index}: {group.group_id}",
                    "",
                    "### Query",
                    "",
                    "```sql",
                    group.query,
                    "```",
                    "",
                    "### Checks",
                    "",
                ]
            )
            for check in group.checks:
                lines.append(f"- {check.check_name} ({check.metric_key})")
            lines.append("")

        return "\n".join(lines).rstrip()

    def _load_or_generate_plan(self, planner: SQLDQPlanner, schema_ctx: str) -> DQPlan:
        """Return a cached plan when available, otherwise generate and cache a new one."""
        if self._plan_hash is None:
            if not isinstance(self.prompts, dict):
                raise TypeError("prompts must be a dict[str, str] before generating a DQ plan.")
            self._plan_hash = _compute_plan_hash(self.prompts, self.prompt_version)

        plan_hash = self._plan_hash
        variable_key = f"{_PLAN_VARIABLE_PREFIX}{plan_hash}"

        cached_json = Variable.get(variable_key, default=None)
        if cached_json is not None:
            self.log.info("DQ plan cache hit — key: %r", variable_key)
            plan = DQPlan.model_validate_json(cached_json)
            if not plan.plan_hash:
                plan.plan_hash = plan_hash
            return plan

        self.log.info("DQ plan cache miss — generating via LLM (key: %r).", variable_key)
        plan = planner.generate_plan(self.prompts, schema_ctx)
        plan.plan_hash = plan_hash
        Variable.set(variable_key, plan.model_dump_json())
        return plan

    def _validate_results(
        self,
        results_map: dict[str, Any],
        plan: DQPlan,
    ) -> list[DQCheckResult]:
        """Apply validators to each metric value and return per-check results."""
        check_results: list[DQCheckResult] = []

        for group in plan.groups:
            for check in group.checks:
                value = results_map[check.check_name]
                validator = self.validators.get(check.check_name)

                passed = True
                failure_reason: str | None = None

                if validator is not None:
                    try:
                        passed = bool(validator(value))
                    except Exception as exc:
                        passed = False
                        failure_reason = str(exc)

                    if not passed and failure_reason is None:
                        failure_reason = f"{repr(validator)} returned False"

                check_results.append(
                    DQCheckResult(
                        check_name=check.check_name,
                        metric_key=check.metric_key,
                        value=value,
                        passed=passed,
                        failure_reason=failure_reason,
                    )
                )

        return check_results

    def _validate_validator_keys(self) -> None:
        """
        Raise :class:`ValueError` if any validator key is absent from *prompts*.

        Skips validation when *prompts* is not yet a dict — this happens when the
        operator is constructed via the ``@task.llm_dq`` decorator and *prompts*
        is set to ``SET_DURING_EXECUTION`` at init time.  The decorator calls this
        method again after the callable has populated ``self.prompts``.
        """
        if not isinstance(self.prompts, dict):
            return
        unknown = set(self.validators.keys()) - set(self.prompts.keys())
        if unknown:
            raise ValueError(
                f"validators contains keys that are not present in prompts: {sorted(unknown)}. "
                "Every validator key must correspond to a prompt key."
            )

    def _resolve_db_hook(self) -> DbApiHook | None:
        """Return a DbApiHook when *db_conn_id* is configured, or ``None``."""
        if not self.db_conn_id:
            return None
        return get_db_hook(self.db_conn_id)


def _compute_plan_hash(prompts: dict[str, str], prompt_version: str | None) -> str:
    """
    Return a short, stable hash of *prompts* and *prompt_version*.

    Sorted serialisation ensures the hash is order-independent.
    The result is prefixed with the version tag so cache keys are human-readable.
    """
    payload = json.dumps(sorted(prompts.items()), sort_keys=True)
    digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
    version_tag = prompt_version or "default"
    key = f"{version_tag}_{digest}"
    return key[:_PLAN_VARIABLE_KEY_MAX_LEN]
