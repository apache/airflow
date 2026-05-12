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
"""Operator for running data-quality checks from natural language using an LLM agent."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.toolsets.dataquality.base import BaseDQToolset
from airflow.providers.common.ai.utils.dataquality.models import (
    DQCheckFailedError,
    DQCheckInput,
    DQPlan,
    DQReport,
)
from airflow.providers.common.ai.utils.logging import log_run_summary, wrap_toolsets_for_logging

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.sdk import Context

_DQ_SYSTEM_PROMPT = """\
You are a data-quality expert. Evaluate the user's data-quality checks against a \
live data source using the tools available to you.

WORKFLOW:
1. Call ``list_checks`` to read the user's quality expectations.
2. Call ``list_validators`` to see available validators (names, parameters, descriptions).
3. Use schema-discovery tools (``list_tables``, ``get_schema``) to explore the data source.
4. For each check:
   a. If the check has ``row_level: true`` (from ``list_checks``), follow the ROW-LEVEL CHECKS
      procedure below instead of writing an aggregate query.
   b. Otherwise, write a SELECT query that computes the relevant metric for this check.
   c. Optionally call ``check_query`` to validate SQL syntax before executing.
   d. Execute the query using the ``query`` tool.
   e. You MUST call ``apply_validator`` for EVERY check — never skip this step, even for
      fixed validators (``has_fixed_validator: true``).  Use ``validator_name: "fixed"`` for those.
5. Return a ``DQReport`` with one ``DQCheckResult`` per check — every check must appear exactly once.

SQL GENERATION RULES:
- Generate ONLY SELECT statements.  NEVER use INSERT, UPDATE, DELETE, DROP, TRUNCATE, or DDL.
- Begin each query with a SQL comment that names the check it serves:
    -- check: <check_name>
  Example: -- check: null_order_id
- For conditional counts, use CASE expressions (FILTER WHERE is not universally supported):
    CORRECT:   COUNT(CASE WHEN col IS NULL THEN 1 END)
    INCORRECT: COUNT(*) FILTER (WHERE col IS NULL)
- For null/invalid percentages:
    COUNT(CASE WHEN condition THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)
- For duplicate percentages:
    (COUNT(*) - COUNT(DISTINCT col)) * 1.0 / NULLIF(COUNT(*), 0)
- For float division, cast to avoid integer truncation:
    CAST(numerator AS DOUBLE) / CAST(denominator AS DOUBLE)
- Give each metric column a descriptive snake_case alias (e.g. ``null_email_pct``).

VALIDATOR SELECTION:
- ``list_validators`` returns each validator's name, parameter signature, and description.
  Read the parameter names and types before calling ``apply_validator``.
- Pass the required ``validator_args`` as a JSON object matching the parameter signature exactly.
- If a parameter has no default, it is REQUIRED — always include it in ``validator_args``.
  Example: ``null_pct_check`` requires ``max_pct`` — you must pass ``{"max_pct": <value>}``.
  If you are unsure of a threshold, use a safe default (e.g. ``0.05`` for percentage checks).
- NEVER pass empty ``{}`` args for a validator that has required parameters.
- For checks marked ``has_fixed_validator: true`` in ``list_checks``, call ``apply_validator``
  with ``validator_name: "fixed"`` — the pre-assigned validator runs automatically, no args needed.
- If no suitable validator exists, pass ``validator_name: "none"`` — the check passes by default
  but its metric value is still recorded.

DQREPORT OUTPUT RULES:
- ``check_name`` must exactly match the name returned by ``list_checks`` (no abbreviation).
- Each check must appear in the report exactly once.
- Set ``passed: false`` and provide a clear ``failure_reason`` for every failed check.
- Set ``metric_key`` to the SQL column alias you used for this check's metric value.
- Set ``sql_query`` to the exact SQL statement you executed for this check (including the leading comment).
- Set ``validator_info`` to ``{"name": "<validator_name>", "args": {<validator_args>}}`` when a validator
  was applied, or ``null`` when no validator was used (``validator_name: "none"``).

CHECK CATEGORIES (use to guide SQL and validator selection):
  null_check    — null / missing value counts or percentages
  uniqueness    — duplicate detection, cardinality checks
  validity      — regex / format / pattern matching on string columns
  numeric_range — range, bounds, or statistical checks on numeric columns
  row_count     — total row counts or existence checks
  string_format — length, encoding, whitespace, or character-set checks
  row_level     — per-row anomaly checks; validator receives a list, not an aggregate

ROW-LEVEL CHECKS (applies when ``list_checks`` returns ``row_level: true`` for a check):
- Do NOT write an aggregate query.  Write a plain ``SELECT <column> FROM <table>`` that
  returns one value per row — no GROUP BY, no COUNT, no CASE expressions.
  Example: -- check: customer_email_format\n  SELECT email FROM customers
- After executing, extract the column values into a Python list:
    value = [row["<column>"] for row in result["rows"]]
  where ``result`` is the JSON object returned by the ``query`` tool.
- Call ``apply_validator`` with ``value`` set to that list and ``validator_name: "fixed"``.
- The validator evaluates each item and returns a ``value`` payload containing a
    ``RowLevelResult`` summary (total/invalid/invalid_pct/sample_violations/sample_size).
- Set ``metric_key`` to the SQL column name (e.g. ``"email"``).
- Set ``value`` in the DQCheckResult to the ``value`` object returned by ``apply_validator``.
"""

_DQ_PLAN_SYSTEM_PROMPT = """\
You are a data-quality expert.  Your task is to plan *how* to evaluate each
data-quality check: write the SQL query and choose the validator for each check.
A human reviewer will inspect your plan before any SQL is executed.

WORKFLOW (PLANNING MODE — no SQL execution, no apply_validator):
1. Call ``list_checks`` to read the user's quality expectations.
2. Call ``list_validators`` to see available validators (names, parameters, descriptions).
3. Use schema-discovery tools (``list_tables``, ``get_schema``) to understand the data model.
4. For each check:
   a. If the check has ``row_level: true`` (from ``list_checks``), follow the ROW-LEVEL
      procedure below.  Otherwise, write an aggregate SELECT query for the metric.
   b. Optionally call ``check_query`` to validate your SQL syntax — but do NOT execute it.
   c. Select the appropriate validator name and arguments:
      - For ``has_fixed_validator: true`` checks: use ``validator_name: "fixed"``, empty args.
      - If no validator fits: use ``validator_name: "none"``.
5. Return a ``DQPlan`` with one ``DQCheckPlan`` per check — every check must appear exactly once.

IMPORTANT: The ``query`` tool is NOT available.  Do NOT try to execute SQL.
Do NOT call ``apply_validator`` — it is not available in planning mode.
SQL will be executed after the human reviewer approves the plan.

SQL GENERATION RULES:
- Generate ONLY SELECT statements.  NEVER use INSERT, UPDATE, DELETE, DROP, TRUNCATE, or DDL.
- Begin each query with a SQL comment that names the check it serves:
    -- check: <check_name>
- For conditional counts, use CASE expressions:
    COUNT(CASE WHEN col IS NULL THEN 1 END)
- For null/invalid percentages:
    COUNT(CASE WHEN condition THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)
- Give each metric column a descriptive snake_case alias (e.g. ``null_email_pct``).

ROW-LEVEL CHECKS (applies when ``list_checks`` returns ``row_level: true``):
- Do NOT write an aggregate query.  Write a plain ``SELECT <column> FROM <table>``.
- Set ``row_level: true`` in the DQCheckPlan output.
- Set ``metric_key`` to the column name (e.g. ``"email"``).

DQCheckPlan FIELDS:
- ``check_name``: exact name from ``list_checks`` (no abbreviation).
- ``sql_query``: the SQL statement to execute later (with leading ``-- check: <name>`` comment).
- ``metric_key``: the SQL column alias / name to read as the primary metric value.
- ``row_level``: ``true`` for row-level checks (SELECT returns one row per record), ``false`` for aggregate.
- ``validator_name``: ``"fixed"``, a registered validator name, or ``"none"``.
- ``validator_args``: kwargs for the validator factory, or ``{}`` for ``"fixed"``/``"none"``.

VALIDATOR ARGS RULES (critical — missing args cause hard failures in Phase 2):
- ``list_validators`` returns a ``parameters`` field for each validator showing its exact
  parameter names, types, and defaults.  Read this carefully before setting ``validator_args``.
- If a parameter has no default (required), you MUST include it in ``validator_args``.
  Example: ``null_pct_check`` requires ``max_pct`` — always set ``{"max_pct": <value>}``.
- If you are unsure of the right threshold, pick a reasonable default (e.g. 0.05 for pct checks).
- NEVER leave ``validator_args`` as ``{}`` for a validator that has required parameters.
- Only use ``{}`` when ``validator_name`` is ``"fixed"`` or ``"none"``.
"""

_DIALECT_SQL_NOTES: dict[str, str] = {
    "postgres": (
        "  - Regex match: `col ~ 'pattern'`; not match: `col !~ 'pattern'` (case-sensitive).\n"
        "    Case-insensitive variants: `~*` and `!~*`.\n"
        "  - Float division: `numerator * 1.0 / NULLIF(denominator, 0)` (no CAST needed).\n"
        "  - String functions: LENGTH(), LOWER(), TRIM(), SUBSTRING().\n"
    ),
    "mysql": (
        "  - Regex match: `col REGEXP 'pattern'`; not match: `col NOT REGEXP 'pattern'`.\n"
        "  - Float division: `numerator * 1.0 / NULLIF(denominator, 0)`.\n"
        "  - String length (multibyte-safe): CHAR_LENGTH(col); byte length: LENGTH(col).\n"
    ),
    "sqlite": (
        "  - No native regex operator; use LIKE or GLOB:\n"
        "    `col NOT LIKE '%@%'` or `col GLOB '*@*.*'`.\n"
        "  - Float division: `CAST(numerator AS REAL) / NULLIF(denominator, 0)`.\n"
    ),
    "tsql": (
        "  - No regex operator; use LIKE or PATINDEX:\n"
        "    `PATINDEX('%@%.%', col) = 0` means no match.\n"
        "  - Float division: `CAST(numerator AS FLOAT) / NULLIF(denominator, 0)`.\n"
        "  - Use TOP n instead of LIMIT n; no OFFSET without ORDER BY.\n"
        "  - String length: LEN(col) (not LENGTH).\n"
    ),
    "bigquery": (
        "  - Regex: `REGEXP_CONTAINS(col, r'pattern')` — returns TRUE if match.\n"
        "    Negate with `NOT REGEXP_CONTAINS(...)`.\n"
        "  - Safe division: `SAFE_DIVIDE(numerator, denominator)` (NULL on zero denominator).\n"
        "  - Quote reserved names with backticks.\n"
    ),
    "snowflake": (
        "  - Regex: `REGEXP_LIKE(col, 'pattern')` (full-string match) or `col RLIKE 'pattern'`.\n"
        "  - Float division: `numerator * 1.0 / NULLIF(denominator, 0)`.\n"
        "  - Unquoted identifiers are uppercased; use double-quotes when case matters.\n"
    ),
}


def _extract_schema_table_names(schema_context: str) -> list[str]:
    """Extract table names from a schema context string produced by build_schema_context."""
    return re.findall(r"^Table:\s+(\S+)", schema_context, re.MULTILINE)


class LLMDataQualityOperator(LLMOperator):
    """
    Run data-quality checks described in natural language using an LLM agent.

    The agent discovers the database schema, writes and executes SQL queries,
    applies validators, and produces a
    :class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport`.  The task
    fails when any check does not pass, gating downstream tasks on data quality.

    Supply the data-source toolset and DQ toolset together in ``toolsets``::

        from airflow.providers.common.ai.toolsets.sql import SQLToolset
        from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset

        LLMDataQualityOperator(
            task_id="quality_check",
            checks=[
                DQCheckInput(name="email_nulls", description="Check for null emails"),
                DQCheckInput(
                    name="row_count",
                    description="At least 1000 rows",
                    validator=row_count_check(min_count=1000),
                ),
            ],
            llm_conn_id="pydanticai_default",
            toolsets=[
                SQLToolset(db_conn_id="postgres_default", allowed_tables=["customers"]),
                SQLDQToolset(),
            ],
        )

    When ``toolsets`` is omitted, the operator auto-creates
    :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
    :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset` from
    ``db_conn_id`` and ``table_names``.

    For config-generation backends (e.g. ``SodaDQToolset``), the operator
    returns the generated config string as its XCom value instead of a report.

    :param checks: List of :class:`~airflow.providers.common.ai.utils.dataquality.models.DQCheckInput`
        objects (or plain dicts with ``name``, ``description``, and optional
        ``validator`` keys).  Names must be unique.
    :param toolsets: Pydantic-AI toolsets for the agent.  Must include exactly
        one :class:`~airflow.providers.common.ai.toolsets.dataquality.base.BaseDQToolset`
        subclass alongside a data-source toolset.  When ``None``, the operator
        auto-creates toolsets from ``db_conn_id``.
    :param db_conn_id: Connection ID for auto-creating an
        :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset`.
        Ignored when ``toolsets`` is provided.
    :param table_names: Tables passed to the auto-created
        :class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` as
        ``allowed_tables``.  Ignored when ``toolsets`` is provided.
    :param schema_context: Additional schema description injected into the
        system prompt.  Useful when the data source cannot be introspected
        at runtime.
    """

    template_fields: Sequence[str] = (
        *LLMOperator.template_fields,
        "checks",
        "db_conn_id",
        "table_names",
        "schema_context",
    )

    def __init__(
        self,
        *,
        checks: list[DQCheckInput | dict[str, Any]],
        toolsets: list[AbstractToolset] | None = None,
        db_conn_id: str | None = None,
        table_names: list[str] | None = None,
        schema_context: str | None = None,
        **kwargs: Any,
    ) -> None:
        # Pop operator-specific params that must not reach BaseOperator.__init__.
        # Using kwargs.pop (rather than named params) is necessary because Airflow's
        # apply_defaults metaclass captures the full **kwargs dict and uses it for
        # task-map serialization; named params in the dict are NOT removed from the
        # snapshot, so they would be re-injected on deserialization and reach
        # BaseOperator as unknown kwargs.
        durable: bool = kwargs.pop("durable", False)

        kwargs.pop("output_type", None)
        kwargs.setdefault("prompt", "Run the data-quality checks.")
        super().__init__(**kwargs)

        self.checks: list[DQCheckInput] = (
            [DQCheckInput.coerce(c) for c in checks] if isinstance(checks, list) else checks  # type: ignore[assignment]
        )
        self.toolsets = toolsets
        self.db_conn_id = db_conn_id
        self.table_names = table_names
        self.schema_context = schema_context
        self.durable = durable

        self._validate_checks()

        if not toolsets and db_conn_id is None:
            raise ValueError("Either toolsets or db_conn_id must be provided.")

    def execute(self, context: Context) -> Any:
        """
        Run the LLM agent to execute all data-quality checks.

        When ``require_approval=True``, the task runs in two phases:

        1. **Phase 1** — the LLM discovers schema, executes SQL queries, and selects
           validators (but does *not* call ``apply_validator``).  The resulting plan
           (SQL queries + validator choices) is shown to a human reviewer.
        2. **Phase 2** (in :meth:`execute_complete`) — after approval, validators are
           applied in pure Python and the :class:`~...DQReport` is produced.

        :returns: :class:`~airflow.providers.common.ai.utils.dataquality.models.DQReport`
            as a dict when the DQ toolset is in ``"execute"`` mode, or a config
            string when in ``"generate"`` mode.
        :raises DQCheckFailedError: When any check fails in ``"execute"`` mode.
        """
        if self.require_approval:
            plan, _ = self._run_plan_phase(context)
            self.defer_for_approval(  # type: ignore[misc]
                context,
                plan.model_dump_json(),
                subject=f"Review DQ plan for task `{self.task_id}`",
                body=self._build_plan_approval_body(plan),
            )
        return self._run_and_report(context)

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> Any:
        """
        Phase 2: execute SQL and apply validators after human approval.

        Called automatically by Airflow when the HITL trigger fires.  The base
        class validates the approval decision (raises on reject or timeout).
        Then each SQL query from the approved :class:`~...DQPlan` is executed in
        pure Python, the metric values are extracted, and validators are applied
        — no LLM calls.

        :param context: Airflow task context.
        :param generated_output: The :class:`~...DQPlan` JSON that was deferred.
        :param event: Trigger event payload.
        :returns: Same as :meth:`execute`.
        :raises HITLRejectException: If the reviewer rejected the plan.
        """
        approved_output = super().execute_complete(context, generated_output, event)  # type: ignore[misc]
        plan = DQPlan.model_validate_json(approved_output)

        toolsets = self._resolve_toolsets()
        dq_toolset = self._find_dq_toolset(toolsets)
        dq_toolset.set_checks(self.checks)

        report = self._execute_plan_validators(plan, dq_toolset, toolsets)
        context["task_instance"].xcom_push(key="dq_report", value=report.model_dump())
        if not report.passed:
            raise DQCheckFailedError(report.failure_summary)
        return report.model_dump()

    # ------------------------------------------------------------------
    # Core execution helpers
    # ------------------------------------------------------------------

    def _run_plan_phase(self, context: Context) -> tuple[DQPlan, BaseDQToolset]:
        """
        Phase 1 of the two-phase approval flow.

        Runs the LLM agent in planning mode: schema-discovery tools
        (``list_tables``, ``get_schema``, ``check_query``) are available, but
        the ``query`` and ``apply_validator`` tools are hidden.  The agent
        writes SQL strings and selects validators for each check, then outputs
        a :class:`~...DQPlan` — without executing any SQL.

        :returns: ``(plan, dq_toolset)`` — the plan for the approval body and the
            toolset (already configured with checks) for Phase 2.
        """
        toolsets = self._resolve_toolsets()
        dq_toolset = self._find_dq_toolset(toolsets)
        dq_toolset._planning_mode = True  # omit apply_validator from tool list
        dq_toolset.set_checks(self.checks)

        # Hide the SQL 'query' tool so the LLM cannot execute DQ queries.
        for ts in toolsets:
            if ts is not dq_toolset and hasattr(ts, "_query"):
                ts._planning_mode = True  # type: ignore[attr-defined]

        instructions = self._build_plan_system_prompt(toolsets)
        logged_toolsets = wrap_toolsets_for_logging(toolsets, self.log)

        if self.durable:
            agent, counter, _ = self._build_durable_agent(DQPlan, instructions, logged_toolsets)
        else:
            agent = self.llm_hook.create_agent(
                output_type=DQPlan,
                instructions=instructions,
                toolsets=logged_toolsets,
                **self.agent_params,
            )
            counter = None

        result = agent.run_sync(self.prompt, usage_limits=self.usage_limits)
        log_run_summary(self.log, result)

        if counter is not None and (counter.replayed_model > 0 or counter.replayed_tool > 0):
            self.log.info(
                "Durable cache replay (plan phase): model_steps=%d/%d, tool_steps=%d/%d",
                counter.replayed_model,
                counter.replayed_model + counter.cached_model,
                counter.replayed_tool,
                counter.replayed_tool + counter.cached_tool,
            )

        return result.output, dq_toolset

    def _run_and_report(self, context: Context) -> Any:
        """
        Resolve toolsets, run the LLM agent, push XCom, and handle failures.

        This is the single place where the agent executes regardless of whether
        the task took the direct path or the approval-gated path.
        """
        toolsets = self._resolve_toolsets()
        dq_toolset = self._find_dq_toolset(toolsets)
        dq_toolset.set_checks(self.checks)

        output_type: type = DQReport if dq_toolset.output_mode == "execute" else str
        instructions = self._build_system_prompt(dq_toolset, toolsets)
        logged_toolsets = wrap_toolsets_for_logging(toolsets, self.log)

        if self.durable:
            agent, counter, _storage = self._build_durable_agent(output_type, instructions, logged_toolsets)
        else:
            agent = self.llm_hook.create_agent(
                output_type=output_type,
                instructions=instructions,
                toolsets=logged_toolsets,
                **self.agent_params,
            )
            counter = None

        result = agent.run_sync(self.prompt, usage_limits=self.usage_limits)
        log_run_summary(self.log, result)

        if counter is not None and (counter.replayed_model > 0 or counter.replayed_tool > 0):
            self.log.info(
                "Durable cache replay: model_steps=%d/%d, tool_steps=%d/%d",
                counter.replayed_model,
                counter.replayed_model + counter.cached_model,
                counter.replayed_tool,
                counter.replayed_tool + counter.cached_tool,
            )

        output = result.output

        if dq_toolset.output_mode == "execute":
            report: DQReport = output
            context["task_instance"].xcom_push(key="dq_report", value=report.model_dump())
            if not report.passed:
                raise DQCheckFailedError(report.failure_summary)
            return report.model_dump()

        return output

    def _build_durable_agent(
        self,
        output_type: type,
        instructions: str,
        toolsets: list[AbstractToolset],
    ) -> tuple[Any, Any, Any]:
        """Build a pydantic-ai Agent with CachingModel and CachingToolset wrappers."""
        from pydantic_ai import Agent

        from airflow.providers.common.ai.durable.caching_model import CachingModel
        from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
        from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
        from airflow.providers.common.ai.durable.storage import DurableStorage

        plan_hash = self._compute_plan_hash(instructions)
        storage = DurableStorage(cache_id=plan_hash)
        counter = DurableStepCounter()

        wrapped_model = CachingModel(self.llm_hook.get_conn(), storage=storage, counter=counter)
        cached_toolsets = [CachingToolset(ts, storage=storage, counter=counter) for ts in toolsets]

        agent = Agent(
            wrapped_model,
            output_type=output_type,
            instructions=instructions,
            toolsets=cached_toolsets,
            **self.agent_params,
        )
        return agent, counter, storage

    def _compute_plan_hash(self, instructions: str) -> str:
        """Return a stable short hash of the check plan for cross-run cache keying."""
        checks_data = sorted(
            [{"name": c.name, "description": c.description} for c in self.checks],
            key=lambda x: x["name"],
        )
        payload = json.dumps(
            {
                "checks": checks_data,
                "schema_context": self.schema_context or "",
                "instructions": instructions,
            },
            sort_keys=True,
        )
        digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
        return f"dq_{digest}"

    def _build_plan_approval_body(self, plan: DQPlan) -> str:
        """
        Build a Markdown body for the HITL approval form.

        Shows the LLM-generated SQL queries and validator selections so the
        reviewer can inspect the plan before any SQL is executed or any
        validator is applied.
        """
        lines = [
            "## Data Quality Check Plan \u2014 Awaiting Approval",
            "",
            f"**Total checks:** {len(plan.checks)}",
            "",
            "Review the SQL queries and validator selections below.  ",
            "**Approve** to execute the SQL, apply validators, and produce the quality report.  ",
            "**Reject** to cancel without executing anything.",
            "",
            "| # | Check | Metric Key | Row Level | Validator | Args |",
            "|---|-------|------------|-----------|-----------|------|",
        ]
        for i, cp in enumerate(plan.checks, 1):
            args_display = json.dumps(cp.validator_args) if cp.validator_args else "{}"
            lines.append(
                f"| {i} | `{cp.check_name}` | `{cp.metric_key}` "
                f"| {'Yes' if cp.row_level else 'No'} "
                f"| `{cp.validator_name}` | `{args_display}` |"
            )

        lines += ["", "---", "", "### SQL Queries", ""]
        for cp in plan.checks:
            lines += [f"**{cp.check_name}**", "", "```sql", cp.sql_query.strip(), "```", ""]

        return "\n".join(lines)

    def _execute_plan_validators(
        self,
        plan: DQPlan,
        dq_toolset: BaseDQToolset,
        toolsets: list[AbstractToolset],
    ) -> DQReport:
        """
        Phase 2 of the two-phase approval flow.

        For each check in the approved :class:`~...DQPlan`:

        1. Executes the SQL query via the data-source toolset.
        2. Extracts the metric value from the result.
        3. Applies the chosen validator in pure Python (no LLM calls).
        4. Builds the final :class:`~...DQReport`.

        :param plan: The approved :class:`~...DQPlan` from Phase 1.
        :param dq_toolset: The DQ toolset (must implement ``_apply_validator``).
        :param toolsets: All toolsets (used to find the SQL toolset for execution).
        :raises ValueError: If no SQL-capable toolset or no validator executor found.
        """
        from airflow.providers.common.ai.utils.dataquality.models import DQCheckResult

        sql_toolset = next((ts for ts in toolsets if hasattr(ts, "_query")), None)
        if sql_toolset is None:
            raise ValueError(
                "require_approval Phase 2 requires a toolset with a _query() method "
                "(e.g. SQLToolset). Add a data-source toolset to the toolsets list."
            )

        apply_fn = getattr(dq_toolset, "_apply_validator", None)
        if apply_fn is None:
            raise ValueError(
                "require_approval two-phase execution requires a DQ toolset that "
                "implements _apply_validator (e.g. SQLDQToolset)."
            )

        results: list[DQCheckResult] = []
        for cp in plan.checks:
            try:
                raw_result = sql_toolset._query(cp.sql_query)  # type: ignore[union-attr]
                result_data = json.loads(raw_result)
                rows: list[dict[str, Any]] = result_data.get("rows", [])
            except Exception as exc:
                results.append(
                    DQCheckResult(
                        check_name=cp.check_name,
                        passed=False,
                        failure_reason=f"SQL execution failed: {exc}",
                        sql_query=cp.sql_query,
                        validator_info={"name": cp.validator_name, "args": cp.validator_args},
                    )
                )
                continue

            if cp.row_level:
                metric_value: Any = [row.get(cp.metric_key) for row in rows]
            else:
                metric_value = rows[0].get(cp.metric_key) if rows else None

            raw = apply_fn(cp.check_name, metric_value, cp.validator_name, cp.validator_args)
            parsed = json.loads(raw)
            passed = bool(parsed.get("passed", False))
            result_value = parsed.get("value", metric_value)
            results.append(
                DQCheckResult(
                    check_name=cp.check_name,
                    passed=passed,
                    value=result_value,
                    failure_reason=parsed.get("reason") if not passed else None,
                    metric_key=cp.metric_key,
                    sql_query=cp.sql_query,
                    validator_info={"name": cp.validator_name, "args": cp.validator_args},
                )
            )

        return DQReport.build(results)

    # ------------------------------------------------------------------
    # Toolset / prompt helpers
    # ------------------------------------------------------------------

    def _resolve_toolsets(self) -> list[AbstractToolset]:
        """
        Return explicit toolsets or auto-create from ``db_conn_id``.

        When ``toolsets`` is provided but contains no
        :class:`BaseDQToolset`, a default
        :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset`
        is appended automatically.
        """
        if self.toolsets:
            has_dq = any(isinstance(ts, BaseDQToolset) for ts in self.toolsets)
            if not has_dq:
                from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset

                self.toolsets.append(SQLDQToolset())
            return self.toolsets

        from airflow.providers.common.ai.toolsets.dataquality.sql import SQLDQToolset
        from airflow.providers.common.ai.toolsets.sql import SQLToolset

        return [
            SQLToolset(db_conn_id=self.db_conn_id, allowed_tables=self.table_names),  # type: ignore[arg-type]
            SQLDQToolset(),
        ]

    @staticmethod
    def _find_dq_toolset(toolsets: list[AbstractToolset]) -> BaseDQToolset:
        """Return the first :class:`BaseDQToolset` found in *toolsets*."""
        for toolset in toolsets:
            if isinstance(toolset, BaseDQToolset):
                return toolset
        raise ValueError(
            "No BaseDQToolset found in toolsets. "
            "Add SQLDQToolset (or another BaseDQToolset subclass) to the toolsets list."
        )

    def _build_system_prompt(self, dq_toolset: BaseDQToolset, toolsets: list[AbstractToolset]) -> str:
        """Return the full DQ system prompt for the normal (non-planning) execution path."""
        prompt = self._make_prompt(_DQ_SYSTEM_PROMPT, toolsets)

        fixed_checks = [c.name for c in self.checks if c.validator is not None]
        if fixed_checks:
            names = ", ".join(f'"{n}"' for n in fixed_checks)
            prompt += (
                "\nFIXED VALIDATORS:\n"
                f"  Checks {names} have pre-assigned validators.\n"
                '  For these, call apply_validator with validator_name="fixed" — '
                "the system uses the pre-configured validator automatically.\n"
            )

        if self.system_prompt:
            prompt += f"\nAdditional instructions:\n{self.system_prompt}\n"

        return prompt

    def _build_plan_system_prompt(self, toolsets: list[AbstractToolset]) -> str:
        """Return the full DQ system prompt for Phase 1 (planning mode, no apply_validator)."""
        prompt = self._make_prompt(_DQ_PLAN_SYSTEM_PROMPT, toolsets)

        # In planning mode the FIXED VALIDATORS section uses different wording:
        # the LLM records "fixed" in the DQPlan instead of calling apply_validator.
        fixed_checks = [c.name for c in self.checks if c.validator is not None]
        if fixed_checks:
            names = ", ".join(f'"{n}"' for n in fixed_checks)
            prompt += (
                "\nFIXED VALIDATORS:\n"
                f"  Checks {names} have pre-assigned validators.\n"
                '  For these, set validator_name: "fixed" and validator_args: {} '
                "in your DQCheckPlan output.\n"
            )

        if self.system_prompt:
            prompt += f"\nAdditional instructions:\n{self.system_prompt}\n"

        return prompt

    def _make_prompt(self, base: str, toolsets: list[AbstractToolset]) -> str:
        """Inject dialect, schema context, and fixed-validator sections into *base*."""
        prompt = base

        dialect = self._detect_sql_dialect(toolsets)
        if dialect:
            notes = _DIALECT_SQL_NOTES.get(dialect, "")
            dialect_section = f"\nSQL DIALECT: {dialect.upper()}\n"
            if notes:
                dialect_section += "  Adapt your SQL to the following dialect-specific rules:\n" + notes
            prompt += dialect_section

        if self.schema_context:
            table_names = _extract_schema_table_names(self.schema_context)
            if table_names:
                prompt += (
                    "\nTABLE NAME CONSTRAINT:\n"
                    f"  The ONLY tables you may reference in FROM clauses are: {', '.join(table_names)}.\n"
                    "  Use these exact names — do not rename, abbreviate, or invent new table names.\n"
                )
            prompt += f"\nSchema context:\n{self.schema_context}\n"

        return prompt

    @staticmethod
    def _detect_sql_dialect(toolsets: list[AbstractToolset]) -> str | None:
        """Return the sqlglot dialect of the first dialect-aware toolset found."""
        for toolset in toolsets:
            dialect = getattr(toolset, "sqlglot_dialect", None)
            if dialect:
                return dialect
        return None

    def _validate_checks(self) -> None:
        """Raise :class:`ValueError` when checks are empty or contain duplicates."""
        if not isinstance(self.checks, list):
            return
        if not self.checks:
            raise ValueError("checks must not be empty. Provide at least one DQCheckInput.")
        duplicates = sorted(name for name, cnt in Counter(c.name for c in self.checks).items() if cnt > 1)
        if duplicates:
            raise ValueError(
                f"checks contains duplicate name(s): {duplicates}. Each check name must be unique."
            )
