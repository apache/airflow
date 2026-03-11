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
SQL-based data-quality plan generation and execution.

:class:`SQLDQPlanner` is the single entry-point for all SQL DQ logic.
It is deliberately kept separate from the operator so it can be unit-tested
without an Airflow context and later swapped for GEX/SODA planners without
touching the operator.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.common.ai.utils.sql_validation import (
        DEFAULT_ALLOWED_TYPES,
        SQLSafetyError,
        validate_sql as _validate_sql,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.utils.db_schema import build_schema_context, resolve_dialect
from airflow.providers.common.ai.utils.dq_models import DQCheckGroup, DQPlan

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelMessage

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
    from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)

_PLANNING_SYSTEM_PROMPT = """\
You are a data-quality SQL expert.

Given a set of named data-quality checks and a database schema, produce a \
DQPlan that minimises the number of SQL queries executed.

PRIMARY RULE — combine everything on the same table into ONE SELECT:
  All checks that query the same table MUST be merged into a single SELECT
  statement. Each check becomes one output column in that statement.

  CORRECT (one query for two checks on the same table):
    SELECT
      (COUNT(*) FILTER (WHERE email IS NULL) * 100.0 / COUNT(*)) AS null_email_pct,
      COUNT(*) FILTER (WHERE bathrooms < 0)                      AS invalid_bathrooms
    FROM customers

  WRONG (two separate queries for the same table):
    SELECT COUNT(*) FILTER (WHERE email IS NULL) AS null_email_count FROM customers
    SELECT COUNT(*) FILTER (WHERE bathrooms < 0) AS invalid_bathrooms FROM customers

GROUPING STRATEGY:
  Assign a group_id that describes the table being queried (e.g. "sales_data_checks").
  Only split into multiple groups when the checks genuinely require different tables
  or subqueries that cannot be expressed as columns of a single SELECT.

OUTPUT RULES:
  1. Each output column must be aliased to exactly the metric_key of its check.
     Example: ... AS null_email_pct
  2. Each check_name must exactly match the key in the prompts dict.
  3. metric_key values must be valid SQL column aliases (snake_case, no spaces).
  4. Generates only SELECT queries — no INSERT, UPDATE, DELETE, DROP, or DDL.
  5. Use {dialect} syntax.
  6. Each check must appear in exactly ONE group.
  7. Return a valid DQPlan object. No extra commentary.
"""


class SQLDQPlanner:
    """
    Generates and executes a SQL-based :class:`~airflow.providers.common.ai.utils.dq_models.DQPlan`.

    :param llm_hook: Hook used to call the LLM for plan generation.
    :param db_hook: Hook used to execute generated SQL against the database.
    :param dialect: SQL dialect forwarded to the LLM prompt and ``validate_sql``.
        Auto-detected from *db_hook* when ``None``.
    :param max_sql_retries: Maximum number of times a failing SQL group query is sent
        back to the LLM for correction before the error is re-raised.  Default ``2``.
    """

    def __init__(
        self,
        *,
        llm_hook: PydanticAIHook,
        db_hook: DbApiHook | None,
        dialect: str | None = None,
        max_sql_retries: int = 2,
        datasource_config: DataSourceConfig | None = None,
        system_prompt: str = "",
        agent_params: dict[str, Any] | None = None,
    ) -> None:
        self._llm_hook = llm_hook
        self._db_hook = db_hook
        self._datasource_config = datasource_config
        self._dialect = resolve_dialect(db_hook, dialect)
        self._max_sql_retries = max_sql_retries
        self._extra_system_prompt = system_prompt
        self._agent_params: dict[str, Any] = agent_params or {}
        # Populated by generate_plan; used by _retry_fix_group to continue the conversation.
        self._plan_agent: Agent[None, DQPlan] | None = None
        self._plan_all_messages: list[ModelMessage] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_schema_context(
        self,
        table_names: list[str] | None,
        schema_context: str | None,
    ) -> str:
        """
        Return a schema description string for inclusion in the LLM prompt.

        Delegates to :func:`~airflow.providers.common.ai.utils.db_schema.build_schema_context`.
        """
        return build_schema_context(
            db_hook=self._db_hook,
            table_names=table_names,
            schema_context=schema_context,
            datasource_config=self._datasource_config,
        )

    def generate_plan(self, prompts: dict[str, str], schema_context: str) -> DQPlan:
        """
        Ask the LLM to produce a :class:`~airflow.providers.common.ai.utils.dq_models.DQPlan`.

        The LLM receives the user prompts, schema context, and planning instructions
        as a structured-output call (``output_type=DQPlan``).  After generation the
        method verifies that the returned ``check_names`` exactly match
        ``prompts.keys()``.

        :param prompts: ``{check_name: natural_language_description}`` dict.
        :param schema_context: Schema description previously built via
            :meth:`build_schema_context`.
        :raises ValueError: If the LLM's plan does not cover every prompt key
            exactly once.
        """
        dialect_label = self._dialect or "SQL"
        system_prompt = _PLANNING_SYSTEM_PROMPT.format(dialect=dialect_label)

        if schema_context:
            system_prompt += f"\nAvailable schema:\n{schema_context}\n"

        if self._extra_system_prompt:
            system_prompt += f"\nAdditional instructions:\n{self._extra_system_prompt}\n"

        user_message = self._build_user_message(prompts)

        log.info("Using system prompt:\n%s", system_prompt)
        log.info("Using user message:\n%s", user_message)

        agent = self._llm_hook.create_agent(
            output_type=DQPlan, instructions=system_prompt, **self._agent_params
        )
        result = agent.run_sync(user_message)

        # Persist the agent and full conversation so execute_plan can continue
        # the same chat thread when asking for SQL corrections.
        self._plan_agent = agent
        self._plan_all_messages = result.all_messages()

        plan: DQPlan = result.output

        self._validate_plan_coverage(plan, prompts)
        return plan

    def execute_plan(self, plan: DQPlan) -> dict[str, Any]:
        """
        Execute every SQL group in *plan* and return a flat ``{check_name: value}`` map.

        Each group's query is safety-validated via
        :func:`~airflow.providers.common.ai.utils.sql_validation.validate_sql` before
        execution.  The first row of each result-set is used; each column corresponds
        to the ``metric_key`` of one :class:`~airflow.providers.common.ai.utils.dq_models.DQCheck`.

        :param plan: Plan produced by :meth:`generate_plan`.
        :raises ValueError: If neither *db_hook* nor *datasource_config* was supplied.
        :raises SQLSafetyError: If a generated query fails AST validation even after
            ``max_sql_retries`` LLM correction attempts.
        :raises ValueError: If a query result does not contain an expected metric column.
        """
        if self._db_hook is None and self._datasource_config is None:
            raise ValueError("Either db_conn_id or datasource_config is required to execute the DQ plan.")

        datafusion_engine: DataFusionEngine | None = None
        if self._db_hook is None:
            datafusion_engine = self._build_datafusion_engine()

        results: dict[str, Any] = {}

        for raw_group in plan.groups:
            group = self._validate_or_fix_group(raw_group)
            log.debug("Executing DQ group %r:\n%s", group.group_id, group.query)

            if datafusion_engine is not None:
                row = self._run_datafusion_group(datafusion_engine, group)
            else:
                row = self._run_db_group(group)

            for check in group.checks:
                if check.metric_key not in row:
                    raise ValueError(
                        f"Query for group {group.group_id!r} did not return "
                        f"column {check.metric_key!r} required by check {check.check_name!r}. "
                        f"Available columns: {list(row.keys())}"
                    )
                results[check.check_name] = row[check.metric_key]

        return results

    # ------------------------------------------------------------------
    # Private helpers — execution
    # ------------------------------------------------------------------

    def _build_datafusion_engine(self) -> DataFusionEngine:
        """
        Instantiate a DataFusionEngine and register *datasource_config*.

        Ensures the datasource table name is available to all group queries.

        :raises AirflowOptionalProviderFeatureException: When the ``datafusion`` package
            is not installed.
        """
        try:
            # Lazy load dependency for worker isolation and optional feature degradation
            from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(e)

        engine = DataFusionEngine()
        engine.register_datasource(self._datasource_config)  # type: ignore[arg-type]
        return engine

    def _run_datafusion_group(self, engine: DataFusionEngine, group: DQCheckGroup) -> dict[str, Any]:
        """
        Execute *group.query* via DataFusion and return the first row as a dict.

        :class:`~airflow.providers.common.sql.datafusion.engine.DataFusionEngine`
        returns column-oriented results ``{col: [val, ...]}``.  This method
        converts the first row to a flat ``{col: val}`` dict.
        """
        col_lists: dict[str, list[Any]] = engine.execute_query(group.query)
        if not col_lists or not any(col_lists.values()):
            return {}
        return {col: vals[0] for col, vals in col_lists.items() if vals}

    def _run_db_group(self, group: DQCheckGroup) -> dict[str, Any]:
        """Execute *group.query* via the relational DB hook and return the first row as a dict."""
        rows = self._db_hook.get_records(group.query)  # type: ignore[union-attr]
        if not rows:
            log.warning("Query for group %r returned no rows.", group.group_id)
            return {}

        metric_keys = [check.metric_key for check in group.checks]
        raw_row = rows[0]
        if isinstance(raw_row, dict):
            return raw_row

        if isinstance(raw_row, Sequence) and not isinstance(raw_row, str | bytes | bytearray):
            if len(raw_row) != len(metric_keys):
                raise ValueError(
                    f"Query for group {group.group_id!r} returned {len(raw_row)} value(s) but "
                    f"{len(metric_keys)} metric key(s) are required ({metric_keys})."
                )
            return dict(zip(metric_keys, raw_row, strict=True))

        raise ValueError(
            f"Unsupported row type from DbApiHook.get_records for group {group.group_id!r}: "
            f"{type(raw_row).__name__}. Expected dict or sequence."
        )

    def _validate_or_fix_group(self, group: DQCheckGroup) -> DQCheckGroup:
        """
        Validate *group*'s SQL query and return the (possibly corrected) group.

        If validation passes immediately, the original group is returned unchanged.
        On :class:`~airflow.providers.common.ai.utils.sql_validation.SQLSafetyError`,
        the failing query and the error message are sent back to the LLM as a
        follow-up message in the same conversation.  This repeats up to
        ``max_sql_retries`` times before re-raising.
        """
        try:
            _validate_sql(
                group.query,
                allowed_types=DEFAULT_ALLOWED_TYPES,
                dialect=self._dialect,
            )
            return group
        except SQLSafetyError as exc:
            return self._retry_fix_group(group, exc)

    def _retry_fix_group(self, group: DQCheckGroup, initial_error: SQLSafetyError) -> DQCheckGroup:
        """
        Ask the LLM to fix *group*'s query by continuing the planning conversation.

        Sends up to ``max_sql_retries`` correction requests.  Each message includes
        the failing query, the validation error, and a reminder of the SQL rules so
        the model has full context without needing to re-read the original prompt.
        """
        if self._plan_agent is None or self._plan_all_messages is None:
            raise initial_error

        dialect_label = self._dialect or "SQL"
        last_error: SQLSafetyError = initial_error
        current_group = group

        for attempt in range(1, self._max_sql_retries + 1):
            log.warning(
                "SQL validation failed for group %r (attempt %d/%d): %s",
                current_group.group_id,
                attempt,
                self._max_sql_retries,
                last_error,
            )

            fix_prompt = self._build_fix_prompt(
                group=current_group,
                last_error=last_error,
                dialect_label=dialect_label,
            )

            result = self._plan_agent.run_sync(
                fix_prompt,
                message_history=self._plan_all_messages,
            )
            self._plan_all_messages = result.all_messages()
            corrected_plan: DQPlan = result.output

            fixed_group = self._extract_group_by_id(
                corrected_plan=corrected_plan,
                target_group_id=current_group.group_id,
            )
            if fixed_group is None:
                log.warning(
                    "LLM did not return group %r in correction attempt %d — retrying.",
                    current_group.group_id,
                    attempt,
                )
                continue

            try:
                _validate_sql(
                    fixed_group.query,
                    allowed_types=DEFAULT_ALLOWED_TYPES,
                    dialect=self._dialect,
                )
                log.info(
                    "SQL for group %r corrected successfully on attempt %d.",
                    fixed_group.group_id,
                    attempt,
                )
                return fixed_group
            except SQLSafetyError as new_exc:
                last_error = new_exc
                current_group = fixed_group

        raise SQLSafetyError(
            f"SQL for group {group.group_id!r} could not be corrected after "
            f"{self._max_sql_retries} attempt(s). Last error: {last_error}"
        )

    @staticmethod
    def _build_fix_prompt(*, group: DQCheckGroup, last_error: SQLSafetyError, dialect_label: str) -> str:
        """Build the retry prompt used to ask the LLM for a corrected query."""
        return (
            f'The SQL query for group "{group.group_id}" failed safety validation.\n\n'
            f"Failing query:\n{group.query}\n\n"
            f"Validation error: {last_error}\n\n"
            f"Rules reminder:\n"
            f"- Generate only SELECT queries (no INSERT, UPDATE, DELETE, DROP, or DDL).\n"
            f"- Use {dialect_label} syntax.\n"
            f"- Keep the same check_name and metric_key values for every check in this group.\n\n"
            f"Please return a corrected DQPlan with a valid SELECT-only query for group "
            f'"{group.group_id}".'
        )

    @staticmethod
    def _extract_group_by_id(*, corrected_plan: DQPlan, target_group_id: str) -> DQCheckGroup | None:
        """Return a group from *corrected_plan* by group_id, or ``None`` when absent."""
        return next((group for group in corrected_plan.groups if group.group_id == target_group_id), None)

    @staticmethod
    def _build_user_message(prompts: dict[str, str]) -> str:
        """Format the prompts dict as a numbered list for the LLM."""
        lines = [
            "Generate a DQPlan for the following data-quality checks.\n",
            "IMPORTANT: All checks that query the same table MUST be combined into a "
            "single SELECT with one output column per check.\n",
            "Checks:",
        ]
        for check_name, description in prompts.items():
            lines.append(f'  - check_name="{check_name}": {description}')
        return "\n".join(lines)

    @staticmethod
    def _validate_plan_coverage(plan: DQPlan, prompts: dict[str, str]) -> None:
        """
        Raise :class:`ValueError` when the plan's ``check_names`` differ from ``prompts.keys()``.

        Surfaces both missing checks (LLM dropped a prompt) and extra checks
        (LLM hallucinated a check not in the prompts) in a single error message.
        """
        expected = set(prompts.keys())
        returned = set(plan.check_names)

        missing = expected - returned
        extra = returned - expected

        if missing or extra:
            parts: list[str] = ["LLM-generated DQPlan does not match the provided prompts."]
            if missing:
                parts.append(f"  Missing checks (in prompts but not in plan): {sorted(missing)}")
            if extra:
                parts.append(f"  Extra checks (in plan but not in prompts):   {sorted(extra)}")
            raise ValueError("\n".join(parts))
