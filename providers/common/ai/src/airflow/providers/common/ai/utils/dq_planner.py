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
from airflow.providers.common.ai.utils.dq_models import DQCheckGroup, DQPlan, UnexpectedResult

if TYPE_CHECKING:
    from pydantic_ai import Agent
    from pydantic_ai.messages import ModelMessage

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
    from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)

_MAX_CHECKS_PER_GROUP = 5

_PLANNING_SYSTEM_PROMPT = """\
You are a data-quality SQL expert.

Given a set of named data-quality checks and a database schema, produce a \
DQPlan that minimises the number of SQL queries while keeping each group \
focused and manageable.

GROUPING STRATEGY (multi-dimensional):
  Group checks by **(target_table, check_category)**.  Checks on the same table
  that belong to different categories MUST be in separate groups.

  Allowed check_category values (assign one per check based on its description):
    - null_check      — null / missing value counts or percentages
    - uniqueness      — duplicate detection, cardinality checks
    - validity        — regex / format / pattern matching on string columns
    - numeric_range   — range, bounds, or statistical checks on numeric columns
    - row_count       — total row counts or existence checks
    - string_format   — length, encoding, whitespace, or character-set checks

  MAX {max_checks_per_group} CHECKS PER GROUP:
    If a (table, category) pair has more than {max_checks_per_group} checks,
    split them into sub-groups of at most {max_checks_per_group}.

  GROUP-ID NAMING:
    Use the pattern "{{table}}_{{category}}_{{part}}".
    Examples: customers_null_check_1, orders_validity_1, orders_validity_2

  RATIONALE:
    Keeping string-column checks (validity, string_format) apart from
    numeric-column checks (numeric_range, null_check on numbers) produces
    simpler SQL and makes failures easier to diagnose.

  CORRECT (two groups for same table, different categories):
    Group customers_null_check_1:
      SELECT
        (COUNT(CASE WHEN email IS NULL THEN 1 END) * 100.0 / COUNT(*)) AS null_email_pct,
        (COUNT(CASE WHEN name IS NULL THEN 1 END) * 100.0 / COUNT(*)) AS null_name_pct
      FROM customers

    Group customers_validity_1:
      SELECT
        COUNT(CASE WHEN phone NOT LIKE '+___-___-____' THEN 1 END) AS invalid_phone_fmt
      FROM customers

  WRONG (mixing null-check and regex-validity in one group):
    SELECT
      (COUNT(CASE WHEN email IS NULL THEN 1 END) * 100.0 / COUNT(*)) AS null_email_pct,
      COUNT(CASE WHEN phone NOT LIKE '+___-___-____' THEN 1 END) AS invalid_phone_fmt
    FROM customers

OUTPUT RULES:
  1. Each output column must be aliased to exactly the metric_key of its check.
     Example: ... AS null_email_pct
  2. Each check_name must exactly match the key in the prompts dict.
  3. metric_key values must be valid SQL column aliases (snake_case, no spaces).
  4. Generates only SELECT queries — no INSERT, UPDATE, DELETE, DROP, or DDL.
  5. Use {dialect} syntax.
  6. Each check must appear in exactly ONE group.
  7. Each check must have a check_category from the allowed list above.
  8. Return a valid DQPlan object. No extra commentary.
"""

_DATAFUSION_SYNTAX_SECTION = """\

DATAFUSION SQL SYNTAX RULES:
  The target engine is Apache DataFusion.  Observe these syntax differences
  from standard PostgreSQL / ANSI SQL:

  1. NO "FILTER (WHERE ...)" clause.  Use CASE expressions instead:
       WRONG:  COUNT(*) FILTER (WHERE email IS NULL)
       RIGHT:  COUNT(CASE WHEN email IS NULL THEN 1 END)

  2. Regex matching uses the tilde operator:
       column ~ 'pattern'    (match)
       column !~ 'pattern'   (no match)
     Do NOT use SIMILAR TO or POSIX-style ~* (case-insensitive).

  3. CAST syntax — prefer CAST(expr AS type) over :: shorthand.

  4. String functions: Use CHAR_LENGTH (not LEN), SUBSTR (not SUBSTRING with FROM/FOR).

  5. Integer division: DataFusion performs integer division for INT/INT.
     Use CAST(expr AS DOUBLE) to force floating-point division.

  6. Boolean literals: Use TRUE / FALSE (not 1 / 0).

  7. LIMIT is supported.  OFFSET is supported.  FETCH FIRST is NOT supported.

  8. NULL handling: COALESCE, NULLIF, IFNULL are all supported.
     NVL and ISNULL are NOT supported.
"""

_UNEXPECTED_QUERY_PROMPT_SECTION = """\

UNEXPECTED VALUE COLLECTION:
  For checks whose check_category is "validity" or "string_format", also
  generate an unexpected_query field on the DQCheck.  This query must:
    - SELECT the primary key column(s) and the column(s) being validated
    - WHERE the row violates the check condition (the negation of the check)
    - LIMIT {sample_size}
    - Use {dialect} syntax
    - Be a standalone SELECT (not a subquery of the group query)

  For all other categories (null_check, uniqueness, numeric_range, row_count),
  set unexpected_query to null — these are aggregate checks where individual
  violating rows are not meaningful.

  Example for a phone-format validity check:
    unexpected_query: "SELECT id, phone FROM customers WHERE phone !~ '^\\d{{4}}-\\d{{4}}-\\d{{4}}$' LIMIT 100"
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
        collect_unexpected: bool = False,
        unexpected_sample_size: int = 100,
    ) -> None:
        self._llm_hook = llm_hook
        self._db_hook = db_hook
        self._datasource_config = datasource_config
        self._dialect = resolve_dialect(db_hook, dialect)
        # Track whether the execution target is DataFusion so the prompt can
        # include DataFusion-specific syntax rules.  The dialect stays None
        # (generic SQL) for sqlglot validation — sqlglot has no DataFusion dialect.
        self._is_datafusion = db_hook is None and datasource_config is not None
        # When targeting DataFusion, use PostgreSQL dialect for sqlglot validation
        # because DataFusion shares regex operators (~, !~) that the generic SQL
        # parser does not recognise.
        self._validation_dialect: str | None = "postgres" if self._is_datafusion else self._dialect
        self._max_sql_retries = max_sql_retries
        self._extra_system_prompt = system_prompt
        self._agent_params: dict[str, Any] = agent_params or {}
        self._collect_unexpected = collect_unexpected
        self._unexpected_sample_size = unexpected_sample_size
        # Populated by generate_plan; used by _retry_fix_group to continue the conversation.
        self._plan_agent: Agent[None, DQPlan] | None = None
        self._plan_all_messages: list[ModelMessage] | None = None

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
        dialect_label = self._dialect or ("DataFusion-compatible SQL" if self._is_datafusion else "SQL")
        system_prompt = _PLANNING_SYSTEM_PROMPT.format(
            dialect=dialect_label, max_checks_per_group=_MAX_CHECKS_PER_GROUP
        )

        if self._is_datafusion:
            system_prompt += _DATAFUSION_SYNTAX_SECTION

        if self._collect_unexpected:
            system_prompt += _UNEXPECTED_QUERY_PROMPT_SECTION.format(
                dialect=dialect_label, sample_size=self._unexpected_sample_size
            )

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
        self._validate_group_sizes(plan)
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
                dialect=self._validation_dialect,
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

        dialect_label = self._dialect or ("DataFusion-compatible SQL" if self._is_datafusion else "SQL")
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
                is_datafusion=self._is_datafusion,
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
                    dialect=self._validation_dialect,
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
    def _build_fix_prompt(
        *,
        group: DQCheckGroup,
        last_error: SQLSafetyError,
        dialect_label: str,
        is_datafusion: bool = False,
    ) -> str:
        """Build the retry prompt used to ask the LLM for a corrected query."""
        prompt = (
            f'The SQL query for group "{group.group_id}" failed safety validation.\n\n'
            f"Failing query:\n{group.query}\n\n"
            f"Validation error: {last_error}\n\n"
            f"Rules reminder:\n"
            f"- Generate only SELECT queries (no INSERT, UPDATE, DELETE, DROP, or DDL).\n"
            f"- Use {dialect_label} syntax.\n"
            f"- Keep the same check_name and metric_key values for every check in this group.\n"
        )
        if is_datafusion:
            prompt += (
                "- DataFusion does NOT support FILTER (WHERE ...). "
                "Use COUNT(CASE WHEN ... THEN 1 END) instead.\n"
                "- Use CAST(expr AS DOUBLE) for floating-point division.\n"
            )
        prompt += (
            f'\nPlease return a corrected DQPlan with a valid SELECT-only query for group "{group.group_id}".'
        )
        return prompt

    @staticmethod
    def _extract_group_by_id(*, corrected_plan: DQPlan, target_group_id: str) -> DQCheckGroup | None:
        """Return a group from *corrected_plan* by group_id, or ``None`` when absent."""
        return next((group for group in corrected_plan.groups if group.group_id == target_group_id), None)

    @staticmethod
    def _build_user_message(prompts: dict[str, str]) -> str:
        """Format the prompts dict as a numbered list for the LLM."""
        lines = [
            "Generate a DQPlan for the following data-quality checks.\n",
            "IMPORTANT: Group checks by (table, check_category). Max "
            f"{_MAX_CHECKS_PER_GROUP} checks per group — split into sub-groups if needed.\n",
            "Checks:",
        ]
        for check_name, description in prompts.items():
            lines.append(f'  - check_name="{check_name}": {description}')
        return "\n".join(lines)

    def execute_unexpected_queries(
        self, plan: DQPlan, failed_check_names: set[str]
    ) -> dict[str, UnexpectedResult]:
        """
        Execute unexpected-value queries for failed checks that have one.

        Only checks whose ``check_name`` is in *failed_check_names* **and** whose
        ``unexpected_query`` is not ``None`` are executed.  Each query is
        safety-validated before execution; if validation fails the check is
        skipped with a warning rather than failing the entire run.

        :param plan: Plan produced by :meth:`generate_plan`.
        :param failed_check_names: Set of check names that failed validation.
        :returns: ``{check_name: UnexpectedResult}`` for each successfully executed query.
        """
        if self._db_hook is None and self._datasource_config is None:
            log.warning("Cannot collect unexpected values — no db_hook or datasource_config.")
            return {}

        datafusion_engine: DataFusionEngine | None = None
        if self._db_hook is None:
            datafusion_engine = self._build_datafusion_engine()

        results: dict[str, UnexpectedResult] = {}

        for group in plan.groups:
            for check in group.checks:
                if check.check_name not in failed_check_names:
                    continue
                if not check.unexpected_query:
                    continue

                try:
                    _validate_sql(
                        check.unexpected_query,
                        allowed_types=DEFAULT_ALLOWED_TYPES,
                        dialect=self._validation_dialect,
                    )
                except SQLSafetyError:
                    log.warning(
                        "Unexpected query for check %r failed safety validation — skipping.",
                        check.check_name,
                    )
                    continue

                try:
                    rows = self._execute_unexpected_query(check.unexpected_query, datafusion_engine)
                except Exception:
                    log.exception("Unexpected query execution failed for check %r.", check.check_name)
                    continue

                results[check.check_name] = UnexpectedResult(
                    check_name=check.check_name,
                    unexpected_records=rows,
                    sample_size=self._unexpected_sample_size,
                )

        return results

    def _execute_unexpected_query(
        self,
        query: str,
        datafusion_engine: DataFusionEngine | None,
    ) -> list[str]:
        """Execute a single unexpected-value query and return violating values as strings."""
        if datafusion_engine is not None:
            col_lists: dict[str, list[Any]] = datafusion_engine.execute_query(query)
            if not col_lists:
                return []
            num_rows = max(len(v) for v in col_lists.values()) if col_lists else 0
            return [
                ", ".join(str(vals[i]) if i < len(vals) else "" for col, vals in col_lists.items())
                for i in range(num_rows)
            ]

        rows = self._db_hook.get_records(query)  # type: ignore[union-attr]
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return [", ".join(str(v) for v in row.values()) for row in rows]
        return [", ".join(str(v) for v in row) for row in rows]

    @staticmethod
    def _validate_group_sizes(plan: DQPlan) -> None:
        """
        Log a warning for any group that exceeds :data:`_MAX_CHECKS_PER_GROUP`.

        This is a safety net — the system prompt already instructs the LLM to
        split large groups.  A warning (rather than a hard failure) avoids
        breaking plans that are otherwise correct but slightly over the limit.
        """
        for group in plan.groups:
            if len(group.checks) > _MAX_CHECKS_PER_GROUP:
                log.warning(
                    "Group %r has %d checks (max recommended: %d). "
                    "Consider splitting into smaller groups for better diagnostics.",
                    group.group_id,
                    len(group.checks),
                    _MAX_CHECKS_PER_GROUP,
                )

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
