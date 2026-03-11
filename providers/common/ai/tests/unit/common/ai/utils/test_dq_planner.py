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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.utils.dq_models import DQCheck, DQCheckGroup, DQPlan
from airflow.providers.common.ai.utils.dq_planner import SQLDQPlanner
from airflow.providers.common.ai.utils.sql_validation import SQLSafetyError


def _make_plan(*check_names: str) -> DQPlan:
    """Helper: build a minimal DQPlan with one group per check."""
    groups = [
        DQCheckGroup(
            group_id="numeric_aggregate",
            query=f"SELECT COUNT(*) AS {name}_count FROM t",
            checks=[DQCheck(check_name=name, metric_key=f"{name}_count", group_id="numeric_aggregate")],
        )
        for name in check_names
    ]
    return DQPlan(groups=groups)


def _make_llm_hook(plan: DQPlan) -> MagicMock:
    """Helper: mock PydanticAIHook that returns *plan* from agent.run_sync."""
    mock_result = MagicMock(spec=["output", "all_messages"])
    mock_result.output = plan
    mock_result.all_messages.return_value = []
    mock_agent = MagicMock(spec=["run_sync"])
    mock_agent.run_sync.return_value = mock_result
    mock_hook = MagicMock(spec=PydanticAIHook)
    mock_hook.create_agent.return_value = mock_agent
    return mock_hook


class TestSQLDQPlannerBuildSchema:
    def test_returns_manual_schema_context_verbatim(self):
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=None)
        result = planner.build_schema_context(
            table_names=None,
            schema_context="Table: t\nColumns: id INT",
        )
        assert result == "Table: t\nColumns: id INT"

    def test_introspects_via_db_hook_when_no_manual_context(self):
        mock_db_hook = MagicMock()
        mock_db_hook.get_table_schema.return_value = [{"name": "id", "type": "INT"}]

        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        result = planner.build_schema_context(
            table_names=["customers"],
            schema_context=None,
        )

        mock_db_hook.get_table_schema.assert_called_once_with("customers")
        assert "customers" in result
        assert "id INT" in result

    def test_manual_context_takes_priority_over_db_hook(self):
        mock_db_hook = MagicMock()

        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        result = planner.build_schema_context(
            table_names=["t"],
            schema_context="manual override",
        )

        mock_db_hook.get_table_schema.assert_not_called()
        assert result == "manual override"

    def test_returns_empty_string_when_no_source(self):
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=None)
        result = planner.build_schema_context(
            table_names=None,
            schema_context=None,
        )
        assert result == ""


class TestSQLDQPlannerGeneratePlan:
    def test_returns_plan_when_check_names_match(self):
        prompts = {"null_emails": "check for null emails", "dup_ids": "check for duplicate ids"}
        plan = _make_plan("null_emails", "dup_ids")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        result = planner.generate_plan(prompts, schema_context="")

        assert set(result.check_names) == set(prompts.keys())

    def test_raises_when_llm_drops_a_check(self):
        prompts = {"null_emails": "...", "dup_ids": "..."}
        plan = _make_plan("null_emails")  # missing dup_ids
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        with pytest.raises(ValueError, match="dup_ids"):
            planner.generate_plan(prompts, schema_context="")

    def test_raises_when_llm_adds_extra_check(self):
        prompts = {"null_emails": "..."}
        plan = _make_plan("null_emails", "hallucinated_check")  # unexpected extra
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        with pytest.raises(ValueError, match="hallucinated_check"):
            planner.generate_plan(prompts, schema_context="")

    def test_agent_receives_schema_context_in_prompt(self):
        prompts = {"row_count": "count rows"}
        plan = _make_plan("row_count")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        planner.generate_plan(prompts, schema_context="Table: orders\nColumns: id INT")

        call_kwargs = mock_hook.create_agent.call_args
        instructions = call_kwargs.kwargs["instructions"]
        assert "orders" in instructions

    def test_extra_system_prompt_appended_to_instructions(self):
        prompts = {"row_count": "count rows"}
        plan = _make_plan("row_count")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(
            llm_hook=mock_hook, db_hook=None, system_prompt="Always use lowercase aliases."
        )
        planner.generate_plan(prompts, schema_context="")

        call_kwargs = mock_hook.create_agent.call_args
        instructions = call_kwargs.kwargs["instructions"]
        assert "Always use lowercase aliases." in instructions
        # Built-in planning prompt must still be present.
        assert "DQPlan" in instructions

    def test_empty_system_prompt_not_appended(self):
        """When system_prompt is empty (default), instructions must not contain 'Additional instructions'."""
        prompts = {"row_count": "count rows"}
        plan = _make_plan("row_count")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        planner.generate_plan(prompts, schema_context="")

        call_kwargs = mock_hook.create_agent.call_args
        instructions = call_kwargs.kwargs["instructions"]
        assert "Additional instructions" not in instructions

    def test_agent_params_forwarded_to_create_agent(self):
        prompts = {"row_count": "count rows"}
        plan = _make_plan("row_count")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None, agent_params={"retries": 3})
        planner.generate_plan(prompts, schema_context="")

        call_kwargs = mock_hook.create_agent.call_args
        assert call_kwargs.kwargs.get("retries") == 3

    def test_agent_params_empty_by_default(self):
        prompts = {"row_count": "count rows"}
        plan = _make_plan("row_count")
        mock_hook = _make_llm_hook(plan)

        planner = SQLDQPlanner(llm_hook=mock_hook, db_hook=None)
        planner.generate_plan(prompts, schema_context="")

        call_kwargs = mock_hook.create_agent.call_args
        # Only output_type and instructions should be present — no extra kwargs.
        extra = {k: v for k, v in call_kwargs.kwargs.items() if k not in ("output_type", "instructions")}
        assert extra == {}


class TestSQLDQPlannerExecutePlan:
    def test_returns_flat_check_name_to_value_map(self):
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"null_email_count": 5}]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="null_check",
                    query="SELECT COUNT(*) AS null_email_count FROM t WHERE email IS NULL",
                    checks=[
                        DQCheck(
                            check_name="null_emails",
                            metric_key="null_email_count",
                            group_id="null_check",
                        )
                    ],
                )
            ]
        )
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        results = planner.execute_plan(plan)

        assert results["null_emails"] == 5

    def test_raises_safety_error_for_unsafe_sql(self):
        mock_db_hook = MagicMock()
        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="DROP TABLE customers",
                    checks=[DQCheck(check_name="c", metric_key="c_val", group_id="g")],
                )
            ]
        )
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        with pytest.raises(SQLSafetyError):
            planner.execute_plan(plan)

        mock_db_hook.get_records.assert_not_called()

    def test_raises_when_metric_key_missing_from_result(self):
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"wrong_column": 0}]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="SELECT 0 AS wrong_column FROM t",
                    checks=[DQCheck(check_name="c", metric_key="expected_column", group_id="g")],
                )
            ]
        )
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        with pytest.raises(ValueError, match="expected_column"):
            planner.execute_plan(plan)

    def test_raises_when_db_hook_is_none(self):
        plan = _make_plan("some_check")
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=None)
        with pytest.raises(ValueError, match="db_conn_id|datasource_config"):
            planner.execute_plan(plan)

    def test_handles_tuple_rows_from_get_records(self):
        """get_records may return plain tuples; ensure positional mapping works."""
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [(42,)]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="SELECT COUNT(*) AS row_count FROM t",
                    checks=[DQCheck(check_name="rows", metric_key="row_count", group_id="g")],
                )
            ]
        )
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        results = planner.execute_plan(plan)
        assert results["rows"] == 42

    def test_raises_when_tuple_length_does_not_match_metric_keys(self):
        """Tuple-shaped rows must match the number of expected metric keys."""
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [(1, 2)]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="SELECT COUNT(*) AS row_count FROM t",
                    checks=[DQCheck(check_name="rows", metric_key="row_count", group_id="g")],
                )
            ]
        )

        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        with pytest.raises(ValueError, match=r"returned 2 value\(s\)"):
            planner.execute_plan(plan)

    def test_raises_for_unsupported_row_type(self):
        """Unexpected row types should fail fast with a clear error."""
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [1]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="SELECT COUNT(*) AS row_count FROM t",
                    checks=[DQCheck(check_name="rows", metric_key="row_count", group_id="g")],
                )
            ]
        )

        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        with pytest.raises(ValueError, match="Unsupported row type"):
            planner.execute_plan(plan)


class TestSQLDQPlannerDataFusion:
    """Tests for the DataFusion (object-storage) execution path."""

    def _simple_plan(self, metric_key: str = "null_count") -> DQPlan:
        return DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query=f"SELECT COUNT(*) AS {metric_key} FROM sales",
                    checks=[DQCheck(check_name="null_check", metric_key=metric_key, group_id="g")],
                )
            ]
        )

    def test_raises_when_neither_db_hook_nor_datasource(self):
        """Both db_hook and datasource_config are absent — must raise ValueError."""
        plan = _make_plan("some_check")
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=None)
        with pytest.raises(ValueError, match="db_conn_id|datasource_config"):
            planner.execute_plan(plan)

    @patch("airflow.providers.common.ai.utils.dq_planner.SQLDQPlanner._build_datafusion_engine")
    def test_datafusion_engine_built_when_no_db_hook(self, mock_build_engine):
        """_build_datafusion_engine is called when db_hook is None and datasource_config is set."""
        mock_engine = MagicMock()
        mock_engine.execute_query.return_value = {"null_count": [0]}
        mock_build_engine.return_value = mock_engine

        mock_datasource = MagicMock()
        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=None,
            datasource_config=mock_datasource,
        )
        planner.execute_plan(self._simple_plan())

        mock_build_engine.assert_called_once()
        mock_engine.execute_query.assert_called_once()

    @patch("airflow.providers.common.ai.utils.dq_planner.SQLDQPlanner._build_datafusion_engine")
    def test_datafusion_result_first_value_used(self, mock_build_engine):
        """Column-oriented result {col: [val, ...]} — first element is used as the metric value."""
        mock_engine = MagicMock()
        mock_engine.execute_query.return_value = {"null_count": [7, 99]}  # only first value counts
        mock_build_engine.return_value = mock_engine

        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=None,
            datasource_config=MagicMock(),
        )
        results = planner.execute_plan(self._simple_plan("null_count"))
        assert results["null_check"] == 7

    @patch("airflow.providers.common.ai.utils.dq_planner.SQLDQPlanner._build_datafusion_engine")
    def test_datafusion_empty_result_raises_metric_missing(self, mock_build_engine):
        """An empty DataFusion result triggers the metric-key-missing ValueError."""
        mock_engine = MagicMock()
        mock_engine.execute_query.return_value = {}  # no columns at all
        mock_build_engine.return_value = mock_engine

        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=None,
            datasource_config=MagicMock(),
        )
        with pytest.raises(ValueError, match="null_count"):
            planner.execute_plan(self._simple_plan("null_count"))

    @patch("airflow.providers.common.ai.utils.dq_planner.SQLDQPlanner._build_datafusion_engine")
    def test_datafusion_engine_built_once_for_multiple_groups(self, mock_build_engine):
        """DataFusion engine is instantiated once even when the plan has multiple groups."""
        mock_engine = MagicMock()
        mock_engine.execute_query.side_effect = [
            {"a_count": [3]},
            {"b_count": [0]},
        ]
        mock_build_engine.return_value = mock_engine

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g1",
                    query="SELECT COUNT(*) AS a_count FROM t",
                    checks=[DQCheck(check_name="check_a", metric_key="a_count", group_id="g1")],
                ),
                DQCheckGroup(
                    group_id="g2",
                    query="SELECT COUNT(*) AS b_count FROM t",
                    checks=[DQCheck(check_name="check_b", metric_key="b_count", group_id="g2")],
                ),
            ]
        )
        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=None,
            datasource_config=MagicMock(),
        )
        results = planner.execute_plan(plan)

        mock_build_engine.assert_called_once()
        assert results == {"check_a": 3, "check_b": 0}

    def test_db_path_preferred_when_both_db_hook_and_datasource_set(self):
        """When both db_hook and datasource_config are set, the DB path is used."""
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"null_count": 5}]

        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=mock_db_hook,
            datasource_config=MagicMock(),
        )
        results = planner.execute_plan(self._simple_plan("null_count"))
        mock_db_hook.get_records.assert_called_once()
        assert results["null_check"] == 5


class TestSQLDQPlannerDialect:
    def test_explicit_dialect_forwarded(self):
        mock_db_hook = MagicMock(spec=[])  # no dialect_name attribute
        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook, dialect="postgres"
        )
        assert planner._dialect == "postgres"

    def test_dialect_auto_detected_from_hook(self):
        mock_db_hook = MagicMock()
        mock_db_hook.dialect_name = "postgresql"
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook)
        assert planner._dialect == "postgres"  # normalised by resolve_dialect

    def test_dialect_none_when_not_set(self):
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=None)
        assert planner._dialect is None

    @patch("airflow.providers.common.ai.utils.dq_planner._validate_sql")
    def test_dialect_passed_to_validate_sql(self, mock_validate):
        mock_validate.return_value = []
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"c": 1}]

        plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g",
                    query="SELECT 1 AS c FROM t",
                    checks=[DQCheck(check_name="x", metric_key="c", group_id="g")],
                )
            ]
        )
        planner = SQLDQPlanner(llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook, dialect="mysql")
        planner.execute_plan(plan)

        mock_validate.assert_called_once()
        _, kwargs = mock_validate.call_args
        assert kwargs.get("dialect") == "mysql"


def _single_group_plan(query: str = "SELECT 1 AS c FROM t") -> DQPlan:
    return DQPlan(
        groups=[
            DQCheckGroup(
                group_id="g",
                query=query,
                checks=[DQCheck(check_name="x", metric_key="c", group_id="g")],
            )
        ]
    )


def _make_agent_returning(plan: DQPlan) -> MagicMock:
    """Mock pydantic-ai agent whose run_sync always returns *plan*."""
    mock_result = MagicMock(spec=["output", "all_messages"])
    mock_result.output = plan
    mock_result.all_messages.return_value = []
    mock_agent = MagicMock(spec=["run_sync"])
    mock_agent.run_sync.return_value = mock_result
    return mock_agent


class TestSQLDQPlannerSQLRetry:
    def _planner_with_agent(self, plan_agent, db_hook=None, max_sql_retries=2) -> SQLDQPlanner:
        """Return a planner with _plan_agent already set (simulates post-generate_plan state)."""
        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook),
            db_hook=db_hook,
            max_sql_retries=max_sql_retries,
        )
        planner._plan_agent = plan_agent
        planner._plan_all_messages = []
        return planner

    def test_valid_sql_passes_without_retry(self):
        """A query that passes validation is returned immediately unchanged."""
        group = DQCheckGroup(
            group_id="g",
            query="SELECT COUNT(*) AS c FROM t",
            checks=[DQCheck(check_name="x", metric_key="c", group_id="g")],
        )
        planner = self._planner_with_agent(MagicMock())
        result = planner._validate_or_fix_group(group)

        assert result.query == group.query
        planner._plan_agent.run_sync.assert_not_called()

    def test_first_retry_fixes_sql(self):
        """LLM returns a valid query on the first correction attempt."""
        bad_plan = _single_group_plan("DROP TABLE t")
        good_plan = _single_group_plan("SELECT COUNT(*) AS c FROM t")

        mock_agent = _make_agent_returning(good_plan)
        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"c": 1}]

        planner = self._planner_with_agent(mock_agent, db_hook=mock_db_hook)
        results = planner.execute_plan(bad_plan)

        assert results["x"] == 1
        mock_agent.run_sync.assert_called_once()
        # fix_prompt must name the failing group and include the error
        fix_prompt = mock_agent.run_sync.call_args.args[0]
        assert "g" in fix_prompt
        assert (
            "DROP" in fix_prompt
            or "not allowed" in fix_prompt.lower()
            or "SafetyError" in fix_prompt
            or "safety" in fix_prompt.lower()
        )

    def test_second_retry_fixes_sql_when_first_still_fails(self):
        """Planner retries twice; first correction still fails, second succeeds."""
        bad_plan = _single_group_plan("DELETE FROM t")
        still_bad_plan = _single_group_plan("TRUNCATE t")
        good_plan = _single_group_plan("SELECT 1 AS c FROM t")

        # agent returns a new plan on each call
        call_count = 0
        plans = [still_bad_plan, good_plan]

        def side_effect(*args, **kwargs):
            nonlocal call_count
            result = MagicMock()
            result.output = plans[call_count]
            result.all_messages.return_value = []
            call_count += 1
            return result

        mock_agent = MagicMock()
        mock_agent.run_sync.side_effect = side_effect

        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"c": 1}]

        planner = self._planner_with_agent(mock_agent, db_hook=mock_db_hook, max_sql_retries=2)
        results = planner.execute_plan(bad_plan)

        assert results["x"] == 1
        assert mock_agent.run_sync.call_count == 2

    def test_raises_after_all_retries_exhausted(self):
        """SQLSafetyError is raised when every correction attempt also fails."""
        bad_plan = _single_group_plan("DROP TABLE t")
        still_bad_plan = _single_group_plan("DELETE FROM t")

        mock_agent = _make_agent_returning(still_bad_plan)
        mock_db_hook = MagicMock()

        planner = self._planner_with_agent(mock_agent, db_hook=mock_db_hook, max_sql_retries=2)
        with pytest.raises(SQLSafetyError, match="could not be corrected after 2 attempt"):
            planner.execute_plan(bad_plan)

        assert mock_agent.run_sync.call_count == 2

    def test_max_sql_retries_zero_raises_immediately(self):
        """With max_sql_retries=0 the original error is re-raised after the first failure."""
        bad_plan = _single_group_plan("DROP TABLE t")
        mock_db_hook = MagicMock()

        planner = self._planner_with_agent(MagicMock(), db_hook=mock_db_hook, max_sql_retries=0)
        with pytest.raises(SQLSafetyError):
            planner.execute_plan(bad_plan)

        planner._plan_agent.run_sync.assert_not_called()

    def test_no_plan_agent_re_raises_original_error(self):
        """If execute_plan is called without a prior generate_plan, the error propagates."""
        bad_plan = _single_group_plan("DROP TABLE t")
        mock_db_hook = MagicMock()

        planner = SQLDQPlanner(
            llm_hook=MagicMock(spec=PydanticAIHook), db_hook=mock_db_hook, max_sql_retries=2
        )
        # _plan_agent is None — no generate_plan was called
        with pytest.raises(SQLSafetyError):
            planner.execute_plan(bad_plan)

    def test_message_history_updated_on_each_retry(self):
        """_plan_all_messages is refreshed from result.all_messages() after each retry."""
        bad_plan = _single_group_plan("DROP TABLE t")
        good_plan = _single_group_plan("SELECT 1 AS c FROM t")

        sentinel_messages = [object()]  # unique object to verify update

        mock_result = MagicMock()
        mock_result.output = good_plan
        mock_result.all_messages.return_value = sentinel_messages
        mock_agent = MagicMock()
        mock_agent.run_sync.return_value = mock_result

        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"c": 1}]

        planner = self._planner_with_agent(mock_agent, db_hook=mock_db_hook)
        planner.execute_plan(bad_plan)

        assert planner._plan_all_messages is sentinel_messages

    def test_retry_skips_when_corrected_plan_missing_group(self):
        """If the LLM omits the target group_id, the loop continues to the next attempt."""
        bad_plan = _single_group_plan("DROP TABLE t")
        # first response has a different group_id
        wrong_group_plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="OTHER_GROUP",
                    query="SELECT 1 AS c FROM t",
                    checks=[DQCheck(check_name="x", metric_key="c", group_id="OTHER_GROUP")],
                )
            ]
        )
        good_plan = _single_group_plan("SELECT 1 AS c FROM t")

        plans = [wrong_group_plan, good_plan]
        call_idx = 0

        def side_effect(*args, **kwargs):
            nonlocal call_idx
            result = MagicMock()
            result.output = plans[call_idx]
            result.all_messages.return_value = []
            call_idx += 1
            return result

        mock_agent = MagicMock()
        mock_agent.run_sync.side_effect = side_effect

        mock_db_hook = MagicMock()
        mock_db_hook.get_records.return_value = [{"c": 1}]

        planner = self._planner_with_agent(mock_agent, db_hook=mock_db_hook, max_sql_retries=2)
        results = planner.execute_plan(bad_plan)

        assert results["x"] == 1
        assert mock_agent.run_sync.call_count == 2
