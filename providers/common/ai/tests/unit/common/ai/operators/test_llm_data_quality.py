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

from typing import Any
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_data_quality import (
    LLMDataQualityOperator,
    _compute_plan_hash,
)
from airflow.providers.common.ai.utils.dq_models import DQCheck, DQCheckGroup, DQPlan
from airflow.providers.common.ai.utils.dq_validation import null_pct_check, row_count_check

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROMPTS = {
    "null_emails": "Check for null email addresses",
    "dup_ids": "Check for duplicate customer IDs",
}


def _make_plan() -> DQPlan:
    return DQPlan(
        groups=[
            DQCheckGroup(
                group_id="null_check",
                query="SELECT COUNT(*) AS null_email_count FROM customers WHERE email IS NULL",
                checks=[
                    DQCheck(
                        check_name="null_emails",
                        metric_key="null_email_count",
                        group_id="null_check",
                    )
                ],
            ),
            DQCheckGroup(
                group_id="uniqueness",
                query=(
                    "SELECT COUNT(*) AS dup_id_count FROM ("
                    "SELECT id FROM customers GROUP BY id HAVING COUNT(*) > 1) sub"
                ),
                checks=[
                    DQCheck(
                        check_name="dup_ids",
                        metric_key="dup_id_count",
                        group_id="uniqueness",
                    )
                ],
            ),
        ]
    )


def _make_operator(**overrides: Any) -> LLMDataQualityOperator:
    defaults: dict[str, Any] = dict(
        task_id="test_dq",
        prompts=_PROMPTS,
        llm_conn_id="pydantic_ai_default",
        db_conn_id="postgres_default",
    )
    defaults.update(overrides)
    return LLMDataQualityOperator(**defaults)


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorInit
# ---------------------------------------------------------------------------


class TestLLMDataQualityOperatorInit:
    def test_inherits_from_llm_operator(self):
        assert issubclass(LLMDataQualityOperator, LLMOperator)

    def test_does_not_inherit_from_llm_sql_operator(self):
        from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator

        assert not issubclass(LLMDataQualityOperator, LLMSQLQueryOperator)

    def test_template_fields_include_parent_and_dq_specific(self):
        expected_subset = {
            "prompt",
            "llm_conn_id",
            "model_id",
            "system_prompt",
            "agent_params",
            "prompts",
            "db_conn_id",
            "table_names",
            "schema_context",
            "prompt_version",
        }
        assert expected_subset.issubset(set(LLMDataQualityOperator.template_fields))

    def test_raises_when_validator_key_not_in_prompts(self):
        with pytest.raises(ValueError, match="unknown_check"):
            _make_operator(
                validators={"unknown_check": lambda v: v == 0},
            )

    def test_valid_validator_keys_accepted(self):
        op = _make_operator(validators={"null_emails": lambda v: v == 0})
        assert "null_emails" in op.validators

    def test_no_validators_accepted(self):
        op = _make_operator()
        assert op.validators == {}

    def test_partial_validators_accepted(self):
        """Validators dict may cover a subset of prompts."""
        op = _make_operator(validators={"null_emails": lambda v: v == 0})
        assert "dup_ids" not in op.validators


# ---------------------------------------------------------------------------
# TestComputePlanHash
# ---------------------------------------------------------------------------


class TestComputePlanHash:
    def test_same_prompts_same_version_yields_same_hash(self):
        h1 = _compute_plan_hash(_PROMPTS, "v1")
        h2 = _compute_plan_hash(_PROMPTS, "v1")
        assert h1 == h2

    def test_different_version_yields_different_hash(self):
        h1 = _compute_plan_hash(_PROMPTS, "v1")
        h2 = _compute_plan_hash(_PROMPTS, "v2")
        assert h1 != h2

    def test_different_prompts_yield_different_hash(self):
        other = {"a": "check A"}
        assert _compute_plan_hash(_PROMPTS, None) != _compute_plan_hash(other, None)

    def test_hash_is_order_independent(self):
        prompts_a = {"a": "x", "b": "y"}
        prompts_b = {"b": "y", "a": "x"}
        assert _compute_plan_hash(prompts_a, None) == _compute_plan_hash(prompts_b, None)

    def test_hash_length_within_variable_key_limit(self):
        from airflow.providers.common.ai.operators.llm_data_quality import _PLAN_VARIABLE_KEY_MAX_LEN

        h = _compute_plan_hash(_PROMPTS, "a" * 300)
        assert len(h) <= _PLAN_VARIABLE_KEY_MAX_LEN


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorCache
# ---------------------------------------------------------------------------


class TestLLMDataQualityOperatorCache:
    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable")
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_cache_miss_calls_generate_and_sets_variable(
        self, mock_get_db_hook, mock_planner_cls, mock_variable
    ):
        plan = _make_plan()
        mock_variable.get.return_value = None  # cache miss
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.generate_plan.return_value = plan
        mock_planner.execute_plan.return_value = {"null_emails": 0, "dup_ids": 0}

        op = _make_operator()
        op.execute(context={})

        mock_planner.generate_plan.assert_called_once()
        mock_variable.set.assert_called_once()

    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable")
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_cache_hit_skips_generate(self, mock_get_db_hook, mock_planner_cls, mock_variable):
        plan = _make_plan()
        mock_variable.get.return_value = plan.model_dump_json()  # cache hit
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.execute_plan.return_value = {"null_emails": 0, "dup_ids": 0}

        op = _make_operator()
        op.execute(context={})

        mock_planner.generate_plan.assert_not_called()
        mock_variable.set.assert_not_called()


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorDryRun
# ---------------------------------------------------------------------------


class TestLLMDataQualityOperatorDryRun:
    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable")
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_dry_run_returns_plan_dict_without_executing(
        self, mock_get_db_hook, mock_planner_cls, mock_variable
    ):
        plan = _make_plan()
        mock_variable.get.return_value = None
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.generate_plan.return_value = plan

        op = _make_operator(dry_run=True)
        result = op.execute(context={})

        mock_planner.execute_plan.assert_not_called()
        assert "groups" in result

    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable")
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_dry_run_still_caches_plan(self, mock_get_db_hook, mock_planner_cls, mock_variable):
        plan = _make_plan()
        mock_variable.get.return_value = None
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.generate_plan.return_value = plan

        op = _make_operator(dry_run=True)
        op.execute(context={})

        mock_variable.set.assert_called_once()


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorExecute
# ---------------------------------------------------------------------------


class TestLLMDataQualityOperatorExecute:
    def _run_operator(self, plan, results_map, validators=None):
        with (
            patch("airflow.providers.common.ai.operators.llm_data_quality.Variable") as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = results_map

            op = _make_operator(validators=validators or {})
            return op.execute(context={})

    def test_happy_path_all_checks_pass(self):
        plan = _make_plan()
        result = self._run_operator(
            plan,
            {"null_emails": 0, "dup_ids": 0},
            validators={
                "null_emails": lambda v: v == 0,
                "dup_ids": lambda v: v == 0,
            },
        )
        assert result["passed"] is True
        assert all(r["passed"] for r in result["results"])

    def test_raises_airflow_exception_when_check_fails(self):
        plan = _make_plan()
        with pytest.raises(AirflowException, match="null_emails"):
            self._run_operator(
                plan,
                {"null_emails": 100, "dup_ids": 0},
                validators={"null_emails": lambda v: v == 0},
            )

    def test_failure_message_names_failing_check(self):
        plan = _make_plan()
        with pytest.raises(AirflowException) as exc_info:
            self._run_operator(
                plan,
                {"null_emails": 50, "dup_ids": 3},
                validators={
                    "null_emails": lambda v: v == 0,
                    "dup_ids": lambda v: v == 0,
                },
            )
        msg = str(exc_info.value)
        assert "null_emails" in msg
        assert "dup_ids" in msg

    def test_no_validators_all_pass_by_default(self):
        plan = _make_plan()
        result = self._run_operator(plan, {"null_emails": 999, "dup_ids": 999})
        assert result["passed"] is True

    def test_builtin_validator_factory_works_as_validator(self):
        plan = _make_plan()
        with pytest.raises(AirflowException):
            self._run_operator(
                plan,
                {"null_emails": 0.10, "dup_ids": 0},
                validators={"null_emails": null_pct_check(max_pct=0.05)},
            )

    def test_builtin_row_count_check_passes(self):
        plan = _make_plan()
        result = self._run_operator(
            plan,
            {"null_emails": 0, "dup_ids": 0},
            validators={"null_emails": row_count_check(min_count=0)},
        )
        assert result["passed"] is True

    def test_validator_exception_marks_check_failed(self):
        """If a validator raises, the check is marked failed with the exception message."""
        plan = _make_plan()
        with pytest.raises(AirflowException) as exc_info:
            self._run_operator(
                plan,
                {"null_emails": "not-a-number", "dup_ids": 0},
                validators={"null_emails": null_pct_check(max_pct=0.05)},
            )
        assert "null_emails" in str(exc_info.value)


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorDbHook
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TestLLMDataQualityOperatorSystemPromptAndAgentParams
# ---------------------------------------------------------------------------


class TestLLMDataQualityOperatorSystemPromptAndAgentParams:
    """Verify that LLMOperator's inherited system_prompt / agent_params are forwarded to SQLDQPlanner."""

    def _run_with_planner_spy(self, op: LLMDataQualityOperator, plan: DQPlan):
        """Execute operator and return the kwargs that SQLDQPlanner was constructed with."""
        with (
            patch("airflow.providers.common.ai.operators.llm_data_quality.Variable") as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = {k: 0 for k in plan.check_names}

            op.execute(context={})
            return mock_planner_cls.call_args.kwargs

    def test_system_prompt_forwarded_to_planner(self):
        plan = _make_plan()
        op = _make_operator(system_prompt="Use ANSI SQL only.")
        kwargs = self._run_with_planner_spy(op, plan)
        assert kwargs["system_prompt"] == "Use ANSI SQL only."

    def test_default_empty_system_prompt_forwarded(self):
        plan = _make_plan()
        op = _make_operator()  # system_prompt defaults to ""
        kwargs = self._run_with_planner_spy(op, plan)
        assert kwargs["system_prompt"] == ""

    def test_agent_params_forwarded_to_planner(self):
        plan = _make_plan()
        op = _make_operator(agent_params={"retries": 5, "model_settings": {"temperature": 0.0}})
        kwargs = self._run_with_planner_spy(op, plan)
        assert kwargs["agent_params"] == {"retries": 5, "model_settings": {"temperature": 0.0}}

    def test_default_empty_agent_params_forwarded(self):
        plan = _make_plan()
        op = _make_operator()  # agent_params defaults to {}
        kwargs = self._run_with_planner_spy(op, plan)
        assert kwargs["agent_params"] == {}


class TestLLMDataQualityOperatorDbHook:
    @patch(
        "airflow.providers.common.ai.operators.llm_data_quality.get_db_hook",
        side_effect=ValueError("Connection 'x' does not provide a DbApiHook."),
    )
    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable")
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    def test_raises_value_error_for_non_dbapi_hook(self, mock_planner_cls, mock_variable, mock_get_db_hook):
        op = _make_operator(db_conn_id="bad_conn")
        with pytest.raises(ValueError, match="DbApiHook"):
            op.execute(context={})

    def test_none_db_conn_id_returns_none_hook(self):
        op = _make_operator(db_conn_id=None)
        assert op._resolve_db_hook() is None
