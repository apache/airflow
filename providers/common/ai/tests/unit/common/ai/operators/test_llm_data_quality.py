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
from unittest.mock import ANY, MagicMock, patch

import pytest

from airflow.providers.common.ai.operators.llm_data_quality import (
    LLMDataQualityOperator,
    _compute_plan_hash,
)
from airflow.providers.common.ai.utils.dq_models import DQCheck, DQCheckGroup, DQPlan
from airflow.providers.common.ai.utils.dq_validation import null_pct_check, row_count_check
from airflow.providers.common.compat.sdk import AirflowException

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROMPTS = {
    "null_emails": "Check for null email addresses",
    "dup_ids": "Check for duplicate customer IDs",
}


def _make_context() -> dict:
    """Return a minimal Airflow context with a mock TaskInstance."""
    mock_ti = MagicMock()
    return {"ti": mock_ti}


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
    op = LLMDataQualityOperator(**defaults)
    op.llm_hook = MagicMock()
    return op


class TestLLMDataQualityOperatorInit:
    def test_requires_llm_conn_id(self):
        with pytest.raises(TypeError):
            LLMDataQualityOperator(
                task_id="test_dq",
                prompts=_PROMPTS,
                db_conn_id="postgres_default",
            )

    def test_template_fields(self):
        op = _make_operator()
        assert hasattr(op, "template_fields")
        assert isinstance(op.template_fields, (list, tuple))
        assert set(op.template_fields) >= {"prompts", "system_prompt", "agent_params"}


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


class TestLLMDataQualityOperatorCache:
    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True)
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
        op.execute(context=_make_context())

        mock_planner.generate_plan.assert_called_once_with(_PROMPTS, "")
        mock_variable.set.assert_called_once_with(ANY, plan.model_dump_json())

    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_cache_hit_skips_generate(self, mock_get_db_hook, mock_planner_cls, mock_variable):
        plan = _make_plan()
        mock_variable.get.return_value = plan.model_dump_json()  # cache hit
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.execute_plan.return_value = {"null_emails": 0, "dup_ids": 0}

        op = _make_operator()
        op.execute(context=_make_context())

        mock_planner.generate_plan.assert_not_called()
        mock_variable.set.assert_not_called()


class TestLLMDataQualityOperatorExecute:
    def _run_operator(self, plan, results_map, validators=None):
        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
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
            return op.execute(context=_make_context())

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

    def test_failure_pushes_results_to_xcom_before_raising(self):
        """When checks fail, results are pushed to XCom before the exception is raised."""
        plan = _make_plan()
        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = {"null_emails": 100, "dup_ids": 0}

            op = _make_operator(validators={"null_emails": lambda v: v == 0})
            ctx = _make_context()

            with pytest.raises(AirflowException):
                op.execute(context=ctx)

            ctx["ti"].xcom_push.assert_called_once()
            call_kwargs = ctx["ti"].xcom_push.call_args
            assert call_kwargs.kwargs["key"] == "return_value"
            pushed = call_kwargs.kwargs["value"]
            assert pushed["passed"] is False
            assert any(r["check_name"] == "null_emails" and not r["passed"] for r in pushed["results"])

    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_dry_run_returns_markdown_without_executing(
        self, mock_get_db_hook, mock_planner_cls, mock_variable
    ):
        plan = _make_plan()
        mock_variable.get.return_value = None
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.generate_plan.return_value = plan

        op = _make_operator(dry_run=True)
        result = op.execute(context=_make_context())

        mock_planner.execute_plan.assert_not_called()
        assert isinstance(result, str)
        assert "# LLM Data Quality Dry Run" in result
        assert "## Group 1: null_check" in result
        assert "SELECT COUNT(*) AS null_email_count" in result
        assert "- null_emails (null_email_count)" in result

    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True)
    def test_dry_run_still_caches_plan(self, mock_get_db_hook, mock_planner_cls, mock_variable):
        plan = _make_plan()
        mock_variable.get.return_value = None
        mock_planner = mock_planner_cls.return_value
        mock_planner.build_schema_context.return_value = ""
        mock_planner.generate_plan.return_value = plan

        op = _make_operator(dry_run=True)
        op.execute(context=_make_context())

        mock_variable.set.assert_called_once()


class TestLLMDataQualityOperatorSystemPromptAndAgentParams:
    """Verify that LLMOperator's inherited system_prompt / agent_params are forwarded to SQLDQPlanner."""

    def _run_with_planner_spy(self, op: LLMDataQualityOperator, plan: DQPlan):
        """Execute operator and return the kwargs that SQLDQPlanner was constructed with."""
        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
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

            op.execute(context=_make_context())
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
    @patch("airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True)
    def test_raises_value_error_for_non_dbapi_hook(self, mock_planner_cls, mock_variable, mock_get_db_hook):
        op = _make_operator(db_conn_id="bad_conn")
        with pytest.raises(ValueError, match="DbApiHook"):
            op.execute(context=_make_context())

    def test_none_db_conn_id_returns_none_hook(self):
        op = _make_operator(db_conn_id=None)
        assert op._resolve_db_hook() is None


class TestLLMDataQualityOperatorCollectUnexpected:
    """Tests for collect_unexpected and unexpected_sample_size parameters."""

    def test_collect_unexpected_in_template_fields(self):
        op = _make_operator()
        assert "collect_unexpected" in op.template_fields
        assert "unexpected_sample_size" in op.template_fields

    def test_collect_unexpected_defaults_false(self):
        op = _make_operator()
        assert op.collect_unexpected is False
        assert op.unexpected_sample_size == 100

    def test_collect_unexpected_forwarded_to_planner(self):
        plan = _make_plan()

        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = {"null_emails": 0, "dup_ids": 0}

            op = _make_operator(collect_unexpected=True, unexpected_sample_size=50)
            op.execute(context=_make_context())

            planner_kwargs = mock_planner_cls.call_args.kwargs
            assert planner_kwargs["collect_unexpected"] is True
            assert planner_kwargs["unexpected_sample_size"] == 50

    def test_unexpected_results_in_output_when_check_fails(self):
        from airflow.providers.common.ai.utils.dq_models import UnexpectedResult

        plan = _make_plan()

        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = {"null_emails": 100, "dup_ids": 0}
            mock_planner.execute_unexpected_queries.return_value = {
                "null_emails": UnexpectedResult(
                    check_name="null_emails",
                    unexpected_records=["1, None"],
                    sample_size=100,
                )
            }

            op = _make_operator(
                collect_unexpected=True,
                validators={"null_emails": lambda v: v == 0},
            )
            ctx = _make_context()
            with pytest.raises(AirflowException, match="null_emails"):
                op.execute(context=ctx)

            # execute_unexpected_queries must have been called with the failed check name
            mock_planner.execute_unexpected_queries.assert_called_once()
            call_args = mock_planner.execute_unexpected_queries.call_args
            assert "null_emails" in call_args.args[1]  # failed_check_names

    def test_collect_unexpected_false_skips_unexpected_queries(self):
        """When collect_unexpected=False, execute_unexpected_queries is never called."""
        plan = _make_plan()

        with (
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.Variable", autospec=True
            ) as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner", autospec=True
            ) as mock_planner_cls,
            patch("airflow.providers.common.ai.operators.llm_data_quality.get_db_hook", autospec=True),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = {"null_emails": 100, "dup_ids": 0}

            op = _make_operator(
                collect_unexpected=False,
                validators={"null_emails": lambda v: v == 0},
            )
            with pytest.raises(AirflowException):
                op.execute(context=_make_context())

            mock_planner.execute_unexpected_queries.assert_not_called()


class TestComputePlanHashCollectUnexpected:
    """collect_unexpected changes the plan hash to avoid cache collisions."""

    def test_different_collect_unexpected_yields_different_hash(self):
        h1 = _compute_plan_hash(_PROMPTS, None, collect_unexpected=False)
        h2 = _compute_plan_hash(_PROMPTS, None, collect_unexpected=True)
        assert h1 != h2

    def test_same_collect_unexpected_yields_same_hash(self):
        h1 = _compute_plan_hash(_PROMPTS, None, collect_unexpected=True)
        h2 = _compute_plan_hash(_PROMPTS, None, collect_unexpected=True)
        assert h1 == h2
