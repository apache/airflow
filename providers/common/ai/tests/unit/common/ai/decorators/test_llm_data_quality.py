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

from airflow.providers.common.ai.decorators.llm_data_quality import (
    _LLMDQDecoratedOperator,
    llm_dq_task,
)
from airflow.providers.common.ai.utils.dq_models import DQCheck, DQCheckGroup, DQPlan

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


def _make_op(callable_fn=None, **kwargs) -> _LLMDQDecoratedOperator:
    if callable_fn is None:

        def callable_fn():
            return _PROMPTS

    return _LLMDQDecoratedOperator(
        task_id="test_dq",
        python_callable=callable_fn,
        llm_conn_id="pydantic_ai_default",
        db_conn_id="postgres_default",
        **kwargs,
    )


# ---------------------------------------------------------------------------
# TestLLMDQDecoratedOperatorMeta
# ---------------------------------------------------------------------------


class TestLLMDQDecoratedOperatorMeta:
    def test_custom_operator_name(self):
        assert _LLMDQDecoratedOperator.custom_operator_name == "@task.llm_dq"

    def test_template_fields_include_parent_fields(self):
        expected_subset = {
            "prompts",
            "db_conn_id",
            "table_names",
            "schema_context",
            "prompt_version",
            "llm_conn_id",
        }
        assert expected_subset.issubset(set(_LLMDQDecoratedOperator.template_fields))

    def test_validators_with_unknown_key_accepted_at_init(self):
        """Validator key validation is deferred to execute time when prompts is SET_DURING_EXECUTION."""
        op = _make_op(validators={"unknown_check": lambda v: v == 0})
        # No error at init time because prompts is SET_DURING_EXECUTION
        assert "unknown_check" in op.validators

    def test_llm_dq_task_returns_task_decorator(self):
        """llm_dq_task() is usable as a TaskDecorator factory."""
        decorator = llm_dq_task(llm_conn_id="openai_default")
        assert callable(decorator)


# ---------------------------------------------------------------------------
# TestLLMDQDecoratedOperatorExecute
# ---------------------------------------------------------------------------


class TestLLMDQDecoratedOperatorExecute:
    def _run_op(self, callable_fn, plan, results_map, **op_kwargs) -> dict:
        with (
            patch("airflow.providers.common.ai.operators.llm_data_quality.Variable") as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner",
                autospec=True,
            ) as mock_planner_cls,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.get_db_hook",
                autospec=True,
            ),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = results_map

            op = _make_op(callable_fn, **op_kwargs)
            return op.execute(context={})

    def test_callable_return_value_becomes_prompts(self):
        """The dict returned by the callable is used as prompts for plan generation."""
        plan = _make_plan()
        results_map = {"null_emails": 0, "dup_ids": 0}

        def my_checks():
            return _PROMPTS

        with (
            patch("airflow.providers.common.ai.operators.llm_data_quality.Variable") as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner",
                autospec=True,
            ) as mock_planner_cls,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.get_db_hook",
                autospec=True,
            ),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = plan
            mock_planner.execute_plan.return_value = results_map

            op = _make_op(my_checks)
            op.execute(context={})

            assert op.prompts == _PROMPTS

    def test_happy_path_all_checks_pass(self):
        plan = _make_plan()
        result = self._run_op(
            lambda: _PROMPTS,
            plan,
            {"null_emails": 0, "dup_ids": 0},
            validators={
                "null_emails": lambda v: v == 0,
                "dup_ids": lambda v: v == 0,
            },
        )
        assert result["passed"] is True

    def test_raises_on_invalid_prompts_return_value(self):
        """TypeError when the callable returns a non-dict or empty value."""
        op = _make_op(lambda: "not a dict")
        with pytest.raises(TypeError, match="non-empty dict"):
            op.execute(context={})

    @pytest.mark.parametrize(
        "return_value",
        [{}, None, 42, ""],
        ids=["empty-dict", "none", "int", "empty-string"],
    )
    def test_raises_on_falsy_prompts(self, return_value):
        op = _make_op(lambda: return_value)
        with pytest.raises(TypeError, match="non-empty dict"):
            op.execute(context={})

    def test_validator_key_not_in_prompts_raises_at_execute(self):
        """ValueError is raised at execute time when a validator key is not in the returned prompts."""
        op = _make_op(
            lambda: {"null_emails": "Check nulls"},
            validators={"unknown_check": lambda v: v == 0},
        )
        with pytest.raises(ValueError, match="unknown_check"):
            op.execute(context={})

    def test_merges_op_kwargs_into_callable(self):
        """op_kwargs are passed to the callable when building the prompts dict."""
        results_map = {"row_count": 5000}
        single_check_plan = DQPlan(
            groups=[
                DQCheckGroup(
                    group_id="g1",
                    query="SELECT COUNT(*) AS row_count FROM orders",
                    checks=[DQCheck(check_name="row_count", metric_key="row_count", group_id="g1")],
                )
            ]
        )

        def my_checks(min_rows):
            return {"row_count": f"Orders must have at least {min_rows} rows."}

        with (
            patch("airflow.providers.common.ai.operators.llm_data_quality.Variable") as mock_var,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.SQLDQPlanner",
                autospec=True,
            ) as mock_planner_cls,
            patch(
                "airflow.providers.common.ai.operators.llm_data_quality.get_db_hook",
                autospec=True,
            ),
        ):
            mock_var.get.return_value = None
            mock_planner = mock_planner_cls.return_value
            mock_planner.build_schema_context.return_value = ""
            mock_planner.generate_plan.return_value = single_check_plan
            mock_planner.execute_plan.return_value = results_map

            op = _make_op(
                my_checks,
                op_kwargs={"min_rows": 1000},
            )
            op.execute(context={"task_instance": MagicMock()})
            assert "1000" in op.prompts["row_count"]

    def test_dry_run_returns_plan_dict(self):
        """dry_run=True returns the serialised plan dict without executing checks."""
        plan = _make_plan()
        result = self._run_op(lambda: _PROMPTS, plan, {}, dry_run=True)
        assert "groups" in result
