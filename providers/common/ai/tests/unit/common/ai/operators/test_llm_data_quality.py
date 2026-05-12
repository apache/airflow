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
"""Tests for LLMDataQualityOperator."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
from airflow.providers.common.ai.toolsets.dataquality.base import BaseDQToolset
from airflow.providers.common.ai.utils.dataquality.models import (
    DQCheckFailedError,
    DQCheckInput,
    DQCheckPlan,
    DQCheckResult,
    DQPlan,
    DQReport,
)

_CHECKS = [
    DQCheckInput(name="null_emails", description="Check for null email addresses"),
    DQCheckInput(name="dup_ids", description="Check for duplicate customer IDs"),
]


class _TaskInstanceLike:
    def xcom_push(self, key: str, value: Any) -> None:
        pass


class _UsageLike:
    requests = 1
    tool_calls = 0
    input_tokens = 1
    output_tokens = 1
    total_tokens = 2


class _ResponseLike:
    model_name = "test-model"


class _AgentResultLike:
    def __init__(self, output: Any) -> None:
        self.output = output
        self.response = _ResponseLike()

    def usage(self) -> _UsageLike:
        return _UsageLike()

    def all_messages(self) -> list[Any]:
        return []


def _make_context() -> Any:
    task_instance = MagicMock(spec=_TaskInstanceLike)
    return {"task_instance": task_instance, "ti": task_instance}


def _make_operator(**overrides: Any) -> LLMDataQualityOperator:
    defaults: dict[str, Any] = {
        "task_id": "test_dq",
        "checks": _CHECKS,
        "llm_conn_id": "pydantic_ai_default",
        "db_conn_id": "postgres_default",
    }
    defaults.update(overrides)
    op = LLMDataQualityOperator(**defaults)
    op.llm_hook = MagicMock(spec=PydanticAIHook)
    return op


def _passing_report() -> DQReport:
    return DQReport(
        results=[
            DQCheckResult(check_name="null_emails", passed=True),
            DQCheckResult(check_name="dup_ids", passed=True),
        ],
        passed=True,
    )


def _failing_report() -> DQReport:
    return DQReport(
        results=[
            DQCheckResult(check_name="null_emails", passed=False, failure_reason="100 nulls found"),
            DQCheckResult(check_name="dup_ids", passed=True),
        ],
        passed=False,
        failure_summary="null_emails: 100 nulls found",
    )


def _mock_agent_result(output: Any) -> _AgentResultLike:
    return _AgentResultLike(output)


class TestLLMDataQualityOperatorInit:
    def test_requires_llm_conn_id(self):
        with pytest.raises(TypeError):
            LLMDataQualityOperator(
                task_id="test_dq",
                checks=_CHECKS,
                db_conn_id="postgres_default",
            )

    def test_raises_when_no_toolsets_and_no_db_conn_id(self):
        with pytest.raises(ValueError, match="Either toolsets or db_conn_id"):
            LLMDataQualityOperator(
                task_id="test_dq",
                checks=_CHECKS,
                llm_conn_id="pydantic_ai_default",
            )

    def test_raises_when_empty_toolsets_and_no_db_conn_id(self):
        with pytest.raises(ValueError, match="Either toolsets or db_conn_id"):
            LLMDataQualityOperator(
                task_id="test_dq",
                checks=_CHECKS,
                llm_conn_id="pydantic_ai_default",
                toolsets=[],
            )

    def test_empty_checks_raises_value_error(self):
        with pytest.raises(ValueError, match="checks must not be empty"):
            _make_operator(checks=[])

    def test_duplicate_check_names_raises(self):
        with pytest.raises(ValueError, match="duplicate"):
            _make_operator(
                checks=[
                    DQCheckInput(name="dup", description="first"),
                    DQCheckInput(name="dup", description="second"),
                ]
            )

    def test_dict_checks_auto_coerced(self):
        op = _make_operator(
            checks=[
                {"name": "null_emails", "description": "Check nulls"},
                {"name": "dup_ids", "description": "Check dups"},
            ]
        )
        assert all(isinstance(c, DQCheckInput) for c in op.checks)

    def test_template_fields_include_required_keys(self):
        op = _make_operator()
        tf = set(op.template_fields)
        assert {"checks", "system_prompt", "agent_params", "db_conn_id", "table_names"} <= tf


class TestResolveToolsets:
    def test_explicit_toolsets_returned_unchanged(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        op = _make_operator(toolsets=[mock_dq])
        resolved = op._resolve_toolsets()
        assert resolved is op.toolsets

    def test_auto_creates_sql_toolsets_from_db_conn_id(self):
        op = _make_operator()
        with (
            patch("airflow.providers.common.ai.toolsets.sql.SQLToolset") as mock_sql,
            patch("airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset") as mock_dq,
        ):
            op._resolve_toolsets()
            mock_sql.assert_called_once_with(db_conn_id="postgres_default", allowed_tables=None)
            mock_dq.assert_called_once_with()

    def test_auto_creates_sql_toolsets_with_table_names(self):
        op = _make_operator(table_names=["customers", "orders"])
        with (
            patch("airflow.providers.common.ai.toolsets.sql.SQLToolset") as mock_sql,
            patch("airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset"),
        ):
            op._resolve_toolsets()
            mock_sql.assert_called_once_with(
                db_conn_id="postgres_default", allowed_tables=["customers", "orders"]
            )

    def test_auto_appends_sql_dq_toolset_when_missing(self):
        mock_other = object()
        op = _make_operator(toolsets=[mock_other])
        with patch("airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset") as mock_cls:
            mock_cls.return_value = MagicMock(spec=BaseDQToolset)
            resolved = op._resolve_toolsets()
            mock_cls.assert_called_once_with()
            assert len(resolved) == 2


class TestFindDqToolset:
    def test_returns_first_base_dq_toolset(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_other = object()
        result = LLMDataQualityOperator._find_dq_toolset([mock_other, mock_dq])
        assert result is mock_dq

    def test_raises_when_no_dq_toolset(self):
        mock_other = object()
        with pytest.raises(ValueError, match="No BaseDQToolset found"):
            LLMDataQualityOperator._find_dq_toolset([mock_other])


class TestExecuteMode:
    def _run(self, report: DQReport, **overrides: Any) -> Any:
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        op = _make_operator(toolsets=[mock_dq], **overrides)
        op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result(report)  # type: ignore[attr-defined]
        return op.execute(context=_make_context())

    def test_set_checks_called_on_dq_toolset(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        op = _make_operator(toolsets=[mock_dq])
        op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result(_passing_report())
        op.execute(context=_make_context())
        mock_dq.set_checks.assert_called_once_with(op.checks)

    def test_passing_report_returns_model_dump(self):
        result = self._run(_passing_report())
        assert result["passed"] is True
        assert isinstance(result["results"], list)

    def test_failing_report_raises_dq_check_failed_error(self):
        with pytest.raises(DQCheckFailedError, match="null_emails"):
            self._run(_failing_report())

    def test_output_type_is_dq_report(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        op = _make_operator(toolsets=[mock_dq])
        op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result(_passing_report())
        op.execute(context=_make_context())
        create_agent_kwargs = op.llm_hook.create_agent.call_args.kwargs
        assert create_agent_kwargs["output_type"] is DQReport


class TestGenerateMode:
    def _run_generate(self, config_str: str = "checks:\n  - name: foo") -> Any:
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "generate"
        op = _make_operator(toolsets=[mock_dq])
        op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result(config_str)  # type: ignore[attr-defined]
        return op.execute(context=_make_context())

    def test_returns_config_string(self):
        config = "checks:\n  - name: null_emails"
        result = self._run_generate(config)
        assert result == config

    def test_output_type_is_str(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "generate"
        op = _make_operator(toolsets=[mock_dq])
        op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result("cfg")
        op.execute(context=_make_context())
        create_agent_kwargs = op.llm_hook.create_agent.call_args.kwargs
        assert create_agent_kwargs["output_type"] is str


class TestBuildSystemPrompt:
    def test_base_prompt_always_present(self):
        op = _make_operator()
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        prompt = op._build_system_prompt(mock_dq, [mock_dq])
        assert "data-quality" in prompt

    def test_system_prompt_appended(self):
        op = _make_operator(system_prompt="Use ANSI SQL only.")
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        prompt = op._build_system_prompt(mock_dq, [mock_dq])
        assert "Use ANSI SQL only." in prompt

    def test_schema_context_appended(self):
        op = _make_operator(schema_context="Table: customers(id, email)")
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        prompt = op._build_system_prompt(mock_dq, [mock_dq])
        assert "customers(id, email)" in prompt


class TestExecutePlanValidators:
    def test_uses_value_returned_by_apply_validator(self):
        op = _make_operator()
        plan = DQPlan(
            checks=[
                DQCheckPlan(
                    check_name="email_format",
                    sql_query="SELECT email FROM customers",
                    metric_key="email",
                    row_level=True,
                    validator_name="fixed",
                    validator_args={},
                )
            ]
        )

        sql_toolset = MagicMock(spec=["_query"])
        sql_toolset._query.return_value = '{"rows": [{"email": "ok@example.com"}]}'

        dq_toolset = MagicMock(spec=["_apply_validator"])
        dq_toolset._apply_validator.return_value = (
            '{"check_name": "email_format", "passed": true, "reason": null, '
            '"value": {"total": 1, "invalid": 0, "invalid_pct": 0.0, '
            '"sample_violations": [], "sample_size": 0}}'
        )

        report = op._execute_plan_validators(plan, dq_toolset, [sql_toolset, dq_toolset])
        assert report.passed is True
        assert report.results[0].value["total"] == 1
        assert report.results[0].value["invalid"] == 0

    def test_falls_back_to_metric_value_when_validator_does_not_return_value(self):
        op = _make_operator()
        plan = DQPlan(
            checks=[
                DQCheckPlan(
                    check_name="row_count",
                    sql_query="SELECT COUNT(*) AS row_count FROM customers",
                    metric_key="row_count",
                    row_level=False,
                    validator_name="fixed",
                    validator_args={},
                )
            ]
        )

        sql_toolset = MagicMock(spec=["_query"])
        sql_toolset._query.return_value = '{"rows": [{"row_count": 42}]}'

        dq_toolset = MagicMock(spec=["_apply_validator"])
        dq_toolset._apply_validator.return_value = (
            '{"check_name": "row_count", "passed": true, "reason": null}'
        )

        report = op._execute_plan_validators(plan, dq_toolset, [sql_toolset, dq_toolset])
        assert report.passed is True
        assert report.results[0].value == 42


class TestRequireApprovalFlow:
    @staticmethod
    def _build_plan() -> DQPlan:
        return DQPlan(
            checks=[
                DQCheckPlan(
                    check_name="row_count",
                    sql_query="-- check: row_count\nSELECT COUNT(*) AS row_count FROM customers",
                    metric_key="row_count",
                    row_level=False,
                    validator_name="fixed",
                    validator_args={},
                )
            ]
        )

    def test_execute_defers_with_plan_when_approval_required(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        op = _make_operator(toolsets=[mock_dq], require_approval=True)

        plan = self._build_plan()
        op._run_plan_phase = MagicMock(return_value=(plan, mock_dq))  # type: ignore[method-assign]
        op.defer_for_approval = MagicMock()  # type: ignore[method-assign]
        op._run_and_report = MagicMock(return_value={"passed": True, "results": []})  # type: ignore[method-assign]

        context = _make_context()
        result = op.execute(context=context)

        assert result == {"passed": True, "results": []}
        op._run_plan_phase.assert_called_once_with(context)
        op.defer_for_approval.assert_called_once()
        call_args, call_kwargs = op.defer_for_approval.call_args
        assert call_args[1] == plan.model_dump_json()
        assert call_kwargs["subject"] == "Review DQ plan for task `test_dq`"
        assert "row_count" in call_kwargs["body"]

    def test_execute_complete_returns_report_after_approval(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        sql_toolset = MagicMock(spec=["_query"])

        op = _make_operator(toolsets=[sql_toolset, mock_dq], require_approval=True)
        plan = self._build_plan()
        report = _passing_report()

        op._resolve_toolsets = MagicMock(return_value=[sql_toolset, mock_dq])  # type: ignore[method-assign]
        op._find_dq_toolset = MagicMock(return_value=mock_dq)  # type: ignore[method-assign]
        op._execute_plan_validators = MagicMock(return_value=report)  # type: ignore[method-assign]

        context = _make_context()
        with patch.object(LLMOperator, "execute_complete", return_value=plan.model_dump_json()):
            result = op.execute_complete(
                context=context, generated_output="ignored", event={"approved": True}
            )

        assert result["passed"] is True
        mock_dq.set_checks.assert_called_once_with(op.checks)
        op._execute_plan_validators.assert_called_once_with(plan, mock_dq, [sql_toolset, mock_dq])
        context["task_instance"].xcom_push.assert_called_once_with(key="dq_report", value=report.model_dump())

    def test_execute_complete_raises_when_approved_plan_fails(self):
        mock_dq = MagicMock(spec=BaseDQToolset)
        mock_dq.output_mode = "execute"
        sql_toolset = MagicMock(spec=["_query"])

        op = _make_operator(toolsets=[sql_toolset, mock_dq], require_approval=True)
        plan = self._build_plan()
        report = _failing_report()

        op._resolve_toolsets = MagicMock(return_value=[sql_toolset, mock_dq])  # type: ignore[method-assign]
        op._find_dq_toolset = MagicMock(return_value=mock_dq)  # type: ignore[method-assign]
        op._execute_plan_validators = MagicMock(return_value=report)  # type: ignore[method-assign]

        context = _make_context()
        with patch.object(LLMOperator, "execute_complete", return_value=plan.model_dump_json()):
            with pytest.raises(DQCheckFailedError, match="null_emails"):
                op.execute_complete(context=context, generated_output="ignored", event={"approved": True})
