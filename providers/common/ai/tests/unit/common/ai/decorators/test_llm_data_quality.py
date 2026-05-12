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

from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.decorators.llm_data_quality import (
    _LLMDQDecoratedOperator,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.toolsets.dataquality.base import BaseDQToolset
from airflow.providers.common.ai.utils.dataquality.models import (
    DQCheckInput,
    DQCheckResult,
    DQReport,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CHECKS = [
    DQCheckInput(name="null_emails", description="Check for null email addresses"),
    DQCheckInput(name="dup_ids", description="Check for duplicate customer IDs"),
]


def _passing_report() -> DQReport:
    return DQReport(
        results=[
            DQCheckResult(check_name="null_emails", passed=True),
            DQCheckResult(check_name="dup_ids", passed=True),
        ],
        passed=True,
    )


def _make_op(callable_fn=None, **kwargs) -> _LLMDQDecoratedOperator:
    if callable_fn is None:

        def callable_fn():
            return _CHECKS

    return _LLMDQDecoratedOperator(
        task_id="test_dq",
        python_callable=callable_fn,
        llm_conn_id="pydantic_ai_default",
        db_conn_id="postgres_default",
        **kwargs,
    )


def _mock_agent_result(output) -> MagicMock:
    result = MagicMock()
    result.output = output
    result.usage.return_value = MagicMock(
        requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0
    )
    result.response = MagicMock(model_name="test-model")
    result.all_messages.return_value = []
    return result


def _make_patched_op(callable_fn=None, **op_kwargs) -> _LLMDQDecoratedOperator:
    """Return an op with llm_hook mocked to return a passing DQReport."""
    op = _make_op(callable_fn, **op_kwargs)
    mock_dq = MagicMock(spec=BaseDQToolset)
    mock_dq.output_mode = "execute"
    op.toolsets = [mock_dq]
    op.llm_hook = MagicMock(spec=PydanticAIHook)
    op.llm_hook.create_agent.return_value.run_sync.return_value = _mock_agent_result(_passing_report())
    return op


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLLMDQDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMDQDecoratedOperator.custom_operator_name == "@task.llm_dq"

    def test_callable_return_value_becomes_checks(self):
        """The list returned by the callable is assigned to op.checks before execute."""
        op = _make_patched_op(lambda: _CHECKS)
        op.execute(context={"ti": MagicMock(), "task_instance": MagicMock()})
        assert op.checks == _CHECKS

    def test_raises_on_invalid_return_value(self):
        """TypeError when the callable returns a non-list value."""
        op = _make_op(lambda: "not a list")
        with pytest.raises(TypeError, match="non-empty list"):
            op.execute(context={})

    @pytest.mark.parametrize(
        "return_value",
        [[], None, 42, ""],
        ids=["empty-list", "none", "int", "empty-string"],
    )
    def test_raises_on_falsy_return(self, return_value):
        op = _make_op(lambda: return_value)
        with pytest.raises(TypeError, match="non-empty list"):
            op.execute(context={})

    def test_merges_op_kwargs_into_callable(self):
        """op_kwargs are passed to the callable when building the checks list."""

        def my_checks(min_rows):
            return [
                DQCheckInput(
                    name="row_count",
                    description=f"Orders must have at least {min_rows} rows.",
                ),
            ]

        op = _make_patched_op(my_checks, op_kwargs={"min_rows": 1000})
        op.execute(context={"ti": MagicMock(), "task_instance": MagicMock()})
        assert "1000" in op.checks[0].description

    def test_execute_passes_through_to_llm_operator(self):
        """After setting checks, the decorated op behaves like LLMDataQualityOperator."""
        op = _make_patched_op(lambda: _CHECKS)
        result = op.execute(context={"ti": MagicMock(), "task_instance": MagicMock()})
        assert result["passed"] is True
