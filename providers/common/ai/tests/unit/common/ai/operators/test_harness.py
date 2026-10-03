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

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.ai.harness.base import (
    HarnessBackend,
    HarnessRequest,
    HarnessResult,
    HarnessRunError,
)
from airflow.providers.common.ai.operators.harness import HarnessOperator
from airflow.providers.common.ai.tools import AirflowTool, ToolResult


@dataclass
class _FakeBackend(HarnessBackend):
    """Records the request it was given and returns a canned result."""

    name = "fake"
    result: HarnessResult = field(default_factory=lambda: HarnessResult(output="the answer", is_error=False))
    received: list[HarnessRequest] = field(default_factory=list)

    def run(self, request: HarnessRequest) -> HarnessResult:
        self.received.append(request)
        return self.result


def _tool(name: str = "lookup") -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        return ToolResult(content="ok")

    return AirflowTool(name=name, description="A tool.", parameters={"type": "object"}, function=function)


def test_execute_returns_output_and_pushes_metadata() -> None:
    backend = _FakeBackend(
        result=HarnessResult(
            output="the answer",
            is_error=False,
            subtype="success",
            session_id="session-1",
            num_turns=2,
            cost_usd=0.5,
            usage={"input_tokens": 1},
        )
    )
    op = HarnessOperator(task_id="t", prompt="go", llm_conn_id="anthropic_default", harness=backend)
    context = MagicMock(spec=dict)

    output = op.execute(context)

    assert output == "the answer"
    pushes = {c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list}
    assert pushes["session_id"] == "session-1"
    assert pushes["usage"] == {
        "num_turns": 2,
        "cost_usd": 0.5,
        "subtype": "success",
        "usage": {"input_tokens": 1},
    }


def test_execute_raises_on_error_result_after_pushing_usage() -> None:
    backend = _FakeBackend(result=HarnessResult(output=None, is_error=True, subtype="error_max_turns"))
    op = HarnessOperator(task_id="t", prompt="go", llm_conn_id="anthropic_default", harness=backend)
    context = MagicMock(spec=dict)

    with pytest.raises(HarnessRunError):
        op.execute(context)

    pushes = {c.kwargs["key"] for c in context["task_instance"].xcom_push.call_args_list}
    assert pushes == {"session_id", "usage"}


def test_execute_passes_collected_tools_and_fields() -> None:
    backend = _FakeBackend()
    op = HarnessOperator(
        task_id="t",
        prompt="go",
        llm_conn_id="anthropic_default",
        model_id="claude-x",
        system_prompt="be terse",
        max_turns=3,
        toolsets=[_tool("lookup"), _tool("search")],
        harness=backend,
    )

    op.execute(MagicMock(spec=dict))

    assert len(backend.received) == 1
    request = backend.received[0]
    assert request.llm_conn_id == "anthropic_default"
    assert request.model_id == "claude-x"
    assert request.system_prompt == "be terse"
    assert request.max_turns == 3
    assert [tool.name for tool in request.tools] == ["lookup", "search"]


def test_execute_without_xcom_push_skips_metadata() -> None:
    backend = _FakeBackend()
    op = HarnessOperator(task_id="t", prompt="go", llm_conn_id="anthropic_default", harness=backend)
    op.do_xcom_push = False
    context = MagicMock(spec=dict)

    op.execute(context)

    context["task_instance"].xcom_push.assert_not_called()


@pytest.mark.parametrize("max_turns", [0, -1])
def test_rejects_invalid_max_turns(max_turns: int) -> None:
    with pytest.raises(ValueError, match="max_turns"):
        HarnessOperator(task_id="t", prompt="go", llm_conn_id="anthropic_default", max_turns=max_turns)


def test_template_fields() -> None:
    assert HarnessOperator.template_fields == ("prompt", "llm_conn_id", "model_id", "system_prompt")
