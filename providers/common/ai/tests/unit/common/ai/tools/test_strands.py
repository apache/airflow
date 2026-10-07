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

import asyncio
import copy
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("strands")

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets.function import FunctionToolset
from strands import Agent
from strands.models.model import Model
from strands.tools.registry import ToolRegistry
from strands.types.exceptions import EventLoopException

from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.tools.strands import AirflowTools
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from unit.common.ai.toolsets.test_sql import _make_mock_db_hook

if TYPE_CHECKING:
    from strands.agent import AgentResult


class _OneToolCallModel(Model):
    """Asks for one call of ``tool_name``, then answers with the status and text it got back."""

    def __init__(self, tool_name: str, tool_input: dict[str, Any]) -> None:
        self._tool_name = tool_name
        self._tool_input = tool_input

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> dict[str, Any]:
        return {}

    async def structured_output(self, *args: Any, **kwargs: Any):
        raise NotImplementedError
        yield

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs: Any):
        results = [block["toolResult"] for block in messages[-1]["content"] if "toolResult" in block]
        yield {"messageStart": {"role": "assistant"}}
        if not results:
            start = {"toolUse": {"toolUseId": "call-1", "name": self._tool_name}}
            yield {"contentBlockStart": {"start": start}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(self._tool_input)}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
            return
        answer = f"{results[0]['status']}: {json.dumps(results[0]['content'])}"
        yield {"contentBlockDelta": {"delta": {"text": answer}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


class _RetryingModel(_OneToolCallModel):
    """Calls ``tool_name`` again after every error result, up to ``attempts`` times, then answers."""

    def __init__(self, tool_name: str, tool_input: dict[str, Any], *, attempts: int) -> None:
        super().__init__(tool_name, tool_input)
        self._attempts = attempts

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs: Any):
        calls = sum(1 for message in messages if any("toolResult" in block for block in message["content"]))
        yield {"messageStart": {"role": "assistant"}}
        if calls < self._attempts:
            start = {"toolUse": {"toolUseId": f"call-{calls}", "name": self._tool_name}}
            yield {"contentBlockStart": {"start": start}}
            yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(self._tool_input)}}}}
            yield {"contentBlockStop": {}}
            yield {"messageStop": {"stopReason": "tool_use"}}
            return
        yield {"contentBlockDelta": {"delta": {"text": "gave up"}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


def _run_through_checkpoints(agent: Agent, prompt: str) -> AgentResult:
    """Resume a checkpointing agent from each checkpoint it returns, until it finishes."""
    result = agent(prompt)
    while result.checkpoint is not None:
        # Strands types the prompt as AgentInput, which leaves out its checkpointResume block.
        result = agent({"checkpointResume": {"checkpoint": result.checkpoint.to_dict()}})  # type: ignore[arg-type]
    return result


def _tool(result: ToolResult | Exception) -> AirflowTool:
    async def function(arguments: dict[str, Any]) -> ToolResult:
        if isinstance(result, Exception):
            raise result
        return result

    return AirflowTool(
        name="lookup",
        description="Look something up.",
        parameters={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        function=function,
    )


def _run_agent(plugin: AirflowTools, tool_name: str, tool_input: dict[str, Any]) -> str:
    agent = Agent(model=_OneToolCallModel(tool_name, tool_input), plugins=[plugin], callback_handler=None)
    return str(agent("go"))


class TestAirflowTools:
    def test_adds_every_tool_of_a_toolset(self):
        plugin = AirflowTools(SQLToolset("pg_default"))

        assert [t.tool_name for t in plugin.tools] == ["list_tables", "get_schema", "query", "check_query"]
        query = next(t for t in plugin.tools if t.tool_name == "query")
        assert query.tool_spec["inputSchema"]["json"]["required"] == ["sql"]

    def test_accepts_individual_tools_alongside_toolsets(self):
        plugin = AirflowTools(_tool(ToolResult("ok")), SQLToolset("pg_default"))

        assert [t.tool_name for t in plugin.tools][:2] == ["lookup", "list_tables"]

    def test_registering_does_not_mutate_the_source_schema(self):
        """Strands fills in schema gaps in place; the toolset's own schema must stay untouched."""
        source = _tool(ToolResult("ok"))
        before = copy.deepcopy(source.parameters)

        registry = ToolRegistry()
        for tool in AirflowTools(source).tools:
            registry.register_tool(tool)
        registry.get_all_tool_specs()

        assert source.parameters == before

    @pytest.mark.parametrize(
        ("result", "block"),
        [
            pytest.param(ToolResult("42 rows"), {"text": "42 rows"}, id="text"),
            pytest.param(ToolResult({"rows": [[1]]}), {"json": {"rows": [[1]]}}, id="json"),
        ],
    )
    def test_maps_a_result_to_a_strands_content_block(self, result, block):
        strands_tool = AirflowTools(_tool(result)).tools[0]

        async def collect():
            tool_use = {"toolUseId": "call-1", "name": "lookup", "input": {"key": "a"}}
            return [event async for event in strands_tool.stream(tool_use, {})][-1].tool_result

        assert asyncio.run(collect()) == {"toolUseId": "call-1", "status": "success", "content": [block]}


class TestAgentRun:
    def test_the_agent_reads_a_toolset_result(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Ada")], last_description=[("id",), ("name",)])

        answer = _run_agent(AirflowTools(ts), "query", {"sql": "SELECT id, name FROM users"})

        assert answer.startswith("success:")
        assert "Ada" in answer

    def test_a_correctable_failure_reaches_the_model_as_an_error(self):
        answer = _run_agent(
            AirflowTools(_tool(ToolResult("Unknown column 'nme'", is_error=True))), "lookup", {"key": "a"}
        )

        assert answer.startswith("error:")
        assert "Unknown column" in answer

    def test_any_other_failure_fails_the_run(self):
        """Strands alone would hand the exception to the model and carry on."""
        plugin = AirflowTools(_tool(PermissionError("role cannot read orders")))

        # Strands wraps what ends the run in its own EventLoopException.
        with pytest.raises(EventLoopException) as caught:
            _run_agent(plugin, "lookup", {"key": "a"})

        assert isinstance(caught.value.original_exception, ToolCallError)
        assert "PermissionError: role cannot read orders" in str(caught.value)

    def test_two_plugins_can_be_attached_to_one_agent(self):
        """Strands refuses two plugins with the same name."""
        agent = Agent(
            model=_OneToolCallModel("lookup", {"key": "a"}),
            plugins=[AirflowTools(_tool(ToolResult("a"))), AirflowTools(SQLToolset("pg_default"))],
            callback_handler=None,
        )

        assert {"lookup", "query"} <= set(agent.tool_names)

    def test_each_run_of_a_reused_agent_starts_with_a_fresh_retry_budget(self):
        def lookup(key: str) -> str:
            """Look a key up."""
            raise ModelRetry("no such key")

        agent = Agent(
            model=_OneToolCallModel("lookup", {"key": "a"}),
            plugins=[AirflowTools(*airflow_tools_from_toolset(FunctionToolset([lookup], max_retries=1)))],
            callback_handler=None,
        )

        assert str(agent("go")).startswith("error:")
        assert str(agent("go again")).startswith("error:")

    def test_resuming_from_a_checkpoint_keeps_the_retry_budget(self):
        """With checkpointing every cycle is its own invocation; the budget must still span the run."""

        def lookup(key: str) -> str:
            """Look a key up."""
            raise ModelRetry("no such key")

        agent = Agent(
            model=_RetryingModel("lookup", {"key": "a"}, attempts=3),
            plugins=[AirflowTools(*airflow_tools_from_toolset(FunctionToolset([lookup], max_retries=1)))],
            callback_handler=None,
            checkpointing=True,
        )

        with pytest.raises(EventLoopException) as caught:
            _run_through_checkpoints(agent, "go")

        assert isinstance(caught.value.original_exception, ToolCallError)

    @pytest.mark.enable_redact
    def test_a_secret_in_a_result_reaches_the_model_masked(self, registered_secret):
        answer = _run_agent(
            AirflowTools(_tool(ToolResult(f"key={registered_secret}"))), "lookup", {"key": "a"}
        )

        assert registered_secret not in answer
        assert "key=***" in answer

    def test_the_toolsets_calls_are_counted_as_strands(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        with patch("airflow.providers.common.ai.utils.tool_metrics.Stats", MagicMock(spec=["incr"])) as stats:
            _run_agent(AirflowTools(ts), "list_tables", {})

        assert stats.incr.call_args.kwargs["tags"]["framework"] == "strands"
